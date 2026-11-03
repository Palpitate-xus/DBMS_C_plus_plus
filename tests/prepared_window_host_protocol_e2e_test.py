#!/usr/bin/env python3
"""Typed prepared-window host: fixed PG18.6 ordinary execution/wire oracle.

All Execute quotas are zero and formats are0/1. Raw228 observations survive;
226 window/fixture controls are strict and two catalog facts are separately
reported. Physical origin OIDs come from actual physical LIMIT0 declarations,
never from the window output being tested; enum IDs come from actual pg_type.
No retained frame/provider/portal/custom-planning policy is repaired or tested.
"""
import json
from pathlib import Path
import os
import struct
import tempfile
from compat.numeric_type_review import Review

LABEL='PREPARED_WINDOW_HOST_PROTOCOL'
EXPECTED_CONTROLS=226

def clean(value):
    if isinstance(value,bytes):return {'hex':value.hex()}
    if isinstance(value,(tuple,list)):return [clean(x) for x in value]
    if isinstance(value,dict):return {(k.hex() if isinstance(k,bytes) else str(k)):clean(v) for k,v in value.items()}
    return value

def observe(sock,case,index):
    if case['mode']=='simple':return [wire.simple_query(sock,case['sql'])]
    name=('wh_'+str(index)).encode();body=name+b'\0'+case['sql'].encode()+b'\0'+struct.pack('!H',len(case['input_oids']))+b''.join(struct.pack('!I',oid) for oid in case['input_oids'])
    sock.sendall(wire.typed(b'P',body)+wire.typed(b'D',b'S'+name+b'\0')+wire.typed(b'S'));first=wire.read_until_ready(sock)
    if any(kind==b'E' for kind,_ in first):return [first]
    bind=b'\0'+name+b'\0'+struct.pack('!H',1 if case['binary'] else 0)+(struct.pack('!H',1) if case['binary'] else b'')+struct.pack('!H',len(case['inputs']))
    for value,oid in zip(case['inputs'],case['input_oids']):
        raw=None if value is None else struct.pack('!q' if oid==20 else '!i',int(value)) if case['binary'] else value.encode()
        bind+=struct.pack('!i',-1 if raw is None else len(raw))+(raw or b'')
    bind+=(struct.pack('!HH',1,1) if case['binary'] else struct.pack('!H',0))
    sock.sendall(wire.typed(b'B',bind)+wire.typed(b'D',b'P\0')+wire.typed(b'E',b'\0'+struct.pack('!I',0))+wire.typed(b'S'))
    return [first,wire.read_until_ready(sock)]

def decoded(messages):
    fields=wire.row_description_fields(messages) if any(kind==b'T' for kind,_ in messages) else []
    errors=[wire.diagnostic_fields(body) for kind,body in messages if kind==b'E']
    raw_rows=wire.data_row_values(messages);rows=[]
    for row in raw_rows:
        values=[]
        for value,field in zip(row,fields):
            if value is None:values.append(None)
            elif field[6]==0:values.append(value.decode())
            elif field[3] in (20,21,23):values.append(str(int.from_bytes(value,'big',signed=True)))
            elif field[3]==25:values.append(value.decode())
            elif field[3]==701:values.append(str(struct.unpack('!d',value)[0]))
            else:values.append({'binary_hex':value.hex()})
        rows.append(values)
    commands=[body.rstrip(b'\0').decode() for kind,body in messages if kind==b'C']
    result=[rows,errors[0].get(b'C',b'').decode() if errors else None,
            errors[0].get(b'M',b'').decode() if errors else '',
            [field[0].decode() for field in fields],commands[-1] if commands else None,[field[3] for field in fields]]
    return dict(result=result,rows=clean(raw_rows),descriptions=clean([wire.row_description_fields([m]) for m in messages if m[0]==b'T']),
                errors=clean(errors),kinds=[kind.decode() for kind,_ in messages],raw=clean(messages))

def normalize_phase(phase,tables,types):
    descriptions=[]
    for description in phase['descriptions']:
        fields=[]
        for original in description:
            field=list(original)
            if field[1]:field[1]=tables.get(field[1],'UNRESOLVED_ORIGIN:'+str(field[1]))
            field[3]=types.get(field[3],field[3]);fields.append(field)
        descriptions.append(fields)
    parameters=[]
    for kind,body in phase['raw']:
        if kind.get('hex')=='74':
            payload=bytes.fromhex(body['hex']);count=int.from_bytes(payload[:2],'big')
            parameters.append([int.from_bytes(payload[2+4*i:6+4*i],'big') for i in range(count)])
    return dict(rows=phase['rows'],descriptions=descriptions,kinds=phase['kinds'],
                state=phase['result'][1],tag=phase['result'][4],parameters=parameters)


def main():
    fixture=json.loads((Path(__file__).with_name('compat')/'prepared_window_host_pg18_expected.json').read_text())
    review=Review(LABEL);global runner,wire;runner,wire=review.runner,review.client
    observations=[];origin_probes=[];catalog_differences=[]
    try:
        for index,item in enumerate(fixture['cases'],1):
            phases=observe(review.sock,item['case'],index)
            observations.append(dict(index=index,case=item['case'],phases=[decoded(phase) for phase in phases],catalog_observation=item['catalog_observation']))
        tables={}
        for name in ['wh_source','wh_other','wh_float','wh_enum_source']:
            sql='SELECT id FROM '+name+' LIMIT 0'
            messages=wire.simple_query(review.sock,sql);phase=decoded(messages);fields=phase['descriptions'][0] if phase['descriptions'] else []
            valid=(phase['result'][1] is None and len(fields)==1 and fields[0][1]>0 and fields[0][2]==1 and fields[0][3]==23 and fields[0][4:]==[4,-1,0])
            origin_probes.append(dict(sql=sql,phase=phase,valid=valid))
            if not valid:raise AssertionError('physical fixture has no genuine complete origin: '+name)
            if fields[0][1] in tables:raise AssertionError('different actual physical relations share an origin OID')
            tables[fields[0][1]]='relation:'+name
        type_case=next(item for item in observations if item['case']['label']=='catalog-type-identities')
        type_phase=type_case['phases'][0]
        if type_phase['result'][1]:raise AssertionError('actual enum type identity lookup failed')
        types={int(oid):'type:'+name for oid,name in type_phase['result'][0]}
        if not any(value=='type:wh_enum' for value in types.values()):raise AssertionError('actual enum identity missing')
        for actual,expected in zip(observations,fixture['cases']):
            normalized=[normalize_phase(phase,tables,types) for phase in actual['phases']]
            valid=normalized==expected['expected'];actual['normalized']=normalized;actual['match']=valid
            if actual['catalog_observation']:
                # Catalog row numeric identities and unsorted catalog order
                # are fixture facts. Preserve every real difference as raw;
                # they are not window controls and are never labelled passes.
                catalog_differences.append(dict(label=actual['case']['label'],actual=actual['phases'],reference=expected['expected'],raw_comparison_match=valid))
            else:
                review.record(actual['case']['label'],valid,normalized,expected['expected'])
        review.complete(EXPECTED_CONTROLS)
        print(LABEL+'_CATALOG_OBSERVATIONS',len(catalog_differences),flush=True)
    finally:
        configured=os.environ.get('PREPARED_WINDOW_HOST_OUTPUT_DIR');output=Path(configured) if configured else Path(tempfile.mkdtemp(prefix='dbms-window-host-'))
        output.mkdir(parents=True,exist_ok=True)
        destination=output/('reference-observations.json' if '--reference18' in __import__('sys').argv else 'observations.json')
        destination.write_text(json.dumps(dict(controls=review.controls,failures=len(review.failures),observations=observations,origin_probes=origin_probes,catalog_observations=catalog_differences),ensure_ascii=False,indent=2)+'\n')
        print(LABEL+'_OBSERVATIONS',str(destination),flush=True);review.close()

if __name__=='__main__':main()
