#!/usr/bin/env python3
"""Full real pg_stats SQL descriptors and owning format0/1 array values."""
from pathlib import Path
from decimal import Decimal
from datetime import datetime, date, timezone
import csv
import importlib.util
import json
import os
import struct

FIELDS = [('schemaname',19,64),('tablename',19,64),('attname',19,64),('inherited',16,1),
          ('null_frac',700,4),('avg_width',23,4),('n_distinct',700,4),('most_common_vals',2277,-1),
          ('most_common_freqs',1021,-1),('histogram_bounds',2277,-1),('correlation',700,4),
          ('most_common_elems',2277,-1),('most_common_elem_freqs',1021,-1),('elem_count_histogram',1021,-1),
          ('range_length_histogram',2277,-1),('range_empty_frac',700,4),('range_bounds_histogram',2277,-1)]
RANGES = {3904:23,3926:20,3906:1700,3912:1082,3908:1114,3910:1184}


def module(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


def numeric(text):
    d=Decimal(text)
    if not d.is_finite():return struct.pack('!hhhh',0,0,{'NaN':-16384,'Infinity':-12288,'-Infinity':-4096}[str(d)],0)
    negative=d.is_signed();t=d.as_tuple();scale=max(-t.exponent,0)
    digits=''.join(str(i)for i in t.digits)
    point=len(digits)+t.exponent
    whole=digits[:point] if point>0 else ''
    if point>len(digits):whole+='0'*(point-len(digits))
    fraction=('0'*(-point)+digits)if point<0 else digits[point:]
    whole=whole.rjust((len(whole)+3)//4*4,'0');fraction=fraction.ljust((len(fraction)+3)//4*4,'0')
    groups=[int((whole+fraction)[i:i+4])for i in range(0,len(whole+fraction),4)];weight=len(whole)//4-1
    while groups and groups[0]==0:groups.pop(0);weight-=1
    while groups and groups[-1]==0:groups.pop()
    if not groups:weight=0;negative=False
    return struct.pack('!hhhh',len(groups),weight,0x4000 if negative else 0,scale)+b''.join(struct.pack('!H',g)for g in groups)


def temporal(oid,text):
    if text.lower() in ('infinity','-infinity'):
        bits=32 if oid==1082 else 64
        return ((1<<(bits-1))-1) if text[0]!='-' else -(1<<(bits-1))
    if oid==1082:return(date.fromisoformat(text)-date(2000,1,1)).days
    value=datetime.fromisoformat(text)
    if oid==1114:value=value.replace(tzinfo=None);epoch=datetime(2000,1,1)
    else:value=value.replace(tzinfo=timezone.utc)if value.tzinfo is None else value.astimezone(timezone.utc);epoch=datetime(2000,1,1,tzinfo=timezone.utc)
    d=value-epoch;return(d.days*86400+d.seconds)*1000000+d.microseconds


def scalar(oid,text):
    if oid==16:return b'\1'if text.lower()in('1','true','t')else b'\0'
    if oid in(19,25):return text.encode()
    if oid in(20,23):return int(text).to_bytes(8 if oid==20 else 4,'big',signed=True)
    if oid in(700,701):return struct.pack('!f'if oid==700 else'!d',float(text))
    if oid==1700:return numeric(text)
    if oid in(1082,1114,1184):return temporal(oid,text).to_bytes(4 if oid==1082 else 8,'big',signed=True)
    assert oid in RANGES,oid
    if text=='empty':return b'\1'
    lower,upper=next(csv.reader([text[1:-1]],escapechar='\\',strict=True,skipinitialspace=True))
    flags=(2 if text[0]=='['else 0)|(4 if text[-1]==']'else 0)|(8 if lower==''else 0)|(16 if upper==''else 0)
    data=bytes([flags])
    for bound in(lower,upper):
        if bound!='':
            value=scalar(RANGES[oid],bound);data+=struct.pack('!i',len(value))+value
    return data


def array(element, values, dimensions=None, bitmap=None):
    if dimensions is None:dimensions=[(len(values),1)]if values else[]
    if bitmap is None:bitmap=any(v is None for v in values)
    result=struct.pack('!iii',len(dimensions),int(bitmap),element)
    result+=b''.join(struct.pack('!ii',n,lower)for n,lower in dimensions)
    for value in values:result+=struct.pack('!i',-1 if value is None else len(value))+(value or b'')
    return result


def owned(value, declaration):
    if value['null']:return None
    if declaration['kind']==0:return scalar(declaration['codec'],value['text'])
    assert declaration['kind']==2
    data=value['compound'];assert data and data['elements']is not None
    children=[owned(v,declaration['element'])for v in data['elements']]
    bitmap=data['bitmap']==2 if data['bitmap']else any(v is None for v in children)
    return array(declaration['element']['actual'],children,data['dimensions'],bitmap)


def expected(column, name):
    slots=column['slots']
    def values(kind):
        s=slots.get(kind);v=s and s['values']
        return None if v is None else array(v['type']['actual'],[owned(c,v['type'])for c in v['cells']])
    def numbers(kind):
        s=slots.get(kind);v=s and s['numbers']
        return None if v is None else array(700,[struct.pack('!f',n)for n in v])
    def first(kind):
        s=slots.get(kind);v=s and s['numbers']
        return struct.pack('!f',v[0])if v else None
    return [b'public',b'pg_stats_source_proto',name.encode(),bytes([int(column['inherited'])]),
            struct.pack('!f',column['null_fraction']),struct.pack('!i',column['width']),struct.pack('!f',column['distinct']),
            values(1),numbers(1),values(2),first(3),values(4),numbers(4),numbers(5),values(6),first(6),values(7)]


def row_values(body):
    count=struct.unpack_from('!H',body)[0];offset=2;values=[]
    for _ in range(count):
        size=struct.unpack_from('!i',body,offset)[0];offset+=4;assert size>=-1 and offset+max(size,0)<=len(body)
        values.append(None if size==-1 else body[offset:offset+size]);offset+=max(size,0)
    assert offset==len(body);return values


def main():
    root=Path(__file__).resolve().parent.parent
    runner=module('pg_stats_protocol_runner',root/'tests/compat/pg_diff_runner.py')
    frames=module('pg_stats_frames',root/'tests/analyzed_array_auto_protocol_e2e_test.py')
    client=runner.load_protocol_client();server=runner.start_ours(client)
    path=Path(server['dir'])/'info/.analyzed_statistics'
    evidence=Path(os.environ['DBMS_TEST_EVIDENCE_DIR'])if os.environ.get('DBMS_TEST_EVIDENCE_DIR')else None
    trace={'server_pid':server['process'].pid,'commands':[],'complete':False}
    trace['server_exe']=str(Path('/proc/'+str(server['process'].pid)+'/exe').resolve())
    def remember(sql,messages):
        trace['commands'].append({'sql':sql,'raw':[{'kind':k.decode(),'body_hex':b.hex()}for k,b in messages]})
        errors=[client.diagnostic_fields(b)for k,b in messages if k==b'E'];assert not errors,(sql,errors)
        assert messages[-1]==(b'Z',b'I'),(sql,messages[-1]);return messages
    def query(sql):
        messages=remember(sql,client.simple_query(server['sock'],sql))
        rows,state,message,_,tag=runner.decode_wire_result(messages);assert state is None,(sql,state,message)
        return rows,messages
    def describe(sql):
        raw=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        server['sock'].sendall(client.typed(b'P',raw)+client.typed(b'D',b'S\0')+client.typed(b'S'))
        messages=remember('DESCRIBE '+sql,client.read_until_ready(server['sock']))
        assert not any(k in(b'D',b'C')for k,_ in messages);return client.row_description_fields(messages)
    def extended(sql,formats):
        raw=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=b'\0\0'+struct.pack('!HHH',0,0,len(formats))+b''.join(struct.pack('!H',f)for f in formats)
        server['sock'].sendall(client.typed(b'P',raw)+client.typed(b'B',bind)+client.typed(b'D',b'P\0')+client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        messages=remember('FORMAT '+repr(formats)+' '+sql,client.read_until_ready(server['sock']))
        assert [k for k,_ in messages][:3]==[b'1',b'2',b'T']
        return client.row_description_fields(messages),[row_values(b)for k,b in messages if k==b'D']
    def fields(actual,view,formats):
        assert len(actual)==17
        for n,(f,spec)in enumerate(zip(actual,FIELDS)):
            assert f[0].decode()==spec[0]and f[1]==view and f[2]==n+1 and f[3]==spec[1]and f[4]==spec[2]and f[5]==-1 and f[6]==formats[n],(f,spec)
    names=['i','t','a','r','b','n','d','ts','tz']
    try:
        query('CREATE TABLE pg_stats_source_proto(i integer,t text,a integer[],r int4range,b int8range,n numrange,d daterange,ts tsrange,tz tstzrange)')
        ints=[1,1,1,1,2,2,3,None,None,None,4,5]
        texts=['','NULL','x','x','x','a,b','界',None,'x','"quote','x','slash\\']
        arrays=['[0:1]={1,NULL}']*3+['{}','{2,10}','{2,10}',None,'{NULL}','{2,10}','{1,1}','{}','{2,10}']
        ranges=[None,'empty','[1,4)','[2,6)','(,)']
        bigs=[None,'empty','[9007199254740992,9007199254740993)','[-10,10)','(,)']
        nums=[None,'empty','[1.00,2.25)','[0,3)','(,)']
        dates=[None,'empty','[2000-01-01,2000-01-03)','[2024-02-28,2024-03-02)','(,)']
        times=[None,'empty','["2000-01-01 00:00:00.1","2000-01-01 00:00:00.9")','[-infinity,infinity]','(,)']
        zones=[None,'empty','["2000-01-01 01:00:00+01","2000-01-01 01:00:01+01")','[-infinity,infinity]','(,)']
        def literal(v):return 'NULL'if v is None else "'"+str(v).replace("'","''")+"'"
        rows=[[ints[i],texts[i],arrays[i]]+[v[i%5]for v in(ranges,bigs,nums,dates,times,zones)]for i in range(12)]
        query('INSERT INTO pg_stats_source_proto VALUES'+','.join('('+','.join(literal(v)for v in row)+')'for row in rows))
        query('ANALYZE pg_stats_source_proto')
        generation,columns=frames.snapshot(path);assert len(columns)==9
        by_attribute={c['attribute']:c for c in columns};assert set(by_attribute)==set(range(1,10))
        identities,_=query("SELECT oid,relkind,relnatts FROM pg_catalog.pg_class WHERE relname='pg_stats' AND relnamespace=11")
        assert len(identities)==1 and identities[0][1:]==['v','17']and int(identities[0][0])>0;view=int(identities[0][0])
        expected_rows={name:expected(by_attribute[n+1],name)for n,name in enumerate(names)}
        source_sql="SELECT * FROM pg_catalog.pg_stats WHERE tablename='pg_stats_source_proto' ORDER BY attname"
        binary_fields,binary_rows=extended(source_sql,[1]);fields(binary_fields,view,[1]*17)
        assert binary_rows==[expected_rows[n]for n in sorted(names)],'every complete binary field equals independent encoding of actual snapshot'
        for source in ['pg_stats','pg_catalog.pg_stats','"pg_catalog"."pg_stats"']:
            text_rows,messages=query("SELECT * FROM "+source+" WHERE tablename='pg_stats_source_proto' ORDER BY attname")
            fields(client.row_description_fields(messages),view,[0]*17)
            assert len(text_rows)==9 and [r[2]for r in text_rows]==sorted(names)
            for values in text_rows:
                expected_values=expected_rows[values[2]]
                assert all((value is None)==(wanted is None)for value,wanted in zip(values,expected_values))
                for index in(0,1,2,3,4,5,6,10,15):
                    if values[index]is not None:assert scalar(FIELDS[index][1],values[index])==expected_values[index]
            zero,messages=query('SELECT * FROM '+source+' WHERE FALSE');assert zero==[];fields(client.row_description_fields(messages),view,[0]*17)
            before=path.read_bytes();fields(describe('SELECT * FROM '+source+' LIMIT 0'),view,[0]*17);assert path.read_bytes()==before
        mixed=[1 if i in(7,8,9,11,12,13,14,16)else 0 for i in range(17)]
        mixed_fields,mixed_rows=extended(source_sql,mixed);fields(mixed_fields,view,mixed)
        for values,name in zip(mixed_rows,sorted(names)):
            for i,value in enumerate(values):
                if mixed[i]:assert value==expected_rows[name][i]
        assert len(mixed_rows)==9
        count,_=query("SELECT count(*) AS n FROM pg_catalog.pg_stats WHERE tablename='pg_stats_source_proto'");assert count==[['9']]
        cte,_=query("WITH q AS(SELECT attname,null_frac FROM pg_catalog.pg_stats WHERE tablename='pg_stats_source_proto') SELECT attname FROM q WHERE attname='i'");assert cte==[['i']]
        joined,_=query("SELECT s.attname FROM pg_catalog.pg_stats s JOIN pg_catalog.pg_class c ON s.tablename=c.relname WHERE c.relname='pg_stats_source_proto' ORDER BY s.attname")
        assert joined==[[name]for name in sorted(names)]
        aliases=','.join('c'+str(i)for i in range(17))
        alias_sql='SELECT s.* FROM pg_catalog.pg_stats AS s('+aliases+") WHERE s.c1='pg_stats_source_proto' ORDER BY s.c2"
        alias_fields,alias_rows=extended(alias_sql,[1]);assert len(alias_fields)==17 and alias_rows==binary_rows
        for i,f in enumerate(alias_fields):assert f[0].decode()=='c'+str(i)and f[1:]==binary_fields[i][1:]
        identity_sql="SELECT most_common_vals::anyarray FROM pg_catalog.pg_stats WHERE tablename='pg_stats_source_proto' ORDER BY attname"
        identity_fields,identity_rows=extended(identity_sql,[1])
        assert len(identity_fields)==1 and identity_fields[0][3:5]==(2277,-1)
        assert identity_rows==[[expected_rows[name][7]]for name in sorted(names)],'identity cast preserves the actual owning array'
        null_rows,_=query('SELECT NULL::anyarray');assert null_rows==[[None]]
        null_fields,null_binary=extended('SELECT NULL::anyarray',[1])
        assert null_fields[0][3:5]==(2277,-1) and null_binary==[[None]]
        direct_sql='SELECT r,b,n,d,ts,tz FROM pg_stats_source_proto WHERE i=1'
        direct_text,_=query(direct_sql);direct_fields,direct_binary=extended(direct_sql,[1])
        assert [f[3]for f in direct_fields]==[3904,3926,3906,3912,3908,3910]
        wanted=[[None if value is None else scalar(oid,value)for oid,value in zip([3904,3926,3906,3912,3908,3910],row)]for row in direct_text]
        assert sorted(repr(v)for v in wanted)==sorted(repr(v)for v in direct_binary)
        for sql,wanted_state in [("SELECT '{}'::anyarray",'0A000'),("SELECT '{}'::text::anyarray",'42846')]:
            messages=client.simple_query(server['sock'],sql)
            trace['commands'].append({'sql':sql,'raw':[{'kind':k.decode(),'body_hex':b.hex()}for k,b in messages]})
            rows,state,_,_,tag=runner.decode_wire_result(messages)
            assert state==wanted_state and rows==[]and tag is None and messages[-1]==(b'Z',b'I'),(sql,state)
        assert frames.snapshot(path)[0]==generation
        trace.update(complete=True,view_oid=view,statistics_generation=generation,fields=FIELDS,
                     binary_rows_checked=9,all17_binary_cells_checked=153,all6_direct_range_binary_codecs_checked=True)
    finally:
        if evidence:
            evidence.mkdir(parents=True,exist_ok=True)
            if path.exists():(evidence/'actual-snapshot.bin').write_bytes(path.read_bytes())
        runner.stop_ours(server);trace['server_exit']=server['process'].returncode
        if evidence:(evidence/'trace-v1.json').write_text(json.dumps(trace,ensure_ascii=False,indent=2)+'\n')
        assert trace['server_exit']==0
    print('PG_STATS_SOURCE_PROTOCOL commands='+str(len(trace['commands']))+' all17=1 binary_cells=153 range_codecs=6 input_rejected=2')


if __name__=='__main__':
    main()
