#!/usr/bin/env python3
"""Fixed PG18.6 window ordering, projection and error-prefix demand.

Simple and normal Extended text protocol use unlimited Execute (zero quota).
Counters measure calls at real window/projection sites, not source reads.
Physical origin comes from a separate actual physical LIMIT0 descriptor.
No retained plan, parameter-frame, provider or portal policy is changed.
"""
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
from compat.numeric_type_review import Review

LABEL='PREPARED_WINDOW_ORDER_DEMAND'
EXPECTED_CONTROLS=201


def clean(value):
    if isinstance(value,bytes):return {'hex':value.hex()}
    if isinstance(value,(list,tuple)):return [clean(item) for item in value]
    if isinstance(value,dict):return {str(key):clean(item) for key,item in value.items()}
    return value


def main():
    review=Review(LABEL);wire=review.client;observations=[];table_oid=None
    output=Path(os.environ['PREPARED_WINDOW_ORDER_DEMAND_OUTPUT_DIR']) if os.environ.get('PREPARED_WINDOW_ORDER_DEMAND_OUTPUT_DIR') else Path(tempfile.mkdtemp(prefix='dbms-window-order-demand-'))
    output.mkdir(parents=True,exist_ok=True)

    def observe(sql,mode):
        if mode=='simple':return wire.simple_query(review.sock,sql)
        name=('wod_'+str(review.controls)).encode()
        parse=name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=b'\0'+name+b'\0'+struct.pack('!HHH',0,0,0)
        review.sock.sendall(wire.typed(b'P',parse)+wire.typed(b'D',b'S'+name+b'\0')+
            wire.typed(b'B',bind)+wire.typed(b'D',b'P\0')+
            wire.typed(b'E',b'\0'+struct.pack('!I',0))+wire.typed(b'S'))
        return wire.read_until_ready(review.sock)

    def decoded(messages):
        rows=[[None if value is None else value.decode('utf-8',errors='replace')
               for value in row] for row in wire.data_row_values(messages)]
        descriptions=[wire.row_description_fields([message]) for message in messages if message[0]==b'T']
        errors=[wire.diagnostic_fields(body) for kind,body in messages if kind==b'E']
        states=[error.get(b'C',b'').decode(errors='replace') for error in errors]
        tags=[body.rstrip(b'\0').decode(errors='replace') for kind,body in messages if kind==b'C']
        return dict(rows=rows,descriptions=descriptions,states=states,tags=tags,
                    kinds=[kind.decode('ascii') for kind,_ in messages])

    def fields(schema):
        return [(name.encode(),table_oid if physical else 0,1 if physical else 0,oid,
                 4 if oid==23 else 8,-1,0) for name,oid,physical in schema]

    def query(label,sql,rows,schema,mode='simple',state=None,tag=None):
        messages=observe(sql,mode);actual=decoded(messages)
        wanted_tag=tag if tag is not None else 'SELECT '+str(len(rows))
        prefix=['1','t','T','2','T'] if mode=='extended' else ['T'] if schema else []
        wanted_kinds=prefix+['D']*len(rows)+(['E'] if state else ['C'])+['Z']
        wanted_descriptions=[fields(schema)]*(2 if mode=='extended' else 1) if schema else []
        expected=dict(rows=rows,descriptions=wanted_descriptions,states=[state] if state else [],
                      tags=[] if state else [wanted_tag],kinds=wanted_kinds)
        good=actual==expected and messages[-1:]==[(b'Z',b'I')]
        if mode=='extended':good=good and [body for kind,body in messages if kind==b't']==[b'\x00\x00']
        observations.append(dict(control=review.controls+1,label=label,sql=sql,mode=mode,
                                 actual=clean(actual),expected=clean(expected),match=good,raw=clean(messages)))
        review.record(label,good,actual,expected)

    integer=[('id',23,True)]
    window=[('id',23,True),('v',20,False)]
    paired=[('id',23,True),('w',20,False),('pr',20,False)]
    counter=[('nextval',20,False)]
    try:
        query('create-physical-source','CREATE TABLE wod_source(id INTEGER,p INTEGER,t TEXT)',[],[],tag='CREATE TABLE')
        query('insert-six-physical-rows',"INSERT INTO wod_source VALUES(1,0,'a'),(2,0,'b'),(3,0,'c'),(4,1,'d'),(5,1,'e'),(6,1,'f')",[],[],tag='INSERT 0 6')
        origin_messages=wire.simple_query(review.sock,'SELECT id FROM wod_source LIMIT 0')
        origin=decoded(origin_messages);origin_fields=origin['descriptions'][0] if len(origin['descriptions'])==1 else []
        origin_good=(len(origin_fields)==1 and origin_fields[0][0]==b'id' and origin_fields[0][1]>0 and
                     origin_fields[0][2:]==(1,23,4,-1,0) and origin['rows']==[] and origin['states']==[] and
                     origin['tags']==['SELECT 0'] and origin['kinds']==['T','C','Z'] and origin_messages[-1:]==[(b'Z',b'I')])
        if origin_good:table_oid=origin_fields[0][1]
        observations.append(dict(control=review.controls+1,label='genuine-physical-origin',actual=clean(origin),match=origin_good,raw=clean(origin_messages)))
        review.record('genuine-physical-origin',origin_good,origin,'one actual nonzero physical OID, attribute1 INTEGER, full zero-row descriptor')

        # The first five final ORDER keys are complete prefixes of the final
        # window's partition+order keys. The last three require a real sort.
        order_shapes=[
            ('asc','ORDER BY id','id',[['1','2']],1),
            ('desc','ORDER BY id DESC','id DESC',[['6','2']],1),
            ('partition_full','PARTITION BY p ORDER BY id','p,id',[['1','2']],1),
            ('partition_desc','PARTITION BY p ORDER BY id DESC','p,id DESC',[['3','2']],1),
            ('partition_prefix','PARTITION BY p ORDER BY id,t','p,id',[['1','2']],1),
            ('opposite_direction','ORDER BY id','id DESC',[['6','7']],6),
            ('opposite_partition','PARTITION BY p ORDER BY id','p DESC,id',[['4','5']],6),
            ('different_expression','ORDER BY id','id+0',[['1','2']],6),
        ]
        paired_shapes=[
            ('limit1','ORDER BY id','id','LIMIT 1',[['1','2','2']],1),
            ('offset_limit1','ORDER BY id','id','OFFSET 1 LIMIT 1',[['2','3','3']],2),
            ('ties1','ORDER BY id','id','FETCH FIRST 1 ROW WITH TIES',[['1','2','2']],2),
            ('offset_ties1','ORDER BY id','id','OFFSET 1 FETCH FIRST 1 ROW WITH TIES',[['2','3','3']],3),
            ('partition_ties','PARTITION BY p ORDER BY id','p','FETCH FIRST 1 ROW WITH TIES',[['1','2','2'],['2','3','3'],['3','4','4']],4),
            ('partition_offset_ties','PARTITION BY p ORDER BY id','p','OFFSET 1 FETCH FIRST 1 ROW WITH TIES',[['2','3','3'],['3','4','4']],4),
            ('zero','ORDER BY id','id','LIMIT 0',[],0),
        ]
        bare_ties=[
            ('unique','ORDER BY id','id','FETCH FIRST 1 ROW WITH TIES',[['1','2']],2),
            ('offset_unique','ORDER BY id','id','OFFSET 1 FETCH FIRST 1 ROW WITH TIES',[['2','3']],3),
            ('partition','PARTITION BY p ORDER BY id','p','FETCH FIRST 1 ROW WITH TIES',[['1','2'],['2','3'],['3','4']],4),
            ('offset_partition','PARTITION BY p ORDER BY id','p','OFFSET 1 FETCH FIRST 1 ROW WITH TIES',[['2','3'],['3','4']],4),
        ]
        for mode,short in [('simple','s'),('extended','e')]:
            for index,(label,over,order,wanted,calls) in enumerate(order_shapes):
                sequence='wod_'+short+'_o'+str(index)
                query(mode+' '+label+' create','CREATE SEQUENCE '+sequence,[],[],tag='CREATE SEQUENCE')
                query(mode+' '+label+' seed',"SELECT nextval('"+sequence+"')",[['1']],counter)
                sql="SELECT id,lag(nextval('"+sequence+"'),0) OVER("+over+") AS v FROM wod_source ORDER BY "+order+' LIMIT 1'
                query(mode+' '+label,sql,wanted,window,mode)
                query(mode+' '+label+' post',"SELECT nextval('"+sequence+"')",[[str(calls+2)]],counter)
            for index,(label,over,order,tail,wanted,calls) in enumerate(paired_shapes):
                argument='wod_'+short+'_a'+str(index);projection='wod_'+short+'_p'+str(index)
                query(mode+' paired '+label+' create-argument','CREATE SEQUENCE '+argument,[],[],tag='CREATE SEQUENCE')
                query(mode+' paired '+label+' create-projection','CREATE SEQUENCE '+projection,[],[],tag='CREATE SEQUENCE')
                query(mode+' paired '+label+' seed-argument',"SELECT nextval('"+argument+"')",[['1']],counter)
                query(mode+' paired '+label+' seed-projection',"SELECT nextval('"+projection+"')",[['1']],counter)
                sql="SELECT id,lag(nextval('"+argument+"'),0) OVER("+over+") AS w,nextval('"+projection+"') AS pr FROM wod_source ORDER BY "+order+' '+tail
                query(mode+' paired '+label,sql,wanted,paired,mode)
                query(mode+' paired '+label+' post-argument',"SELECT nextval('"+argument+"')",[[str(calls+2)]],counter)
                query(mode+' paired '+label+' post-projection',"SELECT nextval('"+projection+"')",[[str(calls+2)]],counter)
            for index,(label,over,order,tail,wanted,calls) in enumerate(bare_ties):
                sequence='wod_'+short+'_t'+str(index)
                query(mode+' bare-ties '+label+' create','CREATE SEQUENCE '+sequence,[],[],tag='CREATE SEQUENCE')
                query(mode+' bare-ties '+label+' seed',"SELECT nextval('"+sequence+"')",[['1']],counter)
                sql="SELECT id,lag(nextval('"+sequence+"'),0) OVER("+over+") AS v FROM wod_source ORDER BY "+order+' '+tail
                query(mode+' bare-ties '+label,sql,wanted,window,mode)
                query(mode+' bare-ties '+label+' post',"SELECT nextval('"+sequence+"')",[[str(calls+2)]],counter)
            error_schema=[('id',23,True),('v',23,False)]
            expression='SELECT id,lag(1/(id-3),0) OVER(ORDER BY id) AS v FROM wod_source ORDER BY id'
            query(mode+' argument-error-pruned-by-limit1',expression+' LIMIT 1',[['1','0']],error_schema,mode)
            query(mode+' argument-error-two-row-prefix',expression+' LIMIT 3',[['1','0'],['2','-1']],error_schema,mode,state='22012')
        review.complete(EXPECTED_CONTROLS)
    finally:
        destination=output/('reference-observations.json' if '--reference18' in sys.argv else 'observations.json')
        destination.write_text(json.dumps(dict(controls=review.controls,failures=len(review.failures),source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),observations=observations),ensure_ascii=False,indent=2)+'\n')
        print(LABEL+'_OBSERVATIONS',str(destination),flush=True)
        review.close()


if __name__=='__main__':main()
