#!/usr/bin/env python3
"""Exact TEXT/BYTEA TRIM overloads, analysis errors and actual callee ownership."""
import sys
from pathlib import Path
if not (Path(__file__).parent/'compat').is_dir():
    sys.path.insert(0,str(Path.cwd()/'tests'))
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire as TrimWire
from quoted_unqualified_scalar_projection_protocol_e2e_test import datum, identifier
import json
import struct
import uuid

NAMES=('btrim','ltrim','rtrim')
MODES=('simple','extended','binary')
PLANNED=3780
POSITIVE=(
    ("'  x  '",'x','x  ','  x',25), ("'xxhixx','x'",'hi','hixx','xxhi',25),
    ("'丰x丰','中估'",'丰x丰','丰x丰','丰x丰',25), ("'ééxé','é'",'x','xé','ééx',25),
    ("''",'','','',25), ('NULL',None,None,None,25), ('NULL::text',None,None,None,25),
    ("'NULL'",'NULL','NULL','NULL',25), ("'abc'::varchar,'b'",'abc','abc','abc',25),
    ("' x '::char(6)",'x','x',' x',25), ("' x '::name",'x','x ',' x',25),
    ("NULL::bytea,'\\x78'::bytea",None,None,None,17),
    ("'\\x00ff7800'::bytea,'\\x00ff'::bytea",'\\x78','\\x7800','\\x00ff78',17),
    ("'\\x61'::bytea,NULL::bytea",None,None,None,17),
    ("'\\x78'::bytea,''",'\\x78','\\x78','\\x78',17),
    ('NULL,NULL',None,None,None,25), ("'x',''",'x','x','x',25),
    ("'a  '::char(3),'a '::char(2)",'','','',25),
    ("'\\x7878617878'::bytea,'\\x78'::bytea",'\\x61','\\x617878','\\x787861',17),
    ("'x'::name,'x'::name",'','','',25))
INVALID=('', '1','NULL::integer','true','1.25::numeric','NULL::uuid','ARRAY[1]',"'2024-01-01'::date",
    "1,'x'","'x',1","'x',NULL::uuid",'NULL::bytea',"'\\x78'::bytea,'x'::text","'x'::text,'\\x78'::bytea","'x','y','z'","missing=>'x'")


class Wire(TrimWire):
    def bind(self,statement,inputs=(),values=(),binary_input=False,binary_output=False,execute=True):
        assert len(inputs)==len(values)
        body=b'\0'+statement+b'\0'+struct.pack('!H',len(inputs))+b''.join(struct.pack('!H',int(binary_input)) for _ in inputs)+struct.pack('!H',len(values))
        for oid,value in zip(inputs,values):
            raw=None if value is None else bytes.fromhex(value[2:]) if oid==17 and binary_input else datum(value,oid,binary_input)
            body+=struct.pack('!i',-1 if raw is None else len(raw))+(raw or b'')
        body+=struct.pack('!HH',1,int(binary_output))
        packet=self.client.typed(b'B',body)+self.client.typed(b'D',b'P\0')
        if execute:
            packet+=self.client.typed(b'E',b'\0'+struct.pack('!I',0))
        return packet


def literal(value,oid=25):
    return 'NULL::'+('bytea' if oid==17 else 'text') if value is None else "'"+value.replace("'","''")+"'"+('::bytea' if oid==17 else '')


def expected(source,characters,name,oid=25):
    if source is None or characters is None:
        return None
    if oid==17:
        source,characters=bytes.fromhex(source[2:]),bytes.fromhex(characters[2:])
    result=source if not characters else source.lstrip(characters) if name=='ltrim' else source.rstrip(characters) if name=='rtrim' else source.strip(characters)
    return '\\x'+result.hex() if oid==17 else result


def plan(schema):
    commands=[]
    def add(method,*args,**kwargs):commands.append(dict(method=method,args=args,kwargs=kwargs))
    other=schema+'_text';q,qt=identifier(schema),identifier(other)
    empty,table,sequence=q+'.empty_',q+'.rows_',q+'.effects_'
    add('command','CREATE SCHEMA '+q+';','CREATE SCHEMA')
    add('command','CREATE SCHEMA '+qt+';','CREATE SCHEMA')
    add('command','CREATE TABLE '+empty+'(i integer,t text);','CREATE TABLE')
    add('command','CREATE TABLE '+table+'(id integer,s text,c text,b bytea,bc bytea);','CREATE TABLE')
    rows=(('xxhixx','x','\\x7878617878','\\x78'),(None,'x',None,'\\x78'),('','x','\\x','\\x78'),
          ('ééxé','é','\\x00ff7800','\\x00ff'),('NULL','N','\\x7878',None),(' x ','','\\x00','\\x'))
    add('command','INSERT INTO '+table+' VALUES'+','.join('('+str(i)+','+','.join(literal(v,17 if column>=2 else 25) for column,v in enumerate(row))+')' for i,row in enumerate(rows,1))+';','INSERT 0 6')
    suffixes=('', ' FROM '+empty+' WHERE false',' FROM '+empty+' LIMIT 0')
    for index,name in enumerate(NAMES):
        for args in INVALID:
            for spelling in (name,'pg_catalog.'+name):
                for suffix in suffixes:
                    for role in ('',' OVER()'):
                        for mode in MODES:add('query',mode,'SELECT '+spelling+'('+args+')'+role+' AS v'+suffix+';',(),25,state='42883')
            for expression in ("CASE WHEN false THEN "+name+'('+args+") ELSE 'kept' END","COALESCE('kept',"+name+'('+args+'))'):
                for mode in MODES:add('query',mode,'SELECT '+expression+' AS v;',(),25,state='42883')
        for args,both,left,right,oid in POSITIVE:
            value=(both,left,right)[index]
            for spelling in (name,'pg_catalog.'+name):
                for suffix in suffixes:
                    for mode in MODES:add('query',mode,'SELECT '+spelling+'('+args+') AS v'+suffix+';',() if suffix else (value,),oid)
            for mode in MODES:add('query',mode,'SELECT '+name+'('+args+') OVER() AS v;',(),oid,state='42809')
        for suffix in (' WHERE false',' LIMIT 0'):
            for mode in MODES:add('query',mode,'SELECT '+name+'(i) AS v FROM '+empty+suffix+';',(),25,state='42883')
        for args in ("missing,'x'","'x',missing"):
            for mode in MODES:add('query',mode,'SELECT '+name+'('+args+') AS v FROM '+empty+' LIMIT 0;',(),25,state='42703')
        for args in ("'\\xq0','\\x00'::bytea","'\\x00'::bytea,'\\xq0'"):
            for suffix in suffixes:
                for role in ('',' OVER()'):
                    for mode in MODES:add('query',mode,'SELECT '+name+'('+args+')'+role+' AS v'+suffix+';',(),17,state='42809' if role else '22023')
        for mode in MODES:
            for source,chars,oid,offset in (('s','c',25,0),('b','bc',17,2)):
                prefix='SELECT '+name+'('+source+','+chars+') AS v FROM '+table
                add('query',mode,prefix+' ORDER BY id;',tuple(expected(row[offset],row[offset+1],name,oid) for row in rows),oid)
                add('query',mode,prefix+' WHERE false;',(),oid)
                add('query',mode,prefix+' LIMIT 0;',(),oid)
        for inputs,pairs in (((25,),(("  x  ",),('',),(None,),('NULL',))),
                            ((25,25),(('xxhixx','x'),('',None),(None,'x'),('ééxé','é'))),
                            ((17,17),(('\\x7878617878','\\x78'),('\\x',None),(None,'\\x78'),('\\x00ff6800','\\x00ff')))):
            oid=17 if inputs[0]==17 else 25
            statement=name+'_parameters_'+str(len(inputs))+'_'+str(oid)
            sql='SELECT '+name+'('+','.join('$'+str(i) for i in range(1,len(inputs)+1))+') AS v;'
            add('describe',statement,sql,oid,inputs)
            add('bind_describe',statement,oid,inputs,pairs[0],False,False)
            for binary in (False,True):
                for pair in pairs:add('execute',statement,oid,(expected(pair[0],pair[1] if len(pair)>1 else ' ',name,oid),),inputs,pair,binary_input=binary,binary_output=binary)
            add('close',statement)
        for inputs in ((23,),(17,),(23,25),(25,23),(17,25)):
            add('describe',name+'_bad_'+str(inputs), 'SELECT '+name+'('+','.join('$'+str(i) for i in range(1,len(inputs)+1))+') AS v;',25,inputs,state='42883')
        inferred=name+'_inferred'
        add('infer_describe',inferred,'SELECT '+name+'($1,$2) AS v;')
        for binary in (False,True):add('execute',inferred,25,(('hi','hixx','xxhi')[index],),(25,25),('xxhixx','x'),binary_input=binary,binary_output=binary)
        add('close',inferred)
    for name in NAMES:add('command','CREATE FUNCTION '+q+'.'+name+'(s integer) RETURNS integer LANGUAGE SQL IMMUTABLE AS $$SELECT s+100$$;','CREATE FUNCTION')
    add('command','SET search_path=pg_catalog,'+schema+';','SET',parameter_status='pg_catalog, '+schema)
    for name in NAMES:
        for mode in MODES:
            add('query',mode,'SELECT '+name+'(1) AS v;',(101,),23)
            add('query',mode,'SELECT '+q+'.'+name+'(1) AS v;',(101,),23)
            add('query',mode,'SELECT pg_catalog.'+name+'(1) AS v;',(),25,state='42883')
            add('query',mode,'SELECT "'+name+'"(1) AS v;',(101,),23)
    for name in NAMES:add('command','CREATE FUNCTION '+qt+'.'+name+"(s text,c text) RETURNS text LANGUAGE SQL IMMUTABLE AS $$SELECT 'shadow'::text$$;",'CREATE FUNCTION')
    add('command','SET search_path='+other+',pg_catalog;','SET',parameter_status=other+', pg_catalog')
    for index,name in enumerate(NAMES):
        for mode in MODES:
            add('query',mode,'SELECT '+name+"('xxhixx','x') AS v;",('shadow',),25)
            add('query',mode,'SELECT "'+name+"\"('xxhixx','x') AS v;",('shadow',),25)
            add('query',mode,'SELECT pg_catalog.'+name+"('xxhixx','x') AS v;",(('hi','hixx','xxhi')[index],),25)
            add('query',mode,'SELECT '+qt+'.'+name+'(1) AS v;',(),25,state='42883')
            direction=('both','leading','trailing')[index]
            add('query',mode,'SELECT trim('+direction+" 'x' from 'xxhixx') AS v;",(('hi','hixx','xxhi')[index],),25)
            add('query',mode,'SELECT "'+name.upper()+"\"('x') AS v;",(),25,state='42883')
    add('command','SET search_path=pg_catalog,public;','SET',parameter_status='pg_catalog, public')
    add('command','CREATE SEQUENCE '+sequence+';','CREATE SEQUENCE')
    add('command','CREATE FUNCTION '+q+".effect() RETURNS text LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('"+sequence+"'); RETURN 'xxhixx'; END;$$;",'CREATE FUNCTION')
    current=0
    for index,name in enumerate(NAMES):
        statement=name+'_effects';effect='SELECT '+name+'('+q+".effect(),'x') AS v;"
        observe="SELECT nextval('"+sequence+"') AS v;"
        add('describe',statement,effect,25)
        add('bind_describe',statement,25,(),(),False,False)
        current+=1;add('query','simple',observe,(current,),20)
        for binary in (False,True,False):
            add('execute',statement,25,(('hi','hixx','xxhi')[index],),binary_output=binary)
            current+=2;add('query','simple',observe,(current,),20)
        add('close',statement)
        for mode in MODES:add('query',mode,"SELECT COALESCE('kept',"+name+'('+q+".effect(),'x')) AS v;",('kept',),25)
        for mode in MODES:add('query',mode,'SELECT '+name+'('+q+".effect(),'x') AS v FROM "+empty+' LIMIT 0;',(),25)
        current+=1;add('query','simple',observe,(current,),20)
        add('query','extended','SELECT '+name+"(nextval('"+sequence+"')) AS v;",(),25,state='42883')
        current+=1;add('query','simple',observe,(current,),20)
        prepared=name+'_sql_parameters'
        add('command','PREPARE '+prepared+'(text,text) AS SELECT '+name+'($1,$2) AS v;','PREPARE')
        add('query','simple','EXECUTE '+prepared+"('xxhixx','x');",(('hi','hixx','xxhi')[index],),25)
        add('command','DEALLOCATE '+prepared+';','DEALLOCATE')
        add('query','simple','PREPARE '+prepared+'_bad(integer) AS SELECT pg_catalog.'+name+'($1);',(),25,state='42883')
        add('command','DEALLOCATE ALL;','DEALLOCATE ALL')
    add('command','DROP SCHEMA '+q+' CASCADE;','DROP SCHEMA',notices=True)
    add('command','DROP SCHEMA '+qt+' CASCADE;','DROP SCHEMA',notices=True)
    assert len(commands)==PLANNED,len(commands)
    return commands


def main():
    if sys.argv[1:]==['--list']:
        print(json.dumps(plan('trim_signature_'+'0'*32),ensure_ascii=False,indent=2));return
    if sys.argv[1:]==['--reference']:sys.argv[1:]=['--reference18']
    schema='trim_signature_'+uuid.uuid4().hex
    commands=plan(schema);review=Review('TRIM_SIGNATURE_PROTOCOL');wire=Wire(review);owned=[]
    try:
        for index,operation in enumerate(commands):
            method,args,kwargs=operation['method'],list(operation['args']),operation['kwargs']
            if method in ('describe','bind_describe','execute','close','infer_describe'):args[0]=args[0].encode('ascii')
            before=len(review.failures)
            if method=='infer_describe':
                statement,sql=args
                wire.collect(('INFER_PARSE',sql),lambda:wire.exchange(wire.parse(statement,sql,(0,0))),lambda messages:messages==[(b'1',b''),(b't',wire.parameters((25,25))),(b'T',wire.description(25)),(b'Z',b'I')],'two actual TEXT input OIDs and TEXT output; no execution')
            else:getattr(wire,method)(*args,**kwargs)
            if index<2:
                assert len(review.failures)==before,'private schema ownership not established'
                owned.append(schema if index==0 else schema+'_text')
        owned=[];review.complete(PLANNED)
    finally:
        for name in owned:review.client.simple_query(review.sock,'DROP SCHEMA '+identifier(name)+' CASCADE;')
        review.close()


if __name__=='__main__':main()
