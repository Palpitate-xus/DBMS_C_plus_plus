#!/usr/bin/env python3
"""STARTS_WITH declarations, pure preparation, NULL and boolean wire output."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, strict_data_rows
import json
import struct
import sys

LABEL='STARTS_WITH_SIGNATURE'
MODES=('simple','extended','binary')

class BooleanWire(Wire):
    def description(self, oid, binary=False):
        if oid != 16:
            return super().description(oid,binary)
        return struct.pack('!H',1)+b'v\0'+struct.pack('!IhIhih',0,0,16,1,-1,int(binary))

    def result_ok(self,messages,values,oid,state=None,binary=False,
                  parsed=False,bound=False,simple=False,inputs=()):
        if oid != 16:
            return super().result_ok(messages,values,oid,state,binary,parsed,bound,simple,inputs)
        rows,valid=strict_data_rows(messages)
        if state:
            return (valid and [k for k,b in messages]==[b'E',b'Z'] and
                    self.errors(messages)==[state] and messages[-1:]==[(b'Z',b'I')])
        prefix=[b'T'] if simple else [b'1',b't',b'T'] if parsed else []
        descriptions=[self.description(16)] if parsed else []
        if simple or bound:descriptions.append(self.description(16,binary))
        if bound:prefix += [b'2',b'T']
        wanted=[[None if v is None else (b'\x01' if v else b'\x00') if binary else
                 (b't' if v else b'f')] for v in values]
        return (valid and not self.errors(messages) and
                [k for k,b in messages]==prefix+[b'D']*len(values)+[b'C',b'Z'] and
                [b for k,b in messages if k==b'T']==descriptions and rows==wanted and
                [b for k,b in messages if k==b't']==([self.parameters(inputs)] if parsed else []) and
                [b for k,b in messages if k==b'C']==[('SELECT '+str(len(values))).encode()+b'\0'] and
                messages[-1:]==[(b'Z',b'I')] and
                all(b==b'' for k,b in messages if k in (b'1',b'2')))

    def query(self,mode,sql,values=(),oid=16,state=None):
        binary=mode=='binary'
        producer=(lambda:self.client.simple_query(self.sock,sql)) if mode=='simple' else (
            lambda:self.exchange(self.parse(b'',sql)+self.bind(b'',binary_output=binary)))
        self.collect((mode,sql),producer,
            lambda messages:self.result_ok(messages,values,oid,state,binary,
                parsed=mode!='simple',bound=mode!='simple',simple=mode=='simple'),
            (values,oid,state,binary))

    def execute(self,statement,oid,values,inputs=(),parameters=(),binary_input=False,
                binary_output=False,repeat=None):
        self.collect(('BIND/EXECUTE',statement,parameters,binary_input,binary_output),
            lambda:self.exchange(self.bind(statement,inputs,parameters,binary_input,binary_output)),
            lambda messages:self.result_ok(messages,values,oid,binary=binary_output,bound=True),
            (values,oid,binary_output))

def plan():
    operations=[]
    def add(method,*args,**kwargs):operations.append(dict(method=method,args=args,kwargs=kwargs))
    empty='r316_empty'; rows='r316_rows'
    add('command','CREATE TABLE '+empty+'(i integer,t text)','CREATE TABLE')
    add('command','CREATE TABLE '+rows+'(id integer,t text,p text)','CREATE TABLE')
    add('command',"INSERT INTO "+rows+" VALUES(1,'alphabet','alpha'),(2,'中é','中'),(3,NULL,'x'),(4,'NULL','N'),(5,'',''),(6,'abc',NULL),(7,'abc','z')",'INSERT 0 7')
    bad_types=('1','NULL::integer','true','1.5::numeric','1::bigint','1::real',
               "'x'::bytea","B'1'",'ARRAY[1]','NULL::uuid',"'2024-01-01'::date")
    bad=['',"'a'","'a','b','c'","missing=>'a'","'a',missing=>'b'"]
    for value in bad_types:bad += [value+",'a'","'a',"+value]
    suffixes=('', ' FROM '+empty+' WHERE false',' FROM '+empty+' LIMIT 0')
    for arguments in bad:
        for suffix in suffixes:
            for mode in MODES:add('query',mode,'SELECT starts_with('+arguments+') AS v'+suffix,(),16,state='42883')
    positive=(("'alphabet','alpha'",True),("'alphabet','beta'",False),
        ("'中é','中'",True),("'😀a','😀'",True),("'abc',''",True),("'',''",True),
        ("'','a'",False),("NULL,'a'",None),("'a',NULL",None),('NULL,NULL',None),
        ("'a  '::char(3),'a'",True),("'a','a  '::char(3)",True),
        ("'   '::char(3),''",True),("'a'::varchar,'a'::name",True),
        ("'NULL','N'",True),("'abc','A'",False))
    for arguments,value in positive:
        for suffix in suffixes:
            for mode in MODES:add('query',mode,'SELECT starts_with('+arguments+') AS v'+suffix,() if suffix else (value,))
    for mode in MODES:
        for expression in ("CASE WHEN false THEN starts_with(1,'a') ELSE false END",
                           "COALESCE(true,starts_with('a',1))"):
            add('query',mode,'SELECT '+expression+' AS v',(),16,state='42883')
        for arguments in ("i,'a'","'a',i"):
            add('query',mode,'SELECT starts_with('+arguments+') AS v FROM '+empty+' LIMIT 0',(),16,state='42883')
        add('query',mode,"SELECT starts_with(missing,'a') AS v FROM "+empty+' LIMIT 0',(),16,state='42703')
        for callee in ('pg_catalog.starts_with','"pg_catalog"."starts_with"','"STARTS_WITH"'):
            add('query',mode,'SELECT '+callee+"(1,'a') AS v",(),16,state='42883')
        add('query',mode,"SELECT starts_with('a','a') OVER() AS v",(),16,state='42809')
        add('query',mode,'SELECT starts_with(t,p) AS v FROM '+rows+' ORDER BY id',(True,True,None,True,True,None,False))
        add('query',mode,'SELECT starts_with(t,p) AS v FROM '+rows+' WHERE starts_with(t,p) ORDER BY id',(True,True,True,True))
    add('infer','both','SELECT starts_with($1,$2) AS v',(25,25))
    for binary in (False,True):
        add('bind_describe','both',16,(25,25),('中é','中'),binary,binary)
        for parameters,value in ((('中é','中'),True),(('abc','z'),False),((None,'x'),None),(('x',None),None),(('',''),True)):
            add('execute','both',16,(value,),(25,25),parameters,binary_input=binary,binary_output=binary)
    add('close','both')
    for oids in ((23,25),(25,23),(16,25),(25,17),(20,25),(25,2950)):
        add('describe','bad_'+str(oids[0])+'_'+str(oids[1]),'SELECT starts_with($1,$2) AS v',16,oids,state='42883')
    add('query','simple',"SELECT starts_with('ok','o') AS v",(True,))
    return operations

def main():
    operations=plan()
    if sys.argv[1:]==['--list']:
        print(json.dumps(operations,ensure_ascii=False,indent=2));return
    review=Review(LABEL);wire=BooleanWire(review)
    try:
        for operation in operations:
            method=operation['method'];args=list(operation['args'])
            if method in ('infer','bind_describe','execute','close','describe'):args[0]=args[0].encode('ascii')
            if method=='infer':
                statement,sql,oids=args
                wire.collect(('INFER_PARSE',sql),lambda:wire.exchange(wire.parse(statement,sql,(0,0))),
                    lambda messages:messages==[(b'1',b''),(b't',wire.parameters(oids)),(b'T',wire.description(16)),(b'Z',b'I')],
                    (oids,16,'no execution'))
            else:getattr(wire,method)(*args,**operation['kwargs'])
        review.complete(len(operations))
    finally:review.close()

if __name__=='__main__':main()
