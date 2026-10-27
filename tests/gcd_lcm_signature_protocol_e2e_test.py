#!/usr/bin/env python3
"""GCD/LCM declarations, ambiguity, pure binding and actual wire identity.

Expected overloads come from the three concrete PG declarations, not from
runtime argument spelling or product output. Run this whole fixture on PG18.
The scalar matrix also retains runtime errors over zero-row queries.
"""
from compat.numeric_type_review import Review
from tsvector_concat_protocol_e2e_test import Wire as BaseWire
from quoted_unqualified_scalar_projection_protocol_e2e_test import identifier, WIDTHS, strict_data_rows
import struct
import sys
import uuid

MODES=('simple','extended','binary')
WIDTHS.update({1700:-1})
PLANNED=5244
ARGS=(("'6'",'U',False),('NULL','U',True),('6::smallint','S',False),('6::integer','I',False),('6::bigint','B',False),('6::numeric','N',False),("'6'::text",'X',False),('6::real','X',False),('6::double precision','X',False),('true','X',False),("'2000-01-01'::date",'X',False),('ARRAY[6]','X',False))
TYPES=(('smallint','S',21),('integer','I',23),('bigint','B',20),('numeric','N',1700),('text','X',25),('real','X',700),('double precision','X',701),('boolean','X',16))


def signature(left,right):
    if 'X' in (left,right):return 23,'42883'
    for kind,oid in (('N',1700),('B',20),('I',23)):
        if kind in (left,right):return oid,None
    return 23,'42725'


def scalar_cases():
    cases=[]
    for fn in ('gcd','lcm'):
        for a,at,an in ARGS:
            for b,bt,bn in ARGS:
                oid,state=signature(at,bt);value=None if an or bn else '6' if oid==1700 else 6
                cases.append((fn+'('+a+','+b+')',value,oid,state))
        for args,state in (('','42883'),('1','42883'),('1,2,3','42883'),('x=>1,y=>2','42883'),('NULL::text,NULL::integer','42883'),("'bad',1",'22P02'),("'bad','bad'",'42725'),('NULL::smallint,1::smallint','42725')):
            cases.append((fn+'('+args+')',None,23,state))
        cases.append((fn+"('-2147483648'::integer,0::integer)",None if fn=='gcd' else 0,23,'22003' if fn=='gcd' else None))
        for args,state in (('1,2','42809'),('1::smallint,2::smallint','42725'),("'bad',1",'42809'),('1::text,2::text','42883')):
            for role in ('over','distinct','filter','order'):
                call=fn+'('+('DISTINCT ' if role=='distinct' else '')+args+(' ORDER BY 1' if role=='order' else '')+')'
                call+=' OVER ()' if role=='over' else ' FILTER (WHERE true)' if role=='filter' else ''
                cases.append((call,None,23,state))
        for condition,state in (('missing','42703'),('1','42804'),('NULL','42809')):
            cases.append((fn+'(1,2) FILTER (WHERE '+condition+')',None,23,state))
        cases.extend((("COALESCE(1,"+fn+"('bad',1))",None,23,'22P02'),("CASE WHEN false THEN "+fn+"('bad',1) ELSE 0 END",None,23,'22P02'),('"'+fn.upper()+'"(1,2)',None,23,'42883'),('pg_catalog.'+fn+'(1,2)',1 if fn=='gcd' else 2,23,None)))
        for type_,kind,_ in TYPES:
            normal="'6'::"+type_ if kind=='X' else '6::'+type_
            if type_=='boolean':normal='true'
            for a,b,at,bt in (('NULL::'+type_,normal,kind,kind),(normal,'NULL::'+type_,kind,kind),('NULL::'+type_,'NULL::'+type_,kind,kind),('NULL::'+type_,'1::integer',kind,'I')):
                oid,state=signature(at,bt);cases.append((fn+'('+a+','+b+')',None,oid,state))
        for args,value,oid in (('6::numeric(9,2),6::integer','6.00',1700),('6::bigint,6::smallint',6,20),('6::integer,6::bigint',6,20),('NULL::numeric(9,2),6::integer',None,1700),("'NaN'::numeric,1",'NaN',1700),("'Infinity'::numeric,1",'NaN',1700),("'-Infinity'::numeric,1",'NaN',1700)):
            cases.append((fn+'('+args+')',value,oid,None))
    cases.append(('42',42,23,None))
    return cases


def numeric_bytes(text):
    if text is None:return None
    if text=='NaN':return struct.pack('!HhHH',0,0,0xc000,0)
    whole,_,fraction=str(text).partition('.')
    assert whole in ('0','6') and not fraction.strip('0')
    return struct.pack('!HhHH',int(whole!='0'),0,0,len(fraction))+(struct.pack('!H',int(whole)) if whole!='0' else b'')


class Wire(BaseWire):
    def bind(self,statement,inputs=(),values=(),binary_input=False,binary_output=False,execute=True):
        if binary_input:
            values=tuple(struct.pack('!h',value) if oid==21 and value is not None else numeric_bytes(value) if oid==1700 else value for oid,value in zip(inputs,values))
        return super().bind(statement,inputs,values,binary_input,binary_output,execute)

    def result_ok(self,messages,values,oid,state=None,binary=False,parsed=False,bound=False,simple=False,inputs=()):
        if state=='22003':
            rows,valid=strict_data_rows(messages)
            return valid and not rows and self.errors(messages)==[state] and not any(k in (b'D',b'C',b'N',b's') for k,_ in messages) and messages[-1:]==[(b'Z',b'I')]
        if oid==1700 and binary and not state:values=tuple(numeric_bytes(value) for value in values)
        return super().result_ok(messages,values,oid,state,binary,parsed,bound,simple,inputs)

    def inferred(self,statement,sql,requested,resolved,oid,state=None):
        if state:
            return self.collect(('INFERRED',sql,requested),lambda:self.exchange(self.parse(statement,sql,requested)),lambda m:m[-1:]==[(b'Z',b'I')] and [k for k,_ in m]==[b'E',b'Z'] and self.errors(m)==[state],state)
        self.collect(('INFERRED',sql,requested),lambda:self.exchange(self.parse(statement,sql,requested)),lambda m:m==[(b'1',b''),(b't',self.parameters(resolved)),(b'T',self.description(oid)),(b'Z',b'I')],(resolved,oid,'no execution'))


def plan(schema):
    operations=[]
    def add(method,*args,**kwargs):operations.append((method,args,kwargs))
    q=identifier(schema);table=q+'.rows_';empty=q+'.empty_';seq=q+'.effects_'
    add('command','CREATE SCHEMA '+q,'CREATE SCHEMA')
    columns='(id integer,s smallint,i integer,b bigint,n numeric(9,2),t text)'
    add('command','CREATE TABLE '+table+columns,'CREATE TABLE');add('command','CREATE TABLE '+empty+columns,'CREATE TABLE')
    add('command','INSERT INTO '+table+" VALUES(1,6,6,6,6,'6'),(2,NULL,NULL,NULL,NULL,NULL)",'INSERT 0 2')
    for expression,value,oid,state in scalar_cases():
        for suffix in ('',' FROM '+empty+' WHERE false',' FROM '+empty+' LIMIT 0'):
            for mode in MODES:add('query',mode,'SELECT '+expression+' AS v'+suffix,() if suffix else (value,),oid,state=state)
    cols=(('s','S'),('i','I'),('b','B'),('n','N'),('t','X'))
    for fn in ('gcd','lcm'):
        for a,at in cols:
            for b,bt in cols:
                oid,state=signature(at,bt);value='6.00' if oid==1700 else 6
                for tail,rows in ((' FROM '+table+' ORDER BY id',(value,None)),(' FROM '+table+' WHERE false',()),(' FROM '+table+' LIMIT 0',()),(' FROM '+empty,())):
                    for mode in MODES:add('query',mode,'SELECT '+fn+'('+a+','+b+') AS v'+tail,rows,oid,state=state)
        for _,ak,ao in TYPES:
            for _,bk,bo in TYPES:
                oid,state=signature(ak,bk);name=('sig_'+fn+'_'+str(ao)+'_'+str(bo)).encode()
                add('describe',name,'SELECT '+fn+'($1,$2) AS v',oid,(ao,bo),state=state)
                if state:continue
                for binary_input in (False,True):
                    for binary_output in (False,True):
                        for null_left,null_right in ((False,False),(True,False),(False,True),(True,True)):
                            values=(None if null_left else '6.00' if ao==1700 else 6,None if null_right else '6.00' if bo==1700 else 6)
                            expected=None if null_left or null_right else '6.00' if oid==1700 else 6
                            add('execute',name,oid,(expected,),(ao,bo),values,binary_input=binary_input,binary_output=binary_output)
                add('close',name)
        for left,right in (('U','U'),('U','S'),('S','U'),('U','I'),('I','U'),('U','B'),('B','U'),('U','N'),('N','U')):
            entry={'U':0,'S':21,'I':23,'B':20,'N':1700};requested=(entry[left],entry[right]);oid,state=signature(left,right)
            resolved=tuple(oid if value==0 else value for value in requested);name=('infer_'+fn+'_'+left+right).encode()
            add('inferred',name,'SELECT '+fn+'($1,$2) AS v',requested,resolved,oid,state)
            if state:continue
            for binary in (False,True):
                values=tuple('6.00' if item==1700 else 6 for item in resolved)
                add('execute',name,oid,('6.00' if oid==1700 else 6,),resolved,values,binary_input=binary,binary_output=binary)
            add('close',name)
        for spelling,state in ((fn,None),(fn.upper(),None),('"'+fn+'"',None),('"'+fn.upper()+'"','42883'),('pg_catalog.'+fn,None)):
            for mode in MODES:add('query',mode,'SELECT '+spelling+'(6::bigint,6::integer) AS v',(6,),20,state=state)
    add('command','CREATE SEQUENCE '+seq,'CREATE SEQUENCE')
    for fn in ('gcd','lcm'):
        name=('effects_'+fn).encode();add('describe',name,"SELECT "+fn+"(nextval('"+seq+"'),6::bigint) AS v",20)
        add('bind_describe',name,20,(),(),False,False)
        start=1 if fn=='gcd' else 6
        add('query','simple',"SELECT nextval('"+seq+"') AS v",(start,),20)
        for binary,value,probe in ((False,2 if fn=='gcd' else 42,start+2),(True,2 if fn=='gcd' else 18,start+4)):
            add('execute',name,20,(value,),binary_output=binary)
            add('query','simple',"SELECT nextval('"+seq+"') AS v",(probe,),20)
        add('close',name)
        add('command','CREATE FUNCTION '+q+'.'+fn+"(text,text) RETURNS text LANGUAGE SQL AS 'SELECT $1'",'CREATE FUNCTION')
        for mode in MODES:
            add('query',mode,"SELECT "+q+'.'+fn+"('stored-left'::text,'stored-right'::text) AS v",('stored-left',),25)
            add('query',mode,"SELECT "+q+'.'+fn+"(1,2) AS v",(),23,state='42883')
            add('query',mode,'SELECT pg_catalog.'+fn+"('6'::text,'6'::text) AS v",(),23,state='42883')
    add('query','simple','SELECT 42 AS v',(42,),23)
    add('command','DROP SCHEMA '+q+' CASCADE','DROP SCHEMA',notices=True)
    if PLANNED:assert len(operations)==PLANNED,len(operations)
    return operations


def main():
    if sys.argv[1:]==['--list']:print(len(scalar_cases()),len(plan('gcd_signature_'+'0'*32)));return
    assert PLANNED
    operations=plan('gcd_signature_'+uuid.uuid4().hex);review=Review('GCD_LCM_SIGNATURE');wire=Wire(review);owned=False
    try:
        for i,(method,args,kwargs) in enumerate(operations):
            before=len(review.failures);getattr(wire,method)(*args,**kwargs)
            if i==0:
                owned=len(review.failures)==before;assert owned,'private schema ownership not confirmed'
        owned=False;review.complete(PLANNED)
    finally:
        if owned:review.client.simple_query(review.sock,operations[-1][1][0])
        review.close()


if __name__=='__main__':main()
