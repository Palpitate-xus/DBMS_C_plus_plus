#!/usr/bin/env python3
"""Declared int4/int8 GCD/LCM results must fit, including actual binary datums.

The independent oracle uses Python's unbounded integers and math.gcd. This
fixture deliberately tests resolved int4/int8 overloads; smallint/smallint
overload ambiguity is separately recorded, not made into a product oracle.
Run this exact fixture against PostgreSQL 18 before product acceptance.
"""
from compat.numeric_type_review import Review
from tsvector_concat_protocol_e2e_test import Wire as BaseWire
from quoted_unqualified_scalar_projection_protocol_e2e_test import identifier, strict_data_rows
import math
import sys
import uuid

MODES=('simple','extended','binary')
VALUES=(-2147483648,-2147483647,-1073741824,-46349,-1,0,1,2,46337,1073741824,2147483647)
WIDE=(-9223372036854775808,-9223372036854775807,-4294967296,0,2,9223372036854775807)
PARAMETERS=((2147483647,2),(1073741824,3),(-2147483648,0),(0,-2147483648),(-2147483648,-2147483648),(-2147483648,1),(-2147483648,2),(2147483647,1),(-1,-1),(None,-2147483648),(-2147483648,None),(None,None))
PLANNED=2133


def expected(fn,left,right,oid):
    if left is None or right is None:return None,None
    g=math.gcd(left,right)
    result=g if fn=='gcd' else 0 if not left or not right else abs(left//g*right)
    return (None,'22003') if result>(2**(31 if oid==23 else 63)-1) else (result,None)


class Wire(BaseWire):
    def result_ok(self,messages,values,oid,state=None,binary=False,parsed=False,bound=False,simple=False,inputs=()):
        if not state:return super().result_ok(messages,values,oid,None,binary,parsed,bound,simple,inputs)
        rows,valid=strict_data_rows(messages)
        # Runtime range errors can occur while Bind plans a constant query or
        # while Execute consumes real rows. Neither may return a value/tag.
        return valid and not rows and self.errors(messages)==[state] and not any(k in (b'D',b'C',b'N',b's') for k,_ in messages) and messages[-1:]==[(b'Z',b'I')]

    def execute_range(self,statement,fn,left,right,inputs,binary_input,binary_output):
        oid=20 if 20 in inputs else 23
        value,state=expected(fn,left,right,oid)
        self.collect(('PARAMETER',statement,left,right,inputs,binary_input,binary_output),
            lambda:self.exchange(self.bind(statement,inputs,(left,right),binary_input,binary_output)),
            lambda messages:self.result_ok(messages,(value,),oid,state,binary_output,bound=True),
            (value,state,oid,binary_output))


def plan(schema):
    operations=[]
    def add(method,*args,**kwargs):operations.append((method,args,kwargs))
    q=identifier(schema);table=q+'.rows_';empty=q+'.empty_'
    add('command','CREATE SCHEMA '+q,'CREATE SCHEMA')
    add('command','CREATE TABLE '+table+'(id integer,a integer,b integer)','CREATE TABLE')
    add('command','CREATE TABLE '+empty+'(a integer,b integer)','CREATE TABLE')
    add('command','INSERT INTO '+table+" VALUES(1,2147483647,2),(2,'-2147483648',0),(3,NULL,'-2147483648')",'INSERT 0 3')
    for fn in ('gcd','lcm'):
        for oid,type_,inputs in ((23,'integer',VALUES),(20,'bigint',VALUES),(20,'bigint',WIDE)):
            for left in inputs:
                for right in inputs:
                    value,state=expected(fn,left,right,oid)
                    sql="SELECT "+fn+"('"+str(left)+"'::"+type_+",'"+str(right)+"'::"+type_+") AS v"
                    for mode in MODES:add('query',mode,sql,(value,),oid,state=state)
        for mode in MODES:
            for left,right in ((None,-2147483648),(-2147483648,None),(None,None)):
                literal=lambda n:'NULL::integer' if n is None else "'"+str(n)+"'::integer"
                add('query',mode,'SELECT '+fn+'('+literal(left)+','+literal(right)+') AS v',(None,),23)
            for suffix in (' WHERE false',' LIMIT 0'):
                add('query',mode,'SELECT '+fn+'(a,b) AS v FROM '+table+suffix,(),23)
            add('query',mode,'SELECT '+fn+'(a,b) AS v FROM '+empty,(),23)
            for i in range(1,4):
                left,right=PARAMETERS[0] if i==1 else PARAMETERS[2] if i==2 else PARAMETERS[9]
                value,state=expected(fn,left,right,23)
                add('query',mode,'SELECT '+fn+'(a,b) AS v FROM '+table+' WHERE id='+str(i),(value,),23,state=state)
            add('query',mode,'SELECT 42 AS v',(42,),23)
        for inputs in ((23,23),(20,20),(20,23),(23,20)):
            oid=20 if 20 in inputs else 23;name=('range_'+fn+'_'+str(inputs[0])+'_'+str(inputs[1])).encode()
            add('describe',name,'SELECT '+fn+'($1,$2) AS v',oid,inputs)
            for binary_input in (False,True):
                for binary_output in (False,True):
                    for left,right in PARAMETERS:add('execute_range',name,fn,left,right,inputs,binary_input,binary_output)
            add('close',name)
    add('command','DROP SCHEMA '+q+' CASCADE','DROP SCHEMA',notices=True)
    if PLANNED:assert len(operations)==PLANNED,len(operations)
    return operations


def main():
    if sys.argv[1:]==['--list']:print(len(plan('range_'+'0'*32)));return
    assert PLANNED
    operations=plan('range_'+uuid.uuid4().hex);review=Review('GCD_LCM_INTEGER_RANGE');wire=Wire(review);owned=False
    try:
        for i,(method,args,kwargs) in enumerate(operations):
            before=len(review.failures);getattr(wire,method)(*args,**kwargs)
            if i==0:
                owned=len(review.failures)==before
                assert owned,'private schema ownership not confirmed'
        owned=False;review.complete(PLANNED)
    finally:
        if owned:review.client.simple_query(review.sock,operations[-1][1][0])
        review.close()


if __name__=='__main__':main()
