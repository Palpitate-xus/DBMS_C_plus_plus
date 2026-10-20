#!/usr/bin/env python3
"""Finite BYTEA input error classification; not constant-analysis/Bind phases.

Invalid literals are asserted in Simple Query. Extended success/metadata and
retained valid parameters remain strict. Known earlier-error-stage and BYTEA
grammar gaps are recorded separately; they must not be inferred fixed here.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire as ExactWire, MODES, identifier
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
import sys
import uuid

CASES=(
    ('\\xq0',None,'22023'),('\\x0',None,'22023'),('\\x0 0',None,'22023'),('\\x00 g0',None,'22023'),
    ('\\x00 1',None,'22023'),('\\xGG',None,'22023'),('\\x0\t0',None,'22023'),('\\x   0',None,'22023'),
    ('\\',None,'22P02'),('\\q0',None,'22P02'),('\\400',None,'22P02'),('\\08a',None,'22P02'),('\\00',None,'22P02'),
    (None,None,None),('','\\x',None),('abc','\\x616263',None),('\\x','\\x',None),
    ('\\x00AFff','\\x00afff',None),('\\x00 AF ff','\\x00afff',None),('\\x00\tAF\nff\r','\\x00afff',None),
    ('\\000\\377','\\x00ff',None),('\\\\','\\x5c',None),('NULL','\\x4e554c4c',None))


def literal(value):
    return 'NULL' if value is None else "'"+value.replace("'","''")+"'"


def source(value,type_name):
    return value if type_name=='unknown' else 'CAST('+value+' AS '+type_name+')'


class Wire(ExactWire):
    def query(self,mode,sql,values=(),oid=701,state=None):
        if state is None:
            return super().query(mode,sql,values,oid)
        # This fixture owns the error class, not the already documented
        # earlier/later error-stage discrepancy. If a runtime description is
        # present, still require its exact BYTEA expression metadata.
        assert mode=='simple' and not values
        def valid(messages):
            rows,well_formed=strict_data_rows(messages)
            kinds=[kind for kind,_ in messages]
            descriptions=[body for kind,body in messages if kind==b'T']
            return (well_formed and not rows and self.errors(messages)==[state] and
                    kinds in ([b'E',b'Z'],[b'T',b'E',b'Z']) and
                    descriptions in ([],[self.description(oid)]) and messages[-1:]==[(b'Z',b'I')])
        self.collect(('SQLSTATE_ONLY_SIMPLE',sql),lambda:self.client.simple_query(self.sock,sql),valid,
                     (state,'no rows or successful tag; error stage not asserted'))


def main():
    if sys.argv[1:]==['--reference']:sys.argv[1:]=['--reference18']
    review=Review('BYTEA_HEX_SQLSTATE_PROTOCOL');wire=Wire(review)
    schema='bytea_hex_state_'+uuid.uuid4().hex;q=identifier(schema);table=q+'.rows_';owned=False
    try:
        assert len(CASES)==23
        for type_name in ('unknown','text','varchar','name'):
            for value,wanted,state in CASES:
                sql='SELECT CAST('+source(literal(value),type_name)+' AS bytea) AS v;'
                wire.query('simple',sql,() if state else (wanted,),17,state)
                if not state:
                    for mode in ('extended','binary'):wire.query(mode,sql,(wanted,),17)
        for input_oid in (25,17):
            statement=('bytea_state_parameters_'+str(input_oid)).encode()
            sql='SELECT '+('CAST($1 AS bytea)' if input_oid==25 else '$1')+' AS v;'
            wire.describe(statement,sql,17,(input_oid,))
            wire.bind_describe(statement,17,(input_oid,),('\\x00',),False,False)
            # Both parameter types deliberately use format 0 here. Invalid
            # parameter error stages are documented outside this finite fix.
            for binary_output in (False,True):
                for value,wanted,state in CASES:
                    if not state:wire.execute(statement,17,(wanted,),(input_oid,),(value,),binary_input=False,binary_output=binary_output)
            wire.close(statement)
        before=len(review.failures);wire.command('CREATE SCHEMA '+q+';','CREATE SCHEMA');owned=len(review.failures)==before
        assert owned,'private schema ownership not confirmed'
        wire.command('CREATE TABLE '+table+'(id integer,t text);','CREATE TABLE')
        wire.command('INSERT INTO '+table+' VALUES'+','.join('('+str(i)+','+literal(value)+')' for i,(value,_,_) in enumerate(CASES,1))+';','INSERT 0 23')
        for type_name in ('unknown','text','varchar','name'):
            for i,(_,wanted,state) in enumerate(CASES,1):
                sql='SELECT CAST('+source('t',type_name)+' AS bytea) AS v FROM '+table+' WHERE id='+str(i)+';'
                wire.query('simple',sql,() if state else (wanted,),17,state)
                if not state:
                    for mode in ('extended','binary'):wire.query(mode,sql,(wanted,),17)
        for mode in MODES:
            for suffix in (' WHERE false',' LIMIT 0'):wire.query(mode,'SELECT CAST(t AS bytea) AS v FROM '+table+suffix+';',(),17)
        for type_name,width in (('smallint',2),('integer',4),('bigint',8)):
            for value in (0,-1,1):
                wanted='\\x'+value.to_bytes(width,'big',signed=True).hex()
                for mode in MODES:wire.query(mode,'SELECT CAST(CAST('+str(value)+' AS '+type_name+') AS bytea) AS v;',(wanted,),17)
        wire.command('DROP SCHEMA '+q+' CASCADE;','DROP SCHEMA',notices=True);owned=False
        review.complete(427)
    finally:
        if owned:review.client.simple_query(review.sock,'DROP SCHEMA '+q+' CASCADE;')
        review.close()


if __name__=='__main__':main()
