#!/usr/bin/env python3
"""REPLACE original-input, nonoverlapping semantics and dense replacement work.

Run the whole fixture against PostgreSQL 18 before product acceptance. Elapsed
observations are diagnostics only, not timing assertions or a performance SLA.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import time
import uuid

MODES = ('simple', 'extended', 'binary')
CONTROLS = 330


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    review = Review('REPLACE_LINEAR_PROTOCOL')
    wire = Wire(review)
    schema = 'replace_linear_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    sequence = identifier(schema) + '.effects_'
    owned = False
    try:
        samples = (
            ('', '', '', ''), ('', 'a', 'b', ''), ('abc', '', 'x', 'abc'),
            ('abc', 'x', 'y', 'abc'), ('abc', 'abc', 'X', 'X'), ('abc', 'abc', '', ''),
            ('aaaa', 'a', 'xx', 'xxxxxxxx'), ('aaaa', 'aa', 'b', 'bb'),
            ('aaa', 'aa', 'b', 'ba'), ('ababa', 'aba', 'X', 'Xba'),
            ('aaa', 'a', 'aa', 'aaaaaa'), ('aaaa', 'aa', 'a', 'aa'),
            ('abab', 'ab', '', ''), ('abXXab', 'ab', '界', '界XX界'),
            ('aé界😀', 'é界', '!', 'a!😀'), ('ééé', 'éé', '界', '界é'),
            ('😀😀', '😀', 'é', 'éé'), ('éé', '́', '!', 'e!e!'),
            ('é', 'e', 'x', 'é'), ('a b  ', ' ', '', 'ab'),
            ("a'b'a", "'", '"', 'a"b"a'), ('a\\b\\a', '\\', "'", "a'b'a"),
            ('a\nb\na', '\n', '\t', 'a\tb\ta'), ('xax', 'x', 'xax', 'xaxaxax'))
        for mode in MODES:
            for name in ('replace', '"replace"', 'pg_catalog.replace'):
                for source, old, new, expected in samples:
                    sql = 'SELECT ' + name + '(' + ','.join(map(literal,(source,old,new))) + ') AS v;'
                    wire.query(mode, sql, (expected,), 25)
            for expression, expected in (
                    ("replace('ab  '::char(4),'b','!')", 'a!'),
                    ("replace('ab  '::varchar,'b','!')", 'a!  '),
                    ("replace('a b  ',' '::char(1),'x')", 'a b  '),
                    ("replace('aba','b '::char(2),'X')", 'aXa'),
                    ("replace('aba','b','X  '::char(3))", 'aXa'),
                    ("replace('é界  '::char(4),'界','😀')", 'é😀')):
                wire.query(mode,'SELECT '+expression+' AS v;', (expected,),25)
            for mask in range(1,8):
                args = ['NULL::text' if mask & (1<<i) else literal(v)
                        for i,v in enumerate(('abc','b','X'))]
                wire.query(mode,'SELECT replace('+','.join(args)+') AS v;', (None,),25)
            for count in (16384,65536,262144):
                for expression, expected in (
                        ("replace(repeat('a',N),'a','')", "''"),
                        ("replace(repeat('a',N),'a','bb')", "repeat('b',2*N)"),
                        ("replace(repeat('a',N),'a','b')", "repeat('b',N)"),
                        ("replace('a'||repeat('x',N)||'a','a','')", "repeat('x',N)")):
                    sql = ("SELECT CASE WHEN " + expression.replace('N',str(count)) + '=' +
                           expected.replace('N',str(count)) + " THEN 'same' ELSE 'different' END AS v;")
                    started=time.monotonic_ns()
                    wire.query(mode,sql,('same',),25)
                    print('REPLACE_LINEAR_ELAPSED_NS',mode,count,expression,time.monotonic_ns()-started,flush=True)

        statement=b'replace_linear_parameters'
        wire.describe(statement,'SELECT replace($1::text,$2::text,$3::text) AS v;',25,(25,25,25))
        wire.bind_describe(statement,25,(25,25,25),('aaaa','aa','b'),False,False)
        for binary in (False,True):
            for values, expected in (
                    (('aaaa','aa','b'),'bb'),(('ababa','aba','X'),'Xba'),
                    (('aé界😀','é界','!'),'a!😀'),(('abc','','x'),'abc'),
                    ((None,'a','b'),None),(('abc','b',None),None)):
                wire.execute(statement,25,(expected,),(25,25,25),values,binary_input=binary,binary_output=binary)
        wire.close(statement)
        previous=len(review.failures)
        wire.command('CREATE SCHEMA '+identifier(schema)+';','CREATE SCHEMA')
        owned=len(review.failures)==previous
        assert owned,'private schema ownership not confirmed'
        wire.command('CREATE TABLE '+table+'(id integer,s text,f text,t text);','CREATE TABLE')
        wire.command('INSERT INTO '+table+" VALUES(1,'abc','b','X'),(2,NULL,'a','x'),(3,'aaaa','a',''),(4,'aé界😀','é界','!');",'INSERT 0 4')
        for mode in MODES:
            wire.query(mode,'SELECT replace(s,f,t) AS v FROM '+table+' ORDER BY id;',('aXc',None,'','a!😀'),25)
            wire.query(mode,'SELECT replace(s,f,t) AS v FROM '+table+' WHERE false;',(),25)
            wire.query(mode,'SELECT replace(s,f,t) AS v FROM '+table+' LIMIT 0;',(),25)
        wire.command('CREATE SEQUENCE '+sequence+';','CREATE SEQUENCE')
        effect="SELECT replace(nextval('"+sequence+"')::text,'2','X') AS v;"
        effect_statement=b'replace_linear_effect'
        wire.describe(effect_statement,effect,25)
        wire.bind_describe(effect_statement,25,(),(),False,False)
        wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(1,),20)
        for mode,current,following in zip(MODES,('X','4','6'),(3,5,7)):
            wire.query(mode,effect,(current,),25)
            wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(following,),20)
        wire.close(effect_statement)
        wire.command('DROP SCHEMA '+identifier(schema)+' CASCADE;','DROP SCHEMA',notices=True)
        owned=False
        review.complete(CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock,'DROP SCHEMA '+identifier(schema)+' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
