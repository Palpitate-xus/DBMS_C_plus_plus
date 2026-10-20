#!/usr/bin/env python3
"""STRPOS declarations, output, Parse purity and actual callback ownership.

POSITION grammar/callee argument order is independently open, not repaired here.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import json
import sys
import uuid


LABEL = 'STRPOS_SIGNATURE_PROTOCOL'
PLANNED = 294
MODES = ('simple', 'extended', 'binary')


def plan(schema):
    commands = []
    def add(method, *args, **kwargs):
        commands.append(dict(method=method, args=args, kwargs=kwargs))
    q = identifier(schema)
    empty, rows, sequence = q+'.empty_', q+'.rows_', q+'.effects_'
    add('command', 'CREATE SCHEMA '+q+';', 'CREATE SCHEMA')
    add('command', 'CREATE TABLE '+empty+'(i integer,t text);', 'CREATE TABLE')
    add('command', 'CREATE TABLE '+rows+'(id integer,h text,n text);', 'CREATE TABLE')
    add('command', 'INSERT INTO '+rows+" VALUES(1,'甲乙针','针'),(2,'abc',''),(3,'','x'),(4,NULL,'x'),(5,'abc',NULL);", 'INSERT 0 5')
    suffixes = ('', ' FROM '+empty+' WHERE false', ' FROM '+empty+' LIMIT 0')
    invalid = ('', "'abc'", "'abc','b','x'", '1,2', 'NULL::integer,NULL::integer',
               'true,false', "'\\x61'::bytea,'\\x61'::bytea", "NULL::uuid,''::text",
               "'abc',NULL::uuid", "'abc',B'1'", "'abc',ARRAY[1]", "missing=>'abc'")
    for args in invalid:
        for suffix in suffixes:
            for mode in MODES:
                add('query', mode, 'SELECT strpos('+args+') AS v'+suffix+';', (), 23, state='42883')
    for expression in ('COALESCE(1,strpos(1,2))', 'CASE WHEN false THEN strpos(1,2) ELSE 0 END'):
        for mode in MODES:
            add('query', mode, 'SELECT '+expression+' AS v;', (), 23, state='42883')
    for expression in ("strpos(missing,'')", "strpos('',missing)"):
        for mode in MODES:
            add('query', mode, 'SELECT '+expression+' AS v FROM '+empty+' LIMIT 0;', (), 23, state='42703')
    positive = (("strpos('甲乙针','针')",3), ("strpos('abc','')",1),
                ("strpos('','x')",0), ("strpos(NULL::text,'')",None),
                ('strpos(NULL,NULL)',None), ("strpos('a  '::char(3),' ')",0),
                ("strpos('abc','a '::char(2))",1), ("strpos('abc'::varchar,'b')",2),
                ("strpos('abc'::name,'b'::name)",2), ("pg_catalog.strpos('abc','b')",2))
    for expression, value in positive:
        for suffix in suffixes:
            for mode in MODES:
                add('query', mode, 'SELECT '+expression+' AS v'+suffix+';', () if suffix else (value,), 23)
    for expression in ("strpos('abc','b')", "strpos(NULL::text,'')", "strpos('abc'::char(3),'b')"):
        for mode in MODES:
            add('query', mode, 'SELECT '+expression+' OVER() AS v;', (), 23, state='42809')
    for mode in MODES:
        add('query', mode, 'SELECT strpos(h,n) AS v FROM '+rows+' ORDER BY id;', (3,1,0,None,None), 23)
        add('query', mode, 'SELECT strpos(h,n) AS v FROM '+rows+' WHERE strpos(h,n)>0 ORDER BY id;', (3,1), 23)

    statement = 'strpos_parameters'
    add('describe', statement, 'SELECT strpos($1,$2) AS v;', 23, (25,25))
    add('bind_describe', statement, 23, (25,25), ('',''), False, False)
    add('bind_describe', statement, 23, (25,25), (None,''), True, True)
    for binary in (False,True):
        for haystack, needle, value in (('甲乙针','针',3),('abc','',1),('','x',0),
                                       (None,'x',None),('abc',None,None),('123','2',2)):
            add('execute', statement, 23, (value,), (25,25), (haystack,needle),
                binary_input=binary, binary_output=binary)
    add('close', statement)
    for inputs in ((23,23),(17,17),(2950,25)):
        add('describe', 'strpos_bad_'+str(inputs[0]), 'SELECT strpos($1,$2) AS v;', 23, inputs, state='42883')
    inferred = 'strpos_inferred'
    add('infer_describe', inferred, 'SELECT strpos($1,$2) AS v;')
    for binary in (False,True):
        add('execute', inferred, 23, (3,), (25,25), ('甲乙针','针'), binary_input=binary, binary_output=binary)
    add('close', inferred)

    add('command', 'CREATE FUNCTION '+q+'.strpos(a integer,b integer) RETURNS integer LANGUAGE SQL IMMUTABLE AS $$SELECT a+b+100$$;', 'CREATE FUNCTION')
    add('command', 'SET search_path=pg_catalog,'+q+';', 'SET', parameter_status='pg_catalog, '+schema)
    for mode in MODES:
        add('query', mode, 'SELECT strpos(1,2) AS v;', (103,), 23)
        add('query', mode, 'SELECT '+q+'.strpos(1,2) AS v;', (103,), 23)
        add('query', mode, 'SELECT pg_catalog.strpos(1,2) AS v;', (), 23, state='42883')
        add('query', mode, 'SELECT "strpos"(1,2) AS v;', (103,), 23)
    add('command', 'SET search_path=public,pg_catalog;', 'SET', parameter_status='public, pg_catalog')
    for mode in MODES:
        add('query', mode, "SELECT strpos('123','2') AS v;", (2,), 23)
        add('query', mode, 'SELECT "STRPOS"(\'abc\',\'b\') AS v;', (), 23, state='42883')

    add('command', 'CREATE SEQUENCE '+sequence+';', 'CREATE SEQUENCE')
    add('command', 'CREATE FUNCTION '+q+".effect() RETURNS text LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('"+sequence+"'); RETURN '甲乙针'; END;$$;", 'CREATE FUNCTION')
    effect = 'SELECT strpos('+q+".effect(),'针') AS v;"
    retained = 'strpos_retained_effect'
    add('describe', retained, effect, 23)
    add('bind_describe', retained, 23, (), (), False, False)
    observe = "SELECT nextval('"+sequence+"') AS v;"
    add('query', 'simple', observe, (1,), 20)
    for binary, expected in ((False,3),(True,5),(False,7)):
        add('execute', retained, 23, (3,), binary_output=binary)
        add('query', 'simple', observe, (expected,), 20)
    add('close', retained)
    for mode in MODES:
        add('query', mode, 'SELECT COALESCE(1,strpos('+q+".effect(),'针')) AS v;", (1,), 23)
    add('query', 'simple', observe, (8,), 20)
    for mode in MODES:
        add('query', mode, 'SELECT strpos('+q+".effect(),'针') AS v FROM "+empty+' LIMIT 0;', (), 23)
    add('query', 'simple', observe, (9,), 20)
    add('command', 'DROP SCHEMA '+q+' CASCADE;', 'DROP SCHEMA', notices=True)
    assert len(commands) == PLANNED, len(commands)
    return commands


def main():
    if sys.argv[1:] == ['--list']:
        print(json.dumps(plan('strpos_'+'0'*32),ensure_ascii=False,indent=2))
        return
    commands = plan('strpos_'+uuid.uuid4().hex)
    review = Review(LABEL)
    wire = Wire(review)
    owned = False
    try:
        for index, operation in enumerate(commands):
            method, args, kwargs = operation['method'], list(operation['args']), operation['kwargs']
            if method in ('describe','bind_describe','execute','close','infer_describe'):
                args[0] = args[0].encode('ascii')
            if method == 'infer_describe':
                statement, sql = args
                wire.collect(('INFER_PARSE',sql),
                    lambda: wire.exchange(wire.parse(statement,sql,(0,0))),
                    lambda messages: messages == [(b'1',b''),(b't',wire.parameters((25,25))),
                        (b'T',wire.description(23)),(b'Z',b'I')], 'two actual text parameter OIDs, int4 output, no execution')
            else:
                before = len(review.failures)
                getattr(wire,method)(*args,**kwargs)
                if index == 0:
                    owned = len(review.failures) == before
                    assert owned, 'UUID schema ownership not confirmed'
        owned = False
        review.complete(PLANNED)
    finally:
        if owned:
            review.client.simple_query(review.sock, commands[-1]['args'][0])
        review.close()


if __name__ == '__main__':
    main()
