#!/usr/bin/env python3
"""ASCII/CHR declarations, coercion, lazy branches and exact UTF-8 wire values."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire
import json
import sys


LABEL = 'CHARACTER_CODE_SIGNATURE'
MODES = ('simple', 'extended', 'binary')


def plan():
    commands = []
    def add(method, *args, **kwargs):
        commands.append(dict(method=method, args=args, kwargs=kwargs))
    empty, rows = 'r313_empty', 'r313_rows'
    add('command', 'CREATE TABLE '+empty+'(i integer,t text)', 'CREATE TABLE')
    add('command', 'CREATE TABLE '+rows+'(id integer,t text,n integer)', 'CREATE TABLE')
    add('command', "INSERT INTO "+rows+" VALUES(1,'中',65),(2,'',128512),(3,NULL,NULL),(4,'NULL',233),(5,'é  ',233)", 'INSERT 0 5')
    invalid = {
        'ascii': ('', "'a','b'", '1', 'NULL::integer', 'true', '1.25::numeric',
                  'NULL::uuid', 'ARRAY[1]', "'a'::bytea", "B'1'", "'2024-01-01'::date", "missing=>'a'"),
        'chr': ('', '65,66', "'65'::text", '65::bigint', '65.0::numeric',
                '65::real', '65::double precision', 'true', 'NULL::bytea',
                'ARRAY[65]', "'2024-01-01'::date", "B'1'", 'missing=>65')}
    positive = {
        'ascii': (("'A'",65),("'é'",233),("'中'",20013),("'😀'",128512),
                  ("' '",32),("''",0),('NULL',None),('NULL::text',None),
                  ("'é  '::varchar",233),("'   '::char(3)",0),
                  ("'é  '::char(3)",233),("'a'::name",97)),
        'chr': (('65','A'),("'65'",'A'),('65::smallint','A'),('233','é'),
                ('20013','中'),('128512','😀'),('1114111','\U0010ffff'),
                ('55295','\ud7ff'),('57344','\ue000'),('NULL',None),('NULL::integer',None))}
    suffixes = ('', ' FROM '+empty+' WHERE false', ' FROM '+empty+' LIMIT 0')
    for name in ('ascii','chr'):
        oid = 23 if name == 'ascii' else 25
        for arguments in invalid[name]:
            for suffix in suffixes:
                for mode in MODES:
                    add('query', mode, 'SELECT '+name+'('+arguments+') AS v'+suffix, (), oid, state='42883')
        bad, fallback = ('1','0') if name == 'ascii' else ('true',"'x'")
        for expression in ('CASE WHEN false THEN '+name+'('+bad+') ELSE '+fallback+' END',
                           'COALESCE('+fallback+','+name+'('+bad+'))'):
            for mode in MODES:
                add('query', mode, 'SELECT '+expression+' AS v', (), oid, state='42883')
        for mode in MODES:
            add('query', mode, 'SELECT '+name+'('+('i' if name == 'ascii' else 't')+') AS v FROM '+empty+' LIMIT 0', (), oid, state='42883')
            add('query', mode, 'SELECT '+name+'(missing) AS v FROM '+empty+' LIMIT 0', (), oid, state='42703')
            for callee in ('pg_catalog.'+name, '"pg_catalog"."'+name+'"', '"'+name.upper()+'"'):
                add('query', mode, 'SELECT '+callee+'('+bad+') AS v', (), oid, state='42883')
        for arguments, value in positive[name]:
            for suffix in suffixes:
                for mode in MODES:
                    add('query', mode, 'SELECT '+name+'('+arguments+') AS v'+suffix, () if suffix else (value,), oid)
        for mode in MODES:
            add('query', mode, 'SELECT '+name+'('+positive[name][0][0]+') OVER() AS v', (), oid, state='42809')
        input_oid = 25 if name == 'ascii' else 23
        statement = name+'_parameter'
        add('infer', statement, 'SELECT '+name+'($1) AS v', oid, input_oid)
        add('bind_describe', statement, oid, (input_oid,), (None,), False, False)
        for binary in (False,True):
            parameters = (('中',20013),('',0),(None,None),('NULL',78)) if name == 'ascii' else ((65,'A'),(128512,'😀'),(None,None),(233,'é'))
            for parameter, value in parameters:
                add('execute', statement, oid, (value,), (input_oid,), (parameter,), binary_input=binary, binary_output=binary)
        add('close', statement)
        for bad_oid in ((23,16,17,2950) if name == 'ascii' else (25,20,1700,17)):
            add('describe', name+'_bad_'+str(bad_oid), 'SELECT '+name+'($1) AS v', oid, (bad_oid,), state='42883')
    for mode in MODES:
        add('query', mode, 'SELECT ascii(t) AS v FROM '+rows+' ORDER BY id', (20013,0,None,78,233), 23)
        add('query', mode, 'SELECT chr(n) AS v FROM '+rows+' ORDER BY id', ('A','😀',None,'é','é'), 25)
        add('query', mode, 'SELECT ascii(t) AS v FROM '+rows+' WHERE ascii(t)>0 ORDER BY id', (20013,78,233), 23)
        for argument, state in (("'65x'",'22P02'),("'2147483648'",'22003')):
            add('query', mode, 'SELECT chr('+argument+') AS v', (), 25, state=state)
    for argument, state in (('-1','22023'),('0','54000'),('55296','54000'),('1114112','54000')):
        add('runtime_error', 'SELECT chr('+argument+') AS v', state)
    add('query', 'simple', "SELECT chr(65) AS v", ('A',), 25)
    return commands


def main():
    commands = plan()
    if sys.argv[1:] == ['--list']:
        print(json.dumps(commands, ensure_ascii=False, indent=2))
        return
    review = Review(LABEL)
    wire = Wire(review)
    try:
        for operation in commands:
            method, args = operation['method'], list(operation['args'])
            if method in ('infer','bind_describe','execute','close','describe'):
                args[0] = args[0].encode('ascii')
            if method == 'infer':
                statement, sql, oid, input_oid = args
                wire.collect(('INFER_PARSE',sql),
                    lambda: wire.exchange(wire.parse(statement,sql,(0,))),
                    lambda messages: messages == [(b'1',b''),(b't',wire.parameters((input_oid,))),
                        (b'T',wire.description(oid)),(b'Z',b'I')], (input_oid,oid,'no execution'))
            elif method == 'runtime_error':
                sql, state = args
                wire.query('simple', sql, (), 25, state=state)
            else:
                getattr(wire, method)(*args, **operation['kwargs'])
        review.complete(PLANNED)
    finally:
        review.close()


PLANNED = 533
if __name__ == '__main__':
    main()
