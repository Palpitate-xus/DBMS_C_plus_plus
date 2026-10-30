#!/usr/bin/env python3
"""Unaccepted R222 reproduction: signature identity, resolution and lifecycle.

The complete corpus must pass the private PG18 reference before acceptance.
Aliases every expression as v: the separately deferred R217 label/CTE Describe
work is not an overload oracle and is not resumed by this fixture.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, MODES, WIDTHS, PACK, identifier
import sys
import uuid


PACK[16] = '!?'  # typed binary input, not text bytes labelled as binary bool
WIDTHS[16] = 1


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('ROUTINE_OVERLOAD_IDENTITY_PROTOCOL')
    wire = Wire(review)
    token = uuid.uuid4().hex[:8]
    first, second = 'r222_' + token + '_a', 'r222_' + token + '_b'
    a, b = identifier(first), identifier(second)
    table, empty, sequence = a + '.rows_', a + '.empty_', a + '.effects_'
    function = a + '.pick'
    owned, planned = [], 0

    def command(sql, tag, state=None, status=b'I', notices=False, path=None):
        def valid(messages):
            kinds = [kind for kind, _ in messages if kind not in (b'N', b'S')]
            errors = wire.errors(messages)
            statuses = [body for kind, body in messages if kind == b'S']
            wanted_status = b'search_path\0' + path.encode() + b'\0' if path is not None else None
            valid_status = statuses in ([], [wanted_status]) if path is not None else not statuses
            return (kinds == ([b'E', b'Z'] if state else [b'C', b'Z']) and
                    errors == ([state] if state else []) and
                    [body for kind, body in messages if kind == b'C'] == ([] if state else [tag.encode() + b'\0']) and
                    messages[-1:] == [(b'Z', status)] and valid_status and
                    (notices or not any(kind == b'N' for kind, _ in messages)))
        wire.collect(sql, lambda: wire.client.simple_query(wire.sock, sql), valid, (tag, state, status))

    def create(owner, name, args, result, body, extra='IMMUTABLE'):
        command('CREATE FUNCTION ' + owner + '.' + name + '(' + args + ') RETURNS ' + result +
                ' LANGUAGE SQL ' + extra + ' AS $$SELECT ' + body + '$$', 'CREATE FUNCTION')

    def checked_group(start, expected):
        nonlocal planned
        assert review.controls - start == expected, (review.controls - start, expected)
        planned += expected

    try:
        start = review.controls
        for name in (first, second):
            before = len(review.failures)
            command('CREATE SCHEMA ' + identifier(name), 'CREATE SCHEMA')
            assert len(review.failures) == before, 'private schema not owned'
            owned.append(name)
        columns = '(id integer,i integer,b bigint,t text,flag boolean,a integer[])'
        command('CREATE TABLE ' + table + columns, 'CREATE TABLE')
        command('CREATE TABLE ' + empty + columns, 'CREATE TABLE')
        command('INSERT INTO ' + table + " VALUES(1,-2,9,'x',true,ARRAY[1,2]),(2,0,10,'',false,ARRAY[]::integer[]),(3,NULL,NULL,NULL,NULL,NULL),(4,5,11,'tail',true,ARRAY[3])", 'INSERT 0 4')
        declarations = (
            ('', 'integer', '10'), ('p integer', 'integer', '101'),
            ('p bigint', 'bigint', '10000000003::bigint'), ('p text', 'text', "'text'"),
            ('p boolean', 'text', "'bool'"), ('p integer[]', 'integer', '401'),
            ('p integer,q integer', 'integer', '201'), ('p integer,q text', 'text', "'inttext'"),
            ('p text,q integer', 'text', "'textint'"), ('p integer,q integer,r integer', 'integer', '301'),
        )
        for args, result, body in declarations:
            create(a, 'pick', args, result, body)
        create(b, 'pick', 'p integer', 'integer', '999')
        create(b, 'pick', 'p varchar', 'text', "'varchar'")
        create(a, 'ambiguous_pick', 'p integer', 'integer', '501')
        create(a, 'ambiguous_pick', 'p boolean', 'integer', '502')
        create(a, 'only_integer', 'p integer', 'integer', '503')
        create(a, 'numeric_pick', 'p integer', 'integer', '601')
        create(a, 'numeric_pick', 'p double precision', 'double precision', '6.5::double precision')
        quoted_names = ('Pick', 'name.dot', 'say"hi')
        for name in quoted_names:
            create(a, identifier(name), 'p integer', 'integer', '701')
            create(a, identifier(name), 'p text', 'text', "'quoted'")
        command('CREATE SEQUENCE ' + sequence, 'CREATE SEQUENCE')
        for args in ('', 'p integer'):
            create(a, 'tick', args, 'bigint', "nextval('" + sequence + "')", 'VOLATILE')
        command('SET search_path=' + first + ',' + second + ',pg_catalog', 'SET',
                path=first + ', ' + second + ', pg_catalog')
        checked_group(start, 32)

        start = review.controls
        cases = (
            ('', 10, 23), ('-2', 101, 23), ('NULL::integer', 101, 23),
            ('2::bigint', 10000000003, 20), ('NULL::bigint', 10000000003, 20),
            ("'x'", 'text', 25), ('NULL', 'text', 25), ("'x'::text", 'text', 25),
            ("'x'::varchar", 'varchar', 25), ('true', 'bool', 25), ('NULL::boolean', 'bool', 25),
            ('ARRAY[1,2]', 401, 23), ('NULL::integer[]', 401, 23),
            ('1,2', 201, 23), ("1,'x'", 'inttext', 25), ("'x',2", 'textint', 25),
            ('1,2,3', 301, 23), ('NULL,NULL', 'textint', 25),
            ('1,NULL', 'inttext', 25), ('NULL,2', 'textint', 25),
        )
        callees = ('pick', '"pick"', first.upper() + '.pick', a + '."pick"')
        contexts = (('', 1), (' FROM ' + table + ' ORDER BY id', 4),
                    (' FROM ' + empty, 0), (' FROM ' + table + ' WHERE false', 0),
                    (' FROM ' + table + ' LIMIT 0', 0))
        for callee in callees:
            for args, value, oid in cases:
                if args == "'x'::varchar" and '.' in callee:
                    value = 'text'  # qualification excludes the later varchar overload
                for suffix, rows in contexts:
                    for mode in MODES:
                        state = '42725' if args == 'NULL,NULL' else None
                        wire.query(mode, 'SELECT ' + callee + '(' + args + ') AS v' + suffix,
                                   [] if state else [value]*rows, oid, state)
        checked_group(start, len(callees)*len(cases)*len(contexts)*len(MODES))

        start = review.controls
        inputs = (('i', 101, 23), ('b', 10000000003, 20), ('t', 'text', 25),
                  ('flag', 'bool', 25), ('a', 401, 23), ('i,i', 201, 23),
                  ('i,t', 'inttext', 25), ('t,i', 'textint', 25), ('i,i,i', 301, 23))
        for callee in ('pick', function):
            for args, value, oid in inputs:
                for suffix, rows in contexts[1:]:
                    for mode in MODES:
                        wire.query(mode, 'SELECT ' + callee + '(' + args + ') AS v' + suffix, [value]*rows, oid)
        checked_group(start, 2*len(inputs)*4*len(MODES))

        start = review.controls
        for name in quoted_names:
            for callee in (a + '.' + identifier(name), identifier(name)):
                for args, value, oid in (('1', 701, 23), ("'x'", 'quoted', 25)):
                    for mode in MODES:
                        wire.query(mode, 'SELECT ' + callee + '(' + args + ') AS v', [value], oid)
        checked_group(start, len(quoted_names)*2*2*len(MODES))

        start = review.controls
        prepared_cases = (
            ((), ((), ()), 10, 23), ((23,), ((-2,), (None,)), 101, 23),
            ((20,), ((9,), (None,)), 10000000003, 20), ((25,), (('x',), (None,)), 'text', 25),
            ((16,), ((True,), (None,)), 'bool', 25), ((23,23), ((1,2), (None,None)), 201, 23),
            ((23,25), ((1,'x'), (None,None)), 'inttext', 25), ((23,23,23), ((1,2,3), (None,None,None)), 301, 23),
        )
        for callee in ('pick', function):
            for input_oids, values, value, oid in prepared_cases:
                name = ('r222_p_' + str(review.controls)).encode()
                sql = 'SELECT ' + callee + '(' + ','.join('$'+str(i+1) for i in range(len(input_oids))) + ') AS v'
                wire.describe(name, sql, oid, input_oids)
                for binary_output in (False, True):
                    wire.bind_describe(name, oid, input_oids, values[0], False, binary_output)
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        for parameters in values:
                            wire.execute(name, oid, [value], input_oids, parameters, binary_input, binary_output)
                wire.close(name)
        checked_group(start, 2*len(prepared_cases)*12)

        start = review.controls
        for supplied in ((0,), ()):
            name = ('r222_infer_' + str(review.controls)).encode()
            expected = [(b'1', b''), (b't', wire.parameters((25,))),
                        (b'T', wire.description(25)), (b'Z', b'I')]
            wire.collect(('INFER', supplied), lambda: wire.exchange(wire.parse(name, 'SELECT pick($1) AS v', supplied)),
                         lambda messages: messages == expected, expected)
            for binary_output in (False, True):
                wire.bind_describe(name, 25, (25,), ('x',), False, binary_output)
                for value in ('x', None):
                    wire.execute(name, 25, ['text'], (25,), (value,), False, binary_output)
            wire.close(name)
        checked_group(start, 16)

        start = review.controls
        errors = (('ambiguous_pick(NULL)', '42725'), ("pick('\\x00'::bytea)", '42883'),
                  ('pick(true,2)', '42883'), ("only_integer('bad')", '22P02'),
                  ("ambiguous_pick(nextval('" + sequence + "'))", '42883'))
        for expression, state in errors:
            for suffix in ('', ' WHERE false', ' LIMIT 0'):
                for mode in MODES:
                    wire.query(mode, 'SELECT ' + expression + ' AS v' + suffix, state=state)
        checked_group(start, len(errors)*3*len(MODES))

        start = review.controls
        for args, value, oid in (('1', 601, 23), ('1::float8', 6.5, 701), ('NULL', 6.5, 701), ("'2'", 6.5, 701), ('2::numeric', 6.5, 701)):
            for mode in MODES:
                wire.query(mode, 'SELECT numeric_pick(' + args + ') AS v', [value], oid)
        checked_group(start, 5*len(MODES))

        start = review.controls
        tick_statements = (b'r222_tick0', b'r222_tick1')
        for name, args, oids, values in ((tick_statements[0], '', (), ()), (tick_statements[1], '$1', (23,), (1,))):
            wire.describe(name, 'SELECT tick(' + args + ') AS v', 20, oids)
            for binary in (False, True):
                wire.bind_describe(name, 20, oids, values, False, binary)
        for args in ('', '1'):
            for mode in MODES:
                for suffix in (' FROM ' + empty, ' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0'):
                    wire.query(mode, 'SELECT tick(' + args + ') AS v' + suffix, (), 20)
                wire.query(mode, 'SELECT CASE WHEN false THEN tick(' + args + ') ELSE 7::bigint END AS v', [7], 20)
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", [1], 20)
        for name, oids, values, first_value in ((tick_statements[0], (), (), 2), (tick_statements[1], (23,), (1,), 6)):
            for binary, value in ((False, first_value), (True, first_value+2)):
                wire.execute(name, 20, [value], oids, values, False, binary)
                wire.query('simple', "SELECT nextval('" + sequence + "') AS v", [value+1], 20)
            wire.close(name)
        checked_group(start, 41)

        start = review.controls
        life = a + '.lifecycle_pick'
        moved_life = b + '.lifecycle_pick'
        name = b'r222_lifecycle'
        create(a, 'lifecycle_pick', 'p integer', 'integer', 'p+1')
        create(a, 'lifecycle_pick', 'p bigint', 'bigint', 'p+3::bigint')
        wire.describe(name, 'SELECT ' + life + '($1) AS v', 23, (23,))
        wire.execute(name, 23, [6], (23,), (5,))
        create(a, 'lifecycle_pick', 'p text', 'text', "'later'")
        wire.execute(name, 23, [6], (23,), (5,))
        command('CREATE OR REPLACE FUNCTION ' + life + '(p integer) RETURNS integer LANGUAGE SQL AS $$SELECT p+2$$', 'CREATE FUNCTION')
        wire.execute(name, 23, [7], (23,), (5,))
        def stale_result_error():
            wire.collect(('PREPARED RESULT TYPE INVALIDATION', name),
                         lambda: wire.exchange(wire.bind(name, (23,), (5,))),
                         lambda messages: ([kind for kind, _ in messages] == [b'E', b'Z'] and
                                           wire.errors(messages) == ['0A000'] and messages[-1:] == [(b'Z', b'I')]),
                         'cached plan must not change result type; no DataRow/description')
        command('DROP FUNCTION ' + life + '(integer)', 'DROP FUNCTION')
        stale_result_error()
        create(a, 'lifecycle_pick', 'p integer', 'integer', 'p+4')
        wire.execute(name, 23, [9], (23,), (5,))
        command('ALTER FUNCTION ' + life + '(integer) RENAME TO lifecycle_renamed', 'ALTER FUNCTION')
        stale_result_error()
        command('ALTER FUNCTION ' + a + '.lifecycle_renamed(integer) RENAME TO lifecycle_pick', 'ALTER FUNCTION')
        wire.execute(name, 23, [9], (23,), (5,))
        command('ALTER FUNCTION ' + life + '(integer) SET SCHEMA ' + b, 'ALTER FUNCTION')
        stale_result_error()
        command('ALTER FUNCTION ' + moved_life + '(integer) SET SCHEMA ' + a, 'ALTER FUNCTION')
        wire.execute(name, 23, [9], (23,), (5,))
        wire.close(name)
        checked_group(start, 21)

        start = review.controls
        q = lambda args: function + '(' + args + ')'
        steps = [
            ('cmd', 'CREATE FUNCTION ' + function + '(p int4) RETURNS integer LANGUAGE SQL AS $$SELECT 0$$', 'CREATE FUNCTION', '42723'),
            ('cmd', 'CREATE OR REPLACE FUNCTION ' + function + '(p integer) RETURNS integer LANGUAGE SQL AS $$SELECT 111$$', 'CREATE FUNCTION', None),
            ('read', q('1'), 111, 23, None), ('read', q("'x'::text"), 'text', 25, None),
            ('cmd', 'CREATE OR REPLACE FUNCTION ' + function + '(p integer) RETURNS text LANGUAGE SQL AS $$SELECT \'bad\'$$', 'CREATE FUNCTION', '42P13'),
            ('cmd', 'CREATE OR REPLACE FUNCTION ' + function + '(q integer) RETURNS integer LANGUAGE SQL AS $$SELECT 112$$', 'CREATE FUNCTION', '42P13'),
            ('cmd', 'CREATE OR REPLACE FUNCTION ' + function + '(p text,q text) RETURNS text LANGUAGE SQL AS $$SELECT \'tt\'$$', 'CREATE FUNCTION', None),
            ('read', q("'a'::text,'b'::text"), 'tt', 25, None), ('read', q("1,'b'"), 'inttext', 25, None),
            ('cmd', 'DROP FUNCTION ' + q('text,text'), 'DROP FUNCTION', None),
            ('read', q("'a'::text,'b'::text"), None, 25, '42883'),
            ('cmd', 'DROP FUNCTION ' + function, 'DROP FUNCTION', '42725'),
            ('cmd', 'DROP FUNCTION ' + q('bytea'), 'DROP FUNCTION', '42883'),
            ('cmd_notice', 'DROP FUNCTION IF EXISTS ' + q('bytea'), 'DROP FUNCTION', None),
            ('cmd', 'BEGIN', 'BEGIN', None),
            ('cmd', 'CREATE FUNCTION ' + function + '(p double precision) RETURNS double precision LANGUAGE SQL AS $$SELECT 11.5::float8$$', 'CREATE FUNCTION', None),
            ('read', q('1::float8'), 11.5, 701, None), ('cmd', 'ROLLBACK', 'ROLLBACK', None),
            ('read', q('1::float8'), None, 701, '42883'), ('read', q('1'), 111, 23, None),
            ('cmd', 'BEGIN', 'BEGIN', None),
            ('cmd', 'CREATE FUNCTION ' + function + '(p integer,q bigint) RETURNS integer LANGUAGE SQL AS $$SELECT 211$$', 'CREATE FUNCTION', None),
            ('cmd', 'SAVEPOINT r222_before_second', 'SAVEPOINT', None),
            ('cmd', 'CREATE FUNCTION ' + function + '(p bigint,q integer) RETURNS integer LANGUAGE SQL AS $$SELECT 212$$', 'CREATE FUNCTION', None),
            ('cmd', 'ROLLBACK TO SAVEPOINT r222_before_second', 'ROLLBACK', None),
            ('read', q('1::integer,2::bigint'), 211, 23, None), ('cmd', 'ROLLBACK', 'ROLLBACK', None),
            ('read', q('1::integer,2::bigint'), None, 23, '42883'), ('read', q('1'), 111, 23, None),
            ('cmd', 'BEGIN', 'BEGIN', None),
            ('cmd', 'CREATE OR REPLACE FUNCTION ' + function + '(p integer) RETURNS integer LANGUAGE SQL AS $$SELECT 121$$', 'CREATE FUNCTION', None),
            ('read', q('1'), 121, 23, None), ('cmd', 'ROLLBACK', 'ROLLBACK', None), ('read', q('1'), 111, 23, None),
            ('cmd', 'BEGIN', 'BEGIN', None), ('cmd', 'DROP FUNCTION ' + q('integer'), 'DROP FUNCTION', None),
            ('read', q('1'), 10000000003, 20, None), ('cmd', 'ROLLBACK', 'ROLLBACK', None), ('read', q('1'), 111, 23, None),
            ('cmd', 'DROP FUNCTION ' + q('integer') + ',' + q('text'), 'DROP FUNCTION', None),
            ('read', q('1'), 10000000003, 20, None), ('read', q("'x'::text"), None, 25, '42883'), ('read', q('1,2'), 201, 23, None),
            ('cmd', 'CREATE FUNCTION ' + function + '(p integer) RETURNS integer LANGUAGE SQL AS $$SELECT 111$$', 'CREATE FUNCTION', None),
            ('cmd', 'CREATE FUNCTION ' + function + '(p text) RETURNS text LANGUAGE SQL AS $$SELECT \'text\'$$', 'CREATE FUNCTION', None),
            ('cmd', 'DROP FUNCTION ' + q('int4') + ',' + q('missing_r222_type'), 'DROP FUNCTION', '42704'),
            ('read', q('1'), 111, 23, None), ('read', q("'x'::text"), 'text', 25, None),
            ('cmd', 'ALTER FUNCTION ' + q('integer') + ' RENAME TO renamed_pick', 'ALTER FUNCTION', None),
            ('read', a + '.renamed_pick(1)', 111, 23, None), ('read', q('1'), 10000000003, 20, None),
            ('read', q("'x'::text"), 'text', 25, None),
            ('cmd', 'ALTER FUNCTION ' + a + '.renamed_pick(integer) SET SCHEMA ' + b, 'ALTER FUNCTION', None),
            ('read', b + '.renamed_pick(1)', 111, 23, None), ('read', a + '.renamed_pick(1)', None, 23, '42883'),
            ('cmd', 'ALTER FUNCTION ' + b + '.renamed_pick(integer) RENAME TO pick', 'ALTER FUNCTION', '42723'),
            ('read', b + '.renamed_pick(1)', 111, 23, None), ('read', b + '.pick(1)', 999, 23, None),
            ('cmd', 'ALTER FUNCTION ' + b + '.renamed_pick(integer) SET SCHEMA ' + a, 'ALTER FUNCTION', None),
            ('cmd', 'ALTER FUNCTION ' + a + '.renamed_pick(integer) RENAME TO pick', 'ALTER FUNCTION', None),
            ('read', q('1'), 111, 23, None), ('read', b + '.pick(1)', 999, 23, None),
            ('cmd', 'DROP FUNCTION ' + q(''), 'DROP FUNCTION', None), ('read', q(''), None, 23, '42883'),
            ('read', q('1'), 111, 23, None), ('cmd', 'DROP ROUTINE ' + q('integer'), 'DROP ROUTINE', None),
            ('read', q('1'), 10000000003, 20, None), ('read', q("'x'::text"), 'text', 25, None),
        ]
        in_transaction = False
        # Wire.result_ok's ordinary-query receiver is intentionally exact about
        # ReadyForQuery. Transactional lifecycle reads use a local validator
        # that first checks the actual T status, then validates the same full
        # message bodies with only that framing byte normalized.
        def transaction_query(mode, sql, values, oid):
            binary = mode == 'binary'
            producer = (lambda: wire.client.simple_query(wire.sock, sql)) if mode == 'simple' else (
                lambda: wire.exchange(wire.parse(b'', sql) + wire.bind(b'', binary_output=binary)))
            def valid(messages):
                return (messages[-1:] == [(b'Z', b'T')] and
                        wire.result_ok(messages[:-1]+[(b'Z', b'I')], values, oid, binary=binary,
                                       parsed=mode != 'simple', bound=mode != 'simple', simple=mode == 'simple'))
            wire.collect(('TRANSACTION', mode, sql), producer, valid, (values, oid, b'T'))
        for step in steps:
            if step[0] == 'read':
                _, expression, value, oid, state = step
                assert not (in_transaction and state), 'error transaction needs explicit E-state receiver'
                for mode in MODES:
                    sql = 'SELECT ' + expression + ' AS v'
                    if in_transaction:
                        transaction_query(mode, sql, [value], oid)
                    else:
                        wire.query(mode, sql, [value], oid, state)
            else:
                kind, sql, tag, state = step
                if sql == 'BEGIN':
                    in_transaction = True
                elif sql == 'ROLLBACK':
                    in_transaction = False
                command(sql, tag, state, b'T' if in_transaction else b'I', notices=kind == 'cmd_notice')
        checked_group(start, sum(3 if step[0] == 'read' else 1 for step in steps))
    finally:
        start = review.controls
        command('ROLLBACK', 'ROLLBACK', notices=True)
        command('SET search_path=public', 'SET', path='public')
        for name in reversed(owned):
            command('DROP SCHEMA ' + identifier(name) + ' CASCADE', 'DROP SCHEMA', notices=True)
        checked_group(start, 2 + len(owned))
        review.close()
    review.complete(planned)


if __name__ == '__main__':
    main()
