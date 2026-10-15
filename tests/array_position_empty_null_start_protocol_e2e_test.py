#!/usr/bin/env python3
"""ARRAY_POSITION empty arrays return INTEGER NULL for a NULL search start."""
import argparse
import importlib.util
from pathlib import Path
import socket
import sys

# Reference mode performs SELECT-only checks and does not create test data.
sys.dont_write_bytecode = True


def controls():
    domains = (('integer', '1::integer'),
               ('text', "'needle'::text"), ('numeric', '1::numeric'))
    empty_null = []
    empty_legal = []
    null_arrays = []
    errors = []
    for declaration, needle in domains:
        for array in ("'{}'::" + declaration + '[]',
                      'ARRAY[]::' + declaration + '[]'):
            for search in (needle, 'NULL::' + declaration):
                empty_null.append('array_position(' + array + ',' + search + ',NULL::integer)')
            empty_legal.extend((
                'array_position(' + array + ',' + needle + ')',
                'array_position(' + array + ',' + needle + ',1)',
                'array_position(' + array + ',NULL::' + declaration + ",-2147483648)"))
            errors.append(('array_position(' + array + ',' + needle + ",'bad')", '22P02'))
        for search in (needle, 'NULL::' + declaration):
            null_arrays.extend((
                'array_position(NULL::' + declaration + '[],' + search + ',NULL::integer)',
                'array_position(NULL::' + declaration + '[],' + search + ',1)'))
        array = {'integer': 'ARRAY[1]', 'text': "ARRAY['needle'::text]",
                 'numeric': 'ARRAY[1::numeric]'}[declaration]
        for search in (needle, 'NULL::' + declaration):
            # A NULL needle and no NULL bitmap is a different preexisting
            # shortcut: PG returns NULL before validating a NULL start. Use
            # an array containing NULL here so 22004 is genuinely required.
            source = array if search == needle else array[:-1] + ',NULL]'
            errors.append(('array_position(' + source + ',' + search + ',NULL::integer)', '22004'))

    positions = (
        ('array_position(ARRAY[1,NULL,1,3],1)', '1'),
        ('array_position(ARRAY[1,NULL,1,3],1,2)', '3'),
        ('array_position(ARRAY[1,NULL,1,3],1,4)', None),
        ('array_position(ARRAY[1,NULL,1,3],NULL,1)', '2'),
        ('array_position(ARRAY[1,NULL,1,3],NULL,3)', None),
        ('array_position(ARRAY[NULL,NULL]::integer[],NULL,2)', '2'),
        ("array_position('[0:3]={1,NULL,1,3}'::integer[],1,0)", '0'),
        ("array_position('[0:3]={1,NULL,1,3}'::integer[],1,1)", '2'),
        ("array_position('[0:3]={1,NULL,1,3}'::integer[],NULL,0)", '1'),
        ("array_position('[-3:-1]={7,NULL,7}'::integer[],7,-99)", '-3'),
        ("array_position('[-3:-1]={7,NULL,7}'::integer[],7,-2)", '-1'),
        ("array_position('[-3:-1]={7,NULL,7}'::integer[],7,1)", None),
        ("array_position('[-3:-1]={7,NULL,7}'::integer[],NULL,-3)", '-2'),
        ("array_position('{\"NULL\",NULL,\"\"}'::text[],'NULL'::text)", '1'),
        ("array_position('{\"NULL\",NULL,\"\"}'::text[],NULL::text)", '2'),
        ("array_position('{\"NULL\",NULL,\"\"}'::text[],''::text)", '3'),
        ("array_position('{1,NULL,2}'::numeric[],2::numeric)", '3'),
        ("array_position('{1,NULL,2}'::numeric[],NULL::numeric)", '2'),
        ('array_position(ARRAY[1],1,-2147483648)', '1'),
        ('array_position(ARRAY[1],1,2147483647)', None),
        ("array_position(ARRAY[1,NULL,1],1,'2')", '3'),
        ("pg_catalog.array_position('{}'::integer[],1,NULL::integer)", None),
    )
    errors.extend((
        ("array_position('{}'::integer[],1,'2147483648')", '22003'),
        ("array_position(ARRAY[]::integer[],1,'-2147483649')", '22003'),
        ("array_position('{}'::integer[],1,'bad'::integer)", '22P02'),
        ("array_position(ARRAY[ARRAY[1,2]],1,NULL::integer)", '0A000'),
    ))
    return (
        ('empty_null_start', empty_null, [None] * len(empty_null)),
        ('empty_legal_start', empty_legal, [None] * len(empty_legal)),
        ('null_arrays', null_arrays, [None] * len(null_arrays)),
        ('positions_bounds_null_elements', [p[0] for p in positions], [p[1] for p in positions]),
    ), errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference18', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('array_position_null_start_runner',
                                                root / 'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    server = None
    if args.reference18:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password=password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server['sock']
    checks, scalar_controls, failures = 0, 0, []
    try:
        groups, errors = controls()
        for label, expressions, expected in groups:
            sql = 'SELECT ' + ','.join(expressions) + ';'
            messages = c.simple_query(sock, sql)
            actual = r.decode_wire_result(messages, include_types=True)
            good = actual[1] is None and actual[0] == [expected]
            good = good and actual[5] == [23] * len(expressions)
            good = good and actual[4] == 'SELECT 1' and messages[-1] == (b'Z', b'I')
            checks += 1
            scalar_controls += len(expressions)
            print('ARRAY_POSITION_EMPTY_NULL_START', label, 'SQL', sql,
                  'RESULT', actual, 'PASS', good, flush=True)
            if not good:
                failures.append((label, actual, expected))
        for expression, state in errors:
            sql = 'SELECT ' + expression + ';'
            messages = c.simple_query(sock, sql)
            actual = r.decode_wire_result(messages, include_types=True)
            good = actual[1] == state and not actual[0]
            good = good and messages[-1] == (b'Z', b'I')
            checks += 1
            scalar_controls += 1
            print('ARRAY_POSITION_EMPTY_NULL_START_ERROR', 'SQL', sql,
                  'RESULT', actual, 'EXPECTED', state, 'PASS', good, flush=True)
            if not good:
                failures.append((expression, actual, state))
        print('ARRAY_POSITION_EMPTY_NULL_START_COMPLETE queries=', checks,
              'scalar_controls=', scalar_controls, 'failures=', len(failures), flush=True)
        assert checks == 20 and scalar_controls == 80 and not failures, failures
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
