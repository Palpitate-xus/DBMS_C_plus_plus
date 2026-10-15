#!/usr/bin/env python3
"""Finite MOD result domains; SQL rejection notes do not constrain native APIs."""
import argparse
import importlib.util
from pathlib import Path
import socket
import sys

sys.dont_write_bytecode = True
TYPES = (('smallint', 21), ('integer', 23), ('bigint', 20), ('numeric', 1700))


def result_domain(left, right):
    if left == 'numeric' or right == 'numeric':
        return 'numeric', 1700
    rank = {'smallint': 0, 'integer': 1, 'bigint': 2}
    return TYPES[max(rank[left], rank[right])]


def controls():
    matrix, pairs = [], []
    for left, _ in TYPES:
        for right, _ in TYPES:
            _, oid = result_domain(left, right)
            for null_left, null_right in ((False, False), (True, False), (False, True), (True, True)):
                expression = 'mod(' + ('NULL' if null_left else '9') + '::' + left + ','
                expression += ('NULL' if null_right else '4') + '::' + right + ')'
                matrix.append((expression, None if null_left or null_right else '1', oid))
            pairs.append(('mod(9::' + left + ',4::' + right + ')', '1', oid))
    boundary = [
        ('pg_catalog.mod(5000000000::bigint,6000000000::bigint)', '5000000000', 20),
        ('mod(5000000000::bigint,6000000000::bigint)/2', '2500000000', 20),
    ]
    for declaration, oid in TYPES:
        for left, right, value in (('-10', '3', '-1'), ('10', '-3', '1'), ('-10', '-3', '-1')):
            boundary.append(('mod(' + left + '::' + declaration + ',' + right + '::' + declaration + ')', value, oid))
    for declaration, minimum, oid in (('smallint', '-32768', 21), ('integer', '-2147483648', 23),
                                      ('bigint', '-9223372036854775808', 20)):
        boundary.append(("mod('" + minimum + "'::" + declaration + ',-1::' + declaration + ')', '0', oid))
    boundary.extend((
        ('mod(100000000000000000000001::numeric,3::integer)', '2', 1700),
        ('mod(10.50::numeric,3.0::numeric)', '1.50', 1700),
    ))
    special = []
    for left, right, value in (
        ('NaN', '0', 'NaN'), ('0', 'NaN', 'NaN'), ('NaN', 'Infinity', 'NaN'),
        ('Infinity', '2', 'NaN'), ('-Infinity', '2', 'NaN'), ('Infinity', 'Infinity', 'NaN'),
        ('2.00', 'Infinity', '2.00'), ('-2.00', 'Infinity', '-2.00'),
        ('2.00', '-Infinity', '2.00'),
    ):
        special.append(("mod('" + left + "'::numeric,'" + right + "'::numeric)", value, 1700))
    special.append(("mod(NULL::numeric,'NaN'::numeric)", None, 1700))
    errors = [('mod(9::' + declaration + ',0::' + declaration + ')', '22012') for declaration, _ in TYPES]
    errors.append(("mod('Infinity'::numeric,0::numeric)", '22012'))
    return matrix, pairs, boundary, special, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference18', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('mod_result_domain_runner', root / 'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r); c = r.load_protocol_client()
    server = None
    if args.reference18:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password=password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    checks, scalar_controls, failures = 0, 0, []

    def group(label, cases, tail=''):
        nonlocal checks, scalar_controls
        sql = 'SELECT ' + ','.join(item[0] for item in cases) + tail + ';'
        messages = c.simple_query(sock, sql)
        actual = r.decode_wire_result(messages, include_types=True)
        rows = [] if tail else [[item[1] for item in cases]]
        good = actual[1] is None and actual[0] == rows and actual[5] == [item[2] for item in cases]
        good = good and actual[4] == 'SELECT ' + str(len(rows)) and messages[-1] == (b'Z', b'I')
        checks += 1; scalar_controls += len(cases)
        print('MOD_DOMAIN', label, 'SQL', sql, 'RESULT', actual, 'PASS', good, flush=True)
        if not good: failures.append((label, actual, rows))

    try:
        matrix, pairs, boundary, special, errors = controls()
        group('typed_and_NULL_matrix', matrix)
        group('negative_width_and_bigint_chain', boundary)
        group('numeric_nonfinite', special)
        group('empty_source_metadata', pairs, ' WHERE false')
        group('limit_zero_metadata', pairs, ' LIMIT 0')
        for expression, state in errors:
            sql = 'SELECT ' + expression + ';'
            messages = c.simple_query(sock, sql)
            actual = r.decode_wire_result(messages, include_types=True)
            good = actual[1] == state and not actual[0] and messages[-1] == (b'Z', b'I')
            checks += 1; scalar_controls += 1
            print('MOD_DOMAIN_ERROR', sql, actual, 'EXPECTED', state, 'PASS', good, flush=True)
            if not good: failures.append((expression, actual, state))
        if args.reference18:
            # Evidence only: do not copy these SQL rejection expectations into
            # native API assertions or expand this result-domain-only repair.
            for sql in (
                "SELECT oidvectortypes(p.proargtypes),format_type(p.prorettype,NULL),p.proisstrict FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pg_catalog' AND p.proname='mod' ORDER BY p.prorettype;",
                'SELECT mod(10.5::double precision,3.0::double precision);',
                'SELECT mod(10.5::real,3.0::real);',
                "SELECT mod('10'::text,'3'::text);",
            ):
                messages = c.simple_query(sock, sql)
                print('MOD_REFERENCE_DECLARATION_NOTE', sql, r.decode_wire_result(messages, include_types=True), flush=True)
        print('MOD_DOMAIN_COMPLETE queries=', checks, 'scalar_controls=', scalar_controls,
              'failures=', len(failures), flush=True)
        assert checks == 10 and scalar_controls == 130 and not failures, failures
    finally:
        if server: r.stop_ours(server)
        else: sock.close()


if __name__ == '__main__':
    main()
