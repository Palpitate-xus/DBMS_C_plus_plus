#!/usr/bin/env python3
"""Single-argument ROUND keeps numeric/float8 output declarations without rows."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'round_result_type_runner', root / 'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h, p, u, d, pw = r._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=r.wire_timeout())
        c.startup_reference(sock, u, d, pw)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server['sock']
    schema = 'round_result_type_' + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(sql, rows=None, oids=None):
        nonlocal checks
        messages = c.simple_query(sock, sql)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] is None and messages[-1] == (b'Z', b'I')
        good = good and not any(kind == b'E' for kind, _ in messages)
        if rows is not None:
            good = good and result[0] == rows
            good = good and result[4] == 'SELECT ' + str(len(rows))
            good = good and sum(kind == b'T' for kind, _ in messages) == 1
            good = good and sum(kind == b'D' for kind, _ in messages) == len(rows)
        if oids is not None:
            good = good and result[5] == oids
        checks += 1
        print('ROUND_RESULT_TYPE', sql, result, 'PASS', good, flush=True)
        if not good:
            failures.append((sql, result, rows, oids))
        return good

    def descriptor(expression, tail, value, oid):
        check('SELECT ' + expression + tail,
              rows=[] if tail else [[value]], oids=[oid])

    inputs = (
        ('smallint', 'i2', False), ('integer', 'i4', False),
        ('bigint', 'i8', False), ('real', 'r4', False),
        ('double precision', 'r8', False),
        ('numeric', 'n', True), ('decimal', 'd', True))
    functions = ('round',)
    literal_tails = ('', ' WHERE false', ' LIMIT 0')
    table_tails = (' FROM ' + schema + '.empty_rows WHERE false',
                   ' FROM ' + schema + '.empty_rows LIMIT 0')
    try:
        assert check('CREATE SCHEMA ' + schema)
        created = True
        assert check('CREATE TABLE ' + schema + '.empty_rows('
                     'i2 SMALLINT,i4 INTEGER,i8 BIGINT,r4 REAL,'
                     'r8 DOUBLE PRECISION,n NUMERIC,d DECIMAL)')
        for callee in functions:
            rounded_fraction = '1'
            for declaration, column, numeric in inputs:
                oid = 1700 if numeric else 701
                for tail in literal_tails:
                    descriptor(callee + '(1::' + declaration + ')', tail, '1', oid)
                    descriptor(callee + '(NULL::' + declaration + ')', tail, None, oid)
                for tail in table_tails:
                    descriptor(callee + '(' + column + ')', tail, None, oid)
            for argument, result in (("'1.25'", rounded_fraction), ('NULL', None)):
                for tail in literal_tails:
                    descriptor(callee + '(' + argument + ')', tail, result, 701)
            for tail in literal_tails:
                descriptor('abs(' + callee + '(1::integer))', tail, '1', 701)
                descriptor(callee + '(' + callee + '(1::smallint))', tail, '1', 701)
                descriptor('abs(' + callee + '(1.25::numeric))', tail,
                           rounded_fraction, 1700)
                descriptor(callee + '(' + callee + '(1.25::decimal))', tail,
                           rounded_fraction, 1700)
            for tail in table_tails:
                descriptor('abs(' + callee + '(i4))', tail, None, 701)
                descriptor('abs(' + callee + '(n))', tail, None, 1700)
                descriptor('pg_catalog.' + callee + '(i4)', tail, None, 701)
                descriptor('pg_catalog.' + callee + '(n)', tail, None, 1700)
        for callee in functions:
            assert check('CREATE FUNCTION ' + schema + '.' + callee +
                         '(i INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE '
                         'AS $$SELECT i+10$$')
            for tail in literal_tails:
                descriptor(schema + '.' + callee + '(1)', tail, '11', 23)
            for tail in table_tails:
                descriptor(schema + '.' + callee + '(i4)', tail, None, 23)
        check("SELECT round(2.5::double precision),round(-2.5::double precision),"
              "round(2.5::numeric),round(-2.5::numeric),"
              "round('NaN'::double precision),round('NaN'::numeric),"
              "round('Infinity'::double precision),round('-Infinity'::double precision)",
              rows=[['2', '-2', '3', '-3', 'NaN', 'NaN', 'Infinity', '-Infinity']],
              oids=[701, 701, 1700, 1700, 701, 1700, 701, 701])
        print('ROUND_RESULT_TYPE_COMPLETE', checks,
              'FAILURES', len(failures), flush=True)
        assert checks == 91 and not failures, failures
    finally:
        if created:
            c.simple_query(sock, 'DROP SCHEMA ' + schema + ' CASCADE')
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
