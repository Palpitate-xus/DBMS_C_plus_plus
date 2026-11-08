#!/usr/bin/env python3
"""Simple-query float8/numeric transcendental overloads against PG 18."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('pg_diff_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    schema = 'transcendental_' + uuid.uuid4().hex[:12]
    table = 'float_default_' + uuid.uuid4().hex[:12]
    failures = []
    checks = 0

    def check(sql, rows=None, oids=None, state=None, ready=b'I'):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        good = result[1] == state and (rows is None or result[0] == rows)
        good = good and (oids is None or result[5] == oids)
        good = good and messages[-1] == (b'Z', ready)
        if state:
            good = good and not result[0] and sum(k == b'E' for k, _ in messages) == 1
        checks += 1
        if not good: failures.append((sql, result, rows, oids, state))

    try:
        for fn in ('exp', 'ln', 'log', 'log10'):
            float_value = '2.718281828459045' if fn == 'exp' else '0'
            for arg in ('1::smallint', '1::integer', '1::bigint', '1::real',
                        '1::double precision', "'1'"):
                check('SELECT ' + fn + '(' + arg + ')', [[float_value]], [701])
            for arg, oid in (('NULL', 701), ('NULL::real', 701),
                             ('NULL::double precision', 701), ('NULL::numeric', 1700)):
                check('SELECT ' + fn + '(' + arg + ')', [[None]], [oid])
            for arg in ("'1'::text", 'true', 'NULL::text'):
                check('SELECT ' + fn + '(' + arg + ')', state='42883')
            for arg in ('', '1,2,3'):
                check('SELECT ' + fn + '(' + arg + ')', state='42883')
            if fn != 'log':
                check('SELECT ' + fn + '(1,2)', state='42883')
            check('SELECT ' + fn + "('invalid')", state='22P02')
            for arg, expected in (("'NaN'::double precision", 'NaN'),
                                  ("'Infinity'::double precision", 'Infinity')):
                check('SELECT ' + fn + '(' + arg + ')', [[expected]], [701])
            check('SELECT pg_typeof(' + fn + '(1))::text', [['double precision']], [25])
            check('SELECT pg_typeof(' + fn + '(1::numeric))::text', [['numeric']], [25])
        for fn, expected in (('exp', '1.1051709197224806'),
                             ('ln', '-2.3025850780928847'),
                             ('log', '-0.999999993528508'),
                             ('log10', '-0.999999993528508')):
            check('SELECT ' + fn + '(0.1::real)', [[expected]], [701])
        check('SELECT exp(-40::integer)', [['4.248354255291589e-18']], [701])
        check('SELECT exp(-1::double precision)', [['0.36787944117144233']], [701])
        check("SELECT exp('-Infinity'::double precision)", [['0']], [701])
        check('SELECT exp(-1000::double precision)', state='22003')
        check('SELECT exp(710::double precision)', state='22003')
        for fn in ('ln', 'log', 'log10'):
            for arg in ('0::double precision', '-1::double precision', "'-Infinity'::double precision"):
                check('SELECT ' + fn + '(' + arg + ')', state='2201E')
        check('SELECT log10(100::double precision)+1::double precision', [['3']], [701])
        check('SELECT log(2::integer,8::integer)', [['3.0000000000000000']], [1700])
        check('SELECT log(2::double precision,8::double precision)', state='42883')
        check('CREATE TABLE ' + table + '(id INT, a DOUBLE PRECISION DEFAULT exp(-40::integer), b DOUBLE PRECISION DEFAULT log10(100::double precision))')
        check('INSERT INTO ' + table + '(id) VALUES(1)')
        check('SELECT id,a,b FROM ' + table + ' ORDER BY id', [['1', '4.248354255291589e-18', '2']], [23, 701, 701])
        check('BEGIN', ready=b'T')
        check('INSERT INTO ' + table + '(id) VALUES(2)', ready=b'T')
        check('SELECT id,a,b FROM ' + table + ' ORDER BY id', [['1', '4.248354255291589e-18', '2'], ['2', '4.248354255291589e-18', '2']], [23, 701, 701], ready=b'T')
        check('ROLLBACK')
        check('SELECT id,a,b FROM ' + table + ' ORDER BY id', [['1', '4.248354255291589e-18', '2']], [23, 701, 701])
        check('DROP TABLE ' + table)
        check('CREATE SCHEMA ' + schema)
        for fn in ('exp', 'ln', 'log', 'log10'):
            check('CREATE FUNCTION ' + schema + '.' + fn + '(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$')
        check('SET search_path=pg_catalog,' + schema)
        for fn in ('exp', 'ln', 'log', 'log10'):
            check('SELECT ' + fn + '(1)', [['11']], [23])
            check('SELECT ' + schema + '.' + fn + '(1)', [['11']], [23])
        check('SELECT pg_catalog.exp(1)', [['2.718281828459045']], [701])
        check('RESET search_path')
        check('DROP SCHEMA ' + schema + ' CASCADE')
        print('FLOAT_OVERLOAD_SIMPLE_WIRE', checks, 'FAILURES', len(failures), flush=True)
        assert not failures, failures
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == '__main__':
    main()
