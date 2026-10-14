#!/usr/bin/env python3
"""Pure overload analysis must not depend on row/branch demand."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('signature_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    schema = 'signature_' + uuid.uuid4().hex[:12]
    failures, checks, created = [], 0, False

    def check(sql, rows=None, state=None, oids=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and not result[0] and result[4] is None
        checks += 1
        print('SCALAR_SIGNATURE', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oids))
        return valid

    try:
        assert check('CREATE SCHEMA ' + schema)
        created = True
        assert check('CREATE TABLE ' + schema + '.empty_rows(i INT,t TEXT)')
        invalid = ('upper(1)', 'upper()', "upper('x'::text,'y'::text)",
                   "abs('x'::text)", 'abs()', 'abs(1,2)', 'length(1)', 'length()',
                   "length('a'::text,'UTF8'::name)", 'lower(true)')
        for expression in invalid:
            for suffix in ('', ' FROM ' + schema + '.empty_rows WHERE false',
                           ' FROM ' + schema + '.empty_rows LIMIT 0'):
                check('SELECT ' + expression + suffix, state='42883')
                check('SELECT ' + expression + ' OVER()' + suffix, state='42883')
        for expression in ('upper(i)', 'length(i)', 'abs(t)'):
            for suffix in (' WHERE false', ' LIMIT 0'):
                check('SELECT ' + expression + ' FROM ' + schema + '.empty_rows' + suffix, state='42883')
        for suffix in ('', ' FROM ' + schema + '.empty_rows WHERE false',
                       ' FROM ' + schema + '.empty_rows LIMIT 0'):
            check("SELECT abs('bad')" + suffix, state='22P02')
            check("SELECT abs('bad') OVER()" + suffix, state='42809')
        for sql in ("SELECT CASE WHEN false THEN upper(1) ELSE 'kept' END",
                    'SELECT COALESCE(1,length(1))',
                    'SELECT 1 WHERE false AND upper(1)=\'1\'',
                    'SELECT upper(missing) FROM ' + schema + '.empty_rows WHERE false'):
            check(sql, state='42703' if 'missing' in sql else '42883')
        for expression, value, oid in (
                ("upper('abc')", 'ABC', 25), ("lower('ABC'::varchar)", 'abc', 25),
                ("upper(NULL)", None, 25), ('abs(7)', '7', 23),
                ("abs('-7'::smallint)", '7', 21), ("abs('-7'::bigint)", '7', 20),
                ('abs(1.25::numeric)', '1.25', 1700), ('abs(NULL)', None, 701),
                ("abs('7')", '7', 701), ("length('abc')", '3', 23),
                ("length('x  '::character(3))", '1', 23),
                ("length(B'101')", '3', 23), ("length('é'::text)", '1', 23),
                ("length('abc'::bytea)", '3', 23),
                ("length('abc'::bytea,'UTF8'::name)", '3', 23),
                ("upper('[1,4)'::int4range)", '4', 23),
                ("lower('[1,4)'::int8range)", '1', 20),
                ("length('[(0,0),(3,4)]'::lseg)", '5', 701),
                ("length('[(0,0),(3,4)]'::path)", '5', 701),
                ("length('((0,0),(3,4))'::path)", '10', 701),
                ("length('a:1 b:2'::tsvector)", '2', 23)):
            check('SELECT ' + expression, rows=[[value]], oids=[oid])
            check('SELECT ' + expression + ' FROM ' + schema + '.empty_rows LIMIT 0', rows=[], oids=[oid])
        assert check('CREATE FUNCTION ' + schema + '.upper(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$')
        assert check('SET search_path=pg_catalog,' + schema)
        check('SELECT upper(1)', rows=[['11']], oids=[23])
        check("SELECT upper('abc'::text)", rows=[['ABC']], oids=[25])
        check('SELECT upper(1) OVER()', state='42809')
        check('SELECT pg_catalog.upper(1)', state='42883')
        check('SELECT ' + schema + ".upper('abc'::text)", state='42883')
        check('SELECT ' + schema + '.upper(1)', rows=[['11']], oids=[23])
        check('PREPARE signature_bad(INT) AS SELECT pg_catalog.upper($1)', state='42883')
        assert check('PREPARE signature_good(TEXT) AS SELECT pg_catalog.upper($1)')
        check("EXECUTE signature_good('abc')", rows=[['ABC']], oids=[25])
        check('DEALLOCATE signature_good')
        print('SCALAR_SIGNATURE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 132 and not failures, failures
    finally:
        if created:
            client.simple_query(sock, 'RESET search_path')
            client.simple_query(sock, 'DROP SCHEMA ' + schema + ' CASCADE')
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
