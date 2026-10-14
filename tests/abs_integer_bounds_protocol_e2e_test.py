#!/usr/bin/env python3
"""ABS uses its resolved integer width and pure constant-plan demand."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('abs_runner', root / 'tests/compat/pg_diff_runner.py')
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
    schema = 'abs_bounds_' + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(sql, rows=None, state=None, oids=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and not result[0] and result[4] is None
        checks += 1
        print('ABS_INTEGER_BOUNDS', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oids))
        return valid

    try:
        assert check('CREATE SCHEMA ' + schema)
        created = True
        assert check('CREATE TABLE ' + schema + '.empty_rows(i INT)')
        assert check('CREATE SEQUENCE ' + schema + '.effects')
        assert check('CREATE FUNCTION ' + schema + '.effect() RETURNS BIGINT LANGUAGE SQL VOLATILE AS $$SELECT nextval(\'' + schema + '.effects\')$$')
        for argument in ("'-32768'::smallint", '-2147483648', "'-9223372036854775808'::bigint"):
            expression = 'abs(' + argument + ')'
            for suffix in ('', ' WHERE false', ' LIMIT 0',
                           ' FROM ' + schema + '.empty_rows',
                           ' FROM ' + schema + '.empty_rows WHERE false',
                           ' FROM ' + schema + '.empty_rows LIMIT 0'):
                check('SELECT ' + expression + suffix, state='22003')
            check('SELECT CASE WHEN i>0 THEN ' + expression + ' ELSE 1 END FROM ' + schema + '.empty_rows', state='22003')
            check('SELECT CASE WHEN false THEN ' + expression + ' ELSE 1 END', rows=[['1']])
            check('SELECT COALESCE(1,' + expression + ')', rows=[['1']])
        for expression, value, oid in (
                ("abs('-32767'::smallint)", '32767', 21),
                ('abs(-2147483647)', '2147483647', 23),
                ("abs('-9223372036854775807'::bigint)", '9223372036854775807', 20),
                ('abs(-2147483649)', '2147483649', 20),
                ('abs(-9223372036854775808::numeric)', '9223372036854775808', 1700),
                ('abs(NULL::smallint)', None, 21), ('abs(NULL::integer)', None, 23),
                ('abs(NULL::bigint)', None, 20), ('abs(-1.25::numeric)', '1.25', 1700),
                ('abs(abs(-7))', '7', 23)):
            check('SELECT ' + expression, rows=[[value]], oids=[oid])
            check('SELECT ' + expression + ' FROM ' + schema + '.empty_rows LIMIT 0', rows=[], oids=[oid])
        check('PREPARE abs_constant AS SELECT abs(-2147483648)')
        check('EXECUTE abs_constant', state='22003')
        check('DEALLOCATE abs_constant')
        assert check('PREPARE abs_parameter(INT) AS SELECT abs($1)')
        check('EXECUTE abs_parameter(-2147483648)', state='22003')
        check('EXECUTE abs_parameter(-7)', rows=[['7']], oids=[23])
        check('DEALLOCATE abs_parameter')
        check('SELECT abs(' + schema + '.effect()) FROM ' + schema + '.empty_rows LIMIT 0', rows=[], oids=[20])
        check('SELECT abs((SELECT nextval(\'' + schema + '.effects\'))) LIMIT 0', rows=[], oids=[20])
        check("SELECT nextval('" + schema + ".effects')", rows=[['1']], oids=[20])
        assert check('CREATE FUNCTION ' + schema + '.abs(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i$$')
        assert check('SET search_path=' + schema + ',pg_catalog')
        check('SELECT abs(-2147483648)', rows=[['-2147483648']], oids=[23])
        check('SELECT abs(-2147483648) FROM empty_rows LIMIT 0', rows=[], oids=[23])
        check('SELECT pg_catalog.abs(-2147483648)', state='22003')
        check('SELECT 1', rows=[['1']], oids=[23])
        print('ABS_INTEGER_BOUNDS_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 67 and not failures, failures
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
