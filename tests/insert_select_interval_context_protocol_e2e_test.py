#!/usr/bin/env python3
"""UNKNOWN INSERT SELECT input must be parsed in the target interval range."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('interval_source_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def check(sql, rows=None, state=None, tag=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[3] == ['id', 'v'] and result[5] == [23, 1186]
        if tag:
            valid = valid and result[4] == tag
        if state:
            valid = valid and result[0] == [] and result[4] is None
        checks += 1
        print('INTERVAL_SOURCE', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, tag))
        return valid

    try:
        for ordinal, (suffix, unknown) in enumerate([
                ('YEAR', '4 years'), ('MONTH', '4 mons'), ('DAY', '4 days'),
                ('HOUR', '04:00:00'), ('MINUTE', '00:04:00'), ('SECOND(2)', '00:00:04'),
                ('YEAR TO MONTH', '4 mons'), ('DAY TO HOUR', '04:00:00'),
                ('DAY TO MINUTE', '00:04:00'), ('DAY TO SECOND(2)', '00:00:04'),
                ('HOUR TO MINUTE', '00:04:00'), ('HOUR TO SECOND(2)', '00:00:04'),
                ('MINUTE TO SECOND(2)', '00:00:04')]):
            table = 'interval_source_' + str(ordinal)
            assert check('CREATE TEMP TABLE ' + table + '(id INT,v INTERVAL ' + suffix + ')')
            check('INSERT INTO ' + table + " SELECT 1,'4'", tag='INSERT 0 1')
            check('INSERT INTO ' + table + " SELECT 2,INTERVAL '4 seconds'", tag='INSERT 0 1')
            check('INSERT INTO ' + table + ' SELECT 3,NULL FROM (SELECT 1) q', tag='INSERT 0 1')
            typed = '00:00:04' if 'SECOND' in suffix else '00:00:00'
            check('SELECT id,v FROM ' + table + ' ORDER BY id',
                  rows=[['1', unknown], ['2', typed], ['3', None]], tag='SELECT 3')
            for source in ("SELECT 4,'4'::text WHERE false",
                           "SELECT 4,v FROM (SELECT '4' AS v) q WHERE false",
                           "WITH q AS (SELECT '4' AS v) SELECT 4,v FROM q WHERE false",
                           "SELECT 4,'4' UNION ALL SELECT 5,'4'"):
                check('INSERT INTO ' + table + ' ' + source, state='42804')
            check('INSERT INTO ' + table + " SELECT 6,'not an interval' WHERE false", state='22007')
        assert check('CREATE TEMP TABLE interval_source_array(v INTERVAL YEAR[])')
        assert check("INSERT INTO interval_source_array SELECT '{4,NULL,-2}'", tag='INSERT 0 1')
        result = runner.decode_wire_result(client.simple_query(sock, 'SELECT v FROM interval_source_array'), include_types=True)
        checks += 1
        # PostgreSQL's array input first resolves each element as generic
        # INTERVAL, then applies the array element modifier. Unlike a scalar
        # UNKNOWN input, bare numeric array elements therefore mean seconds.
        expected = [['{00:00:00,NULL,00:00:00}']]
        print('INTERVAL_SOURCE_ARRAY', result, 'EXPECTED', expected, flush=True)
        if result[0] != expected or result[1] is not None or result[5] != [1187]:
            failures.append(('array', result, expected))
        print('INTERVAL_SOURCE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 133 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
