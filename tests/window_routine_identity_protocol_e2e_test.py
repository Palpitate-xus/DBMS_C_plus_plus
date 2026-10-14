#!/usr/bin/env python3
"""Window roles reject real scalar routines and retain explicit namespaces."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('window_identity_runner', root / 'tests/compat/pg_diff_runner.py')
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
    schema = 'window_identity_' + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(sql, rows=None, state=None, oids=None):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and not result[0] and result[4] is None
        checks += 1
        print('WINDOW_ROUTINE_IDENTITY', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oids))
        return valid

    try:
        assert check('CREATE SCHEMA ' + schema)
        created = True
        assert check('CREATE TABLE ' + schema + '.rows(v INT,t TEXT)')
        assert check("INSERT INTO " + schema + ".rows VALUES(1,'a'),(2,'b')")
        for name, args, body in (('scalar_value', 'i INT', 'i'), ('array_agg', 'i INT', 'i'),
                                 ('sum', 'i INT', 'i'), ('row_number', '', '7'), ('lag', 'i INT', 'i')):
            assert check('CREATE FUNCTION ' + schema + '.' + name + '(' + args +
                         ') RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT ' + body + '$$')
        for expression in ('upper(t)', 'pg_catalog.upper(t)', 'abs(v)', 'length(t)',
                           schema + '.scalar_value(v)'):
            for suffix in ('', ' WHERE false', ' LIMIT 0'):
                check('SELECT ' + expression + ' OVER() FROM ' + schema + '.rows' + suffix, state='42809')
        check('SELECT missing_window(v) OVER() FROM ' + schema + '.rows', state='42883')
        check('SELECT upper(missing) OVER() FROM ' + schema + '.rows WHERE false', state='42703')
        for sql in ("SELECT upper('x') OVER()",
                    "SELECT CASE WHEN false THEN upper('x') OVER() ELSE 'kept' END",
                    "SELECT 1 WHERE upper('x') OVER()='X'"):
            check(sql, state='42809')
        for sql in ("SELECT CASE WHEN false THEN upper(t) OVER() ELSE 'kept' END FROM ",
                    "SELECT 1 FROM "):
            for suffix in ('', ' LIMIT 0'):
                where = " WHERE upper(t) OVER()='X'" if sql == 'SELECT 1 FROM ' else ''
                check(sql + schema + '.rows' + where + suffix, state='42809')
            where = " WHERE false AND upper(t) OVER()='X'" if sql == 'SELECT 1 FROM ' else ' WHERE false'
            check(sql + schema + '.rows' + where, state='42809')
        check('SELECT upper(t) FROM ' + schema + '.rows ORDER BY v', rows=[['A'], ['B']], oids=[25])
        check('SELECT ' + schema + '.scalar_value(v) FROM ' + schema + '.rows ORDER BY v',
              rows=[['1'], ['2']], oids=[23])
        assert check('SET search_path=' + schema + ',pg_catalog,public')
        for expression in ('array_agg(v)', 'sum(v)', 'row_number()', 'lag(v)', 'scalar_value(v)',
                           schema + '.array_agg(v)'):
            for suffix in ('', ' WHERE false', ' LIMIT 0'):
                check('SELECT ' + expression + ' OVER(ORDER BY v) FROM rows' + suffix, state='42809')
        for expression, rows, oid in (
                ('pg_catalog.array_agg(v)', [['{1}'], ['{1,2}']], 1007),
                ('"pg_catalog"."array_agg"(v)', [['{1}'], ['{1,2}']], 1007),
                ('pg_catalog.sum(v)', [['1'], ['3']], 20),
                ('pg_catalog.row_number()', [['1'], ['2']], 20),
                ('pg_catalog.lag(v)', [[None], ['1']], 23)):
            check('SELECT ' + expression + ' OVER(ORDER BY v) AS x FROM rows ORDER BY v', rows=rows, oids=[oid])
            check('SELECT ' + expression + ' OVER(ORDER BY v) AS x FROM rows WHERE false', rows=[], oids=[oid])
            check('SELECT ' + expression + ' OVER(ORDER BY v) AS x FROM rows LIMIT 0', rows=[], oids=[oid])
        check('SELECT pg_catalog.array_agg(v) FILTER(WHERE v>1) OVER(ORDER BY v) FROM rows ORDER BY v',
              rows=[[None], ['{2}']], oids=[1007])
        check('SELECT pg_catalog.array_agg(v+1) OVER w FROM rows WINDOW w AS(ORDER BY v) ORDER BY v',
              rows=[['{2}'], ['{2,3}']], oids=[1007])
        check('SELECT count(*) FILTER(WHERE v>1) OVER(ORDER BY v) FROM rows ORDER BY v',
              rows=[['0'], ['1']], oids=[20])
        print('WINDOW_ROUTINE_IDENTITY_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 73 and not failures, failures
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
