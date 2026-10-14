#!/usr/bin/env python3
"""ARRAY_AGG windows retain real input types, NULLs, frames and output OIDs."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('window_array_runner', root / 'tests/compat/pg_diff_runner.py')
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

    def check(sql, rows=None, state=None, oids=None):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if oids is not None:
            valid = valid and result[5] == oids
            valid = valid and all(field[5] == -1 for field in client.row_description_fields(messages))
        if state:
            valid = valid and result[0] == [] and result[4] is None
        checks += 1
        print('WINDOW_ARRAY', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oids))
        return valid

    try:
        assert check('CREATE TEMP TABLE window_array_rows(id INT,g INT,v INT,t TEXT,n NUMERIC(6,2),b BOOL)')
        assert check("INSERT INTO window_array_rows VALUES(1,1,2,'',1.20,true),(2,1,NULL,NULL,NULL,NULL),"
                     "(3,2,1,'NULL',2.30,false),(4,2,1,'a b',1.20,true)")
        for expression, values, oid in (
                ('array_agg(v)', ['{2}', '{2,NULL}', '{2,NULL,1}', '{2,NULL,1,1}'], 1007),
                ('pg_catalog.array_agg(v)', ['{2}', '{2,NULL}', '{2,NULL,1}', '{2,NULL,1,1}'], 1007),
                ('array_agg(t)', ['{""}', '{"",NULL}', '{"",NULL,"NULL"}', '{"",NULL,"NULL","a b"}'], 1009),
                ('array_agg(n)', ['{1.20}', '{1.20,NULL}', '{1.20,NULL,2.30}', '{1.20,NULL,2.30,1.20}'], 1231),
                ('array_agg(b)', ['{t}', '{t,NULL}', '{t,NULL,f}', '{t,NULL,f,t}'], 1000),
                ('array_agg(v+1)', ['{3}', '{3,NULL}', '{3,NULL,2}', '{3,NULL,2,2}'], 1007),
                ('array_agg(v::bigint)', ['{2}', '{2,NULL}', '{2,NULL,1}', '{2,NULL,1,1}'], 1016)):
            check('SELECT ' + expression + ' OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id',
                  rows=[[v] for v in values], oids=[oid])
            check('SELECT ' + expression + ' OVER(ORDER BY id) AS value FROM window_array_rows WHERE false',
                  rows=[], oids=[oid])
            check('SELECT ' + expression + ' OVER(ORDER BY id) AS value FROM window_array_rows LIMIT 0',
                  rows=[], oids=[oid])
        check('SELECT array_agg(v) OVER(PARTITION BY g ORDER BY id) AS value FROM window_array_rows ORDER BY id',
              rows=[['{2}'], ['{2,NULL}'], ['{1}'], ['{1,1}']], oids=[1007])
        check('SELECT array_agg(v) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS value FROM window_array_rows ORDER BY id',
              rows=[['{2}'], ['{2,NULL}'], ['{NULL,1}'], ['{1,1}']], oids=[1007])
        check('SELECT array_agg(v) FILTER(WHERE v IS NOT NULL) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id',
              rows=[['{2}'], ['{2}'], ['{2,1}'], ['{2,1,1}']], oids=[1007])
        check('SELECT array_agg(v) OVER() AS value FROM window_array_rows ORDER BY id',
              rows=[['{2,NULL,1,1}']] * 4, oids=[1007])
        check('SELECT array_agg(v) OVER(PARTITION BY g) AS value FROM window_array_rows ORDER BY id',
              rows=[['{2,NULL}'], ['{2,NULL}'], ['{1,1}'], ['{1,1}']], oids=[1007])
        check('SELECT array_agg(v) OVER(ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS value FROM window_array_rows ORDER BY id',
              rows=[['{NULL}'], ['{1}'], ['{1}'], [None]], oids=[1007])
        check('SELECT array_agg(v) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS value FROM window_array_rows ORDER BY id',
              rows=[[None], ['{2}'], ['{2,NULL}'], ['{2,NULL,1}']], oids=[1007])
        check('SELECT array_agg(v) FILTER(WHERE false) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id',
              rows=[[None]] * 4, oids=[1007])
        check('SELECT array_agg(v::numeric(5,2)) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id',
              rows=[['{2.00}'], ['{2.00,NULL}'], ['{2.00,NULL,1.00}'], ['{2.00,NULL,1.00,1.00}']], oids=[1231])
        check('SELECT array_agg(v) OVER(ORDER BY id) AS value FROM window_array_rows WHERE v IS NULL',
              rows=[['{NULL}']], oids=[1007])
        check('SELECT array_agg(t) OVER(ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS value FROM window_array_rows ORDER BY id',
              rows=[['{NULL}'], ['{"NULL"}'], ['{"a b"}'], [None]], oids=[1009])
        check("SELECT array_agg(t) FILTER(WHERE t<>'') OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id",
              rows=[[None], [None], ['{"NULL"}'], ['{"NULL","a b"}']], oids=[1009])
        check('SELECT array_agg(v) OVER w AS value FROM window_array_rows WINDOW w AS(ORDER BY id) ORDER BY id',
              rows=[['{2}'], ['{2,NULL}'], ['{2,NULL,1}'], ['{2,NULL,1,1}']], oids=[1007])
        check("SELECT array_agg('null'::text) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id",
              rows=[['{"null"}'], ['{"null","null"}'], ['{"null","null","null"}'],
                    ['{"null","null","null","null"}']], oids=[1009])
        check('SELECT array_agg(NULL::INT) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id',
              rows=[['{NULL}'], ['{NULL,NULL}'], ['{NULL,NULL,NULL}'], ['{NULL,NULL,NULL,NULL}']], oids=[1007])
        check("SELECT array_agg(B'1'::bit(5)) OVER(ORDER BY id) AS value FROM window_array_rows ORDER BY id",
              rows=[['{10000}'], ['{10000,10000}'], ['{10000,10000,10000}'], ['{10000,10000,10000,10000}']], oids=[1561])
        for expression, state in (('array_agg(*)', '42883'), ('array_agg(NULL)', '42725'),
                                  ("array_agg('x')", '42725'), ('array_agg(DISTINCT v)', '0A000'),
                                  ('array_agg(missing)', '42703'), ('array_agg(v) FILTER(WHERE 1)', '42804')):
            check('SELECT ' + expression + ' OVER() FROM window_array_rows WHERE false', state=state)
        print('WINDOW_ARRAY_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 45 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
