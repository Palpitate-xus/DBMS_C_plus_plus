#!/usr/bin/env python3
"""OLD/NEW RETURNING must not divert SET into case-folded eager evaluation."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('versioned_update_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        print('VERSIONED_UPDATE', sql, result, flush=True)
        return result

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def check(sql, rows, names, types, tag, state=None):
        result = query(sql)
        # This runtime-error control checks SQLSTATE and absence of rows/tag;
        # RowDescription-before-error framing has its own protocol contract.
        actual = (result[0], result[1], result[4]) if state else (result[0], result[1], result[3], result[5], result[4])
        expected = (rows, state, tag) if state else (rows, state, names, types, tag)
        if actual != expected:
            failures.append((sql, actual, expected))
            print('VERSIONED_UPDATE_FAILURE', failures[-1], flush=True)

    try:
        setup('CREATE TEMP TABLE versioned_update_rows(id INT PRIMARY KEY,"V" BIGINT,v INT);')
        setup('INSERT INTO versioned_update_rows VALUES(1,2147483648,3);')
        check('UPDATE versioned_update_rows SET "V"="V"+1,v=v+1 '
              'RETURNING WITH(OLD AS o,NEW AS "O") o."V" AS before_wide,"O"."V" AS after_wide,o.v AS before_small,"O".v AS after_small;',
              [['2147483648', '2147483649', '3', '4']],
              ['before_wide', 'after_wide', 'before_small', 'after_small'], [20, 20, 23, 23], 'UPDATE 1')
        check('UPDATE versioned_update_rows SET "V"=v+10,v=CAST("V" AS BIGINT)-2147483647 '
              'RETURNING old."V" AS before_wide,new."V" AS after_wide,old.v AS before_small,new.v AS after_small;',
              [['2147483649', '14', '4', '2']],
              ['before_wide', 'after_wide', 'before_small', 'after_small'], [20, 20, 23, 23], 'UPDATE 1')
        setup('CREATE TEMP SEQUENCE versioned_update_effect;')
        check("UPDATE versioned_update_rows SET v=nextval('versioned_update_effect') WHERE false "
              'RETURNING old."V" AS before_wide,new."V" AS after_wide;',
              [], ['before_wide', 'after_wide'], [20, 20], 'UPDATE 0')
        check("SELECT currval('versioned_update_effect');", [], [], [], None, '55000')
        check('SELECT "V",v FROM versioned_update_rows;', [['14', '2']], ['V', 'v'], [20, 23], 'SELECT 1')
        setup('UPDATE versioned_update_rows SET "V"=NULL,v=NULL;')
        check('UPDATE versioned_update_rows SET "V"="V"+1,v=v+1 '
              'RETURNING old."V" AS before_wide,new."V" AS after_wide,old.v AS before_small,new.v AS after_small;',
              [[None, None, None, None]], ['before_wide', 'after_wide', 'before_small', 'after_small'], [20, 20, 23, 23], 'UPDATE 1')
        assert not failures, failures
        print('[VERSIONED UPDATE BINDING '+('PG18 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:
            server['sock'].close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
