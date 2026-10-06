#!/usr/bin/env python3
"""Target interval inputs preserve structured errors and prepare-before-effects."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('interval_state_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        print('INTERVAL_STATE', sql, result, flush=True)
        return result

    def check(label, actual, expected):
        if actual != expected:
            failures.append((label, actual, expected))
            print('INTERVAL_STATE_FAILURE', failures[-1], flush=True)

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    try:
        if reference:
            assert query('SHOW server_version_num;')[0] == [['170002']]
        setup('CREATE TEMP TABLE interval_state_rows(id INT PRIMARY KEY,v INTERVAL);')
        setup("INSERT INTO interval_state_rows VALUES(1,'1 day'),(2,NULL);")
        for text, state in (
            ('2147483648 months', '22015'), ('-2147483649 months', '22015'),
            ('2147483648 days', '22015'), ('-2147483649 days', '22015'),
            ('178956971 years', '22008'),
            ('9223372036854775808 microseconds', '22015'),
            ('-9223372036854775809 microseconds', '22015'),
            ('1 fortnight', '22007'), ('1 year bogus', '22007'), ('', '22007'),
            ('NULL', '22007'), ('1 day (SQLSTATE 99999)', '22007'),
            ('9'*256+' days', '22007'),
        ):
            for sql in (
                "INSERT INTO interval_state_rows VALUES(3,'%s');" % text,
                "UPDATE interval_state_rows SET v='%s' WHERE false;" % text,
                "UPDATE interval_state_rows SET v='%s' WHERE id=1;" % text,
            ):
                check(sql, query(sql)[1], state)
            check('unchanged-'+text, query('SELECT id,v FROM interval_state_rows ORDER BY id;')[0],
                  [['1', '1 day'], ['2', None]])
        # Unknown string input is coerced while preparing the complete VALUES
        # list, before a volatile sibling or earlier VALUES row can execute.
        setup('CREATE TEMP SEQUENCE interval_state_insert_seq;')
        for expression, state in (('missing_interval_function(1)', '42883'),
                                  ('missing_interval_column', '42703')):
            check('insert-binding-priority-'+expression,
                  query("INSERT INTO interval_state_rows VALUES(%s,'2147483648 months');" % expression)[1], state)
            check('insert-reverse-binding-priority-'+expression,
                  query("INSERT INTO interval_state_rows(v,id) VALUES('2147483648 months',%s);" % expression)[1], state)
            check('insert-later-row-priority-'+expression,
                  query("INSERT INTO interval_state_rows VALUES(3,'2147483648 months'),(%s,'1 day');" % expression)[1], '22015')
        for sql in (
            "INSERT INTO interval_state_rows VALUES(((3)),'2147483648 months' /* ) VALUES ( */),(4,'1 day');",
            "INSERT INTO interval_state_rows VALUES(3,'2147483648 months') RETURNING missing_return_column;",
            "INSERT INTO interval_state_rows VALUES(3,'2147483648 months') ON CONFLICT(id) DO UPDATE SET v=missing_conflict_column;",
        ):
            check('insert-preparation-boundary-'+sql, query(sql)[1], '22015')
        check('insert-whole-syntax-priority',
              query("INSERT INTO interval_state_rows VALUES(3,'2147483648 months'),(4,'1 day' bogus);")[1], '42601')
        check('insert-row-width-after-earlier-coercion',
              query("INSERT INTO interval_state_rows VALUES(3,'2147483648 months'),(4);")[1], '22015')
        check('insert-row-width-with-valid-prefix',
              query("INSERT INTO interval_state_rows VALUES(3,'1 day'),(4);")[1], '42601')
        result = query("INSERT INTO interval_state_rows VALUES(nextval('interval_state_insert_seq'),'1 day'),(4,'2147483648 months');")
        check('insert-preflight-state', result[1], '22015')
        check('insert-preflight-no-effects', query("SELECT currval('interval_state_insert_seq');")[1], '55000')
        setup('CREATE TEMP SEQUENCE interval_state_update_seq;')
        result = query("UPDATE interval_state_rows SET id=nextval('interval_state_update_seq'),v='1 fortnight' WHERE true;")
        check('update-preflight-state', result[1], '22007')
        check('update-preflight-no-effects', query("SELECT currval('interval_state_update_seq');")[1], '55000')
        check('unchanged-effects', query('SELECT id,v FROM interval_state_rows ORDER BY id;')[0],
              [['1', '1 day'], ['2', None]])
        setup("UPDATE interval_state_rows SET v='-9223372036854775808 microseconds' WHERE id=1;")
        result = query('SELECT v FROM interval_state_rows ORDER BY id;')
        check('boundary-and-null', result[0], [['-2562047788:00:54.775808'], [None]])
        check('interval-oid', result[5], [1186])
        setup('UPDATE interval_state_rows SET v=NULL WHERE id=1;')
        check('null-assignment', query('SELECT v FROM interval_state_rows ORDER BY id;')[0], [[None], [None]])
        setup('CREATE TEMP TABLE interval_state_quoted("I" INTERVAL,t TEXT);')
        setup("INSERT INTO interval_state_quoted(t,\"I\") VALUES('','1 us'),(NULL,NULL);")
        result = query('SELECT "I",t FROM interval_state_quoted ORDER BY t NULLS LAST;')
        check('quoted-target-empty-text', result[0], [['00:00:00.000001', ''], [None, None]])
        check('quoted-target-oids', result[5], [1186, 25])
        check('quoted-target-invalid-input',
              query("UPDATE interval_state_quoted SET \"I\"='2147483648 months' WHERE false;")[1], '22015')
        setup("UPDATE interval_state_quoted SET \"I\"='2 ms' WHERE t='';")
        check('quoted-target-update', query('SELECT "I",t FROM interval_state_quoted ORDER BY t NULLS LAST;')[0],
              [['00:00:00.002', ''], [None, None]])
        setup('BEGIN;')
        check('transaction-input-error', query("UPDATE interval_state_rows SET v='2147483648 months';")[1], '22015')
        check('aborted-transaction', query('SELECT 1;')[1], '25P02')
        setup('ROLLBACK;')
        check('transaction-recovery', query('SELECT v FROM interval_state_rows ORDER BY id;')[0], [[None], [None]])
        assert not failures, '%s interval input assertions failed: %r' % (len(failures), failures)
        print('[INTERVAL INPUT SQLSTATE '+('PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
