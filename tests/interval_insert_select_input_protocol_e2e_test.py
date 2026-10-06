#!/usr/bin/env python3
"""INSERT SELECT coerces declared source descriptors before any source effects."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('interval_select_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
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
        print('INTERVAL_INSERT_SELECT', sql, result, flush=True)
        return result
    def check(label, actual, expected):
        if actual != expected:
            failures.append((label, actual, expected))
            print('INTERVAL_INSERT_SELECT_FAILURE', failures[-1], flush=True)
    def setup(sql):
        result = query(sql); assert result[1] is None, (sql, result)
    try:
        if reference: assert query('SHOW server_version_num;')[0] == [['170002']]
        setup('CREATE TEMP TABLE interval_select_rows(id INT PRIMARY KEY,v INTERVAL);')
        setup('CREATE TEMP TABLE interval_select_text(id INT,v TEXT);')
        setup("INSERT INTO interval_select_text VALUES(1,'1 day'),(2,NULL);")
        setup('CREATE TEMP TABLE interval_select_typed(id INT,v INTERVAL);')
        setup("INSERT INTO interval_select_typed VALUES(1,'1 us'),(2,NULL);")
        for label, source, state in (
            ('unknown-overflow-empty', "SELECT 10,'2147483648 months' WHERE false", '22015'),
            ('unknown-format-empty', "SELECT 10,'1 fortnight' WHERE false", '22007'),
            ('unknown-empty-string', "SELECT 10,'' WHERE false", '22007'),
            ('unknown-null-string', "SELECT 10,'NULL' WHERE false", '22007'),
            ('known-text-value', "SELECT 10,CAST('1 day' AS TEXT)", '42804'),
            ('known-text-empty', "SELECT 10,CAST('1 day' AS TEXT) WHERE false", '42804'),
            ('known-text-null', 'SELECT 10,CAST(NULL AS TEXT) WHERE false', '42804'),
            ('known-integer-empty', 'SELECT 10,1 WHERE false', '42804'),
            ('known-array-empty', "SELECT 10,ARRAY['1 day'] WHERE false", '42804'),
            ('table-text-empty', 'SELECT id,v FROM interval_select_text WHERE false', '42804'),
            ('table-text-nonempty', 'SELECT id,v FROM interval_select_text', '42804'),
            ('table-text-star', 'SELECT * FROM interval_select_text WHERE false', '42804'),
            ('derived-unknown-finalized', "SELECT 10,q.v FROM (SELECT '1 day' AS v) q WHERE false", '42804'),
            ('derived-null-finalized', 'SELECT 10,q.v FROM (SELECT NULL AS v) q WHERE false', '42804'),
            ('typed-literal-overflow-empty', "SELECT 10,INTERVAL '2147483648 months' WHERE false", '22015'),
            ('unknown-cast-overflow-empty', "SELECT 10,CAST('2147483648 months' AS INTERVAL) WHERE false", '22015'),
            ('unknown-colon-overflow-empty', "SELECT 10,'2147483648 months'::INTERVAL WHERE false", '22015'),
            ('same-source-missing-column', "SELECT missing_select_column,'2147483648 months' WHERE false", '42703'),
            ('same-source-missing-function', "SELECT missing_select_function(1),'2147483648 months' WHERE false", '42883'),
            ('where-missing-function', "SELECT 10,'2147483648 months' WHERE missing_select_function(1)=1", '42883'),
            ('typed-input-before-where-binding', "SELECT 10,CAST('2147483648 months' AS INTERVAL) WHERE missing_select_function(1)=1", '22015'),
            ('typed-literal-before-where-binding', "SELECT 10,INTERVAL '1 fortnight' WHERE missing_select_function(1)=1", '22007'),
            ('source-input-before-returning', "SELECT 10,'2147483648 months' RETURNING missing_returning_column", '22015'),
        ):
            check(label, query('INSERT INTO interval_select_rows '+source+';')[1], state)
            check(label+'-no-write', query('SELECT id FROM interval_select_rows;')[0], [])
            # Retain every failed no-write assertion, then isolate later
            # controls from rows written by an incorrect baseline candidate.
            setup('DELETE FROM interval_select_rows;')
        for index, source in enumerate((
                "SELECT nextval('interval_select_seq_0'),'2147483648 months' WHERE false",
                "SELECT nextval('interval_select_seq_1'),'2147483648 months'",
                "SELECT nextval('interval_select_seq_2'),CAST('1 day' AS TEXT)",
                "SELECT nextval('interval_select_seq_3'),v FROM interval_select_text",
        )):
            sequence='interval_select_seq_'+str(index)
            setup('CREATE TEMP SEQUENCE '+sequence+';')
            check('source-effects-'+str(index), query('INSERT INTO interval_select_rows '+source+';')[1],
                  '22015' if index < 2 else '42804')
            check('source-effects-not-evaluated-'+str(index), query("SELECT currval('%s');" % sequence)[1], '55000')
            check('source-effects-no-write-'+str(index), query('SELECT id FROM interval_select_rows;')[0], [])
            setup('DELETE FROM interval_select_rows;')
        for source in ("SELECT 10,'1 day' WHERE false", 'SELECT 10,NULL WHERE false',
                       'SELECT 10,CAST(NULL AS INTERVAL) WHERE false'):
            result=query('INSERT INTO interval_select_rows '+source+';')
            check('valid-empty-'+source, (result[1],result[4]), (None,'INSERT 0 0'))
        setup("INSERT INTO interval_select_rows SELECT 10,'1 us';")
        setup('INSERT INTO interval_select_rows SELECT 11,NULL;')
        setup('INSERT INTO interval_select_rows SELECT id,v FROM interval_select_typed;')
        result=query('SELECT id,v FROM interval_select_rows ORDER BY id;')
        check('typed-values-null', result[0], [['1','00:00:00.000001'],['2',None],['10','00:00:00.000001'],['11',None]])
        check('typed-result-oids', result[5], [23,1186])
        setup('CREATE TEMP TABLE interval_select_quote("I" INTERVAL,k INT);')
        setup("INSERT INTO interval_select_quote(k,\"I\") SELECT 1,'1 us';")
        result=query('SELECT "I",k FROM interval_select_quote;')
        check('quoted-target-order', result[0], [['00:00:00.000001','1']])
        check('quoted-target-oids', result[5], [1186,23])
        setup('CREATE TEMP TABLE interval_select_prefix(id INT,t TEXT);')
        setup("INSERT INTO interval_select_prefix VALUES(1,'');")
        setup('CREATE TEMP TABLE interval_select_wide(id INT,t TEXT,v INTERVAL);')
        for label,sql,state in (
            ('star-expanded-trailing-unknown', "INSERT INTO interval_select_wide SELECT *,'2147483648 months' FROM interval_select_prefix WHERE false;", '22015'),
            ('qualified-star-expanded-trailing-unknown', "INSERT INTO interval_select_wide SELECT p.*,'1 fortnight' FROM interval_select_prefix p WHERE false;", '22007'),
            ('star-expanded-leading-unknown', "INSERT INTO interval_select_wide(v,id,t) SELECT '2147483648 months',p.* FROM interval_select_prefix p WHERE false;", '22015'),
        ):
            check(label,query(sql)[1],state)
            check(label+'-no-writes',query('SELECT id FROM interval_select_wide;')[0],[])
            setup('DELETE FROM interval_select_wide;')
        setup("INSERT INTO interval_select_wide SELECT *,'1 us' FROM interval_select_prefix;")
        result=query('SELECT id,t,v FROM interval_select_wide;')
        check('star-expanded-unknown-values',result[0],[['1','','00:00:00.000001']])
        check('star-expanded-unknown-oids',result[5],[23,25,1186])
        setup('BEGIN;')
        check('explicit-transaction-input', query("INSERT INTO interval_select_rows SELECT 20,'2147483648 months' WHERE false;")[1], '22015')
        check('explicit-transaction-aborted', query('SELECT 1;')[1], '25P02')
        setup('ROLLBACK;')
        check('rollback-preserves-original-count', query('SELECT count(*) FROM interval_select_rows;')[0], [['4']])
        assert not failures, '%d INSERT SELECT input assertions failed: %r' % (len(failures), failures)
        print('[INTERVAL INSERT SELECT INPUT '+('PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=='__main__': main()
