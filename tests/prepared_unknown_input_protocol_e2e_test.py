#!/usr/bin/env python3
"""Unknown SQL input conversion belongs to preparation, not lazy execution."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('unknown_input_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference18 = '--reference18' in sys.argv[1:]
    reference = reference18 or '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password); server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []
    writer = ('pg_temp.' if reference else '') + 'prepared_input_writer'
    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        print('PREPARED_UNKNOWN_INPUT', sql, result, flush=True)
        return result
    def check(label, sql, state=None, rows=None):
        result = query(sql)
        if result[1] != state or (rows is not None and result[0] != rows):
            failures.append((label, state, rows, result))
    try:
        if reference:
            assert query('SHOW server_version_num')[0] == [[('180006' if reference18 else '170002')]]
        assert query('CREATE TEMP TABLE prepared_input_rows(id INT PRIMARY KEY,a INT)')[1] is None
        assert query('CREATE TEMP SEQUENCE prepared_input_effects')[1] is None
        # A writer is an actual volatile expression; metadata never calls it.
        assert query('CREATE FUNCTION ' + writer + '() RETURNS INT VOLATILE LANGUAGE sql AS '
                     "$$ SELECT nextval('prepared_input_effects')::INT $$;")[1] is None
        bad = [
            ("CAST('bad' AS INT)", '22P02'), ("'bad'::INT", '22P02'),
            ("CAST('1.2' AS INT)", '22P02'), ("CAST('32768' AS SMALLINT)", '22003'),
            ("CAST('9223372036854775808' AS BIGINT)", '22003'),
            ("CAST('not-bool' AS BOOLEAN)", '22P02'), ("CAST('bad' AS NUMERIC)", '22P02'),
            ("CAST('bad' AS REAL)", '22P02'), ("CAST('bad' AS DOUBLE PRECISION)", '22P02'),
            ("CAST('bad' AS UUID)", '22P02'),
            ("BOOLEAN 'not-bool'", '22P02'), ("NUMERIC 'bad'", '22P02'),
            ("CAST('bad' AS pg_catalog.int4)", '22P02'),
            ('CAST(\'bad\' AS "pg_catalog"."int4")', '22P02'),
            ('CAST($value$bad$value$ AS INT)', '22P02'),
            ("CAST(E'b\\x61d' AS INT)", '22P02'),
        ]
        for expression, state in bad:
            assert query('DELETE FROM prepared_input_rows')[1] is None
            check('unused ' + expression,
                  'WITH unused AS(SELECT ' + expression + ' AS value) '
                  'INSERT INTO prepared_input_rows VALUES(1,1) RETURNING id', state, [])
            check('failed preparation leaves rows empty', 'SELECT id FROM prepared_input_rows', rows=[])
        assert query('DELETE FROM prepared_input_rows')[1] is None
        check('before writer',
              "WITH writer AS(INSERT INTO prepared_input_rows VALUES(" + writer + "(),1) RETURNING id), "
              "unused AS(SELECT CAST('bad' AS INT)) INSERT INTO prepared_input_rows VALUES(2,2)", '22P02', [])
        check('preparation no irreversible sequence effect', "SELECT currval('prepared_input_effects')", '55000', [])
        check('dead CASE arm still input conversion',
              "WITH c AS(SELECT CASE WHEN false THEN CAST('bad' AS INT) ELSE 1 END) "
              'INSERT INTO prepared_input_rows VALUES(3,3)', '22P02', [])
        check('empty qualifying input',
              "WITH c AS(SELECT CAST('bad' AS INT) WHERE false) INSERT INTO prepared_input_rows VALUES(4,4)", '22P02', [])
        check('limit zero input',
              "WITH c AS(SELECT CAST('bad' AS INT) LIMIT 0) INSERT INTO prepared_input_rows VALUES(5,5)", '22P02', [])
        check('left projection input priority',
              "WITH c AS(SELECT CAST('bad' AS INT), missing_input_function(1)) INSERT INTO prepared_input_rows VALUES(6,6)",
              '22P02', [])
        check('left unknown function priority',
              "WITH c AS(SELECT missing_input_function(1), CAST('bad' AS INT)) INSERT INTO prepared_input_rows VALUES(7,7)",
              '42883', [])
        # Typmods, typed narrowing, arithmetic, and volatile expressions must
        # not be folded during analysis of an unused query.
        for i, expression in enumerate([
            '1/0', 'CAST(2147483648 AS INT)', "CAST('bad' AS TEXT)::INT",
            "CAST('9999' AS NUMERIC(2,0))", 'CAST(NULL AS INT)',
            writer + '()::INT', "CAST('1.2' AS NUMERIC)::INT",
            'CAST($value$12$value$ AS INT)', "CAST(E'\\x31\\x32' AS INT)",
        ], start=20):
            check('no premature folding ' + expression,
                  'WITH unused AS(SELECT ' + expression + ') INSERT INTO prepared_input_rows VALUES(' +
                  str(i) + ',' + str(i) + ') RETURNING id', rows=[[str(i)]])
        check('unused routine stays lazy', "SELECT currval('prepared_input_effects')", '55000', [])
        check('input error precedes duplicate target',
              "WITH c AS(SELECT 1) UPDATE prepared_input_rows SET a=CAST('bad' AS INT),a=2 WHERE false", '22P02', [])
        check('typed narrowing follows duplicate target',
              'WITH c AS(SELECT 1) UPDATE prepared_input_rows SET a=CAST(2147483648 AS INT),a=2 WHERE false', '42601', [])
        check('arithmetic follows duplicate target',
              'WITH c AS(SELECT 1) UPDATE prepared_input_rows SET a=1/0,a=2 WHERE false', '42601', [])
        check('transaction begin', 'BEGIN')
        check('prior successful write', 'INSERT INTO prepared_input_rows VALUES(90,90)')
        check('savepoint', 'SAVEPOINT keep_input')
        check('input error in user transaction',
              "WITH c AS(SELECT CAST('bad' AS INT)) INSERT INTO prepared_input_rows VALUES(91,91)", '22P02', [])
        check('failed block', 'SELECT 1', '25P02', [])
        check('recover prior command', 'ROLLBACK TO SAVEPOINT keep_input')
        check('prior row retained', 'SELECT id FROM prepared_input_rows WHERE id=90', rows=[['90']])
        check('failed row absent', 'SELECT id FROM prepared_input_rows WHERE id=91', rows=[])
        check('rollback', 'ROLLBACK')
        assert not failures, failures
        print('[PREPARED UNKNOWN INPUT] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__ == '__main__': main()
