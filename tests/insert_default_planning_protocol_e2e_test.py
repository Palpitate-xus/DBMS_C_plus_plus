"""Retained INSERT defaults: pure planning, real row demand, strict PG 18."""
import importlib.util
import json
import socket
import sys
from pathlib import Path

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('insert_default_runner', repo / 'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
c = r.load_protocol_client()
reference = sys.argv[1:] == ['--reference18']
assert not sys.argv[1:] or reference
server = None
if reference:
    h, p, u, d, pw = r._reference_connection_settings()
    sock = socket.create_connection((h, p), timeout=r.wire_timeout())
    c.startup_reference(sock, u, d, pw)
    r.verify_reference_version(c, sock)
else:
    server = r.start_ours(c)
    sock = server['sock']
failures = []
counter = 1


def q(sql, rows=None, state=None):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('INSERT_DEFAULT', sql, result, flush=True)
    if result[1] != state:
        failures.append((sql, 'state', result[1], state))
    if rows is not None and result[0] != rows:
        failures.append((sql, 'rows', result[0], rows))
    if state and (result[0] or result[4] is not None):
        failures.append((sql, 'partial output', result[0], result[4]))
    return result


# SQL, actual default/writer calls, RETURNING rows, plan error, runtime error.
cases = [
    ('INSERT INTO idp_bad DEFAULT VALUES', 0, 0, '22012', None),
    ('INSERT INTO idp_bad(v) VALUES(7)', 0, 0, '22012', None),
    ('INSERT INTO idp_bad(id,v) VALUES(DEFAULT,7)', 0, 0, '22012', None),
    ('INSERT INTO idp_bad(v) SELECT 7 WHERE false', 0, 0, '22012', None),
    ('INSERT INTO idp_bad(v) SELECT id FROM idp_source WHERE false', 0, 0, '22012', None),
    ('INSERT INTO idp_bad(id,v) VALUES(3,7) RETURNING id,v', 0, 1, None, None),
    ('INSERT INTO idp_bad(id,v) SELECT 3,7 WHERE false RETURNING id,v', 0, 0, None, None),
    ('INSERT INTO idp_bad(id,v) VALUES(NULL,7) RETURNING id,v', 0, 1, None, None),
    ('INSERT INTO idp_bad(v) VALUES(idp_writer(7)) RETURNING missing_idp(v)', 0, 0, '42883', None),
    ('INSERT INTO idp_bad(v) VALUES(idp_writer(7)) RETURNING missing_column', 0, 0, '42703', None),
    ('INSERT INTO idp_rows DEFAULT VALUES RETURNING v', 1, 1, None, None),
    ('INSERT INTO idp_rows(id) VALUES(3) RETURNING id,v', 1, 1, None, None),
    ('INSERT INTO idp_rows(id,v) VALUES(3,DEFAULT),(4,DEFAULT) RETURNING id,v', 2, 2, None, None),
    ('INSERT INTO idp_rows(id,v) VALUES(3,DEFAULT),(4,9) RETURNING id,v', 1, 2, None, None),
    ('INSERT INTO idp_rows(id) SELECT id FROM idp_source RETURNING id,v', 2, 2, None, None),
    ('INSERT INTO idp_rows(id) SELECT idp_writer(id) FROM idp_source WHERE false RETURNING v', 0, 0, None, None),
    ('INSERT INTO idp_rows(id,v) VALUES(3,NULL) RETURNING id,v', 0, 1, None, None),
    ('INSERT INTO idp_dead DEFAULT VALUES RETURNING v', 0, 1, None, None),
    ('INSERT INTO idp_round DEFAULT VALUES RETURNING v', 0, 1, None, None),
    ('INSERT INTO idp_nonnull DEFAULT VALUES RETURNING v', 0, 0, None, '23502'),
    ('INSERT INTO idp_check DEFAULT VALUES RETURNING v', 0, 0, None, '23514'),
    ('INSERT INTO idp_fail DEFAULT VALUES RETURNING v', 1, 0, None, 'P0001'),
    ('INSERT INTO idp_rows(id,v) VALUES(3,DEFAULT),(1,DEFAULT) RETURNING id,v', 2, 0, None, '23505'),
    ('INSERT INTO idp_domain DEFAULT VALUES RETURNING v,explicit_v', 0, 1, None, None),
    ('INSERT INTO idp_serial(v) VALUES(7),(8) RETURNING id,v', 0, 2, None, None),
    ('INSERT INTO idp_identity(v) VALUES(7),(8) RETURNING id,v', 0, 2, None, None),
    ('INSERT INTO idp_generated(id,v) VALUES(3,DEFAULT) RETURNING id,v', 0, 1, None, None),
    ('INSERT INTO idp_nullcase DEFAULT VALUES RETURNING v', 0, 1, None, None),
    ('INSERT INTO idp_array DEFAULT VALUES RETURNING v', 0, 1, None, None),
]

try:
    setup = [
        'BEGIN', 'CREATE TEMP SEQUENCE idp_calls',
        "CREATE FUNCTION idp_writer(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('idp_calls');RETURN p;END$$",
        "CREATE FUNCTION idp_failure() RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('idp_calls');RAISE EXCEPTION 'insert default failure';END$$",
        'CREATE TEMP TABLE idp_bad(id INT DEFAULT 1/0,v INT)',
        'CREATE TEMP TABLE idp_rows(id INT PRIMARY KEY DEFAULT 9,v INT DEFAULT idp_writer(7))',
        'INSERT INTO idp_rows VALUES(1,10),(2,20)',
        'CREATE TEMP TABLE idp_source(id INT)', 'INSERT INTO idp_source VALUES(3),(4)',
        'CREATE TEMP TABLE idp_dead(v INT DEFAULT CASE WHEN false THEN 1/0 ELSE 7 END)',
        'CREATE TEMP TABLE idp_round(v INT DEFAULT CAST(1.6 AS NUMERIC))',
        'CREATE TEMP TABLE idp_nonnull(v INT NOT NULL DEFAULT NULL)',
        'CREATE TEMP TABLE idp_check(v INT DEFAULT -1 CHECK(v>0))',
        'CREATE TEMP TABLE idp_fail(v INT DEFAULT idp_failure())',
        'CREATE DOMAIN idp_domain_type AS INT DEFAULT 1',
        'CREATE TEMP TABLE idp_domain(v idp_domain_type,explicit_v idp_domain_type DEFAULT 8)',
        'ALTER DOMAIN idp_domain_type SET DEFAULT 2',
        'CREATE TEMP TABLE idp_serial(id SERIAL,v INT)',
        'CREATE TEMP TABLE idp_identity(id INT GENERATED ALWAYS AS IDENTITY,v INT)',
        'CREATE TEMP TABLE idp_generated(id INT,v INT GENERATED ALWAYS AS(id+1) STORED)',
        'CREATE TEMP TABLE idp_nullcase(v INT DEFAULT CASE WHEN false THEN 1/0 ELSE NULL END CHECK(v>0))',
        'CREATE TEMP TABLE idp_array(v INT[] DEFAULT ARRAY[1,2])',
        "SELECT nextval('idp_calls')",
    ]
    for sql in setup:
        assert q(sql)[1] is None
    for analyze, as_json in [(False, False), (True, False), (False, True), (True, True)]:
        for sql, calls, returned, static_error, runtime_error in cases:
            q('SAVEPOINT idp_case')
            prefix = 'EXPLAIN (' + ('ANALYZE TRUE,' if analyze else '') + 'FORMAT ' + ('JSON' if as_json else 'TEXT') + ',TIMING FALSE) '
            expected = static_error or (runtime_error if analyze else None)
            result = q(prefix + sql, state=expected)
            if not expected and result[1] is None:
                if not result[0] or result[4] != 'EXPLAIN':
                    failures.append((sql, 'real plan/completion', result))
                if as_json:
                    try:
                        doc = json.loads('\n'.join(row[0] for row in result[0]))
                        root = doc[0]['Plan'] if reference else doc['plan']
                        node = root['Node Type'] if reference else root['nodeType']
                        operation = root['Operation'] if reference else root['operation']
                        if node != 'ModifyTable' or operation != 'Insert':
                            failures.append((sql, 'actual Insert plan', root))
                        if analyze:
                            actual = root['Actual Rows'] if reference else root['actualRows']
                            if actual != returned:
                                failures.append((sql, 'actual returning rows', actual, returned))
                    except (ValueError, KeyError, TypeError) as error:
                        failures.append((sql, 'plan document', str(error)))
                if analyze and runtime_error is None:
                    if 'idp_domain DEFAULT' in sql:
                        q('SELECT v,explicit_v FROM idp_domain', [['2', '8']])
                    if 'idp_dead DEFAULT' in sql:
                        q('SELECT v FROM idp_dead', [['7']])
                    if 'idp_round DEFAULT' in sql:
                        q('SELECT v FROM idp_round', [['2']])
                    if 'idp_generated' in sql:
                        q('SELECT id,v FROM idp_generated', [['3', '4']])
                    if 'idp_nullcase' in sql:
                        q('SELECT v FROM idp_nullcase', [[None]])
                    if 'idp_array' in sql:
                        q('SELECT v FROM idp_array', [['{1,2}']])
            q('ROLLBACK TO SAVEPOINT idp_case')
            q('RELEASE SAVEPOINT idp_case')
            if analyze and not static_error:
                counter += calls
            q("SELECT currval('idp_calls')", [[str(counter)]])
            q('SELECT id,v FROM idp_rows ORDER BY id', [['1', '10'], ['2', '20']])
    q('ROLLBACK')
    print('INSERT DEFAULT PLANNING FAILURES', failures, flush=True)
    assert not failures, failures
    print('[INSERT DEFAULT PLANNING ' + ('STRICT18' if reference else 'PROTOCOL') + '] passed', flush=True)
finally:
    try:
        c.simple_query(sock, 'ROLLBACK')
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()
