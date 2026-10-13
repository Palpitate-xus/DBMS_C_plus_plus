#!/usr/bin/env python3
"""Scientific SQL literals keep NUMERIC identity through metadata and DML."""
from decimal import Decimal
import importlib.util
from pathlib import Path
import socket
import struct
import sys


LITERALS = (
    '1e40', '1E40', '1e+40', '1e-40', '-1e40', '+1e40',
    '.5e2', '-.5E-2', '5.e1', '-5.E+1', '1.25e2', '1.250e-2',
    '0e0', '0E+40', '0e-40', '1e500', '1E-500', '9.999e+3', '9.999e-3',
)


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('scientific_numeric_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    failures = []
    controls = 0

    def check(sql, rows=None, oids=None, state=None, tag=None, numeric=False):
        nonlocal controls
        controls += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        actual_rows = result[0]
        if rows is not None:
            if numeric:
                try:
                    actual_rows = [[None if cell is None else Decimal(cell) for cell in row] for row in actual_rows]
                    expected_rows = [[None if cell is None else Decimal(cell) for cell in row] for row in rows]
                except Exception:
                    expected_rows = rows
                    valid = False
            else:
                expected_rows = rows
            valid = valid and actual_rows == expected_rows
            valid = valid and result[4] == (tag or 'SELECT '+str(len(rows)))
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print('SCIENTIFIC_NUMERIC', sql, result, 'match', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids, state, tag))

    def describe(sql, oids, state=None):
        nonlocal controls
        controls += 1
        name = ('scientific_numeric_'+str(controls)).encode()
        payload = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H', 0)
        sock.sendall(client.typed(b'P', payload)+client.typed(b'D', b'S'+name+b'\0')+client.typed(b'S'))
        messages = client.read_until_ready(sock)
        states = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        valid = states == ([state] if state else [])
        valid = valid and not any(kind in (b'D', b'C') for kind, _ in messages)
        if state:
            valid = valid and not any(kind == b'1' for kind, _ in messages)
        else:
            valid = valid and [field[3] for field in fields] == oids
            valid = valid and sum(kind == b'1' for kind, _ in messages) == 1
        print('SCIENTIFIC_NUMERIC_DESCRIBE', sql, [field[3] for field in fields], states, 'match', valid, flush=True)
        if not valid:
            failures.append((sql, messages, oids, state))

    try:
        check('CREATE TEMP TABLE scientific_numeric_rows(id INT PRIMARY KEY,n NUMERIC)')
        check('CREATE TEMP TABLE scientific_numeric_update(id INT PRIMARY KEY,n NUMERIC)')
        check('INSERT INTO scientific_numeric_update VALUES(1,0)')
        check('CREATE TEMP SEQUENCE scientific_numeric_calls')
        for index, literal in enumerate(LITERALS, 1):
            expected = [[literal]]
            check('SELECT '+literal+' AS n', expected, [1700], numeric=True)
            check('INSERT INTO scientific_numeric_rows VALUES('+str(index)+','+literal+') RETURNING n',
                  expected, [1700], tag='INSERT 0 1', numeric=True)
            check('UPDATE scientific_numeric_update SET n='+literal+' WHERE id=1 RETURNING n',
                  expected, [1700], tag='UPDATE 1', numeric=True)
            describe('SELECT '+literal+' AS n', [1700])
        for index, literal in enumerate(('1e40', '-.5E-2', '1e500', '1E-500'), 101):
            check('INSERT INTO scientific_numeric_rows SELECT '+str(index)+','+literal+' RETURNING n',
                  [[literal]], [1700], tag='INSERT 0 1', numeric=True)
        for expression, expected in (
            ('1e40+1', '10000000000000000000000000000000000000001'),
            ('CASE WHEN true THEN 1e40 ELSE 2 END', '1e40'),
            ('COALESCE(NULL,1e40)', '1e40'),
        ):
            check('SELECT '+expression+' AS n', [[expected]], [1700], numeric=True)
            describe('SELECT '+expression+' AS n', [1700])
        # R32 covers metadata here. R34 separately fixes/tests arithmetic;
        # compat/scientific_numeric_scale_known_gap.py retains its original probe.
        describe('SELECT 1e-40+1e-40 AS n', [1700])
        check("SELECT '1e40' AS n", [['1e40']], [25])
        describe("SELECT '1e40' AS n", [25])
        check("INSERT INTO scientific_numeric_rows VALUES(201,'1e40') RETURNING n",
              [['1e40']], [1700], tag='INSERT 0 1', numeric=True)
        check("INSERT INTO scientific_numeric_rows VALUES(202,'1e40'::TEXT)", state='42804')
        check("UPDATE scientific_numeric_update SET n='1e40'::TEXT WHERE false", state='42804')
        check("INSERT INTO scientific_numeric_rows SELECT 203,'1e40'::TEXT WHERE false", state='42804')
        check("INSERT INTO scientific_numeric_rows VALUES(204,'1e+' )", state='22P02')
        check("UPDATE scientific_numeric_update SET n='1e+' WHERE false", state='22P02')
        check('INSERT INTO scientific_numeric_rows SELECT 205,1e40 WHERE false')
        check('UPDATE scientific_numeric_update SET n=1e40 WHERE false')
        check('SELECT 1e40 AS n WHERE false', [], [1700])
        check('SELECT 1e40 AS n LIMIT 0', [], [1700])
        describe("SELECT 1e40+nextval('scientific_numeric_calls') AS n", [1700])
        describe("INSERT INTO scientific_numeric_rows VALUES(nextval('scientific_numeric_calls'),1e40) RETURNING n", [1700])
        describe("UPDATE scientific_numeric_update SET n=1e40+nextval('scientific_numeric_calls') RETURNING n", [1700])
        check("SELECT currval('scientific_numeric_calls')", state='55000')
        check('SELECT count(*) FROM scientific_numeric_rows', [[str(len(LITERALS)+5)]], [20])
        persisted = [[str(index), literal] for index, literal in enumerate(LITERALS, 1)]
        persisted += [[str(index), literal] for index, literal in enumerate(('1e40', '-.5E-2', '1e500', '1E-500'), 101)]
        persisted += [['201', '1e40']]
        check('SELECT id,n FROM scientific_numeric_rows ORDER BY id', persisted, [23,1700], numeric=True)
        check('SELECT n FROM scientific_numeric_update WHERE id=1', [[LITERALS[-1]]], [1700], numeric=True)
        assert controls == 110, controls
        print('SCIENTIFIC_NUMERIC_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
