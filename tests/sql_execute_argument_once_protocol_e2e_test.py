#!/usr/bin/env python3
"""SQL EXECUTE freezes each actual argument, including unused parameters."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('execute_once_runner', root / 'tests/compat/pg_diff_runner.py')
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
    prefix = 'execute_once_' + uuid.uuid4().hex[:12]
    table = prefix + '_rows'
    sequences, statements, functions, failures = [], [], [], []
    checks = 0

    def query(sql, rows=None, oids=None, headers=None, state=None, tag=None):
        nonlocal checks
        checks += 1
        messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        good = actual[1] == state and [body for kind, body in messages if kind == b'Z'] == [b'I']
        for expected, observed in [(rows, actual[0]), (oids, actual[5]), (headers, actual[3]), (tag, actual[4])]:
            if expected is not None:
                good = good and expected == observed
        print('SQL_EXECUTE_ONCE', sql, actual, 'PASS', good, flush=True)
        if not good:
            failures.append((sql, actual, rows, oids, headers, state, tag))

    def prepare(suffix, types, sql):
        name = prefix + '_' + suffix
        statements.append(name)
        query(f'PREPARE {name}({types}) AS {sql}')
        return name

    def sequence():
        name = prefix + '_seq' + str(len(sequences))
        sequences.append(name)
        query(f'CREATE SEQUENCE {name}')
        return name

    try:
        query(f'CREATE TABLE {table}(id INT PRIMARY KEY,v BIGINT)')
        query(f'INSERT INTO {table} VALUES(1,10),(2,20)')
        repeat = prepare('repeat', 'bigint', 'SELECT $1,$1')
        seq = sequence()
        query(f"EXECUTE {repeat}(nextval('{seq}'))", [['1', '1']], [20, 20], ['?column?', '?column?'], tag='SELECT 1')
        query(f"SELECT currval('{seq}')", [['1']], [20])
        query(f"EXECUTE {repeat}(nextval('{seq}'))", [['2', '2']], [20, 20])
        query(f"SELECT currval('{seq}')", [['2']], [20])
        seq = sequence()
        function = prefix + '_volatile'
        functions.append(function)
        query(f"CREATE FUNCTION {function}() RETURNS BIGINT LANGUAGE SQL VOLATILE AS $$SELECT nextval('{seq}')$$")
        query(f'EXECUTE {repeat}({function}())', [['1', '1']], [20, 20])
        query(f"SELECT currval('{seq}')", [['1']], [20])
        unused = prepare('unused', 'bigint', 'SELECT 7')
        seq = sequence()
        query(f"EXECUTE {unused}(nextval('{seq}'))", [['7']], [23])
        query(f"SELECT currval('{seq}')", [['1']], [20])
        empty = prepare('empty', 'bigint', f'SELECT $1 FROM {table} WHERE FALSE')
        seq = sequence()
        query(f"EXECUTE {empty}(nextval('{seq}'))", [], [20], tag='SELECT 0')
        query(f"SELECT currval('{seq}')", [['1']], [20])
        multi = prepare('multi', 'bigint', f'SELECT id,$1,$1 FROM {table} ORDER BY id')
        seq = sequence()
        query(f"EXECUTE {multi}(nextval('{seq}'))", [['1', '1', '1'], ['2', '1', '1']], [23, 20, 20], ['id', '?column?', '?column?'], tag='SELECT 2')
        query(f"SELECT currval('{seq}')", [['1']], [20])
        multiple = prepare('multiple', 'bigint,bigint', 'SELECT $2,$1,$2,$1')
        seq = sequence()
        query(f"EXECUTE {multiple}(nextval('{seq}'),nextval('{seq}'))", [['2', '1', '2', '1']], [20]*4)
        query(f"SELECT currval('{seq}')", [['2']], [20])
        query(f'EXECUTE {repeat}(NULL)', [[None, None]], [20, 20])
        text = prepare('text', 'text', "SELECT $1,$1,'$1'")
        query(f"EXECUTE {text}('a''b')", [["a'b", "a'b", '$1']], [25]*3)
        insert = prepare('insert', 'bigint', f'INSERT INTO {table} VALUES(3,$1),(4,$1) RETURNING id,v')
        seq = sequence()
        query(f"EXECUTE {insert}(nextval('{seq}'))", [['3', '1'], ['4', '1']], [23, 20], ['id', 'v'], tag='INSERT 0 2')
        query(f"SELECT currval('{seq}')", [['1']], [20])
        update = prepare('update', 'bigint', f'UPDATE {table} SET v=$1 WHERE id<=2 RETURNING id,v')
        seq = sequence()
        query(f"EXECUTE {update}(nextval('{seq}'))", [['1', '1'], ['2', '1']], [23, 20], tag='UPDATE 2')
        query(f"SELECT currval('{seq}')", [['1']], [20])
        invalid = prepare('invalid', 'bigint,integer', 'SELECT $1')
        for argument, state in [("'bad'", '22P02'), ('1/0', '22012'), ('missing_argument_column', '42703')]:
            seq = sequence()
            query(f"EXECUTE {invalid}(nextval('{seq}'),{argument})", state=state)
            query(f"SELECT currval('{seq}')", state='55000')
        seq = sequence()
        query(f"EXECUTE {unused}((SELECT nextval('{seq}')))", state='0A000')
        query(f"SELECT currval('{seq}')", state='55000')
        seq = sequence()
        query(f"EXECUTE {repeat}(nextval('{seq}'),1)", state='42601')
        query(f"SELECT currval('{seq}')", state='55000')
        for argument, state in [('sum(1)', '42803'), ('row_number() OVER()', '42P20'),
                                ('generate_series(1,1)', '0A000'), ("'1'::text", '42804')]:
            query(f'EXECUTE {repeat}({argument})', state=state)
        print('SQL_EXECUTE_ONCE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert not failures, failures
    finally:
        for name in statements:
            client.simple_query(sock, 'DEALLOCATE ' + name)
        for name in functions:
            client.simple_query(sock, 'DROP FUNCTION IF EXISTS ' + name + '()')
        client.simple_query(sock, 'DROP TABLE IF EXISTS ' + table)
        for name in sequences:
            client.simple_query(sock, 'DROP SEQUENCE IF EXISTS ' + name)
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
