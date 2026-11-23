#!/usr/bin/env python3
"""Check composite primary keys across index pages and transaction snapshots."""

import importlib.util
from pathlib import Path
import select
import socket


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'composite_primary_key_wire', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    first = server['sock']
    second = None

    def check(frames, tag, rows=None, state=None, ready=b'I'):
        actual, code, message, _, command = runner.decode_wire_result(frames)
        assert (code, command, frames[-1]) == (state, tag, (b'Z', ready)), frames
        if rows is not None:
            assert actual == rows, (actual, rows, message)

    def execute(connection, sql, tag, **expected):
        check(client.simple_query(connection, sql), tag, **expected)

    prefix = 'shared-long-prefix-012345'
    insert = f"INSERT INTO wire_compound VALUES ('{prefix}', 199)"

    def start_waiting_insert():
        second.sendall(client.typed(b'Q', insert.encode() + b'\0'))
        assert not select.select([second], [], [], 0.25)[0]

    try:
        execute(first, 'CREATE TABLE wire_compound (a text, b integer, PRIMARY KEY(a,b))',
                'CREATE TABLE')
        for start in range(0, 160, 32):
            values = ','.join(f"('{prefix}', {key})" for key in range(start, start + 32))
            execute(first, 'INSERT INTO wire_compound VALUES ' + values, 'INSERT 0 32')
        execute(first, 'REINDEX TABLE wire_compound', 'REINDEX')
        for key in (0, 159):
            execute(first, f"INSERT INTO wire_compound VALUES ('{prefix}', {key})", None,
                    state='23505')

        second = socket.create_connection(('127.0.0.1', server['port']),
                                          timeout=runner.wire_timeout())
        client.startup(second, 'alice', 'info')
        execute(second, 'BEGIN ISOLATION LEVEL REPEATABLE READ', 'BEGIN', ready=b'T')
        execute(second, 'SELECT count(*) FROM wire_compound', 'SELECT 1',
                rows=[['160']], ready=b'T')
        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, insert, 'INSERT 0 1', ready=b'T')
        start_waiting_insert()
        execute(first, 'COMMIT', 'COMMIT')
        check(client.read_until_ready(second), None, state='23505', ready=b'E')
        execute(second, 'ROLLBACK', 'ROLLBACK')

        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, 'DELETE FROM wire_compound WHERE b=199', 'DELETE 1', ready=b'T')
        start_waiting_insert()
        execute(first, 'ROLLBACK', 'ROLLBACK')
        check(client.read_until_ready(second), None, state='23505')

        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, 'UPDATE wire_compound SET b=200 WHERE b=199', 'UPDATE 1', ready=b'T')
        start_waiting_insert()
        execute(first, 'COMMIT', 'COMMIT')
        check(client.read_until_ready(second), 'INSERT 0 1')
        execute(first, 'SELECT b FROM wire_compound WHERE b >= 159 ORDER BY b',
                'SELECT 3', rows=[['159'], ['199'], ['200']])

        execute(second, 'BEGIN ISOLATION LEVEL REPEATABLE READ', 'BEGIN', ready=b'T')
        execute(second, 'SELECT b FROM wire_compound WHERE b=150', 'SELECT 1',
                rows=[['150']], ready=b'T')
        execute(first, "INSERT INTO wire_compound VALUES ('new-prefix', 151)", 'INSERT 0 1')
        execute(second, "UPDATE wire_compound SET a='new-prefix', b=151 WHERE b=150",
                None, state='23505', ready=b'E')
        execute(second, 'ROLLBACK', 'ROLLBACK')

        execute(first, 'CREATE TABLE wire_collated (a text COLLATE nocase, b integer, PRIMARY KEY(a,b))',
                'CREATE TABLE')
        execute(first, "INSERT INTO wire_collated VALUES ('Apple', 1), ('apple', 2)", 'INSERT 0 2')
        execute(first, 'REINDEX TABLE wire_collated', 'REINDEX')
        execute(first, "INSERT INTO wire_collated VALUES ('aPPLE', 1)", None, state='23505')
        execute(first, 'UPDATE wire_collated SET b=1 WHERE b=2', None, state='23505')
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)
    print('[COMPOSITE PRIMARY KEY] split index, concurrent writes and old snapshot passed')


if __name__ == '__main__':
    main()
