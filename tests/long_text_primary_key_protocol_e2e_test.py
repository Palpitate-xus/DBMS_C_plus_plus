#!/usr/bin/env python3
"""Check concurrent uniqueness for text primary keys with a shared prefix."""

import importlib.util
from pathlib import Path
import select
import socket


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'text_primary_key_wire', root / 'tests/compat/pg_diff_runner.py')
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

    prefix = 'prefixabcdefghijklmnopqrst-'
    insert = f"INSERT INTO write_text_pk VALUES ('{prefix}A', 2)"

    def try_insert_while_key_is_locked():
        second.sendall(client.typed(b'Q', insert.encode() + b'\0'))
        assert not select.select([second], [], [], 0.25)[0]

    try:
        execute(first, 'CREATE TABLE write_text_pk (k text PRIMARY KEY, v integer)', 'CREATE TABLE')
        second = socket.create_connection(('127.0.0.1', server['port']),
                                          timeout=runner.wire_timeout())
        client.startup(second, 'alice', 'info')
        execute(second, 'BEGIN ISOLATION LEVEL REPEATABLE READ', 'BEGIN', ready=b'T')
        execute(second, f"SELECT v FROM write_text_pk WHERE k='{prefix}A'",
                'SELECT 0', rows=[], ready=b'T')
        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, f"INSERT INTO write_text_pk VALUES ('{prefix}A', 1)",
                'INSERT 0 1', ready=b'T')
        try_insert_while_key_is_locked()
        execute(first, 'COMMIT', 'COMMIT')
        check(client.read_until_ready(second), None, state='23505', ready=b'E')
        execute(second, 'ROLLBACK', 'ROLLBACK')
        execute(first, 'SELECT v FROM write_text_pk ORDER BY v', 'SELECT 1', rows=[['1']])

        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, f"DELETE FROM write_text_pk WHERE k='{prefix}A'", 'DELETE 1', ready=b'T')
        try_insert_while_key_is_locked()
        execute(first, 'ROLLBACK', 'ROLLBACK')
        check(client.read_until_ready(second), None, state='23505')

        execute(first, 'BEGIN', 'BEGIN', ready=b'T')
        execute(first, f"UPDATE write_text_pk SET k='{prefix}B' WHERE k='{prefix}A'",
                'UPDATE 1', ready=b'T')
        try_insert_while_key_is_locked()
        execute(first, 'COMMIT', 'COMMIT')
        check(client.read_until_ready(second), 'INSERT 0 1')
        execute(first, f"SELECT v FROM write_text_pk WHERE k='{prefix}A'", 'SELECT 1', rows=[['2']])
        execute(first, f"SELECT v FROM write_text_pk WHERE k='{prefix}B'", 'SELECT 1', rows=[['1']])
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)
    print('[LONG TEXT PRIMARY KEY] concurrent insert, rollback and key replacement passed')


if __name__ == '__main__':
    main()
