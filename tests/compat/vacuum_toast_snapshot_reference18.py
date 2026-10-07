#!/usr/bin/env python3
"""Two actual PG18 sessions verify lazy/established RR snapshot TOAST reads."""
import importlib.util
import os
from pathlib import Path
import socket


def main():
    spec = importlib.util.spec_from_file_location(
        'toast_snapshot_runner', Path(__file__).with_name('pg_diff_runner.py'))
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    host, port, user, database, password = runner._reference_connection_settings()
    connections = []
    schema = 'toast_snapshot_reference_' + str(os.getpid())

    def query(sock, sql, label=None):
        result = runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)
        print(label or sql, result, flush=True)
        assert result[1] is None, (label or sql, result)
        return result[0]

    def payload(seed):
        alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        state = seed
        value = []
        for _ in range(9000):
            state ^= (state << 13) & 0xffffffff
            state ^= state >> 17
            state ^= (state << 5) & 0xffffffff
            state &= 0xffffffff
            value.append(alphabet[state % len(alphabet)])
        return ''.join(value)

    try:
        for _ in range(2):
            sock = socket.create_connection((host, port), timeout=60)
            connections.append(sock)
            client.startup_reference(sock, user, database, password)
            runner.verify_reference_version(client, sock)
        reader, writer = connections
        query(writer, 'CREATE SCHEMA ' + schema)
        for sock in connections:
            query(sock, 'SET search_path=' + schema)
        query(writer, 'CREATE TABLE documents(id INT PRIMARY KEY,payload TEXT)')
        old, deleted, new = [payload(seed) for seed in
                             (0x10203040, 0x50607080, 0x90a0b0c0)]

        def seed_rows():
            for ident, value in ((1, old), (2, deleted)):
                query(writer, "INSERT INTO documents VALUES(%d,'%s')" %
                      (ident, value), 'INSERT EXACT 9000-BYTE PAYLOAD ' + str(ident))

        def update_rows():
            query(writer, "UPDATE documents SET payload='%s' WHERE id=1" %
                  new, 'UPDATE EXACT 9000-BYTE PAYLOAD')
            query(writer, 'DELETE FROM documents WHERE id=2')

        seed_rows()
        query(reader, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
        update_rows()
        assert query(reader, "SELECT id,length(payload),payload='%s' "
                     'FROM documents ORDER BY id' % new,
                     'LAZY SNAPSHOT EXACT NEW PAYLOAD') == [['1', '9000', 't']]
        query(reader, 'COMMIT')
        query(writer, 'TRUNCATE documents')
        seed_rows()
        query(reader, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
        assert query(reader, 'SELECT id,length(payload) FROM documents ORDER BY id') == [
            ['1', '9000'], ['2', '9000']]
        update_rows()
        query(writer, 'VACUUM documents')
        assert query(reader, "SELECT id,length(payload),payload=CASE id WHEN 1 "
                     "THEN '%s' ELSE '%s' END FROM documents ORDER BY id" %
                     (old, deleted), 'ESTABLISHED SNAPSHOT EXACT OLD PAYLOADS') == [
            ['1', '9000', 't'], ['2', '9000', 't']]
        query(reader, 'COMMIT')
        query(writer, 'VACUUM FULL documents')
        assert query(writer, 'SELECT id,length(payload) FROM documents ORDER BY id') == [
            ['1', '9000']]
        print('[PG18.6 RR SNAPSHOT TOAST REFERENCE] passed', flush=True)
    finally:
        if len(connections) == 2:
            client.simple_query(connections[0], 'ROLLBACK')
            client.simple_query(connections[1],
                                'DROP SCHEMA IF EXISTS ' + schema + ' CASCADE')
        for sock in connections:
            sock.close()


if __name__ == '__main__':
    main()
