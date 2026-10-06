#!/usr/bin/env python3
"""Failed implicit/explicit/extended starts preserve SQLSTATE and ownership."""
import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('begin_state_pgdiff',
        root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    second = None

    def query(sock, sql, rows=None, ready=b'I'):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None, (sql, result)
        assert messages[-1] == (b'Z', ready), (sql, messages)
        if rows is not None: assert result[0] == rows, (sql, result)
        return result

    try:
        first = server['sock']
        second = socket.create_connection(('127.0.0.1', server['port']), timeout=15)
        second.settimeout(15)
        client.startup(second, 'alice', 'info')
        query(first, 'CREATE TABLE begin_state_rows(id INT PRIMARY KEY);')
        query(first, 'INSERT INTO begin_state_rows VALUES(1);')
        query(first, 'CREATE FUNCTION begin_state_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO begin_state_rows(id) VALUES(arg); RETURN arg; END; $$;')
        query(second, 'PREPARE begin_state_prepared AS SELECT id FROM begin_state_rows;')
        query(second, 'SET lock_timeout=50;')
        query(first, 'BEGIN;', ready=b'T')
        query(first, 'ALTER TABLE begin_state_rows ADD COLUMN v INT;', ready=b'T')
        for sql in (
            'SELECT id FROM begin_state_rows;',
            'SELECT id FROM begin_state_rows WHERE id=1;',
            'SELECT begin_state_writer(90);',
            'INSERT INTO begin_state_rows(id) VALUES(91);',
            'EXPLAIN SELECT id FROM begin_state_rows;',
            'EXPLAIN ANALYZE SELECT begin_state_writer(92);',
            'EXECUTE begin_state_prepared;',
            'BEGIN;',
        ):
            messages = client.simple_query(second, sql)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] == '55P03', (sql, result)
            assert result[0] == [] and result[4] is None, (sql, result)
            assert not any(kind in (b'D', b'C') for kind, _ in messages), (sql, messages)
            assert messages[-1] == (b'Z', b'I'), (sql, messages)
            query(first, 'SELECT id FROM begin_state_rows;', [['1']], ready=b'T')
            print('[BEGIN ERROR STATE]', sql, '55P03 / owner remains T', flush=True)
        sql = 'SELECT id FROM begin_state_rows;'
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        second.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
            client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(second)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == '55P03' and result[0] == [], result
        assert result[4] is None and messages[-1] == (b'Z', b'I'), (result, messages)
        print('[BEGIN ERROR STATE] extended 55P03 / Sync I', flush=True)
        query(first, 'SELECT id FROM begin_state_rows;', [['1']], ready=b'T')
        query(first, 'ROLLBACK;')
        query(second, 'SELECT 1;', [['1']])
        query(second, 'SELECT id FROM begin_state_rows;', [['1']])
        query(second, 'SELECT begin_state_writer(2);', [['2']])
        query(first, 'SELECT id FROM begin_state_rows ORDER BY id;', [['1'], ['2']])
        print('[TRANSACTION BEGIN ERROR STATE PROTOCOL E2E] passed', flush=True)
    finally:
        if second is not None: second.close()
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
