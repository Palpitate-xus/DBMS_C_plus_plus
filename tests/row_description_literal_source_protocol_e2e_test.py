#!/usr/bin/env python3
"""FROM in data/comments cannot attach a physical row-description origin."""
import importlib.util
from pathlib import Path
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('literal_origin_pgdiff', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def descriptions(messages):
        result = []
        for kind, payload in messages:
            if kind != b'T': continue
            count = struct.unpack('!H', payload[:2])[0]
            offset = 2
            columns = []
            for _ in range(count):
                end = payload.index(b'\0', offset)
                name = payload[offset:end].decode()
                offset = end + 1
                table, attribute, datatype = struct.unpack('!IhI', payload[offset:offset + 10])
                columns.append((name, table, attribute, datatype))
                offset += 18
            result.append(columns)
        return result

    def query(sql, rows=None):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages)
        assert result[1] is None, (sql, result)
        if rows is not None: assert result[0] == rows, (sql, result)
        return messages

    try:
        query('CREATE TABLE descriptor_source(id INT);')
        query('INSERT INTO descriptor_source VALUES(7);')
        messages = query('SELECT id FROM descriptor_source;', [['7']])
        physical = descriptions(messages)
        assert len(physical) == 1 and physical[0][0][1] != 0 and physical[0][0][2:] == (1, 23), physical
        for sql, value in (
            ("SELECT 'body from descriptor_source tail' AS id;", 'body from descriptor_source tail'),
            ("SELECT 'body from descriptor_source tail' AS id /* FROM descriptor_source */;", 'body from descriptor_source tail'),
            ("SELECT 11 AS id /* FROM descriptor_source */;", '11'),
            ("SELECT $$body from descriptor_source tail$$ AS id;", 'body from descriptor_source tail'),
        ):
            messages = query(sql, [[value]])
            origin = descriptions(messages)
            assert len(origin) == 1 and origin[0][0][1:3] == (0, 0), (sql, origin)
            server['sock'].sendall(client.typed(b'P', b'\0' + sql.encode() + b'\0\0\0') +
                client.typed(b'D', b'S\0') + client.typed(b'S'))
            messages = client.read_until_ready(server['sock'])
            result = runner.decode_wire_result(messages)
            assert result[1] is None and result[0] == [], (sql, result)
            origin = descriptions(messages)
            assert len(origin) == 1 and origin[0][0][1:3] == (0, 0), (sql, origin)
            print('[ROW DESCRIPTION LITERAL SOURCE]', sql, 'origin0/attribute0', flush=True)
        print('[ROW DESCRIPTION LITERAL SOURCE PROTOCOL E2E] passed', flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
