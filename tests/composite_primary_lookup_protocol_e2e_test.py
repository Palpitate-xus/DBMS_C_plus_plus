#!/usr/bin/env python3
"""Check actual composite-primary access paths and old snapshots over the wire."""

import importlib.util
from pathlib import Path
import socket
from struct import pack


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'composite_lookup_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    typed = client.typed
    server = runner.start_ours(client)
    primary = server['sock']
    reader = None
    table = 'wire_compound_lookup'
    prefix = 'shared-store-prefix-012345'

    def observe(frames, tag, rows=None, ready=b'I'):
        data, error, message, _, command = runner.decode_wire_result(frames)
        assert (error, command, frames[-1]) == (None, tag, (b'Z', ready)), (message, frames)
        if rows is not None:
            assert data == rows, (data, rows)
        return data

    def sql(connection, statement, tag, rows=None, ready=b'I'):
        return observe(client.simple_query(connection, statement), tag, rows, ready)

    def stats(connection):
        rows = runner.decode_wire_result(
            client.simple_query(connection, 'SELECT * FROM pg_catalog.pg_stat_tables'))
        assert rows[1] is None, rows
        entry, = (row for row in rows[0] if row[0] == table)
        return [int(value) for value in entry[1:5]]

    def check_access(connection, action, expected):
        before = stats(connection)
        action()
        after = stats(connection)
        observed = [right - left for left, right in zip(before, after, strict=True)]
        assert observed == expected, (observed, expected)

    def point(store, code):
        return f"SELECT note FROM {table} WHERE shop='{store}' AND ord={code}"

    def bind(name, shop, order):
        value = shop.encode()
        integer = pack('!i', -1) if order is None else pack('!ii', 4, order)
        payload = (b'\0' + name + b'\0' + pack('!HHHI', 1, 1, 2, len(value)) +
                   value + integer + pack('!HH', 1, 1))
        primary.sendall(typed(b'B', payload) + typed(b'D', b'P\0') +
                        typed(b'E', b'\0' + pack('!I', 0)) + typed(b'S'))
        return client.read_until_ready(primary)

    try:
        sql(primary, f'CREATE TABLE {table} (shop text, ord int, note text, PRIMARY KEY(shop,ord))',
            'CREATE TABLE')
        for start in range(0, 128, 32):
            values = ','.join(f"('shop-{i:06}',{i},'ordinary')" for i in range(start, start + 32))
            sql(primary, f'INSERT INTO {table} VALUES ' + values, 'INSERT 0 32')
        sql(primary, f"INSERT INTO {table} VALUES ('{prefix}',1,NULL),('{prefix}',2,'NULL'),"
            "('',4,'empty')", 'INSERT 0 3')
        sql(primary, f'REINDEX TABLE {table}', 'REINDEX')
        check_access(primary, lambda: sql(primary, point(prefix, 1), 'SELECT 1', [[None]]),
                     [0, 0, 1, 1])
        check_access(primary, lambda: sql(primary, point(prefix, 3), 'SELECT 0', []),
                     [0, 0, 1, 0])
        check_access(primary, lambda: sql(primary, point('absent', 1), 'SELECT 0', []),
                     [0, 0, 1, 0])
        check_access(primary, lambda: sql(primary, point('', 4), 'SELECT 1', [['empty']]),
                     [0, 0, 1, 1])
        check_access(primary, lambda: sql(primary, point(prefix, 2) + ' AND note IS NULL',
                                         'SELECT 0', []), [0, 0, 1, 1])
        plan = sql(primary, 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) ' + point(prefix, 1), 'EXPLAIN')
        assert any('CompositePrimaryKeyScan (access=index' in line[0] for line in plan), plan
        check_access(primary, lambda: sql(primary, f"SELECT note FROM {table} WHERE shop='{prefix}' ORDER BY ord",
                                         'SELECT 2', [[None], ['NULL']]), [1, 131, 0, 0])

        name = b'compound_point'
        text = f'SELECT note FROM {table} WHERE ord=$2 AND shop=$1'.encode()
        primary.sendall(typed(b'P', name+b'\0'+text+b'\0'+pack('!HII', 2, 25, 23)) + typed(b'S'))
        assert client.read_until_ready(primary) == [(b'1', b''), (b'Z', b'I')]
        check_access(primary, lambda: observe(bind(name, prefix, 1), 'SELECT 1', [[None]]),
                     [0, 0, 1, 1])
        check_access(primary, lambda: observe(bind(name, '', 4), 'SELECT 1', [['empty']]),
                     [0, 0, 1, 1])
        observe(bind(name, prefix, None), 'SELECT 0', [])

        reader = socket.create_connection(('127.0.0.1', server['port']),
                                          timeout=runner.wire_timeout())
        client.startup(reader, 'alice', 'info')
        sql(reader, 'BEGIN ISOLATION LEVEL REPEATABLE READ', 'BEGIN', ready=b'T')
        sql(reader, point(prefix, 1), 'SELECT 1', [[None]], ready=b'T')
        sql(primary, f"UPDATE {table} SET ord=3 WHERE shop='{prefix}' AND ord=1", 'UPDATE 1')
        check_access(reader, lambda: sql(reader, point(prefix, 1), 'SELECT 1', [[None]], ready=b'T'),
                     [1, 131, 0, 0])
        sql(reader, point(prefix, 3), 'SELECT 0', [], ready=b'T')
        sql(reader, 'COMMIT', 'COMMIT')
        check_access(primary, lambda: sql(primary, point(prefix, 3), 'SELECT 1', [[None]]),
                     [0, 0, 1, 1])

        sql(primary, 'BEGIN ISOLATION LEVEL SERIALIZABLE', 'BEGIN', ready=b'T')
        check_access(primary, lambda: sql(primary, point(prefix, 3), 'SELECT 1', [[None]], ready=b'T'),
                     [1, 131, 0, 0])
        sql(primary, 'ROLLBACK', 'ROLLBACK')

        sql(primary, 'CREATE TABLE wire_compound_lookup_ci (shop text COLLATE nocase, ord int, '
            'PRIMARY KEY(shop,ord))', 'CREATE TABLE')
        sql(primary, "INSERT INTO wire_compound_lookup_ci VALUES ('Alpha',1)", 'INSERT 0 1')
        select = "SELECT shop FROM wire_compound_lookup_ci WHERE shop='ALPHA' AND ord=1"
        sql(primary, select, 'SELECT 1', [['Alpha']])
        plan = sql(primary, 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) ' + select, 'EXPLAIN')
        assert any('TableScan' in line[0] for line in plan), plan
    finally:
        if reader is not None:
            reader.close()
        runner.stop_ours(server)
    print('[COMPOSITE PRIMARY LOOKUP] actual index access, rebind and repeatable read passed')


if __name__ == '__main__':
    main()
