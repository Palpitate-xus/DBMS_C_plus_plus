#!/usr/bin/env python3
"""Verify actual leading-key and integer-range scans with row visibility."""

import importlib.util
from pathlib import Path
import socket
from struct import pack
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'composite_range_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    typed = client.typed
    server = runner.start_ours(client)
    writer = server['sock']
    reader = None
    table = 'wire_compound_range'
    store = 'shared-store-prefix-20261123-A'
    nearby = 'shared-store-prefix-20261123-B'
    sparse = 'shared-store-prefix-20261123-C'
    select = f'SELECT ord, note FROM {table}'

    def observe(frames, tag, rows=None, ready=b'I'):
        actual, state, message, _, actual_tag = runner.decode_wire_result(frames)
        assert (state, actual_tag, frames[-1]) == (None, tag, (b'Z', ready)), (message, frames)
        if rows is not None:
            assert actual == rows, (actual, rows)
        return actual

    def sql(connection, statement, tag, rows=None, ready=b'I'):
        return observe(client.simple_query(connection, statement), tag, rows, ready)

    def stats(connection):
        result = runner.decode_wire_result(
            client.simple_query(connection, 'SELECT * FROM pg_catalog.pg_stat_tables'))
        assert result[1] is None, result
        entry, = (row for row in result[0] if row[0] == table)
        return [int(value) for value in entry[1:5]]

    def check_access(connection, action, expected):
        before = stats(connection)
        action()
        actual = [new - old for old, new in zip(before, stats(connection), strict=True)]
        assert actual == expected, (actual, expected)

    def lookup(shop, condition='', order='ord'):
        return select + f" WHERE shop='{shop}' {condition} ORDER BY {order}"

    def bind(shop, low, high, name=b'bounds'):
        shop = shop.encode()
        payload = b'\0' + name + b'\0' + pack('!HHHI', 1, 1, 3, len(shop)) + shop
        for value in (low, high):
            payload += pack('!i', -1) if value is None else pack('!iq', 8, value)
        payload += pack('!H', 0)
        writer.sendall(typed(b'B', payload) + typed(b'D', b'P\0') +
                       typed(b'E', b'\0' + pack('!I', 0)) + typed(b'S'))
        return client.read_until_ready(writer)

    try:
        sql(writer, f'CREATE TABLE {table} (ord bigint, shop text, note text)', 'CREATE TABLE')
        note = 'ordinary-' + 'x' * 190
        for start in range(0, 2048, 64):
            values = ','.join(f"({i},'regular-{i:05d}','{note}')" for i in range(start, start + 64))
            sql(writer, f'INSERT INTO {table} VALUES ' + values, 'INSERT 0 64')
        dense = ','.join(f"({i},'{store}','n{i}')" for i in range(-64, 64))
        sql(writer, f'INSERT INTO {table} VALUES ' + dense, 'INSERT 0 128')
        extras = (f"(0,'{nearby}','other'),(-9223372036854775808,'{sparse}','minimum'),"
                  f"(0,'{sparse}',NULL),(9223372036854775807,'{sparse}','maximum')")
        sql(writer, f'INSERT INTO {table} VALUES ' + extras, 'INSERT 0 4')
        sql(writer, f'ALTER TABLE {table} ADD PRIMARY KEY (shop,ord)', 'ALTER TABLE')
        assert int.from_bytes((Path(server['dir']) / 'info' / (table + '.idx')).read_bytes()[10:12],
                              sys.byteorder) == 0xC556
        range_query = lookup(store, 'AND -3 < ord AND ord <= 1', 'ord DESC')
        expected = [[str(i), f'n{i}'] for i in (1, 0, -1, -2)]
        check_access(writer, lambda: sql(writer, range_query, 'SELECT 4', expected), [0, 0, 1, 4])
        plan = sql(writer, 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) ' + range_query, 'EXPLAIN')
        assert any('CompositePrimaryKeyRangeScan (access=index' in row[0] for row in plan), plan
        between = lookup(store, 'AND ord BETWEEN -2 AND 1', 'ord DESC')
        check_access(writer, lambda: sql(writer, between, 'SELECT 4', expected), [0, 0, 1, 4])
        check_access(writer, lambda: sql(writer, lookup(sparse), 'SELECT 3',
                     [[str(-(2**63)), 'minimum'], ['0', None], [str(2**63 - 1), 'maximum']]),
                     [0, 0, 1, 3])
        check_access(writer, lambda: sql(writer, lookup('missing'), 'SELECT 0', []), [0, 0, 1, 0])
        check_access(writer, lambda: sql(writer, lookup(sparse, 'AND ord > 9223372036854775807'),
                     'SELECT 0', []), [0, 0, 1, 0])
        check_access(writer, lambda: sql(writer, lookup(store), 'SELECT 128',
                     [[str(i), f'n{i}'] for i in range(-64, 64)]), [1, 2180, 0, 0])

        top = lookup(store, order='ord DESC NULLS LAST') + ' LIMIT 3 OFFSET 2'
        check_access(writer, lambda: sql(writer, top, 'SELECT 3',
                     [[str(i), f'n{i}'] for i in (61, 60, 59)]), [0, 0, 1, 5])
        plan = sql(writer, 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) ' + top, 'EXPLAIN')
        assert any('keyOrder=desc' in row[0] for row in plan), plan
        assert any('presorted' in row[0] for row in plan), plan
        ascending = lookup(store, 'AND ord BETWEEN -30 AND 30') + ' LIMIT 2 OFFSET 1'
        check_access(writer, lambda: sql(writer, ascending, 'SELECT 2',
                     [['-29', 'n-29'], ['-28', 'n-28']]), [0, 0, 1, 3])
        residual = lookup(store, "AND note LIKE 'n63'", 'ord ASC') + ' LIMIT 1'
        check_access(writer, lambda: sql(writer, residual, 'SELECT 1',
                     [['63', 'n63']]), [1, 2180, 0, 0])

        params = select + " WHERE shop=$1 AND ord>=$2 AND $3>ord ORDER BY ord"
        writer.sendall(typed(b'P', b'bounds\0' + params.encode() + b'\0' +
                       pack('!HIII', 3, 25, 20, 20)) + typed(b'S'))
        assert client.read_until_ready(writer) == [(b'1', b''), (b'Z', b'I')]
        check_access(writer, lambda: observe(bind(store, -1, 2), 'SELECT 3',
                     [['-1', 'n-1'], ['0', 'n0'], ['1', 'n1']]), [0, 0, 1, 3])
        check_access(writer, lambda: observe(bind(nearby, -1, 2), 'SELECT 1',
                     [['0', 'other']]), [0, 0, 1, 1])
        observe(bind(store, None, 2), 'SELECT 0', [])
        limited = (select + " WHERE shop=$1 AND ord>=$2 AND $3>ord "
                   "ORDER BY ord DESC LIMIT 2 OFFSET 1")
        writer.sendall(typed(b'P', b'limited\0' + limited.encode() + b'\0' +
                       pack('!HIII', 3, 25, 20, 20)) + typed(b'S'))
        assert client.read_until_ready(writer) == [(b'1', b''), (b'Z', b'I')]
        check_access(writer, lambda: observe(bind(store, -40, 41, b'limited'), 'SELECT 2',
                     [['39', 'n39'], ['38', 'n38']]), [0, 0, 1, 3])
        check_access(writer, lambda: observe(bind(store, -64, -62, b'limited'), 'SELECT 1',
                     [['-64', 'n-64']]), [0, 0, 1, 2])

        reader = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(reader, 'alice', 'info')
        sql(reader, 'BEGIN ISOLATION LEVEL REPEATABLE READ', 'BEGIN', ready=b'T')
        sql(reader, range_query, 'SELECT 4', expected, ready=b'T')
        sql(writer, f"UPDATE {table} SET ord=1000 WHERE shop='{store}' AND ord=-1", 'UPDATE 1')
        check_access(reader, lambda: sql(reader, range_query, 'SELECT 4', expected, ready=b'T'),
                     [1, 2180, 0, 0])
        newest = lookup(store, order='ord DESC') + ' LIMIT 1'
        check_access(reader, lambda: sql(reader, newest, 'SELECT 1', [['63', 'n63']], ready=b'T'),
                     [1, 2180, 0, 0])
        sql(reader, 'COMMIT', 'COMMIT')
        check_access(writer, lambda: sql(writer, newest, 'SELECT 1', [['1000', 'n-1']]),
                     [0, 0, 1, 1])
        current = [[str(i), f'n{i}'] for i in (1, 0, -2)]
        check_access(writer, lambda: sql(writer, range_query, 'SELECT 3', current), [0, 0, 1, 3])
    finally:
        if reader is not None:
            reader.close()
        runner.stop_ours(server)
    print('[COMPOSITE PRIMARY RANGE] bounds, rebinding, ordered limits and snapshots passed')


if __name__ == '__main__':
    main()
