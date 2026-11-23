#!/usr/bin/env python3
"""Check complete-key mutation probes and locked-row requalification."""

import importlib.util
from pathlib import Path
import select
import socket
from struct import pack


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        'composite_mutation_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    typed = client.typed
    first = server['sock']
    second = None
    table = 'wire_compound_mutation'
    prefix = 'shared-store-prefix-012345'

    def observe(frames, tag, rows=None, state=None, ready=b'I'):
        data, code, message, _, command = runner.decode_wire_result(frames)
        assert (code, command, frames[-1]) == (state, tag, (b'Z', ready)), (message, frames)
        if rows is not None:
            assert data == rows, (data, rows)
        return data

    def sql(connection, statement, tag, **expected):
        return observe(client.simple_query(connection, statement), tag, **expected)

    def stats(relation):
        frames = client.simple_query(first, 'SELECT * FROM pg_catalog.pg_stat_tables')
        rows, state, message, _, tag = runner.decode_wire_result(frames)
        assert (state, tag, frames[-1]) == (None, f'SELECT {len(rows)}', (b'Z', b'I')), message
        entry, = (row for row in rows if row[0] == relation)
        return [int(value) for value in entry[1:5]]

    def check_access(action, reads, relation=table):
        before = stats(relation)
        action()
        observed = [right - left for left, right in zip(before, stats(relation), strict=True)]
        assert observed == [0, 0, 1, reads], observed

    def where(order, shop=prefix):
        return f" WHERE shop='{shop}' AND ord={order}"

    def bind(name, shop, order):
        value = shop.encode()
        payload = (b'\0' + name + b'\0' + pack('!HHHI', 1, 1, 2, len(value)) +
                   value + pack('!iiHH', 4, order, 1, 1))
        first.sendall(typed(b'B', payload) + typed(b'D', b'P\0') +
                      typed(b'E', b'\0' + pack('!I', 0)) + typed(b'S'))
        return client.read_until_ready(first)

    def prepare(name, statement):
        first.sendall(typed(b'P', name + b'\0' + statement.encode() + b'\0' +
                            pack('!HII', 2, 25, 23)) + typed(b'S'))
        assert client.read_until_ready(first) == [(b'1', b''), (b'Z', b'I')]

    def overlap(statement, isolation='READ COMMITTED'):
        sql(second, 'BEGIN ISOLATION LEVEL ' + isolation, 'BEGIN', ready=b'T')
        second.sendall(typed(b'Q', statement.encode() + b'\0'))
        assert not select.select([second], [], [], 0.25)[0]

    try:
        sql(first, f'CREATE TABLE {table} (shop text, ord int, note text, PRIMARY KEY(shop,ord))',
            'CREATE TABLE')
        for start in range(0, 128, 32):
            values = ','.join(f"('shop-{i:06}',{i},'ordinary')" for i in range(start, start + 32))
            sql(first, f'INSERT INTO {table} VALUES ' + values, 'INSERT 0 32')
        sql(first, f"INSERT INTO {table} VALUES ('{prefix}',1,NULL),('{prefix}',2,'two'),"
            "('',4,'empty'),('O''Brien',5,'quoted')", 'INSERT 0 4')
        sql(first, f'REINDEX TABLE {table}', 'REINDEX')
        update = f'UPDATE {table}'
        delete = f'DELETE FROM {table}'
        check_access(lambda: sql(first, update+" SET note='changed'"+where(1)+' RETURNING note',
                                 'UPDATE 1', rows=[['changed']]), 2)
        check_access(lambda: sql(first, delete+where(3)+' RETURNING note', 'DELETE 0', rows=[]), 2)
        check_access(lambda: sql(first, update+" SET note='wrong'"+where(3, 'absent'),
                                 'UPDATE 0'), 0)
        check_access(lambda: sql(first, update+" SET note='quote kept'"+where(5, "O''Brien")+
                                 ' RETURNING note', 'UPDATE 1', rows=[['quote kept']]), 1)
        prepare(b'write_order', update+" SET note='binary' WHERE shop=$1 AND ord=$2 RETURNING note")
        prepare(b'remove_order', delete+" WHERE ord=$2 AND shop=$1 RETURNING note")
        check_access(lambda: observe(bind(b'write_order', prefix, 2), 'UPDATE 1', rows=[['binary']]), 2)
        check_access(lambda: observe(bind(b'remove_order', '', 4), 'DELETE 1', rows=[['empty']]), 1)

        second = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(second, 'alice', 'info')
        sql(first, 'BEGIN', 'BEGIN', ready=b'T')
        sql(first, update+' SET ord=3'+where(1), 'UPDATE 1', ready=b'T')
        overlap(delete+where(1)+' RETURNING note')
        sql(first, 'COMMIT', 'COMMIT')
        observe(client.read_until_ready(second), 'DELETE 0', rows=[], ready=b'T')
        sql(second, 'COMMIT', 'COMMIT')
        sql(first, 'SELECT note FROM '+table+where(3), 'SELECT 1', rows=[['changed']])

        sql(first, 'BEGIN', 'BEGIN', ready=b'T')
        sql(first, update+" SET note='first'"+where(3), 'UPDATE 1', ready=b'T')
        overlap(update+" SET note=note || '-second'"+where(3)+' RETURNING note')
        sql(first, 'COMMIT', 'COMMIT')
        observe(client.read_until_ready(second), 'UPDATE 1', rows=[['first-second']], ready=b'T')
        sql(second, 'COMMIT', 'COMMIT')

        sql(first, 'BEGIN', 'BEGIN', ready=b'T')
        sql(first, update+" SET note='last'"+where(3), 'UPDATE 1', ready=b'T')
        overlap(delete+where(3), isolation='REPEATABLE READ')
        sql(first, 'COMMIT', 'COMMIT')
        observe(client.read_until_ready(second), None, state='40001', ready=b'E')
        sql(second, 'ROLLBACK', 'ROLLBACK')
        sql(first, 'SELECT note FROM '+table+where(3), 'SELECT 1', rows=[['last']])

        sql(first, 'CREATE TABLE wire_compound_separator ("Shop code" text, "Order no" text, '
            'note text, PRIMARY KEY("Shop code","Order no"))', 'CREATE TABLE')
        sql(first, "INSERT INTO wire_compound_separator VALUES "
            "('a','b' || chr(1) || 'c','first'),('a' || chr(1) || 'b','c','second')", 'INSERT 0 2')
        collision = '''DELETE FROM wire_compound_separator WHERE "Shop code"='a' ''' \
            '''AND "Order no"='b\x01c' RETURNING note'''
        check_access(lambda: sql(first, collision, 'DELETE 1', rows=[['first']]),
                     2, relation='wire_compound_separator')
        sql(first, 'SELECT note FROM wire_compound_separator', 'SELECT 1', rows=[['second']])
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)
    print('[COMPOSITE PRIMARY MUTATION] checked index probes, binary Bind and row-lock rechecks passed')


if __name__ == '__main__':
    main()
