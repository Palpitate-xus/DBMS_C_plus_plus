#!/usr/bin/env python3
"""Actual system-catalog values and all seven RowDescription fields.

The finite expectations were independently checked against PostgreSQL 18.6.
Only ordinary simple-query source metadata is exercised here.
"""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def field(name, relation, number, oid, size):
    return [name, relation, number, oid, size, -1, 0]


def cases(table):
    physical = [field('oid', 1247, 1, 26, 4), field('typname', 1247, 2, 19, 64),
                field('typlen', 1247, 5, 21, 2), field('typbyval', 1247, 6, 16, 1),
                field('typalign', 1247, 23, 18, 1), field('typstorage', 1247, 24, 18, 1),
                field('typcollation', 1247, 29, 26, 4)]
    result = []

    def add(name, sql, rows, fields, tag):
        result.append(dict(id=name, sql=sql, rows=rows, descriptors=fields, tag=tag, state=None))

    add('create', f'CREATE TABLE {table}(t text,v varchar(9),a text[])', [], [], 'CREATE TABLE')
    add('physical', 'SELECT oid,typname,typlen,typbyval,typalign,typstorage,typcollation FROM pg_type WHERE oid=25',
        [['25', 'text', '-1', 'f', 'i', 'x', '100']], physical, 'SELECT 1')
    add('aliases', 'SELECT t.typname AS label,t.typalign AS alignment,t.typcollation FROM pg_catalog.pg_type AS t WHERE t.oid IN(25,1009) ORDER BY t.oid',
        [['text', 'i', '100'], ['_text', 'i', '100']],
        [field('label', 1247, 2, 19, 64), field('alignment', 1247, 23, 18, 1), field('typcollation', 1247, 29, 26, 4)], 'SELECT 2')
    add('cte-aliases', 'WITH source AS(SELECT typname,typcollation FROM pg_type WHERE oid=25) SELECT label,locale_oid FROM source AS fields(label,locale_oid)',
        [['text', '100']], [field('label', 1247, 2, 19, 64), field('locale_oid', 1247, 29, 26, 4)], 'SELECT 1')
    add('computed', 'SELECT CAST(typname AS text) AS computed,typname FROM pg_type WHERE oid=25',
        [['text', 'text']], [field('computed', 0, 0, 25, -1), field('typname', 1247, 2, 19, 64)], 'SELECT 1')
    add('cte-shadow', "WITH pg_type AS(SELECT 'local'::text AS typname) SELECT typname FROM pg_type",
        [['local']], [field('typname', 0, 0, 25, -1)], 'SELECT 1')
    add('catalog-collations', 'SELECT attrelid,attname,attcollation FROM pg_attribute WHERE attrelid IN(1247,1249,1259) AND attnum>0 AND attcollation<>0 ORDER BY attrelid,attnum',
        [['1247', 'typname', '950'], ['1247', 'typdefaultbin', '950'], ['1247', 'typdefault', '950'],
         ['1249', 'attname', '950'], ['1249', 'attoptions', '950'], ['1249', 'attfdwoptions', '950'],
         ['1259', 'relname', '950'], ['1259', 'reloptions', '950'], ['1259', 'relpartbound', '950']],
        [field('attrelid', 1249, 1, 26, 4), field('attname', 1249, 2, 19, 64), field('attcollation', 1249, 20, 26, 4)], 'SELECT 9')
    add('user-collations', f"SELECT attname,attcollation FROM pg_attribute WHERE attrelid='{table}'::regclass AND attnum>0 ORDER BY attnum",
        [['t', '100'], ['v', '100'], ['a', '100']], [field('attname', 1249, 2, 19, 64), field('attcollation', 1249, 20, 26, 4)], 'SELECT 3')
    add('null-statistics', f"SELECT attstattarget FROM pg_attribute WHERE attrelid='{table}'::regclass AND attname='t'",
        [[None]], [field('attstattarget', 1249, 21, 21, 2)], 'SELECT 1')
    return result


def descriptors(messages):
    frames = [body for kind, body in messages if kind == b'T']
    assert len(frames) <= 1, 'unexpected multiple RowDescriptions'
    if not frames:
        return []
    body = frames[0]
    count = struct.unpack_from('!H', body)[0]
    offset = 2
    result = []
    for _ in range(count):
        end = body.index(b'\0', offset)
        name = body[offset:end].decode('utf-8')
        offset = end + 1
        result.append([name, *struct.unpack_from('!IhIhih', body, offset)])
        offset += 18
    assert offset == len(body), 'unconsumed RowDescription bytes'
    return result


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('catalog_metadata_runner', root / 'tests/compat/pg_diff_runner.py')
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
    table = 'catalog_metadata_' + uuid.uuid4().hex[:10]
    failures = []
    controls = cases(table)
    try:
        for item in controls:
            messages = client.simple_query(sock, item['sql'])
            actual = runner.decode_wire_result(messages, include_types=True)
            metadata = descriptors(messages)
            good = actual[0] == item['rows'] and actual[1] == item['state'] and actual[4] == item['tag']
            good = good and metadata == item['descriptors'] and [body for kind, body in messages if kind == b'Z'] == [b'I']
            print('SYSTEM_CATALOG_METADATA', item['id'], actual, metadata, 'PASS', good, flush=True)
            if not good:
                failures.append(dict(expected=item, actual=actual, descriptors=metadata,
                                     wire=[(kind.hex(), body.hex()) for kind, body in messages]))
        print('SYSTEM_CATALOG_METADATA_CHECKS', len(controls), 'FAILURES', failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, 'ROLLBACK')
            client.simple_query(sock, f'DROP TABLE IF EXISTS {table}')
        finally:
            if server:
                runner.stop_ours(server)
                assert server['process'].returncode == 0, ('owned Main terminal', server['process'].returncode)
            else:
                sock.close()


if __name__ == '__main__':
    main()
