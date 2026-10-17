#!/usr/bin/env python3
"""Temporal view materialization preserves real physical codecs and NULLs."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('temporal_view_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    controls, failures = 0, []
    fields = [('d', 1082, 4), ('t', 1083, 8), ('z', 1266, 12),
              ('ts', 1114, 8), ('tz', 1184, 8), ('i', 1186, 16), ('a', 1007, -1)]

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(mode, sql, expected, rows):
        nonlocal controls
        controls += 1
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', b'\0' + sql.encode() + b'\0\0\0')
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                packet += client.typed(b'B', b'\0\0' + struct.pack('!HHH', 0, 0, 0))
                packet += client.typed(b'D', b'P\0') + client.typed(b'E', b'\0\0\0\0\0')
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages)
        actual = client.row_description_fields(messages) if any(k == b'T' for k, _ in messages) else []
        metadata = [(f[0].decode(), f[3], f[4], f[5], f[6]) for f in actual]
        desired = [(name, oid, size, -1, 0) for name, oid, size in expected]
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = result[1] is None and metadata == desired and messages[-1][0] == b'Z'
        if mode == 'describe':
            good = good and not result[0] and not tags
        else:
            good = good and result[0] == rows and tags == ['SELECT ' + str(len(rows))]
        # Origin propagation is an independently recorded compatibility gap;
        # this regression owns physical lowering, value preservation and NULLs.
        print('TEMPORAL_VIEW_MATERIALIZATION', controls, mode, sql, 'match', good, 'result', result, 'metadata', metadata, flush=True)
        if not good:
            failures.append((mode, sql, result, rows, metadata, desired, tags))

    def query(table, expected, rows):
        for suffix, values in [('', rows), (' WHERE false', [])]:
            sql = 'SELECT * FROM ' + table + suffix
            for mode in ('simple', 'describe', 'execute'):
                check(mode, sql, expected, values)

    def phase(rows):
        query('temporal_view_source', fields, rows)
        for index, field in enumerate(fields):
            query('temporal_view_' + field[0], [field], [[r[index]] for r in rows])
        query('temporal_view_all', fields, rows)
        query('temporal_view_nested', fields, rows)

    try:
        if reference:
            setup('BEGIN')
            schema = 'temporal_views_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        setup('CREATE TEMP TABLE temporal_view_source(d DATE,t TIME,z TIMETZ,ts TIMESTAMP,tz TIMESTAMPTZ,i INTERVAL,a INTEGER[])')
        for name, _, _ in fields:
            setup('CREATE TEMP VIEW temporal_view_' + name + ' AS SELECT ' + name + ' FROM temporal_view_source')
        setup('CREATE TEMP VIEW temporal_view_all AS SELECT * FROM temporal_view_source')
        setup('CREATE TEMP VIEW temporal_view_nested AS SELECT * FROM temporal_view_all')
        phase([])
        setup('INSERT INTO temporal_view_source DEFAULT VALUES')
        phase([[None] * 7])
        setup('DELETE FROM temporal_view_source')
        setup("INSERT INTO temporal_view_source VALUES('2000-02-29','03:04:05',NULL,NULL,NULL,'1 day',ARRAY[1,NULL])")
        phase([['2000-02-29', '03:04:05', None, None, None, '1 day', '{1,NULL}']])
        setup('CREATE TEMP VIEW temporal_view_literal AS SELECT NULL::date AS d,NULL::time AS t,NULL::timetz AS z,NULL::timestamp AS ts,NULL::timestamptz AS tz,NULL::interval AS i,NULL::integer[] AS a')
        query('temporal_view_literal', fields, [[None] * 7])
        assert controls == 186, controls
        assert not failures, failures
        print('[TEMPORAL VIEW MATERIALIZATION] all 186 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, 'ROLLBACK')
            finally:
                sock.close()


if __name__ == '__main__':
    main()
