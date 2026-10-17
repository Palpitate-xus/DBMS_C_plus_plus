#!/usr/bin/env python3
"""A table-backed NULL::timetz is a cast expression, not a column name."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('timetz_postfix_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(sql, mode, oid, length, empty):
        nonlocal checks
        checks += 1
        code = int(mode == 'binary')
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0))
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                packet += client.typed(b'B', b'\0\0' + struct.pack('!HHHH', 0, 0, 1, code))
                packet += client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = fields == [(b'z', 0, 0, oid, length, -1, code)] and not errors
        good = good and messages[-1] == (b'Z', b'T' if reference else b'I')
        if mode == 'describe':
            good = good and not rows and not tags and [body for kind, body in messages if kind == b't'] == [b'\0\0']
        else:
            good = good and rows == ([] if empty else [[None]]) and tags == ['SELECT ' + ('0' if empty else '1')]
        if mode != 'simple':
            good = good and sum(kind == b'1' for kind, _ in messages) == 1
            if mode != 'describe':
                good = good and sum(kind == b'2' for kind, _ in messages) == 1
        print('TIMETZ_POSTFIX_PROJECTION', checks, mode, sql, 'match', good, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, mode, fields, rows, tags, errors))

    try:
        if reference:
            setup('BEGIN')
            schema = 'timetz_postfix_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        for temporary in (False, True):
            table = 'timetz_cast_' + ('temp' if temporary else 'ordinary')
            setup('CREATE ' + ('TEMP ' if temporary else '') + 'TABLE ' + table + '(z TIMETZ)')
            setup('INSERT INTO ' + table + ' DEFAULT VALUES')
            for expression, oid, length in [('NULL::timetz', 1266, 12), ('NULL::TIMETZ', 1266, 12),
                    ('CAST(NULL AS timetz)', 1266, 12), ('CAST(NULL AS TIMETZ)', 1266, 12),
                    ('NULL::time', 1083, 8), ('NULL::timestamptz', 1184, 8)]:
                for empty in (False, True):
                    sql = 'SELECT ' + expression + ' AS z FROM ' + table + (' WHERE false' if empty else '')
                    for mode in ('simple', 'describe', 'text', 'binary'):
                        check(sql, mode, oid, length, empty)
        print('TIMETZ_POSTFIX_PROJECTION_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 96, checks
        assert not failures, failures
        print('[TIMETZ POSTFIX PROJECTION] all 96 controls passed', flush=True)
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
