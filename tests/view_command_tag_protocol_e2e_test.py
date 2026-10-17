#!/usr/bin/env python3
"""View completion tags follow the parsed object, not the second SQL word."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('view_tag_runner', root/'tests/compat/pg_diff_runner.py')
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

    def request(mode, sql):
        if mode == 'simple':
            return client.simple_query(sock, sql)
        parse = b'\0' + sql.encode() + b'\0\0\0'
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'E', b'\0\0\0\0\0') + client.typed(b'S'))
        return client.read_until_ready(sock)

    def setup(sql):
        result = runner.decode_wire_result(request('simple', sql))
        assert result[1] is None, (sql, result)

    def check(mode, sql, tag, rows=None):
        nonlocal controls
        controls += 1
        messages = request(mode, sql)
        result = runner.decode_wire_result(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        expected = [] if rows is None else rows
        good = result[1] is None and result[0] == expected and tags == [tag] and messages[-1][0] == b'Z'
        if mode != 'simple':
            good = good and sum(k == b'1' for k, _ in messages) == 1 and sum(k == b'2' for k, _ in messages) == 1
        print('VIEW_COMMAND_TAG', controls, mode, sql, 'match', good, 'result', result, 'tags', tags, flush=True)
        if not good:
            failures.append((mode, sql, tag, expected, result, tags))

    try:
        if reference:
            setup('BEGIN')
            schema = 'view_tags_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        setup('CREATE TEMP TABLE view_tag_source(id INTEGER)')
        setup('INSERT INTO view_tag_source VALUES(7)')
        prefixes = ['CREATE VIEW', 'CREATE OR REPLACE VIEW', 'CREATE TEMP VIEW',
                    'CREATE TEMPORARY VIEW', 'CREATE OR REPLACE TEMP VIEW',
                    'CREATE OR REPLACE TEMPORARY VIEW', '/* lead */ CREATE TEMP VIEW',
                    'CREATE /* kind */ TEMP VIEW', 'CREATE OR /* replace */ REPLACE TEMPORARY VIEW']
        for index, prefix in enumerate(prefixes):
            for mode in ('simple', 'extended'):
                name = 'tag_view_' + str(index) + '_' + mode
                check(mode, prefix + ' ' + name + ' AS SELECT id FROM view_tag_source', 'CREATE VIEW')
                check(mode, 'SELECT id FROM ' + name, 'SELECT 1', [['7']])
                check(mode, 'DROP VIEW ' + name, 'DROP VIEW')
        # Materialized views/CTAS have a separate completion-tag owner.
        assert controls == 54, controls
        assert not failures, failures
        print('[VIEW COMMAND TAG] all 54 controls passed', flush=True)
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
