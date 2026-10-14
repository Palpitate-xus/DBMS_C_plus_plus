#!/usr/bin/env python3
"""BIT/VARBIT RowDescription modifiers belong to declarations, not text values."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('bit_modifier_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def check(sql, rows, fields, extended=False):
        nonlocal checks
        if extended:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
            bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
            sock.sendall(client.typed(b'P', parse) + client.typed(b'D', b'S\0') +
                         client.typed(b'B', bind) + client.typed(b'D', b'P\0') +
                         client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        descriptions = [[(field[0].decode(),) + field[3:] for field in client.row_description_fields([message])]
                        for message in messages if message[0] == b'T']
        valid = (result[1] is None and result[0] == rows and
                 result[4] == 'SELECT ' + str(len(rows)) and
                 len(descriptions) == (2 if extended else 1) and
                 all(description == fields for description in descriptions))
        checks += 1
        print('BIT_MODIFIER', 'EXTENDED' if extended else 'SIMPLE', sql, result,
              descriptions, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, extended, result, descriptions, rows, fields))

    def ddl(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    try:
        for length in (1, 5, 65):
            for type_name, oid in (('bit', 1560), ('varbit', 1562), ('bit varying', 1562)):
                for expression, value in (("'1'::" + type_name + '(' + str(length) + ')',
                                           '1' + '0' * (length - 1) if oid == 1560 else '1'),
                                          ('CAST(NULL AS ' + type_name + '(' + str(length) + '))', None)):
                    for extended in (False, True):
                        check('SELECT ' + expression + ' AS value', [[value]],
                              [('value', oid, -1, length, 0)], extended)
            for type_name, oid in (('bit', 1561), ('varbit', 1563)):
                for extended in (False, True):
                    check('SELECT NULL::' + type_name + '(' + str(length) + ')[] AS value', [[None]],
                          [('value', oid, -1, length, 0)], extended)
        for extended in (False, True):
            check("SELECT B'10101'::bit(5) AS fixed, NULL::varbit(7) AS varying", [['10101', None]],
                  [('fixed', 1560, -1, 5, 0), ('varying', 1562, -1, 7, 0)], extended)
            check("SELECT NULL::bit(5) AS value WHERE false", [], [('value', 1560, -1, 5, 0)], extended)
            check("SELECT NULL::pg_catalog.bit(5) AS value", [[None]], [('value', 1560, -1, 5, 0)], extended)
        ddl('CREATE TEMP TABLE bit_modifier_rows(f BIT(5),v VARBIT(7),a BIT(3)[],u VARBIT[],cap VARBIT(8388608))')
        physical = [('f', 1560, -1, 5, 0), ('v', 1562, -1, 7, 0), ('a', 1561, -1, 3, 0),
                    ('u', 1563, -1, -1, 0), ('cap', 1562, -1, 8388608, 0)]
        for extended in (False, True):
            check('SELECT f,v,a,u,cap FROM bit_modifier_rows', [], physical, extended)
        ddl('INSERT INTO bit_modifier_rows VALUES(NULL,NULL,NULL,NULL,NULL)')
        ddl('ALTER TABLE bit_modifier_rows ADD COLUMN other INT')
        for extended in (False, True):
            check('SELECT f,v,a,u,cap FROM bit_modifier_rows', [[None] * 5], physical, extended)
        ddl('ALTER TABLE bit_modifier_rows ALTER COLUMN f TYPE BIT(9)')
        physical[0] = ('f', 1560, -1, 9, 0)
        for extended in (False, True):
            check('SELECT f,v,a,u,cap FROM bit_modifier_rows', [[None] * 5], physical, extended)
        print('BIT_MODIFIER_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 60 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
