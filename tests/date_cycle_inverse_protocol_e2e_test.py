#!/usr/bin/env python3
"""Date inverse conversion remains correct at every supported 400-year cycle."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('date_cycle_runner', root/'tests/compat/pg_diff_runner.py')
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

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(sql, value, oid, parameter=None):
        nonlocal controls
        controls += 1
        if parameter is None:
            messages = client.simple_query(sock, sql)
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, oid)
            raw = parameter.encode()
            bind = b'\0\0' + struct.pack('!HHi', 0, 1, len(raw)) + raw + struct.pack('!HH', 1, 0)
            sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                         client.typed(b'D', b'P\0') + client.typed(b'E', b'\0\0\0\0\0') + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        actual = runner.decode_wire_result(messages, include_types=True)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        expected = ([[value]], None, '', ['v'], 'SELECT 1', [oid])
        size = 4 if oid in (23, 1082) else 8
        good = (actual == expected and len(fields) == 1 and fields[0] == (b'v', 0, 0, oid, size, -1, 0)
                and messages[-1] == (b'Z', b'T' if reference else b'I'))
        if parameter is not None:
            good = good and sum(kind == b'1' for kind, _ in messages) == 1 and sum(kind == b'2' for kind, _ in messages) == 1
        print('DATE_CYCLE_SQL', controls, sql, repr(parameter), 'match', good, 'actual', repr(actual), flush=True)
        if not good:
            failures.append((sql, parameter, actual, expected, fields))

    try:
        if reference:
            setup('BEGIN')
        setup("SET TIME ZONE 'UTC'")
        for year in range(400, 10000, 400):
            previous = f'{year-1:04d}-12-31'
            january = f'{year:04d}-01-01'
            check("SELECT DATE '" + previous + "'+1 AS v", january, 1082)
            check("SELECT DATE '" + january + "'-1 AS v", previous, 1082)
            check(f"SELECT DATE '{year:04d}-02-28'+1 AS v", f'{year:04d}-02-29', 1082)
            check(f"SELECT DATE '{year:04d}-02-29'+1 AS v", f'{year:04d}-03-01', 1082)
        for year in (400, 1600, 2000, 2400, 4000, 9600):
            for clock in ('23:59:59.9999995', '24:00:00'):
                for declaration, oid in [('timestamp', 1114), ('timestamptz', 1184)]:
                    zone = '+00' if oid == 1184 else ''
                    datum = f'{year-1:04d}-12-31 ' + clock + zone
                    expected = f'{year:04d}-01-01 00:00:00' + zone
                    check('SELECT ' + declaration + " '" + datum + "' AS v", expected, oid)
                    check('SELECT $1 AS v', expected, oid, parameter=datum)
        for sql, value, oid in [
                ("SELECT DATE '0001-01-01'+0 AS v", '0001-01-01', 1082),
                ("SELECT DATE '9999-12-31'+0 AS v", '9999-12-31', 1082),
                ("SELECT DATE '2000-01-01'-DATE '1999-12-31' AS v", '1', 23),
                ("SELECT DATE '2000-03-01'-DATE '2000-02-28' AS v", '2', 23),
                ("SELECT DATE '1900-02-28'+1 AS v", '1900-03-01', 1082),
                ("SELECT DATE '2000-02-28'+2 AS v", '2000-03-01', 1082),
                ("SELECT DATE '2000-03-01'-2 AS v", '2000-02-28', 1082),
                ('SELECT NULL::date+1 AS v', None, 1082)]:
            check(sql, value, oid)
        print('DATE_CYCLE_SQL_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert controls == 152, controls
        assert not failures, failures
        print('[DATE CYCLE INVERSE] all 152 controls passed', flush=True)
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
