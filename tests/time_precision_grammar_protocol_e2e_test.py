#!/usr/bin/env python3
"""TIME keyword precision syntax fails before descriptions or execution.

Scope is the unquoted, unqualified TIME grammar. Named-type negative
precision semantics and precision-7 clamp/warnings remain separate gaps.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_grammar_runner', root / 'tests/compat/pg_diff_runner.py')
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

    def check(sql, mode, state=None, precision=None, parse_only=False, value=None):
        nonlocal checks
        checks += 1
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0))
            if not parse_only:
                if mode == 'describe':
                    packet += client.typed(b'D', b'S\0')
                else:
                    packet += client.typed(b'B', b'\0\0' + struct.pack('!HHHH', 0, 0, 1, 0))
                    packet += client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        rows = client.data_row_values(messages)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = messages[-1] == (b'Z', b'I')
        if state:
            good = good and [error.get(b'C', b'').decode() for error in errors] == [state]
            good = good and not fields and not rows and not tags and not any(kind in (b'1', b'2') for kind, _ in messages)
        elif parse_only:
            # This proves legal grammar only. PG warns and clamps 7 while
            # the project's missing warning/typmod semantics stay recorded.
            good = good and not errors and not fields and not rows and not tags
            good = good and sum(kind == b'1' for kind, _ in messages) == 1
        else:
            good = good and not errors and fields == [(b'v', 0, 0, 1083, 8, precision, 0)]
            if mode == 'describe':
                good = good and not rows and not tags and [body for kind, body in messages if kind == b't'] == [b'\0\0']
            else:
                good = good and rows == [[None if value is None else value.encode()]] and tags == ['SELECT 1']
        print('TIME_PRECISION_GRAMMAR', checks, mode, sql, 'match', good, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, mode, fields, rows, tags, errors))

    try:
        for modifier in ('-1', '-0', '+3', '6.5', '1,2', "'3'", '"3"', 'NULL', '2147483648',
                         '0x80000000', '0b10000000000000000000000000000000', '999999999999999999999999'):
            target = 'TIME(' + modifier + ')'
            queries = ["SELECT CAST('00:00:00' AS " + target + ') AS v',
                       "SELECT '00:00:00'::" + target + ' AS v',
                       'SELECT ' + target + " '00:00:00' AS v",
                       'SELECT CAST(NULL AS ' + target + ') AS v',
                       'SELECT CAST(NULL AS ' + target + '[]) AS v',
                       "SELECT CAST('00:00:00' AS " + target + ') AS v WHERE false']
            for sql in queries:
                for mode in ('simple', 'describe', 'execute'):
                    check(sql, mode, state='42601')
            for mode in ('simple', 'describe', 'execute'):
                check('CREATE TEMP TABLE invalid_time_precision_' + str(checks) + '(v ' + target + ')', mode, state='42601')
        lexical = ('0x3', '0X3', '0b11', '0B11', '0o3', '0O3', '0_3', '0x0_3', '0b1_1', '0o0_3', '0000000000000000000003')
        for modifier, precision in [(p, int(p)) for p in ('0', '1', '3', '6', '0003')] + [(p, 3) for p in lexical]:
            for mode in ('simple', 'describe', 'execute'):
                check('SELECT CAST(NULL AS TIME(' + modifier + ')) AS v', mode, precision=precision)
        for target in ('TIME(7)', 'TIME(7)[]', 'TIME(3_0)', 'TIME(2147483647)'):
            check('SELECT CAST(NULL AS ' + target + ') AS v', 'describe', parse_only=True)
        for modifier in lexical:
            for mode in ('simple', 'execute'):
                check("SELECT CAST('00:00:00.654321' AS TIME(" + modifier + ')) AS v', mode, precision=3, value='00:00:00.654')
        print('TIME_PRECISION_GRAMMAR_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 326, checks
        assert not failures, failures
        print('[TIME PRECISION GRAMMAR] all 326 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()  # Bad DDL is TEMP even on a faulty old program.


if __name__ == '__main__':
    main()
