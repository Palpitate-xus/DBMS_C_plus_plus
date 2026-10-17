#!/usr/bin/env python3
"""Actual TIME OID binary Bind accepts microseconds and 24h, with typed errors.

Receiver-only scope: no claim for array/TIMETZ receivers, epoch timestamp
receivers, fractional TIME heap storage, or malformed outer wire framing.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

from time_input_normalization_protocol_e2e_test import clock_text


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_binary_bind_runner', root / 'tests/compat/pg_diff_runner.py')
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

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print('TIME_BINARY_PARAMETER', checks, label, 'match', good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def exchange(packet):
        sock.sendall(packet + client.typed(b'S'))
        return client.read_until_ready(sock)

    def bind(raw, output, statement=b'\0'):
        value = struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        return client.typed(b'B', b'\0' + statement + struct.pack('!HHH', 1, 1, 1) + value +
                            struct.pack('!HH', 1, output))

    def check(raw, expected, output, sql='SELECT $1 AS v', precision=-1,
              state=None, oid=1083, size=8, text=None, binary=None):
        packet = client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, oid))
        packet += bind(raw, output) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
        messages = exchange(packet)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        kinds = [kind for kind, _ in messages]
        good = messages[-1] == (b'Z', b'I') and kinds.count(b'1') == 1 and b'N' not in kinds
        if state:
            good = good and [error.get(b'C', b'').decode() for error in errors] == [state]
            good = good and not fields and not rows and not tags and b'2' not in kinds
            recovery = client.simple_query(sock, 'SELECT 1 AS recovered')
            good = good and recovery[-1] == (b'Z', b'I') and client.data_row_values(recovery) == [[b'1']]
            good = good and not any(kind == b'E' for kind, _ in recovery)
        else:
            if oid == 1083:
                text = None if expected is None else clock_text(expected).encode()
                binary = None if expected is None else struct.pack('!q', expected)
            value = binary if output else text
            good = good and not errors and fields == [(b'v', 0, 0, oid, size, precision, output)]
            good = good and rows == [[value]] and tags == ['SELECT 1'] and kinds.count(b'2') == 1
        report(good, str(output) + ' ' + sql + ' input=' + str(None if raw is None else raw.hex()),
               (errors, fields, rows, tags))

    try:
        values = (0, 1, 2, 5, 9, 10, 11, 99, 100, 101, 499, 500, 501, 999,
                  1000, 1001, 123456, 123457, 500000, 999999, 1000000,
                  3723000001, 86399999999, 86400000000, None)
        for value in values:
            for output in (0, 1):
                check(None if value is None else struct.pack('!q', value), value, output)
        # Literal independent precision vectors: binary input already is an
        # exact integer microsecond, so no extra text-input rounding stage.
        vectors = ((500000, (1000000, 500000, 500000, 500000, 500000, 500000, 500000)),
                   (500, (0, 0, 0, 1000, 500, 500, 500)),
                   (86399999999, (86400000000, 86400000000, 86400000000, 86400000000,
                                  86400000000, 86400000000, 86399999999)))
        for value, expected in vectors:
            for precision in range(7):
                for output in (0, 1):
                    check(struct.pack('!q', value), expected[precision], output,
                          sql='SELECT CAST($1 AS TIME(' + str(precision) + ')) AS v', precision=precision)
        for value, expected in ((500, 1000), (123457, 123000), (86399999999, 86400000000), (None, None)):
            for sql in ('SELECT CAST($1 AS TIME(3)) AS v', 'SELECT $1::TIME(3) AS v'):
                for output in (0, 1):
                    check(None if value is None else struct.pack('!q', value), expected, output, sql=sql, precision=3)

        name = b'r128_time_binary_rebind\0'
        messages = exchange(client.typed(b'P', name + b'SELECT $1 AS v\0' + struct.pack('!HI', 1, 1083)))
        kinds = [kind for kind, _ in messages]
        report(messages[-1] == (b'Z', b'I') and kinds.count(b'1') == 1 and
               not any(kind in (b'E', b'T', b'D', b'N', b'C') for kind in kinds), 'named Parse', messages)
        messages = exchange(client.typed(b'D', b'S' + name))
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        report(messages[-1] == (b'Z', b'I') and
               fields == [(b'v', 0, 0, 1083, 8, -1, 0)] and
               [body for kind, body in messages if kind == b't'] == [struct.pack('!HI', 1, 1083)] and
               not any(kind in (b'E', b'N') for kind, _ in messages), 'named Describe', messages)
        for value in (0, 123457, 86400000000, None):
            for output in (0, 1):
                raw = None if value is None else struct.pack('!q', value)
                messages = exchange(bind(raw, output, name) + client.typed(b'D', b'P\0') +
                                    client.typed(b'E', b'\0' + struct.pack('!I', 0)))
                fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
                wanted = None if value is None else raw if output else clock_text(value).encode()
                report(messages[-1] == (b'Z', b'I') and
                       fields == [(b'v', 0, 0, 1083, 8, -1, output)] and
                       client.data_row_values(messages) == [[wanted]] and
                       [body for kind, body in messages if kind == b'C'] == [b'SELECT 1\0'] and
                       sum(kind == b'2' for kind, _ in messages) == 1 and
                       not any(kind in (b'E', b'N') for kind, _ in messages),
                       'named rebind ' + str(value) + ' output=' + str(output), messages)

        invalid = [(b'\0' * length, '08P01') for length in range(8)]
        invalid += [(b'\0' * length, '22P03') for length in (9, 16)]
        invalid += [(struct.pack('!Q', value), '22008') for value in
                    (86400000001, (1 << 63) - 1, 1 << 63, (1 << 64) - 1)]
        invalid += [(b'12:34:56', '22008'),
                    (struct.pack('!Q', 86400000001) + b'\0', '22008'),
                    (struct.pack('!Q', (1 << 64) - 1) + b'\0' * 8, '22008')]
        for raw, state in invalid:
            for output in (0, 1):
                check(raw, None, output, state=state)
        for oid, raw, text, size in ((16, b'\1', b't', 1),
                                     (23, struct.pack('!i', 17), b'17', 4),
                                     (701, struct.pack('!d', 1.25), b'1.25', 8)):
            for output in (0, 1):
                check(raw, None, output, oid=oid, size=size, text=text, binary=raw)
        print('TIME_BINARY_PARAMETER_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 158, checks
        assert not failures, failures
        print('[TIME BINARY PARAMETER] all 158 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
