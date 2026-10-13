#!/usr/bin/env python3
"""Unfixed arithmetic finding from R32; kept separate from literal typing.

Run with --reference18 for PostgreSQL, or DBMS_MAIN for a frozen candidate.
This diagnostic intentionally exits nonzero until the scale bug is fixed.
It is not registered as a passing regression of the R32 type-only change.
"""
from decimal import Decimal
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    spec = importlib.util.spec_from_file_location('scientific_scale_runner', Path(__file__).with_name('pg_diff_runner.py'))
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
    try:
        sql = 'SELECT 1e-40+1e-40 AS n'
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] is None and result[4] == 'SELECT 1' and result[5] == [1700]
        valid = valid and len(result[0]) == 1 and len(result[0][0]) == 1
        valid = valid and result[0][0][0] is not None and Decimal(result[0][0][0]) == Decimal('2e-40')
        print('SCIENTIFIC_NUMERIC_SCALE_KNOWN_GAP', sql, result, 'expected 2e-40, NUMERIC', 'match', valid, flush=True)
        assert valid, result
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
