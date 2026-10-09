"""Postfix field qualifiers belong to INTERVAL string constants."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('interval_literal_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    failures = []
    checked = 0

    def check(sql, rows=None, oids=None, state=None):
        nonlocal checked
        checked += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print('INTERVAL_LITERAL', sql, result, flush=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == 'SELECT ' + str(len(rows))
        if oids is not None:
            valid = valid and result[5] == oids
        if state is not None:
            valid = valid and result[0] == [] and result[4] is None
        if not valid:
            failures.append((sql, result, rows, oids, state))

    try:
        for field, output in (
            ('YEAR', '2 years'), ('MONTH', '2 mons'), ('DAY', '2 days'),
            ('HOUR', '02:00:00'), ('MINUTE', '00:02:00'), ('SECOND', '00:00:02'),
            ('YEAR TO MONTH', '2 mons'), ('DAY TO HOUR', '02:00:00'),
            ('DAY TO MINUTE', '00:02:00'), ('DAY TO SECOND', '00:00:02'),
            ('HOUR TO MINUTE', '00:02:00'), ('HOUR TO SECOND', '00:00:02'),
            ('MINUTE TO SECOND', '00:00:02'),
        ):
            check("SELECT INTERVAL '2' " + field, [[output]], [1186])
        for field in ('SECOND', 'DAY TO SECOND', 'HOUR TO SECOND', 'MINUTE TO SECOND'):
            check("SELECT INTERVAL '2.3456 seconds' " + field + '(3)', [['00:00:02.346']], [1186])
        check("SELECT INTERVAL(3) '2.3456 seconds'", [['00:00:02.346']], [1186])
        check("SELECT INTERVAL '-14 months 3 days' YEAR", [['-1 years']], [1186])
        check("SELECT INTERVAL '2' DAY + INTERVAL '3' DAY", [['5 days']], [1186])
        check("SELECT INTERVAL '2' DAY > INTERVAL '1 day'", [['t']], [16])
        check("SELECT INTERVAL '2' DAY AS \"year\"", [['2 days']], [1186])
        check("SELECT 'INTERVAL ''2'' DAY' AS literal", [["INTERVAL '2' DAY"]], [25])
        check("SELECT INTERVAL '2' DAY LIKE '%'", state='42883')
        for sql in (
            "SELECT INTERVAL DAY '2'", "SELECT INTERVAL YEAR TO MONTH '2'",
            "SELECT INTERVAL(3) '2' DAY", "SELECT INTERVAL '2' YEAR TO DAY",
            "SELECT INTERVAL '2' DAY(3)", "SELECT INTERVAL '2' SECOND(-1)",
        ):
            check(sql, state='42601')
        print('INTERVAL_LITERAL_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
