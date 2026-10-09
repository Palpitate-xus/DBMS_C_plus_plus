"""Preserve interval field grammar, exact values, NULLs and result OIDs."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('interval_fields_runner', root / 'tests/compat/pg_diff_runner.py')
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
        print('INTERVAL_FIELDS', sql, result, flush=True)
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
        value = '1 year 2 months 3 days 04:05:06.789123'
        fields = (
            ('YEAR', '1 year'), ('MONTH', '1 year 2 mons'),
            ('DAY', '1 year 2 mons 3 days'),
            ('HOUR', '1 year 2 mons 3 days 04:00:00'),
            ('MINUTE', '1 year 2 mons 3 days 04:05:00'),
            ('SECOND', value.replace('months', 'mons')),
            ('YEAR TO MONTH', '1 year 2 mons'),
            ('DAY TO HOUR', '1 year 2 mons 3 days 04:00:00'),
            ('DAY TO MINUTE', '1 year 2 mons 3 days 04:05:00'),
            ('DAY TO SECOND', value.replace('months', 'mons')),
            ('HOUR TO MINUTE', '1 year 2 mons 3 days 04:05:00'),
            ('HOUR TO SECOND', value.replace('months', 'mons')),
            ('MINUTE TO SECOND', value.replace('months', 'mons')),
        )
        for field, output in fields:
            check("SELECT '" + value + "'::INTERVAL " + field, [[output]], [1186])
            check('SELECT NULL::INTERVAL ' + field, [[None]], [1186])
        for field in ('SECOND', 'DAY TO SECOND', 'HOUR TO SECOND', 'MINUTE TO SECOND'):
            check("SELECT '" + value + "'::INTERVAL " + field + '(3)',
                  [['1 year 2 mons 3 days 04:05:06.789']], [1186])
        check("SELECT '1.5 seconds'::INTERVAL(0)", [['00:00:02']], [1186])
        check("SELECT '-1.5 seconds'::INTERVAL SECOND(0)", [['-00:00:02']], [1186])
        for field, output in (('YEAR', '2 years'), ('MONTH', '2 mons'), ('DAY', '2 days'),
                              ('HOUR', '02:00:00'), ('MINUTE', '00:02:00'), ('SECOND', '00:00:02')):
            check("SELECT '2'::INTERVAL " + field, [[output]], [1186])
        check("SELECT NULL::TIME(2) WITH TIME ZONE IS NULL", [['t']], [16])
        # Preserve the original whole-suite SQL as a binding error, not a syntax error.
        check("SELECT NULL::INTERVAL DAY TO SECOND(3) LIKE '%'", state='42883')
        for suffix in ('YEAR TO DAY', 'DAY TO MONTH', 'MONTH TO YEAR', 'SECOND TO MINUTE',
                       'DAY(3)', 'YEAR TO MONTH(3)', '(3) DAY TO SECOND', '(3,4)', 'SECOND(-1)'):
            check('SELECT NULL::INTERVAL ' + suffix, state='42601')
        print('INTERVAL_FIELDS_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
