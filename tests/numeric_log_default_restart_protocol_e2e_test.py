#!/usr/bin/env python3
"""NUMERIC logarithm SQL, error precedence, rollback, and persisted defaults.

With --reference18, run the same SQL against an explicitly configured PG 18
reference; only the private local server is restarted by this test. The value
and scale fixtures come from PostgreSQL 18.6 protocol observations, except the
three near-one expressions marked below which must be checked on that reference.
"""
import importlib.util
from pathlib import Path
import socket
import os
import shutil
import tempfile
import subprocess
import sys
import time
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('numeric_log_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    def owns_listener(pid, port):
        """Confirm this exact Linux PID owns the loopback listening socket."""
        assert (Path('/proc') / str(pid)).stat().st_uid == os.geteuid(), 'listener has another owner'
        sockets = set()
        for fd in (Path('/proc') / str(pid) / 'fd').iterdir():
            try:
                target = os.readlink(fd)
            except FileNotFoundError:  # fd closed during inspection
                continue
            if target.startswith('socket:[') and target.endswith(']'):
                sockets.add(target[8:-1])
        for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
            fields = line.split()
            host, hexport = fields[1].split(':')
            if (host in ('0100007F', '00000000') and int(hexport, 16) == port and
                    fields[3] == '0A' and fields[9] in sockets):
                return True
        return False

    if reference:
        # The caller must start and identify a private loopback PG process.
        # Never fall back to the runner's configured Docker endpoint.
        required = ('PGREF_HOST', 'PGREF_PORT', 'PGREF_PASSWORD', 'PGREF_PID')
        assert all(os.environ.get(k) for k in required), 'private PGREF endpoint and PID required'
        assert os.environ['PGREF_HOST'] == '127.0.0.1', 'reference must be private IPv4 loopback'
        host, port, user, database, password = runner._reference_connection_settings()
        assert owns_listener(int(os.environ['PGREF_PID']), port), 'reference PID does not own listener'
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        # All files and server processes below belong exclusively to this run.
        work_dir = Path(tempfile.mkdtemp(prefix='dbms-numeric-log-'))
        (work_dir / 'info').mkdir()
        (work_dir / 'info' / 'tlist.lst').write_bytes(b'')
        client.write_auth_catalog(str(work_dir), 'alice', 'secret')
        (work_dir / 'pg_hba.conf').write_text('host all alice 127.0.0.1/32 scram-sha-256\n')
        probe = socket.socket()
        try:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        finally:
            probe.close()
        server = {'dir': str(work_dir), 'port': port, 'process': None,
                  'sock': None, 'generation': 0, 'waits': []}
        sock = None
    table = 'numeric_log_state_' + uuid.uuid4().hex[:12]
    created = False
    checks = 0

    def check(sql, rows=None, oids=None, state=None, tag=None, ready=b'I', message=None):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result, state)
        # Some existing expression error paths append SQLSTATE to diagnostic
        # text; PG sends it only in the separate protocol field.
        expected_messages = [message]
        if not reference and message is not None:
            expected_messages.append(message + ' (SQLSTATE ' + state + ')')
        assert message is None or result[2] in expected_messages, (sql, result, expected_messages)
        assert rows is None or result[0] == rows, (sql, result, rows)
        assert oids is None or result[5] == oids, (sql, result, oids)
        assert tag is None or result[4] == tag, (sql, result, tag)
        assert messages[-1] == (b'Z', ready), (sql, messages[-1], ready)
        if state:
            assert result[0] == [] and result[4] is None, (sql, result)
            assert sum(kind == b'E' for kind, _ in messages) == 1, (sql, messages)
            assert not any(kind in (b'T', b'D', b'C') for kind, _ in messages), (sql, messages)
        checks += 1

    def start():
        nonlocal sock
        logfile = Path(server['dir']) / ('server-%d.log' % server['generation'])
        with logfile.open('wb') as output:
            process = subprocess.Popen(
                [runner.DBMS_MAIN, '--data-dir', server['dir'], '--server', str(server['port']), '--insecure'],
                cwd=server['dir'], stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        server['process'] = process
        deadline = time.monotonic() + 20
        while True:
            assert process.poll() is None, ('server exited during startup', process.returncode, str(logfile))
            try:
                owned = owns_listener(process.pid, server['port'])
            except FileNotFoundError:
                owned = False
            if owned:
                break
            assert time.monotonic() < deadline, ('server listener timeout', str(logfile))
            time.sleep(.05)
        sock = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        server['sock'] = sock
        client.startup(sock, 'alice', 'info')

    def stop():
        if server['sock'] is not None:
            server['sock'].close()
            server['sock'] = None
        process = server['process']
        if process is None:
            return
        forced = False
        if process.poll() is None:
            process.terminate()
        try:
            exitcode = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            exitcode = process.wait(timeout=20)
        server['waits'].append((process.pid, exitcode, forced))
        server['process'] = None
        print('NUMERIC_LOG_SERVER_WAIT', process.pid, exitcode, 'forced', forced, flush=True)
        assert exitcode == 0 and not forced, ('server did not stop cleanly', server['waits'], server['dir'])

    def restart():
        stop()
        server['generation'] += 1
        start()

    high_input = "'2.0000000000000000000000000000000001'::numeric"
    high_ln = '0.6931471805599453094172321214581766'
    high_log = '0.3010299956639811952137388947244930'
    near = "'1.0000000000000000001'::numeric"
    cases = [
        ('ln(2::numeric)', '0.6931471805599453'),
        ('ln(' + high_input + ')', high_ln),
        ("ln('1.0000000000000002'::numeric)", '0.00000000000000019999999999999998'),
        ("ln('0.9999999999999999999'::numeric)", '-0.00000000000000000010000000000000000'),
        ("ln('1e999'::numeric)", '2300.2825079010516'),
        ('log(' + high_input + ')', high_log),
        ('log10(' + high_input + ')', high_log),
        ("log('2.0000000000000000000000000000000001'::numeric,'3.0000000000000000000000000000000001'::numeric)", '1.5849625007211561814537389439478164'),
        ("log('1.00000001'::numeric,'1.00000002000000010'::numeric)", '2.00000000000000000'),
        ("ln('NaN'::numeric)", 'NaN'),
        ("ln('Infinity'::numeric)", 'Infinity'),
        ("log('NaN'::numeric,0::numeric)", 'NaN'),
        ("log(1::numeric,'Infinity'::numeric)", 'Infinity'),
        ("log('Infinity'::numeric,'Infinity'::numeric)", 'NaN'),
        ("log('Infinity'::numeric,1::numeric)", '0'),
        # Original error regressions; latter two expected strings derive from
        # the local component and MUST be confirmed against --reference18.
        ('ln(' + near + ')', '0.00000000000000000010000000000000000'),
        ('log(' + near + ')', '0.00000000000000000004342944819032518'),
        ('log(2::numeric,' + near + ')', '0.00000000000000000014426950408889634'),
        ('ln(NULL::numeric)', None),
        ('log(NULL::numeric,1::numeric)', None),
        ('log(1::numeric,NULL::numeric)', None),
    ]
    succeeded = False
    try:
        if server:
            start()
        for expression, expected in cases:
            check('SELECT ' + expression, [[expected]], [1700], tag='SELECT 1')
        for expression, state in [
            ("ln('0'::numeric)", '2201E'), ("ln('-1'::numeric)", '2201E'),
            ("ln('-Infinity'::numeric)", '2201E'), ("log10(0::numeric)", '2201E'),
            ('log(0::numeric,10::numeric)', '2201E'), ('log(1::numeric,0::numeric)', '2201E'),
            ('log(1::numeric,1::numeric)', '22012'),
            ("log('Infinity'::numeric,'-Infinity'::numeric)", '2201E'),
        ]:
            check('SELECT ' + expression, state=state)
        for expression, message in [
            ('ln(0::numeric)', 'cannot take logarithm of zero'),
            ('ln(-1::numeric)', 'cannot take logarithm of a negative number'),
            ("ln('-Infinity'::numeric)", 'cannot take logarithm of a negative number'),
            ('log(0::numeric,-1::numeric)', 'cannot take logarithm of zero'),
            ('log(-1::numeric,0::numeric)', 'cannot take logarithm of a negative number'),
            ("log(0::numeric,'-Infinity'::numeric)", 'cannot take logarithm of a negative number'),
            ('log(1::numeric,0::numeric)', 'cannot take logarithm of zero'),
            ("log('Infinity'::numeric,0::numeric)", 'cannot take logarithm of zero'),
            ("log('Infinity'::numeric,-1::numeric)", 'cannot take logarithm of a negative number'),
        ]:
            check('SELECT ' + expression, state='2201E', message=message)
        # The project limits Numeric results to 1000 digits; PostgreSQL can
        # represent the result, so this control intentionally differs by engine.
        if reference:
            check("SELECT log10('1e-999'::numeric)", [['-999.' + '0' * 999]], [1700], tag='SELECT 1')
        else:
            check("SELECT log10('1e-999'::numeric)", state='22003')
        check('SELECT ln(2::double precision)', [['0.6931471805599453']], [701], tag='SELECT 1')
        created = True  # cleanup is safe even if CREATE or its response fails
        check('CREATE TABLE ' + table + '(id INT, a NUMERIC DEFAULT ln(' + high_input + '), b NUMERIC DEFAULT log(' + high_input + '))', tag='CREATE TABLE')
        check('INSERT INTO ' + table + '(id) VALUES(1)', tag='INSERT 0 1')
        select = 'SELECT id,a,b FROM ' + table + ' ORDER BY id'
        one = [['1', high_ln, high_log]]
        check(select, one, [23, 1700, 1700], tag='SELECT 1')
        check('BEGIN', tag='BEGIN', ready=b'T')
        check('INSERT INTO ' + table + '(id) VALUES(2)', tag='INSERT 0 1', ready=b'T')
        check(select, one + [['2', high_ln, high_log]], [23, 1700, 1700], tag='SELECT 2', ready=b'T')
        check('ROLLBACK', tag='ROLLBACK')
        check(select, one, [23, 1700, 1700], tag='SELECT 1')
        if not reference:
            restart()
            check(select, one, [23, 1700, 1700], tag='SELECT 1')
            check('INSERT INTO ' + table + '(id) VALUES(3)', tag='INSERT 0 1')
            check(select, one + [['3', high_ln, high_log]], [23, 1700, 1700], tag='SELECT 2')
        check('DROP TABLE ' + table, tag='DROP TABLE')
        created = False
        if server:
            stop()
        succeeded = True
        print('NUMERIC_LOG_DEFAULT_RESTART', checks, 'REFERENCE' if reference else 'COLD', 'FAILURES 0', flush=True)
    finally:
        if server:
            if server['process'] is not None:
                try:
                    stop()
                except Exception as error:
                    succeeded = False
                    print('NUMERIC_LOG_STOP_ERROR', repr(error), flush=True)
            owned = Path(server['dir']).resolve()
            if succeeded:
                assert owned.parent == Path(tempfile.gettempdir()).resolve() and owned.name.startswith('dbms-numeric-log-')
                shutil.rmtree(owned)
            else:
                print('NUMERIC_LOG_FAILURE_DATA', str(owned), 'waits', server['waits'], flush=True)
        else:
            try:
                if created:
                    client.simple_query(sock, 'DROP TABLE IF EXISTS ' + table)
            finally:
                sock.close()


if __name__ == '__main__':
    main()
