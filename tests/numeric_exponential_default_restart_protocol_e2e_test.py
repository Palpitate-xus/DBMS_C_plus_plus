#!/usr/bin/env python3
"""Numeric exp simple queries and persisted defaults across rollback/restart."""
import importlib.util
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('exp_runner', root / 'tests/compat/pg_diff_runner.py')
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
    table = 'numeric_exp_state_' + uuid.uuid4().hex[:12]
    checks = 0

    def check(sql, rows=None, types=None, tag=None, ready=b'I'):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None, (sql, result)
        assert rows is None or result[0] == rows, (sql, result, rows)
        assert types is None or result[5] == types, (sql, result, types)
        assert tag is None or result[4] == tag, (sql, result, tag)
        assert messages[-1] == (b'Z', ready), (sql, messages[-1], ready)
        checks += 1

    def restart():
        nonlocal sock
        sock.close()
        server['sock'] = None
        old = server['process']
        old.terminate()
        assert old.wait(timeout=20) == 0, 'previous server did not stop cleanly'
        server['process'] = subprocess.Popen(
            [runner.DBMS_MAIN, '--data-dir', server['dir'], '--server', str(server['port']), '--insecure'],
            cwd=server['dir'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                sock = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
                break
            except OSError:
                assert server['process'].poll() is None and time.monotonic() < deadline, 'server restart failed'
                time.sleep(.05)
        try:
            client.startup(sock, 'alice', 'info')
        except BaseException:
            sock.close()
            raise
        server['sock'] = sock

    high = '26881171418161354484126255515800135873611119'
    low = '0.000000000000000004248354255291589'
    try:
        for value, expected in [('0', '1.0000000000000000'), ('1', '2.7182818284590452'),
                                ('100', high), ('-40', low),
                                ('-0.0000000100000000', '0.9999999900000001')]:
            check("SELECT exp('" + value + "'::numeric)", [[expected]], [1700], 'SELECT 1')
        check('SELECT exp(NULL::numeric)', [[None]], [1700], 'SELECT 1')
        check('CREATE TABLE ' + table + '(id INT, a NUMERIC DEFAULT exp(100::numeric), b NUMERIC DEFAULT exp(-40::numeric))', tag='CREATE TABLE')
        check('INSERT INTO ' + table + '(id) VALUES(1)', tag='INSERT 0 1')
        select = 'SELECT id,a,b FROM ' + table + ' ORDER BY id'
        one, two = [['1', high, low]], [['1', high, low], ['2', high, low]]
        check(select, one, [23, 1700, 1700], 'SELECT 1')
        check('BEGIN', tag='BEGIN', ready=b'T')
        check('INSERT INTO ' + table + '(id) VALUES(2)', tag='INSERT 0 1', ready=b'T')
        check(select, two, [23, 1700, 1700], 'SELECT 2', b'T')
        check('ROLLBACK', tag='ROLLBACK')
        check(select, one, [23, 1700, 1700], 'SELECT 1')
        if not reference:
            restart()
            check(select, one, [23, 1700, 1700], 'SELECT 1')
            check('INSERT INTO ' + table + '(id) VALUES(3)', tag='INSERT 0 1')
            check(select, [['1', high, low], ['3', high, low]], [23, 1700, 1700], 'SELECT 2')
        check('DROP TABLE ' + table, tag='DROP TABLE')
        print('NUMERIC_EXP_DEFAULT_RESTART', checks, 'REFERENCE' if reference else 'COLD', 'FAILURES 0', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, 'DROP TABLE IF EXISTS ' + table)
            finally:
                sock.close()


if __name__ == '__main__':
    main()
