#!/usr/bin/env python3
"""Float overloads in defaults survive rollback and a clean server restart."""
import importlib.util
from pathlib import Path
import socket
import subprocess
import time


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('float_restart_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    checks = 0

    def check(sql, rows=None, types=None, tag=None, ready=b'I'):
        nonlocal checks
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None, (sql, result)
        assert rows is None or result[0] == rows, (sql, result, rows)
        assert types is None or result[5] == types, (sql, result, types)
        assert tag is None or result[4] == tag, (sql, result, tag)
        assert messages[-1] == (b'Z', ready), (sql, messages[-1], ready)
        checks += 1

    def restart():
        server['sock'].close()
        server['sock'] = None
        previous = server['process']
        previous.terminate()
        assert previous.wait(timeout=20) == 0, 'previous server did not stop cleanly'
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

    a = '4.248354255291589e-18'
    try:
        check('CREATE TABLE float_default_state(id INT, a DOUBLE PRECISION DEFAULT exp(-40::integer), b DOUBLE PRECISION DEFAULT log10(100::double precision))', tag='CREATE TABLE')
        check('INSERT INTO float_default_state(id) VALUES(1)', tag='INSERT 0 1')
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2']], [23, 701, 701], 'SELECT 1')
        check('BEGIN', tag='BEGIN', ready=b'T')
        check('INSERT INTO float_default_state(id) VALUES(2)', tag='INSERT 0 1', ready=b'T')
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2'], ['2', a, '2']], [23, 701, 701], 'SELECT 2', b'T')
        check('ROLLBACK', tag='ROLLBACK')
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2']], [23, 701, 701], 'SELECT 1')
        restart()
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2']], [23, 701, 701], 'SELECT 1')
        check('INSERT INTO float_default_state(id) VALUES(3)', tag='INSERT 0 1')
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2'], ['3', a, '2']], [23, 701, 701], 'SELECT 2')
        check('BEGIN', tag='BEGIN', ready=b'T')
        check('INSERT INTO float_default_state(id) VALUES(4)', tag='INSERT 0 1', ready=b'T')
        check('ROLLBACK', tag='ROLLBACK')
        check('SELECT id,a,b FROM float_default_state ORDER BY id', [['1', a, '2'], ['3', a, '2']], [23, 701, 701], 'SELECT 2')
        print('FLOAT_OVERLOAD_DEFAULT_RESTART', checks, 'FAILURES 0', flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
