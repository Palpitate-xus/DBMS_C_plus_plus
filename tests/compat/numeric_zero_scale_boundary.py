"""Zero-valued coercion at declared numeric scale 1000, plus durable rereads."""
import importlib.util
import socket
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path


def run(client, runner, server=None, sock=None, reference=False):
    if server is not None:
        sock = server['sock']
    table = "numeric_zero_boundary_" + uuid.uuid4().hex[:16]
    controls = 0
    failures = []
    zero = lambda scale: '0.' + '0' * scale if scale else '0'

    def check(sql, rows=None, oids=None, state=None, tag=None, modifiers=None):
        nonlocal controls
        controls += 1
        if reference and state:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT zero_scale_error'))[1] is None
        sql = sql.replace("numeric_zero_boundary_rows", table)
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == (tag or 'SELECT ' + str(len(rows)))
        if tag is not None:
            valid = valid and result[4] == tag
        if oids is not None:
            valid = valid and result[5] == oids
        if modifiers is not None:
            fields = client.row_description_fields(messages) if any(k == b'T' for k, _ in messages) else []
            valid = valid and [f[5] for f in fields] == modifiers
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print('ZERO_SCALE_BOUNDARY_SQL', controls, sql, 'state=', result[1], 'match=', valid, flush=True)
        if not valid:
            failures.append((controls, sql, result, rows, oids, state, tag, modifiers))
        if sql.startswith("CREATE TABLE") and not valid:
            raise RuntimeError("CONTROL_ERROR private table setup failed")
        if reference and state:
            for statement in ('ROLLBACK TO zero_scale_error', 'RELEASE zero_scale_error'):
                assert runner.decode_wire_result(client.simple_query(sock, statement))[1] is None

    def binary(parameter, result_format):
        nonlocal controls
        controls += 1
        name = ('zero_boundary_' + str(controls)).encode()
        sql = 'SELECT CAST($1 AS NUMERIC(1000,1000)) AS v'
        parse = name + b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, 1700)
        encoded = parameter.encode()
        bind = name + b'\0' + name + b'\0' + struct.pack('!HH', 0, 1)
        bind += struct.pack('!i', len(encoded)) + encoded + struct.pack('!HH', 1, result_format)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
            client.typed(b'D', b'P' + name + b'\0') + client.typed(b'E', name + b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(k == b'T' for k, _ in messages) else []
        rows = client.data_row_values(messages)
        expected = struct.pack('!hhhh', 0, 0, 0, 1000) if result_format else zero(1000).encode()
        valid = not errors and rows == [[expected]] and len(fields) == 1 and fields[0][3] == 1700 and fields[0][6] == result_format
        valid = valid and any(kind == b'C' and body == b'SELECT 1\0' for kind, body in messages)
        print('ZERO_SCALE_BOUNDARY_PARAMETER', controls, parameter, result_format, 'match=', valid, flush=True)
        if not valid:
            failures.append((controls, parameter, result_format, rows, fields, errors))

    if reference:
        assert runner.decode_wire_result(client.simple_query(sock, 'BEGIN'))[1] is None
    try:
        inputs = ('0', '0.0', '-0', '0e0', '0e-2', '0e-1000')
        for value in inputs:
            for scale in (1, 999, 1000):
                check('SELECT CAST(' + value + ' AS NUMERIC(1000,' + str(scale) + ')) AS v', [[zero(scale)]], [1700])
                check('SELECT round(' + value + '::numeric,' + str(scale) + ') AS v', [[zero(scale)]], [1700])
            check('SELECT CAST(' + value + ' AS NUMERIC(1,1000)) AS v', [[zero(1000)]], [1700])
        check("SELECT round('NaN'::numeric,1000)", [['NaN']], [1700])
        check("SELECT round('Infinity'::numeric,1000)", [['Infinity']], [1700])
        check("SELECT round('-Infinity'::numeric,1000)", [['-Infinity']], [1700])
        for value in ('0', '0.0', '-0'):
            for result_format in (0, 1):
                binary(value, result_format)
        check('CREATE TABLE numeric_zero_boundary_rows(id INT PRIMARY KEY,v NUMERIC(1000,1000),s NUMERIC(1,1000))', tag='CREATE TABLE')
        check('INSERT INTO numeric_zero_boundary_rows VALUES(1,0.0,0.00)', tag='INSERT 0 1')
        expected = [['1', zero(1000), zero(1000)]]
        modifiers = [-1, (1000 << 16) + 1004, (1 << 16) + 1004]
        check('SELECT id,v,s FROM numeric_zero_boundary_rows ORDER BY id', expected, [23, 1700, 1700], modifiers=modifiers)
        check('UPDATE numeric_zero_boundary_rows SET v=0,s=-0 WHERE id=1', tag='UPDATE 1')
        check('SELECT id,v,s FROM numeric_zero_boundary_rows WHERE id=1', expected, [23, 1700, 1700])
        check('INSERT INTO numeric_zero_boundary_rows VALUES(2,0,0)', tag='INSERT 0 1')
        check('SELECT id,v,s FROM numeric_zero_boundary_rows WHERE id=2', [['2', zero(1000), zero(1000)]], [23, 1700, 1700])
        check('SELECT CAST(0.1 AS NUMERIC(1000,1000))', [['0.1' + '0' * 999]], [1700])
        check('SELECT CAST(1 AS NUMERIC(1000,1000))', state='22003')
        check('SELECT CAST(NULL AS NUMERIC(1000,1000))', [[None]], [1700])
        if server is not None:
            check('CHECKPOINT', tag='CHECKPOINT')
            sock.close(); server['sock'] = None
            server['process'].terminate(); server['process'].wait(timeout=10)
            server['process'] = subprocess.Popen([runner.DBMS_MAIN, '--data-dir', server['dir'], '--server', str(server['port']), '--insecure'],
                cwd=server['dir'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            sock = socket.socket(); sock.settimeout(runner.wire_timeout()); server['sock'] = sock
            deadline = time.monotonic() + 20
            while True:
                try: sock.connect(('127.0.0.1', server['port'])); break
                except OSError:
                    if time.monotonic() >= deadline: raise
                    time.sleep(0.05)
            client.startup(sock, 'alice', 'info')
        check('SELECT id,v,s FROM numeric_zero_boundary_rows WHERE id=1', expected, [23, 1700, 1700], modifiers=modifiers)
        check('SELECT 42', [['42']], [23])
        print('NUMERIC_ZERO_SCALE_BOUNDARY_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert not failures, failures
    finally:
        if reference:
            client.simple_query(sock, 'ROLLBACK')


def main():
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('zero_boundary_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        try:
            client.startup_reference(sock, user, database, password=password)
            runner.verify_reference_version(client, sock)
            run(client, runner, sock=sock, reference=True)
        finally: sock.close()
    else:
        server = runner.start_ours(client)
        try: run(client, runner, server=server)
        finally: runner.stop_ours(server)


if __name__ == '__main__':
    main()
