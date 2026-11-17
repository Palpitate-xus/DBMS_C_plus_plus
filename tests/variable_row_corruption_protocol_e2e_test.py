#!/usr/bin/env python3
"""Reject malformed directories in private checksum-valid physical heap pages.

This tests the project's own disk format, not PostgreSQL's corruption behavior.
Every mutation is made with the private service stopped and is restored before
the next case. SQL NULL and empty text remain distinct on healthy rows.
"""
import importlib.util
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import tempfile
import time


def checksum(page):
    copy = bytearray(page)
    copy[8:10] = b'\0\0'
    a = b = 0
    for value in copy:
        a = (a + value) % 255
        b = (b + a) % 255
    return (b << 8 | a) or 65535


def owns_listener(pid, port):
    sockets = set()
    for fd in (Path('/proc') / str(pid) / 'fd').iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith('socket:[') and target.endswith(']'):
            sockets.add(target[8:-1])
    for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
        fields = line.split()
        if (fields[3] == '0A' and int(fields[1].rsplit(':', 1)[1], 16) == port
                and fields[9] in sockets):
            return True
    return False


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('variable_row_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    work = Path(tempfile.mkdtemp(prefix='dbms-variable-row-'))
    proc = sock = None
    failures, waits = [], []
    checks = generation = 0
    successful = False

    def start():
        nonlocal proc, sock, generation
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        log = work / ('server-%d.log' % generation)
        generation += 1
        with log.open('xb') as output:
            proc = subprocess.Popen(
                [runner.DBMS_MAIN, '--data-dir', str(work), '--server', str(port), '--insecure'],
                cwd=work, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 30
        while True:
            assert proc.poll() is None, ('private server exited', proc.returncode, log)
            try:
                if owns_listener(proc.pid, port):
                    break
            except FileNotFoundError:
                pass
            assert time.monotonic() < deadline, ('private listener timeout', log)
            time.sleep(.05)
        sock = socket.create_connection(('127.0.0.1', port), timeout=runner.wire_timeout())
        client.startup(sock, 'alice', 'info')

    def stop():
        nonlocal proc, sock
        if sock is not None:
            sock.close()
            sock = None
        if proc is None:
            return
        if proc.poll() is None:
            proc.terminate()
        forced = False
        try:
            code = proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            forced = True
            proc.kill()
            code = proc.wait(timeout=20)
        waits.append((proc.pid, code, forced))
        proc = None
        assert code == 0 and not forced, ('private server shutdown', waits)

    def check(sql, rows=None, state=None, ready=b'I', setup=False):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        ok = result[1] == state and (rows is None or result[0] == rows) and messages[-1] == (b'Z', ready)
        if state is not None:
            ok = ok and result[0] == [] and result[4] is None and sum(k == b'E' for k, _ in messages) == 1
        checks += 1
        if not ok:
            failures.append((sql, result, state, rows, ready.decode()))
        assert not setup or ok, failures[-1:]

    try:
        (work / 'info').mkdir()
        (work / 'info/tlist.lst').write_bytes(b'')
        client.write_auth_catalog(str(work), 'alice', 'secret')
        (work / 'pg_hba.conf').write_text('host all alice 127.0.0.1/32 scram-sha-256\n')
        start()
        check('CREATE TABLE directory_rows(id INTEGER PRIMARY KEY,payload TEXT,spare TEXT)', setup=True)
        check("INSERT INTO directory_rows VALUES (42,'alpha','')", setup=True)
        check('CREATE TABLE directory_nulls(id INTEGER,payload TEXT,spare TEXT)', setup=True)
        check("INSERT INTO directory_nulls VALUES (1,NULL,''),(2,'',NULL)", setup=True)
        check('SELECT id,payload,spare FROM directory_nulls ORDER BY id',
              [['1', None, ''], ['2', '', None]])
        check('SELECT payload,spare FROM directory_rows', [['alpha', '']])
        stop()

        path = work / 'info/directory_rows.dt'
        original = path.read_bytes()
        assert len(original) >= 16384 and len(original) % 8192 == 0
        page = original[8192:16384]
        assert checksum(page) == struct.unpack_from('<H', page, 8)[0]
        lower = struct.unpack_from('<H', page, 12)[0]
        assert lower == 28, 'fixture must have exactly one line pointer'
        offset, length_flags = struct.unpack_from('<HH', page, 24)
        length = length_flags & 0x3fff
        assert length_flags >> 14 == 1 and 23 <= length and offset + length <= 8192
        hoff = page[offset + 22]
        data = offset + hoff
        assert hoff >= 23 and data + 17 == offset + length
        assert struct.unpack_from('<i', page, data)[0] == 42
        assert struct.unpack_from('<HHHH', page, data + 4) == (12, 5, 17, 0)
        assert page[data + 12:data + 17] == b'alpha'

        for name, new_offset, new_length in [
                ('past-end', 65535, 1), ('length', 12, 65535),
                ('fixed-data', 0, 1), ('directory', 4, 4), ('other-value', 17, 0)]:
            bad = bytearray(page)
            struct.pack_into('<HH', bad, data + 4, new_offset, new_length)
            struct.pack_into('<H', bad, 8, checksum(bad))
            assert checksum(bad) == struct.unpack_from('<H', bad, 8)[0]
            assert proc is None
            path.write_bytes(original[:8192] + bad + original[16384:])
            try:
                start()
                check('SELECT payload FROM directory_rows', state='XX001')
                check('SELECT payload FROM directory_rows WHERE id=42', state='XX001')
                # The whole inline directory is structural metadata; reading
                # another variable field also diagnoses that malformed tuple.
                check('SELECT spare FROM directory_rows', state='XX001')
                check('SELECT id FROM directory_rows', [['42']])
                check('BEGIN', ready=b'T')
                check('SELECT payload FROM directory_rows', state='XX001', ready=b'E')
                check('ROLLBACK')
                check('SELECT id,payload,spare FROM directory_nulls ORDER BY id',
                      [['1', None, ''], ['2', '', None]])
                stop()
            finally:
                if proc is not None:
                    stop()
                path.write_bytes(original)
            start()
            check('SELECT id,payload,spare FROM directory_rows', [['42', 'alpha', '']])
            stop()
            print('VARIABLE_ROW_CORRUPTION_CASE', name, flush=True)
        assert not failures, failures
        successful = True
        print('VARIABLE_ROW_CORRUPTION_PASS', checks, 'checks;', len(waits), 'clean server exits', flush=True)
    finally:
        if proc is not None:
            stop()
        if successful:
            shutil.rmtree(work)
        else:
            print('VARIABLE_ROW_FAILURE_EVIDENCE', str(work), flush=True)


if __name__ == '__main__':
    main()
