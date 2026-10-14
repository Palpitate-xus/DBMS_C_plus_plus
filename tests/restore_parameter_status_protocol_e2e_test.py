#!/usr/bin/env python3
"""ParameterStatus observation must survive a real restore publication."""
import importlib.util
import time
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('restore_status_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server['sock']
    directory = Path(server['dir'])
    checks = 0

    def query(sql, rows=None):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and messages[-1] == (b'Z', b'I'), (sql, result, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        checks += 1
        return messages, result

    def submit(sql):
        _, result = query(sql)
        assert result[3] == ['job_id', 'status'] and result[5] == [20, 25], result
        assert len(result[0]) == 1 and result[0][0][1] == 'queued', result
        return int(result[0][0][0])

    def wait_job(job):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            _, result = query('SHOW MAINTENANCE JOB ' + str(job))
            assert len(result[0]) == 1, result
            row = result[0][0]
            if row[4] in ('succeeded', 'failed', 'cancelled'):
                assert row[4] == 'succeeded', row
                return row
            time.sleep(0.02)
        raise AssertionError(('restore status job did not finish', job))

    try:
        query('SET compatibility_mode=extended')
        messages, _ = query("SET application_name='before_restore_status'")
        assert (b'S', b'application_name\0before_restore_status\0') in messages
        query('CREATE TABLE restore_status_rows(id INT PRIMARY KEY)')
        query('INSERT INTO restore_status_rows VALUES(1)')
        backup = directory / 'parameter_status.backup'
        wait_job(submit("BACKUP DATABASE info TO '" + str(backup) + "'"))
        query('INSERT INTO restore_status_rows VALUES(99)')
        size = sum(path.stat().st_size for path in backup.rglob('*') if path.is_file())
        # Use the real maintenance rate limiter to observe an actual exclusive
        # restore stage. This is not a longer socket/statement timeout.
        rate = max(1, (size + 4095) // 4096)
        restore = submit("RESTORE DATABASE info FROM '" + str(backup) + "' RATE " + str(rate))
        deadline = time.monotonic() + 5
        stage = None
        while time.monotonic() < deadline:
            stages = list(directory.glob('info.restore_staging.*'))
            if stages:
                stage = stages[0]
                break
            time.sleep(0.001)
        assert stage is not None and stage.is_dir(), 'no actual restore publication window observed'
        messages, _ = query("SET application_name='during_restore_status'")
        assert stage.is_dir(), 'status reporting waited for the physical restore instead of observing session state'
        assert (b'S', b'application_name\0during_restore_status\0') in messages
        assert (b'S', b'is_superuser\0off\0') not in messages
        # Unlike a session GUC, SHOW JOB performs a real administrator lookup.
        # Do not bypass that lookup just to make status observation succeed.
        messages = client.simple_query(sock, 'SHOW MAINTENANCE JOB ' + str(restore))
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b'Z', b'I') and stage.is_dir(), (result, messages[-1])
        if result[1] is None:
            assert result[0][0][4] == 'running', result
        else:
            assert result[1] == '55P03', result
        checks += 1
        deadline = time.monotonic() + 20
        while stage.is_dir() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not stage.is_dir(), 'restore stage did not finish'
        wait_job(restore)
        query('SELECT id FROM restore_status_rows ORDER BY id', rows=[['1']])
        query('SHOW application_name', rows=[['during_restore_status']])
        query('SELECT 7', rows=[['7']])
        print('RESTORE_PARAMETER_STATUS_COMPLETE', checks,
              'actual stage, mutable status, unchanged role, restore data and live connection passed', flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
