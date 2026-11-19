#!/usr/bin/env python3
"""Crash recovery restores durable data and removes stale temporary relations."""

import importlib.util
from pathlib import Path
import resource
import socket
import subprocess
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "cold_backup_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()

    def query(server, sql, expected=None, ready=b"I", state=None, tag=None):
        messages = client.simple_query(server["sock"], sql)
        rows, actual_state, message, _, actual_tag = runner.decode_wire_result(messages)
        assert actual_state == state, (sql, actual_state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        return messages

    def kill_server(server):
        # Kill only this test-owned, fully started process, retaining its
        # durable transaction/statement images. A new exec (not a native
        # StorageEngine constructed after main) exercises global startup.
        process = server["process"]
        process.kill()
        assert process.wait(timeout=10) == -9
        # EOF on a live backend would roll its transaction back before the
        # kill, so close the client only after the process is confirmed dead.
        server["sock"].close()
        server["sock"] = None

    def start_from_disk(server, diagnostics):
        with diagnostics.open("wb") as stderr:
            server["process"] = subprocess.Popen(
                [runner.DBMS_MAIN, "--data-dir", server["dir"],
                 "--server", str(server["port"]), "--insecure"],
                cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=stderr)

    def reopen(server):
        diagnostics = Path(server["dir"]) / "reopen.stderr"
        start_from_disk(server, diagnostics)
        deadline = time.monotonic() + 20
        while True:
            assert server["process"].poll() is None, (
                "cold-start process exited before accepting clients",
                server["process"].returncode, diagnostics.read_text(errors="replace"))
            connection = socket.socket()
            connection.settimeout(runner.wire_timeout())
            try:
                connection.connect(("127.0.0.1", server["port"]))
                server["sock"] = connection
                break
            except OSError:
                connection.close()
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        client.startup(server["sock"], "alice", "info")

    def restart_after_sigkill(server, check_sequence_recovery=False):
        kill_server(server)
        fixture = Path(server["dir"])
        images = list(fixture.glob("info.txn_backup.*"))
        assert images, "crash fixture did not contain a transaction image"
        assert all((image / ".dbms_physical_backup").is_file()
                   for image in images), images
        if check_sequence_recovery:
            # An invalid live sequence state makes restoration unsafe. Keep
            # the transaction image so startup can retry after repair.
            [runtime_state] = (fixture / "info" / ".sequence_runtime").iterdir()
            valid_state = runtime_state.read_bytes()
            diagnostics = fixture / "sequence-recovery.stderr"
            _, hard_limit = resource.getrlimit(resource.RLIMIT_CORE)
            resource.setrlimit(resource.RLIMIT_CORE, (0, hard_limit))
            try:
                runtime_state.write_bytes(b"invalid sequence state\n")
                start_from_disk(server, diagnostics)
                assert server["process"].wait(timeout=20) != 0
                assert "could not restore transaction image" in diagnostics.read_text()
                assert all((image / ".dbms_physical_backup").is_file()
                           for image in images)
            finally:
                runtime_state.write_bytes(valid_state)
        reopen(server)
        assert not list(fixture.glob("info.txn_backup.*")), (
            "recovered transaction image was not cleaned up")
        assert not list(fixture.glob("info.ddl_statement_backup.*")), (
            "stale nested statement images survived transaction recovery")

    # Every case uses an independently created tiny cluster. No user data,
    # prebuilt copied fixture, or timeout extension is needed to reproduce.
    for action in ("alter", "create", "drop", "nested"):
        server = runner.start_ours(client)
        try:
            query(server, "CREATE TABLE recovery_base(id INT PRIMARY KEY,payload TEXT);")
            query(server, "INSERT INTO recovery_base VALUES(1,'survivor'),(2,NULL);")
            if action == "alter":
                query(server, "CREATE SEQUENCE recovery_sequence START 71 CACHE 1;",
                      tag="CREATE SEQUENCE")
                query(server, "SELECT nextval('recovery_sequence');", [["71"]], tag="SELECT 1")
            query(server, "BEGIN;", ready=b"T")
            if action == "create":
                # Simple CREATE can use catalog undo without a physical
                # image. Ensure this case exercises real snapshot recovery.
                query(server, "ALTER TABLE recovery_base ADD COLUMN pending_extra INT;",
                      ready=b"T")
                query(server, "CREATE TABLE pending_create(id INT);", ready=b"T")
                query(server, "INSERT INTO pending_create VALUES(99);", ready=b"T")
            elif action == "drop":
                query(server, "DROP TABLE recovery_base;", ready=b"T")
            else:
                query(server, "ALTER TABLE recovery_base ADD COLUMN pending_extra INT;",
                      ready=b"T")
                if action == "nested":
                    query(server, "SAVEPOINT after_ddl;", ready=b"T")
                    query(server, "UPDATE recovery_base SET payload='uncommitted' WHERE id=1;",
                          ready=b"T")
                    assert list(Path(server["dir"]).glob("info.ddl_statement_backup.*")), (
                        "nested recovery fixture had no statement images")
            if action == "alter":
                query(server, "SELECT nextval('recovery_sequence');", [["72"]],
                      ready=b"T", tag="SELECT 1")
            restart_after_sigkill(server, check_sequence_recovery=action == "alter")
            messages = query(server, "SELECT * FROM recovery_base ORDER BY id;",
                             [["1", "survivor"], ["2", None]])
            assert [field[0] for field in client.row_description_fields(messages)] == [
                b"id", b"payload"], action
            if action == "alter":
                query(server, "SELECT nextval('recovery_sequence');", [["73"]], tag="SELECT 1")
            if action == "create":
                messages = client.simple_query(server["sock"], "SELECT id FROM pending_create;")
                rows, state, message, _, _ = runner.decode_wire_result(messages)
                assert state == "42P01", (rows, state, message)
            # The newly initialized process-wide registry also has to serve
            # subsequent transactions, not merely let recovery pass once.
            query(server, "BEGIN;", ready=b"T")
            query(server, "INSERT INTO recovery_base VALUES(3,'after restart');", ready=b"T")
            query(server, "COMMIT;")
            query(server, "SELECT id FROM recovery_base ORDER BY id;",
                  [["1"], ["2"], ["3"]])
        finally:
            runner.stop_ours(server)
    print("[COLD START TRANSACTION BACKUP PROTOCOL E2E] passed (create/alter/drop/nested)")

    for archived in (False, True):
        server = runner.start_ours(client)
        try:
            query(server, "CREATE TABLE recovery_durable(id INT PRIMARY KEY, payload TEXT);",
                  tag="CREATE TABLE")
            query(server, "INSERT INTO recovery_durable VALUES(1,'before temp');", tag="INSERT 0 1")
            query(server, "CREATE TEMP TABLE recovery_temp(id INT PRIMARY KEY, payload TEXT);",
                  tag="CREATE TABLE")
            query(server, "BEGIN;", ready=b"T", tag="BEGIN")
            query(server, "INSERT INTO recovery_temp VALUES(9,'temporary');",
                  ready=b"T", tag="INSERT 0 1")
            query(server, "INSERT INTO recovery_durable VALUES(2,'with temp');",
                  ready=b"T", tag="INSERT 0 1")
            query(server, "COMMIT;", tag="COMMIT")
            query(server, "SELECT id,payload FROM recovery_temp;", [["9", "temporary"]], tag="SELECT 1")

            query(server, "ALTER TABLE recovery_temp RENAME TO recovery_temp_renamed;",
                  tag="ALTER TABLE")
            if archived:
                home = Path(server["dir"]) / "info"
                segments = "[0-9A-F]" * 24
                previous = sorted((home / "pg_wal").glob(segments))
                assert len(previous) == 1, previous
                query(server, "SWITCH WAL;")
                query(server, "CHECKPOINT;", tag="CHECKPOINT")
                assert not previous[0].exists(), previous
                assert (home.with_suffix(".archive") / previous[0].name).is_file(), previous
            query(server, "INSERT INTO recovery_temp_renamed VALUES(10,'renamed');", tag="INSERT 0 1")
            query(server, "BEGIN;", ready=b"T", tag="BEGIN")
            query(server, "INSERT INTO recovery_temp_renamed VALUES(11,'uncommitted');",
                  ready=b"T", tag="INSERT 0 1")
            query(server, "INSERT INTO recovery_durable VALUES(3,'uncommitted');",
                  ready=b"T", tag="INSERT 0 1")

            home = Path(server["dir"]) / "info"
            old_temp = list(home.glob("__tmp_*_recovery_temp_renamed.*"))
            assert {file.suffix for file in old_temp} >= {".stc", ".dt"}, old_temp
            kill_server(server)
            reopen(server)
            survivors = [["1", "before temp"], ["2", "with temp"]]
            query(server, "SELECT id,payload FROM recovery_durable ORDER BY id;",
                  survivors, tag="SELECT 2")
            query(server, "SELECT payload FROM recovery_durable WHERE id=2;", [["with temp"]],
                  tag="SELECT 1")
            query(server, "SELECT payload FROM recovery_durable WHERE id=3;", [], tag="SELECT 0")
            query(server, "SELECT id FROM recovery_temp;", [], state="42P01")
            query(server, "SELECT id FROM recovery_temp_renamed;", [], state="42P01")
            assert not list(home.glob("__tmp_*_recovery_temp*"))

            query(server, "CREATE TABLE recovery_temp(id INT PRIMARY KEY);", tag="CREATE TABLE")
            query(server, "INSERT INTO recovery_temp VALUES(27);", tag="INSERT 0 1")
            kill_server(server)
            checksums = subprocess.run(
                [runner.DBMS_MAIN, "--data-dir", server["dir"], "--verify-data-checksums"],
                cwd=server["dir"], capture_output=True, text=True, timeout=10, check=False)
            assert checksums.returncode == 0, checksums
            reopen(server)
            query(server, "SELECT id,payload FROM recovery_durable ORDER BY id;",
                  survivors, tag="SELECT 2")
            query(server, "SELECT id FROM recovery_temp;", [["27"]], tag="SELECT 1")
            assert not list(home.glob("__tmp_*_recovery_temp*"))
        finally:
            runner.stop_ours(server)
    print("[COLD START TEMP WAL RECOVERY PROTOCOL E2E] passed")


if __name__ == "__main__":
    main()
