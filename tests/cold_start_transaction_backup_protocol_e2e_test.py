#!/usr/bin/env python3
"""Real executable cold-start recovery initializes database locks on demand."""

import importlib.util
from pathlib import Path
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

    def query(server, sql, expected=None, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        return messages

    def restart_after_sigkill(server):
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
        fixture = Path(server["dir"])
        images = list(fixture.glob("info.txn_backup.*"))
        assert images, "crash fixture did not contain a transaction image"
        assert all((image / ".dbms_physical_backup").is_file()
                   for image in images), images
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"],
             "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            assert server["process"].poll() is None, (
                "cold-start process exited before accepting clients",
                server["process"].returncode)
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
            restart_after_sigkill(server)
            messages = query(server, "SELECT * FROM recovery_base ORDER BY id;",
                             [["1", "survivor"], ["2", None]])
            assert [field[0] for field in client.row_description_fields(messages)] == [
                b"id", b"payload"], action
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


if __name__ == "__main__":
    main()
