#!/usr/bin/env python3
"""Cold startup preserves durable relations, names and transaction state."""

import importlib.util
import os
from pathlib import Path
import resource
import shutil
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

    def rename_with_shared_prefix(server, old_name, neighbor):
        new_name = "rename_item_moved"
        home = Path(server["dir"]) / "info"
        fixtures = {
            "rename_item": ("neighbor_key", 1, 11, "short-long-value" * 400),
            "rename_item_neighbor": ("indexed_value", 2, 22, "neighbor-payload" * 400),
        }
        # Establish both names first: the short table's secondary file also
        # starts with the long name, although only the short table owns it.
        for table, (column, _, _, _) in fixtures.items():
            query(server, f"CREATE TABLE {table}(id INT PRIMARY KEY, {column} INT, payload TEXT);",
                  tag="CREATE TABLE")
        for table, (column, key, indexed, payload) in fixtures.items():
            query(server, f"CREATE INDEX {table}_lookup ON {table}({column});", tag="CREATE INDEX")
            query(server, f"INSERT INTO {table} VALUES({key},{indexed},'{payload}');",
                  tag="INSERT 0 1")

        def read_record(table, fixture):
            column, key, indexed, payload = fixture
            query(server, f"SELECT id FROM {table} WHERE id={key};", [[str(key)]], tag="SELECT 1")
            query(server, f"SELECT id,payload FROM {table} WHERE {column}={indexed};",
                  [[str(key), payload]], tag="SELECT 1")

        def read_forks(table, fixture):
            column = fixture[0]
            required = (".dt", ".idx", ".toast.dt", ".toast.idx", f"_{column}.idx")
            return {suffix: (home / (table + suffix)).read_bytes() for suffix in required}

        for table, fixture in fixtures.items():
            read_record(table, fixture)
        query(server, "CHECKPOINT;", tag="CHECKPOINT")
        old_forks = read_forks(old_name, fixtures[old_name])
        neighbor_forks = read_forks(neighbor, fixtures[neighbor])
        assert all(old_forks.values()) and all(neighbor_forks.values())

        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, f"ALTER TABLE {old_name} RENAME TO {new_name};",
              ready=b"T", tag="ALTER TABLE")
        restart_after_sigkill(server)
        query(server, f"SELECT id FROM {new_name};", [], state="42P01")
        assert read_forks(old_name, fixtures[old_name]) == old_forks
        assert read_forks(neighbor, fixtures[neighbor]) == neighbor_forks
        read_record(old_name, fixtures[old_name])
        read_record(neighbor, fixtures[neighbor])

        query(server, f"ALTER TABLE {old_name} RENAME TO {new_name};", tag="ALTER TABLE")
        query(server, f"SELECT id FROM {old_name};", [], state="42P01")
        assert not (home / f"{old_name}.stc").exists()
        assert all(not (home / (old_name + suffix)).exists() for suffix in old_forks)
        assert read_forks(new_name, fixtures[old_name]) == old_forks, "renamed files differ"
        assert read_forks(neighbor, fixtures[neighbor]) == neighbor_forks, "neighbor files changed"

        query(server, f"CREATE TABLE {old_name}(id INT PRIMARY KEY, reused INT, payload TEXT);",
              tag="CREATE TABLE")
        query(server, f"CREATE INDEX rename_reused_lookup ON {old_name}(reused);", tag="CREATE INDEX")
        query(server, f"INSERT INTO {old_name} VALUES(3,33,'fresh row');", tag="INSERT 0 1")
        read_record(old_name, ("reused", 3, 33, "fresh row"))
        read_record(new_name, fixtures[old_name])
        read_record(neighbor, fixtures[neighbor])

        query(server, f"DROP TABLE {old_name};", tag="DROP TABLE")
        assert read_forks(new_name, fixtures[old_name]) == old_forks, "reuse changed renamed files"
        assert read_forks(neighbor, fixtures[neighbor]) == neighbor_forks, "reuse changed neighbor files"
        kill_server(server)
        reopen(server)
        query(server, f"SELECT id FROM {old_name};", [], state="42P01")
        read_record(new_name, fixtures[old_name])
        read_record(neighbor, fixtures[neighbor])

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

    for recycle, commit in ((False, True), (True, True), (True, False)):
        server = runner.start_ours(client)
        try:
            cluster = Path(server["dir"])
            query(server, "CREATE TABLE retained_commit(id INT PRIMARY KEY, v INT);",
                  tag="CREATE TABLE")
            query(server, "INSERT INTO retained_commit VALUES (1,10);", tag="INSERT 0 1")
            if not recycle:
                query(server, "SWITCH WAL;")
            query(server, "BEGIN;", ready=b"T", tag="BEGIN")
            query(server, "ALTER TABLE retained_commit ADD COLUMN extra INT;",
                  ready=b"T", tag="ALTER TABLE")
            [image] = cluster.glob("info.txn_backup.*")
            assert (image / ".dbms_physical_backup").is_file()
            saved = cluster / "saved_rollback_image"
            shutil.copytree(image, saved)
            if commit:
                query(server, "COMMIT;", tag="COMMIT")
            else:
                query(server, "ROLLBACK;", tag="ROLLBACK")
                query(server, "ALTER TABLE retained_commit ADD COLUMN extra INT;",
                      tag="ALTER TABLE")
            query(server, "INSERT INTO retained_commit VALUES (2,20,200);", tag="INSERT 0 1")
            query(server, "UPDATE retained_commit SET extra=111 WHERE id=1;", tag="UPDATE 1")
            segment = "000000010000000000000000"
            active = cluster / "info" / "pg_wal" / segment
            archive = cluster / "info.archive"
            if recycle:
                query(server, "SWITCH WAL;")
                query(server, "CHECKPOINT;", tag="CHECKPOINT")
                assert not active.exists() and (archive / segment).is_file()
            else:
                assert active.is_file()
            kill_server(server)
            # Simulate an interrupted cleanup using the exact image that
            # the finished transaction produced.
            assert not image.exists()
            shutil.copytree(saved, image)
            if recycle and commit:
                missing_archive = cluster / "withheld_archive"
                archive.rename(missing_archive)
                diagnostics = cluster / "unknown-transaction.stderr"
                try:
                    start_from_disk(server, diagnostics)
                    assert server["process"].wait(timeout=20) != 0
                    assert "cannot verify transaction image after WAL recycling" in diagnostics.read_text()
                    assert (image / ".dbms_physical_backup").is_file()
                finally:
                    missing_archive.rename(archive)
                archived_segment = archive / segment
                with archived_segment.open("r+b") as file:
                    file.seek(-1, os.SEEK_END)
                    assert file.read(1) == b"\0"
                    file.seek(-1, os.SEEK_END)
                    file.write(b"\x01")
                try:
                    start_from_disk(server, diagnostics)
                    assert server["process"].wait(timeout=20) != 0
                    assert "cannot verify transaction image after WAL recycling" in diagnostics.read_text()
                    assert (image / ".dbms_physical_backup").is_file()
                finally:
                    with archived_segment.open("r+b") as file:
                        file.seek(-1, os.SEEK_END)
                        file.write(b"\0")
            reopen(server)
            query(server, "SELECT id,v,extra FROM retained_commit ORDER BY id;",
                  [["1", "10", "111"], ["2", "20", "200"]], tag="SELECT 2")
            assert not image.exists()
            query(server, "INSERT INTO retained_commit VALUES (3,30,300);", tag="INSERT 0 1")
            kill_server(server)
            reopen(server)
            query(server, "SELECT id,v,extra FROM retained_commit ORDER BY id;",
                  [["1", "10", "111"], ["2", "20", "200"], ["3", "30", "300"]],
                  tag="SELECT 3")
        finally:
            runner.stop_ours(server)
    print("[COLD START COMMITTED IMAGE RETENTION PROTOCOL E2E] passed")

    for old_name, neighbor in (("rename_item", "rename_item_neighbor"),
                               ("rename_item_neighbor", "rename_item")):
        server = runner.start_ours(client)
        try:
            rename_with_shared_prefix(server, old_name, neighbor)
        finally:
            runner.stop_ours(server)
    print("[COLD START RENAME TABLE SHARED PREFIX PROTOCOL E2E] passed")


if __name__ == "__main__":
    main()
