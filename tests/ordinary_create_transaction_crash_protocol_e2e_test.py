#!/usr/bin/env python3
"""Ordinary transactional CREATE and SAVEPOINT survive a real server crash."""

import importlib.util
import socket
import subprocess
import time
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "ordinary_create_crash_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def crash(server):
        # Do not close the live socket first: EOF would roll back the owner.
        process = server["process"]
        process.kill()
        assert process.wait(timeout=10) == -9
        server["sock"].close()
        server["sock"] = None

    def start_recovery(server):
        diagnostic = Path(server["dir"]) / "ordinary-create-reopen.stderr"
        with diagnostic.open("wb") as stderr:
            server["process"] = subprocess.Popen(
                [runner.DBMS_MAIN, "--data-dir", server["dir"],
                 "--server", str(server["port"]), "--insecure"],
                cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=stderr)
        return diagnostic

    def reconnect(server, diagnostic):
        deadline = time.monotonic() + 20
        while True:
            assert server["process"].poll() is None, (
                "cold-start failed", server["process"].returncode,
                diagnostic.read_text(errors="replace"))
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

    def crash_and_reopen(server):
        crash(server)
        reconnect(server, start_recovery(server))
        home = Path(server["dir"]) / "info"
        assert not list(home.glob(".create_table_undo*")), "retirement files survived recovery"

    def setup(server):
        query(server, "CREATE TABLE ordinary_durable(id INT PRIMARY KEY, payload TEXT);",
              tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_durable VALUES(1,'committed');", tag="INSERT 0 1")

    def committed_only(server):
        query(server, "SELECT id,payload FROM ordinary_durable ORDER BY id;",
              [["1", "committed"]], tag="SELECT 1")

    def ordinary(server):
        setup(server)
        home = Path(server["dir"]) / "info"
        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "UPDATE ordinary_durable SET payload='uncommitted' WHERE id=1;",
              ready=b"T", tag="UPDATE 1")
        query(server, "INSERT INTO ordinary_durable VALUES(2,'uncommitted');",
              ready=b"T", tag="INSERT 0 1")
        query(server, "CREATE TABLE ordinary_pending(id INT PRIMARY KEY,payload TEXT) "
              "WITH (fillfactor=61);", ready=b"T", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_pending VALUES(99,'uncommitted');",
              ready=b"T", tag="INSERT 0 1")
        assert b"fillfactor=61" in (home / "ordinary_pending.params").read_bytes().splitlines()
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id FROM ordinary_pending;", [], state="42P01")
        assert not list(home.glob("ordinary_pending.*")), "orphan relation files survived"
        query(server, "CREATE TABLE ordinary_pending(id INT PRIMARY KEY,payload TEXT) "
              "WITH (fillfactor=80);", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_pending VALUES(27,'reused');", tag="INSERT 0 1")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id,payload FROM ordinary_pending;",
              [["27", "reused"]], tag="SELECT 1")
        assert b"fillfactor=80" in (home / "ordinary_pending.params").read_bytes().splitlines()

    def owned_sequence(server):
        setup(server)
        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "CREATE TABLE ordinary_owned(id SERIAL PRIMARY KEY, payload TEXT) "
              "WITH (fillfactor=61);", ready=b"T", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_owned(payload) VALUES('temporary');",
              ready=b"T", tag="INSERT 0 1")
        query(server, "SELECT id,payload FROM ordinary_owned;",
              [["1", "temporary"]], ready=b"T", tag="SELECT 1")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id FROM ordinary_owned;", [], state="42P01")
        query(server, "SELECT relname FROM pg_class WHERE relname='ordinary_owned_id_seq';",
              [], tag="SELECT 0")
        query(server, "CREATE TABLE ordinary_owned(id SERIAL PRIMARY KEY, payload TEXT) "
              "WITH (fillfactor=80);", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_owned(payload) VALUES('reused');", tag="INSERT 0 1")
        query(server, "SELECT id,payload FROM ordinary_owned;", [["1", "reused"]], tag="SELECT 1")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id,payload FROM ordinary_owned;", [["1", "reused"]], tag="SELECT 1")

    def external_tablespace(server):
        work = Path(server["dir"])
        home = work / "info"
        space = work / "external table space"
        location = str(space).replace("'", "''")
        query(server, f"CREATE TABLESPACE recovery_space LOCATION '{location}';")
        storage = space / "info"
        unavailable = space / "temporarily-offline"
        query(server, "CREATE TABLE external_peer(id INT PRIMARY KEY, payload TEXT) "
              "TABLESPACE recovery_space;", tag="CREATE TABLE")
        query(server, "INSERT INTO external_peer VALUES(1,'committed');", tag="INSERT 0 1")

        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "UPDATE external_peer SET payload='uncommitted' WHERE id=1;",
              ready=b"T", tag="UPDATE 1")
        # Check both the default storage and multiple tables sharing one external directory.
        tables = ("default_pending", "external_pending", "external_second")
        for name in tables:
            clause = "" if name == "default_pending" else " TABLESPACE recovery_space"
            query(server, f"CREATE TABLE {name}(id INT PRIMARY KEY,payload TEXT){clause};",
                  ready=b"T", tag="CREATE TABLE")
            query(server, f"INSERT INTO {name} VALUES(9,'pending');", ready=b"T", tag="INSERT 0 1")
            folder = home if not clause else storage
            assert (folder / f"{name}.dt").is_file(), name

        crash(server)
        journal = home / ".create_table_undo"
        receipts_before = {path.name: path.read_bytes() for path in journal.iterdir()}
        table_list = (home / "tlist.lst").read_bytes()
        storage.rename(unavailable)
        try:
            diagnostic = start_recovery(server)
            process = server["process"]
            deadline = time.monotonic() + 15
            while process.poll() is None:
                # A baseline that incorrectly opens the database should fail
                # the test promptly, rather than waiting for a process timeout.
                try:
                    with socket.create_connection(("127.0.0.1", server["port"]), timeout=0.2):
                        pass
                except OSError:
                    assert time.monotonic() < deadline, "recovery did not exit"
                    time.sleep(0.05)
                else:
                    process.terminate()
                    process.wait(timeout=10)
                    raise AssertionError("startup accepted a missing tablespace directory")
            log = diagnostic.read_text()
            assert process.returncode != 0, log
            assert "could not recover CREATE TABLE undo" in log, log
            assert receipts_before == {path.name: path.read_bytes() for path in journal.iterdir()}
            assert (home / "tlist.lst").read_bytes() == table_list
            for name in tables:
                assert (home / f"{name}.stc").is_file(), name
            assert (unavailable / "external_pending.dt").is_file()
            assert (home / "default_pending.dt").is_file()
        finally:
            unavailable.rename(storage)

        reconnect(server, start_recovery(server))
        query(server, "SELECT id,payload FROM external_peer;", [["1", "committed"]], tag="SELECT 1")
        for name in tables:
            query(server, f"SELECT id FROM {name};", [], state="42P01")
            assert not list(home.glob(f"{name}.*")) and not list(storage.glob(f"{name}.*")), name
        assert not journal.exists()
        query(server, "CREATE TABLE external_pending(id INT PRIMARY KEY, payload TEXT) "
              "TABLESPACE recovery_space;", tag="CREATE TABLE")
        query(server, "INSERT INTO external_pending VALUES(27,'reused');", tag="INSERT 0 1")
        crash_and_reopen(server)
        query(server, "SELECT id,payload FROM external_pending;", [["27", "reused"]], tag="SELECT 1")
        query(server, "SELECT id,payload FROM external_peer;", [["1", "committed"]], tag="SELECT 1")

    def savepoint(server):
        setup(server)
        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "CREATE TABLE ordinary_before_savepoint(id INT);", ready=b"T", tag="CREATE TABLE")
        query(server, "SAVEPOINT ordinary_s;", ready=b"T", tag="SAVEPOINT")
        query(server, "CREATE TABLE ordinary_after_savepoint(id INT);", ready=b"T", tag="CREATE TABLE")
        query(server, "ROLLBACK TO SAVEPOINT ordinary_s;", ready=b"T", tag="ROLLBACK")
        query(server, "CREATE TABLE ordinary_after_savepoint(id INT);", ready=b"T", tag="CREATE TABLE")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id FROM ordinary_before_savepoint;", [], state="42P01")
        query(server, "SELECT id FROM ordinary_after_savepoint;", [], state="42P01")

    def savepoint_commit(server):
        setup(server)
        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "CREATE TABLE ordinary_kept(id INT);", ready=b"T", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_kept VALUES(7);", ready=b"T", tag="INSERT 0 1")
        query(server, "SAVEPOINT ordinary_s;", ready=b"T", tag="SAVEPOINT")
        query(server, "CREATE TABLE ordinary_discarded(id INT);", ready=b"T", tag="CREATE TABLE")
        query(server, "ROLLBACK TO SAVEPOINT ordinary_s;", ready=b"T", tag="ROLLBACK")
        query(server, "COMMIT;", tag="COMMIT")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id FROM ordinary_kept;", [["7"]], tag="SELECT 1")
        query(server, "SELECT id FROM ordinary_discarded;", [], state="42P01")
        query(server, "CREATE TABLE ordinary_discarded(id INT);", tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_discarded VALUES(8);", tag="INSERT 0 1")
        crash_and_reopen(server)
        query(server, "SELECT id FROM ordinary_kept;", [["7"]], tag="SELECT 1")
        query(server, "SELECT id FROM ordinary_discarded;", [["8"]], tag="SELECT 1")

    def mixed_create(server, family):
        # The later DDL may take its own transaction image after the first
        # CREATE. Recovery must also undo the earlier CREATE, not just restore
        # that mid-transaction image.
        setup(server)
        query(server, "BEGIN;", ready=b"T", tag="BEGIN")
        query(server, "UPDATE ordinary_durable SET payload='uncommitted' WHERE id=1;",
              ready=b"T", tag="UPDATE 1")
        query(server, "INSERT INTO ordinary_durable VALUES(2,'uncommitted');",
              ready=b"T", tag="INSERT 0 1")
        if family == "inheritance":
            query(server, "CREATE TABLE ordinary_mixed(note TEXT) INHERITS (ordinary_durable);",
                  ready=b"T", tag="CREATE TABLE")
            query(server, "INSERT INTO ordinary_mixed(id,payload,note) VALUES(17,'pending','child');",
                  ready=b"T", tag="INSERT 0 1")
        elif family == "unlogged":
            query(server, "CREATE UNLOGGED TABLE ordinary_mixed(id INT PRIMARY KEY, payload TEXT);",
                  ready=b"T", tag="CREATE TABLE")
            query(server, "INSERT INTO ordinary_mixed VALUES(17,'pending');",
                  ready=b"T", tag="INSERT 0 1")
        else:
            query(server, "CREATE TABLE ordinary_mixed(id INT PRIMARY KEY, payload TEXT);",
                  ready=b"T", tag="CREATE TABLE")
            if family == "alter":
                query(server, "ALTER TABLE ordinary_mixed ADD COLUMN extra INT;",
                      ready=b"T", tag="ALTER TABLE")
            elif family == "index":
                query(server, "CREATE INDEX ordinary_mixed_payload_idx ON ordinary_mixed(payload);",
                      ready=b"T", tag="CREATE INDEX")
            elif family == "serial":
                query(server, "CREATE TABLE ordinary_mixed_aux(id SERIAL PRIMARY KEY, payload TEXT);",
                      ready=b"T", tag="CREATE TABLE")
                query(server, "INSERT INTO ordinary_mixed_aux(payload) VALUES('aux');",
                      ready=b"T", tag="INSERT 0 1")
            else:
                raise AssertionError(("unknown mixed family", family))
            query(server, "INSERT INTO ordinary_mixed(id,payload) VALUES(17,'pending');",
                  ready=b"T", tag="INSERT 0 1")

        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id FROM ordinary_mixed;", [], state="42P01")
        if family == "index":
            query(server, "SELECT relname FROM pg_class WHERE relname='ordinary_mixed_payload_idx';",
                  [], tag="SELECT 0")
        if family == "serial":
            query(server, "SELECT id FROM ordinary_mixed_aux;", [], state="42P01")
            query(server, "SELECT relname FROM pg_class WHERE relname='ordinary_mixed_aux_id_seq';",
                  [], tag="SELECT 0")

        # Reuse the rolled-back name for a committed ordinary table and make
        # sure the next crash does not delete the new physical generation.
        query(server, "CREATE TABLE ordinary_mixed(id INT PRIMARY KEY, payload TEXT);",
              tag="CREATE TABLE")
        query(server, "INSERT INTO ordinary_mixed VALUES(27,'reused');", tag="INSERT 0 1")
        crash_and_reopen(server)
        committed_only(server)
        query(server, "SELECT id,payload FROM ordinary_mixed;", [["27", "reused"]], tag="SELECT 1")

    failures = []
    cases = [("ordinary", ordinary), ("owned_sequence", owned_sequence),
             ("savepoint", savepoint), ("savepoint_commit", savepoint_commit),
             ("external_tablespace", external_tablespace)]
    for family in ("alter", "index", "serial", "inheritance", "unlogged"):
        cases.append(("mixed_" + family, lambda server, item=family: mixed_create(server, item)))
    for name, case in cases:
        server = runner.start_ours(client)
        try:
            case(server)
            print("[ORDINARY CREATE CRASH]", name, "passed", flush=True)
        except Exception as error:
            failures.append((name, repr(error)))
            print("[ORDINARY CREATE CRASH]", name, "failed:", repr(error), flush=True)
        finally:
            private_work = Path(server["dir"])
            runner.stop_ours(server)
            assert not private_work.exists(), ("private test directory remains", private_work)
    assert not failures, failures


if __name__ == "__main__":
    main()
