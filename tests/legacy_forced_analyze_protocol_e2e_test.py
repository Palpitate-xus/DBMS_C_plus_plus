#!/usr/bin/env python3
"""Legacy writes must respect auto_analyze and not force a statement scan."""

import importlib.util
import json
from pathlib import Path
from unittest import mock


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "legacy_analyze_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    original_mkdtemp = runner.tempfile.mkdtemp

    def configured_directory(*args, **kwargs):
        directory = original_mkdtemp(*args, **kwargs)
        (Path(directory) / "dbms.conf").write_text("auto_analyze=off\nauto_vacuum=off\n")
        return directory

    with mock.patch.object(runner.tempfile, "mkdtemp", side_effect=configured_directory):
        server = runner.start_ours(client)
    stats = Path(server["dir"]) / "info" / ".stats"

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def expect_error(sql, expected_state):
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, state, message)

    try:
        query("CREATE TABLE no_forced_analyze (id INT PRIMARY KEY,v INT);")
        initial = stats.read_bytes() if stats.is_file() else b""
        # Cross the default 50-row storage threshold too, proving the
        # startup configuration actually disables that automatic path.
        values = ",".join(f"({row},{row + 6})" for row in range(1, 61))
        query("INSERT INTO no_forced_analyze VALUES " + values + ";")
        after_insert = stats.read_bytes() if stats.is_file() else b""
        assert after_insert == initial, ("INSERT ignored auto_analyze=off", initial, after_insert)
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON) SELECT id FROM no_forced_analyze;")))
        assert document["totalRows"] == 1000, document
        query("VACUUM (ANALYZE = true) no_forced_analyze;")
        vacuum_analyze_stats = stats.read_bytes() if stats.is_file() else b""
        assert b"no_forced_analyze __rows__ 60||" in vacuum_analyze_stats, \
            vacuum_analyze_stats
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON) SELECT id FROM no_forced_analyze;")))
        assert document["totalRows"] == 60, document
        query("ANALYZE no_forced_analyze;")
        before = stats.read_bytes()
        assert b"no_forced_analyze __rows__ 60||" in before, before
        query("UPDATE no_forced_analyze SET v=10 WHERE id=1;")
        assert stats.read_bytes() == before, "UPDATE ignored auto_analyze=off"
        query("DELETE FROM no_forced_analyze WHERE id=2;")
        assert stats.read_bytes() == before, "DELETE ignored auto_analyze=off"
        query("BEGIN;")
        query("INSERT INTO no_forced_analyze VALUES (61,11);")
        query("ROLLBACK;")
        assert stats.read_bytes() == before, "rolled-back write changed ANALYZE metadata"
        assert query("SELECT id FROM no_forced_analyze ORDER BY id;") == [
            [str(row)] for row in range(1, 61) if row != 2]
        query("ANALYZE no_forced_analyze;")
        assert b"no_forced_analyze __rows__ 59||" in stats.read_bytes()
        query("INSERT INTO no_forced_analyze VALUES (61,11);")
        query("VACUUM ANALYZE no_forced_analyze;")
        assert b"no_forced_analyze __rows__ 60||" in stats.read_bytes()

        expect_error("VACUUM (FREEZE) no_forced_analyze;", "0A000")
        query("BEGIN;")
        expect_error("VACUUM no_forced_analyze;", "25001")
        query("ROLLBACK;")

        query("CREATE TABLE full_option_probe (id INT PRIMARY KEY, payload VARCHAR(2048));")
        payload = "x" * 1024
        values = ",".join(
            f"({row},'{payload}')" for row in range(1, 201))
        query("INSERT INTO full_option_probe VALUES " + values + ";")
        query("DELETE FROM full_option_probe WHERE id > 1;")
        heap = Path(server["dir"]) / "info" / "full_option_probe.dt"
        before_full = heap.stat().st_size
        query("VACUUM (FULL) full_option_probe;")
        after_full = heap.stat().st_size
        assert after_full < before_full, (before_full, after_full)
        assert query("SELECT count(*) FROM full_option_probe;") == [["1"]]
        print("[LEGACY FORCED ANALYZE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
