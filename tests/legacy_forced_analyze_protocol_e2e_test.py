#!/usr/bin/env python3
"""Manual statistics refreshes respect the requested relations and columns."""

import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import time
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

    def query(sql, expected_tag=None, ready=b"I"):
        frames = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(
            frames)
        assert state is None, (sql, state, message)
        if expected_tag is not None:
            assert frames == [(b"C", expected_tag.encode() + b"\0"), (b"Z", ready)], (sql, frames)
        return rows

    def expect_error(sql, expected_state, ready=None):
        frames = client.simple_query(server["sock"], sql)
        _, state, message, _, _ = runner.decode_wire_result(frames)
        assert state == expected_state, (sql, state, message)
        if ready is not None:
            assert [kind for kind, _ in frames] == [b"E", b"Z"] and frames[-1] == (b"Z", ready), (sql, frames)

    def cold_restart():
        server["sock"].close()
        server["sock"] = None
        server["process"].terminate()
        assert server["process"].wait(timeout=20) == 0
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                server["sock"] = socket.create_connection(
                    ("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
                break
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline
                time.sleep(0.05)
        client.startup(server["sock"], "alice", "info")

    def check_selected_statistics():
        table = "targeted_stats"
        aux = "Target,Aux"
        query(f'CREATE TABLE {table}(id INT PRIMARY KEY, score INT, spared INT, "Wave,Tag" INT, "Odd""Name" INT)')
        query(f'CREATE TABLE "{aux}" (id INT PRIMARY KEY, "Only,This" INT, spare INT)')
        values = ",".join(f"({i},{1 if i <= 60 else 2},{10 if i <= 50 else 20},"
                          f"{101 if i <= 70 else 102},{201 if i <= 40 else 202})"
                          for i in range(1, 81))
        query(f"INSERT INTO {table} VALUES " + values)
        query(f'INSERT INTO "{aux}" VALUES (1,10,20),(2,10,20),(3,11,21),(4,11,21)')
        query(f"ANALYZE {table}", "ANALYZE")
        query(f'ANALYZE "{aux}"', "ANALYZE")

        typed_stats = stats.with_name(".analyzed_statistics")

        def persisted():
            return stats.read_bytes(), typed_stats.read_bytes()

        def column_stats(name=table):
            stat_query = f"SELECT * FROM pg_catalog.pg_stats WHERE tablename='{name}' ORDER BY attname"
            prefix = (name + " ").encode()
            legacy = {line[len(prefix):].split(b" ", 1)[0].decode(): line
                      for line in stats.read_bytes().splitlines() if line.startswith(prefix)}
            rows = query(stat_query)
            catalog = {row[2]: row for row in rows}
            assert len(catalog) == len(rows) and set(legacy) == set(catalog) | {"__rows__"}, (name, legacy.keys(), catalog.keys())
            return legacy, catalog

        def estimate(predicate):
            document = json.loads("\n".join(row[0] for row in query(
                f"EXPLAIN (FORMAT JSON) SELECT id FROM {table} WHERE {predicate}")))
            return document["totalRows"]

        def selected_ids(predicate):
            return query(f"SELECT id FROM {table} WHERE {predicate} ORDER BY id")

        def check_changed_columns(before, selected, name=table):
            after = column_stats(name)
            for old, new in zip(before, after):
                assert old.keys() == new.keys()
                changed = {column for column in old if old[column] != new[column]}
                assert changed == selected, (name, changed, selected)
            return after

        original = column_stats()
        aux_original = column_stats(aux)
        unrelated = column_stats("no_forced_analyze")
        assert set(original[1]) == {"id", "score", "spared", "Wave,Tag", 'Odd"Name'}
        assert estimate("score=1") == 60 and estimate("spared=10") == 50
        query(f'UPDATE {table} SET score=3, spared=21, "Wave,Tag"=103, "Odd""Name"=203 WHERE id>10 AND id<=60')
        query(f'UPDATE "{aux}" SET "Only,This"=12, spare=22 WHERE id>=3')
        assert column_stats() == original and column_stats(aux) == aux_original
        assert estimate("score=1") == 60
        assert selected_ids("score=1") == [[str(i)] for i in range(1, 11)]

        unchanged = persisted()
        for sql, state in (
            (f"ANALYZE {table} (score, missing_column)", "42703"),
            (f"ANALYZE {table} (score, SCORE)", "42701"),
            (f'ANALYZE {table} ("Wave,Tag", "Wave,Tag")', "42701"),
            (f"ANALYZE {table} (score), missing_stats_target (score)", "42P01"),
            (f"VACUUM (ANALYZE) {table} (score, missing_column)", "42703"),
            (f"VACUUM (ANALYZE) {table} (score, score)", "42701"),
        ):
            expect_error(sql, state, b"I")
            assert persisted() == unchanged, sql
        query(f'ANALYZE {table} (score, "Wave,Tag"), "public"."{aux}" ("Only,This")', "ANALYZE")
        analyzed = check_changed_columns(original, {"score", "Wave,Tag"})
        aux_analyzed = check_changed_columns(aux_original, {"Only,This"}, aux)
        assert column_stats("no_forced_analyze") == unrelated
        assert estimate("score=1") == 10 and estimate("spared=10") == 50
        assert selected_ids("score=1") == selected_ids("spared=10") == [[str(i)] for i in range(1, 11)]

        query(f'UPDATE {table} SET score=4, spared=22, "Wave,Tag"=104, "Odd""Name"=204 WHERE id<=5')
        query(f'UPDATE "{aux}" SET "Only,This"=13, spare=23 WHERE id=1')
        assert column_stats() == analyzed and column_stats(aux) == aux_analyzed
        assert estimate("score=1") == 10
        query(f'VACUUM (ANALYZE) {table} (score, "Odd""Name"), "{aux}" ("Only,This")', "VACUUM")
        vacuumed = check_changed_columns(analyzed, {"score", 'Odd"Name'})
        aux_vacuumed = check_changed_columns(aux_analyzed, {"Only,This"}, aux)
        assert column_stats("no_forced_analyze") == unrelated
        assert estimate("score=1") == 5 and estimate("spared=10") == 50
        assert selected_ids("score=1") == selected_ids("spared=10") == [[str(i)] for i in range(6, 11)]

        durable = persisted()
        cold_restart()
        assert persisted() == durable and column_stats() == vacuumed
        assert column_stats(aux) == aux_vacuumed and column_stats("no_forced_analyze") == unrelated
        assert estimate("score=1") == 5 and estimate("spared=10") == 50
        assert selected_ids("score=1") == [[str(i)] for i in range(6, 11)]

        # A later relation's invalid column does not undo an earlier
        # successful target, once all relation names have resolved.
        query(f"UPDATE {table} SET score=5 WHERE id=6")
        expect_error(f'ANALYZE {table} (score), "{aux}" (missing_column)', "42703", b"I")
        committed = check_changed_columns(vacuumed, {"score"})
        assert column_stats(aux) == aux_vacuumed
        assert estimate("score=1") == 4 and selected_ids("score=1") == [[str(i)] for i in range(7, 11)]

        committed_files = persisted()
        query("BEGIN", "BEGIN", b"T")
        query(f"UPDATE {table} SET score=6 WHERE id=7")
        query(f"ANALYZE {table} (score)", "ANALYZE", b"T")
        check_changed_columns(committed, {"score"})
        query("ROLLBACK", "ROLLBACK")
        assert persisted() == committed_files and column_stats() == committed
        assert estimate("score=1") == 4 and selected_ids("score=1") == [[str(i)] for i in range(7, 11)]

        query(f"UPDATE {table} SET score=8 WHERE id=7")
        schema_file = stats.with_name(table + ".stc")
        before_rewrite = schema_file.read_bytes()
        query(f"VACUUM (FULL, ANALYZE) {table} (score)", "VACUUM")
        assert schema_file.read_bytes() != before_rewrite
        full_stats = check_changed_columns(committed, {"score"})
        assert estimate("score=1") == 3 and estimate("spared=10") == 50
        assert selected_ids("score=1") == [[str(i)] for i in range(8, 11)]
        first_full = persisted()
        assert first_full[1].startswith(b"\0DBMS_ANALYZED_STATS\0\x02\0\0\0")
        cold_restart()
        assert persisted() == first_full and column_stats() == full_stats

        query(f'UPDATE {table} SET score=9, "Wave,Tag"=108 WHERE id=8')
        before_repeat = schema_file.read_bytes()
        query(f'VACUUM (FULL, ANALYZE) {table} ("Wave,Tag")', "VACUUM")
        assert schema_file.read_bytes() != before_repeat
        repeated = check_changed_columns(full_stats, {"Wave,Tag"})
        second_full = persisted()
        assert second_full[1].startswith(b"\0DBMS_ANALYZED_STATS\0\x02\0\0\0")
        cold_restart()
        assert persisted() == second_full and column_stats() == repeated
        assert estimate("score=1") == 3 and estimate("spared=10") == 50
        assert selected_ids("score=1") == [["9"], ["10"]]

        # FULL finishes its storage rewrite before ANALYZE binds its columns.
        full_table = "full_option_probe"
        query(f"ANALYZE {full_table}", "ANALYZE")
        prior_full_stats = column_stats(full_table)
        payload = "x" * 1024
        values = ",".join(f"({i},'{payload}')" for i in range(2, 42))
        query(f"INSERT INTO {full_table} VALUES " + values)
        query(f"DELETE FROM {full_table} WHERE id > 1")
        heap = stats.with_name(full_table + ".dt")
        before_full = heap.stat().st_size
        expect_error(f"VACUUM (FULL, ANALYZE) {full_table} (missing_column)", "42703", b"I")
        assert heap.stat().st_size < before_full
        assert query(f"SELECT id FROM {full_table}") == [["1"]]
        assert column_stats(full_table) == prior_full_stats

        before_samples = column_stats()
        values = ",".join(f"({i},7,23,106,206)" for i in range(101, 181))
        query(f"INSERT INTO {table} VALUES " + values)
        query(f"ANALYZE {table} (score)", "ANALYZE")
        samples, catalog = column_stats()
        assert f"{table} __rows__ 160||".encode() == samples["__rows__"]
        assert samples["score"].endswith(b"|160")
        assert samples["spared"].endswith(b"|80")
        assert {name for name, old in before_samples[0].items() if samples[name] != old} == {"__rows__", "score"}
        assert {name for name, old in before_samples[1].items() if catalog[name] != old} == {"score"}

        # Force the planner to read its legacy statistics. The old 50/80
        # fraction and the new 80/160 fraction both apply to the 160-row table.
        hidden_typed = typed_stats.with_suffix(".saved")
        typed_stats.rename(hidden_typed)
        try:
            assert estimate("spared=10") == 100
            assert estimate("score=7") == 80
            assert selected_ids("spared=10") == [[str(i)] for i in range(6, 11)]
            assert selected_ids("score=7") == [[str(i)] for i in range(101, 181)]
        finally:
            hidden_typed.rename(typed_stats)

        empty = "targeted_empty"
        query(f'CREATE TABLE {empty} (id INT PRIMARY KEY, "select" INT, "from" INT)')
        query(f"INSERT INTO {empty} VALUES (1,11,21),(2,11,21),(3,12,22),(4,12,22)")
        query(f"ANALYZE {empty}", "ANALYZE")
        initial_empty = column_stats(empty)
        query(f'UPDATE {empty} SET "select"=13, "from"=23')
        before_errors = persisted()
        for sql, state in (
            (f"ANALYZE {empty} (select)", "42601"),
            (f"ANALYZE {empty} (from)", "42601"),
            (f"VACUUM (ANALYZE) {empty} (select)", "42601"),
            (f'ANALYZE {empty} ("SELECT")', "42703"),
        ):
            expect_error(sql, state, b"I")
        assert persisted() == before_errors
        query(f'ANALYZE {empty} ("select")', "ANALYZE")
        populated = check_changed_columns(initial_empty, {"select"}, empty)

        query(f"DELETE FROM {empty}")
        query(f'ANALYZE {empty} ("select")', "ANALYZE")
        legacy_empty, catalog_empty = column_stats(empty)
        assert catalog_empty == populated[1]
        assert legacy_empty["__rows__"] == f"{empty} __rows__ 0||".encode()
        assert {name: value for name, value in legacy_empty.items() if name != "__rows__"} == {
            name: value for name, value in populated[0].items() if name != "__rows__"}
        empty_heap = stats.with_name(empty + ".dt")
        populated_size = empty_heap.stat().st_size
        query(f'VACUUM (FULL, ANALYZE) {empty} ("select")', "VACUUM")
        assert empty_heap.stat().st_size < populated_size
        assert column_stats(empty) == (legacy_empty, catalog_empty)
        assert query(f"SELECT id FROM {empty}") == []

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
        check_selected_statistics()
        print("[LEGACY FORCED ANALYZE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
