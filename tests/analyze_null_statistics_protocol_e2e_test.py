#!/usr/bin/env python3
"""ANALYZE and full pg_stats preserve NULL, empty text and literal NULL."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "analyze_null_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    try:
        query("CREATE TABLE null_stats_items (id INT PRIMARY KEY,v TEXT,all_null INT);")
        values = ",".join(
            f"({i}," + ("NULL" if i < 5 else "''" if i < 10 else "'NULL'" if i < 15 else "'other'")
            + ",NULL)" for i in range(20))
        query("INSERT INTO null_stats_items VALUES " + values + ";")
        query("ANALYZE null_stats_items;")
        assert query("SELECT id FROM null_stats_items WHERE v IS NULL ORDER BY id;") == [
            [str(i)] for i in range(5)]
        assert query("SELECT id FROM null_stats_items WHERE v='' ORDER BY id;") == [
            [str(i)] for i in range(5, 10)]
        assert query("SELECT COUNT(*) FROM null_stats_items WHERE all_null IS NULL;") == [["20"]]
        query("CREATE TABLE null_stats_empty (v TEXT);")
        query("ANALYZE null_stats_empty;")
        # The real seventeen-column view is now available. Keep all the
        # original data/NULL assertions and verify its actual slot outputs.
        stats = query("SELECT * FROM pg_stats WHERE tablename='null_stats_items' ORDER BY attname;")
        assert len(stats) == 3 and all(len(row) == 17 for row in stats), stats
        by_name = {row[2]: row for row in stats}
        assert set(by_name) == {"id", "v", "all_null"}, stats
        absent = by_name["all_null"]
        assert absent[:7] == ["public", "null_stats_items", "all_null", "f", "1", "4", "0"], absent
        assert absent[7:] == [None] * 10, absent
        text = by_name["v"]
        assert text[4] == "0.25" and text[6] == "-0.15", text
        assert '""' in text[7] and '"NULL"' in text[7] and 'other' in text[7], text
        assert text[8] == "{0.25,0.25,0.25}" and text[9] is None, text
        assert text[11:] == [None] * 6, text
        assert query("SELECT count(*) FROM pg_stats WHERE tablename='null_stats_empty';") == [["0"]]
        assert query("SELECT 42;") == [["42"]]
        print("[ANALYZE NULL STATISTICS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
