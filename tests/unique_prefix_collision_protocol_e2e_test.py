#!/usr/bin/env python3
"""UNIQUE compares complete values, not a lossy fixed-width index prefix."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "unique_prefix_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def duplicate(sql):
        _, state, _, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state == "23505", (sql, state)

    try:
        prefix = "12345678901234567890"
        first, second = prefix + "-alpha", prefix + "-beta"
        query('CREATE TABLE unique_prefix(id INT PRIMARY KEY,code TEXT COLLATE "C" UNIQUE);')
        query("CREATE INDEX unique_prefix_code ON unique_prefix(code);")
        query("INSERT INTO unique_prefix VALUES(1,'" + first + "');")
        query("INSERT INTO unique_prefix VALUES(2,'" + second + "');")
        query("INSERT INTO unique_prefix VALUES(3,'" + prefix + "'),(5,NULL),(6,NULL);")
        for code, expected in [(first, "1"), (second, "2"), (prefix, "3")]:
            rows = query("SELECT id FROM unique_prefix WHERE code='" + code + "';")
            assert rows == [[expected]], (code, rows)
            duplicate("INSERT INTO unique_prefix VALUES(4,'" + code + "');")
        assert query("SELECT count(*) FROM unique_prefix;") == [["5"]]
        query("DELETE FROM unique_prefix WHERE id=1;")
        query("INSERT INTO unique_prefix VALUES(7,'" + first + "');")
        duplicate("INSERT INTO unique_prefix VALUES(8,'" + second + "');")
        assert query("SELECT id FROM unique_prefix WHERE code='" + first + "';") == [["7"]]
        assert query("SELECT id FROM unique_prefix WHERE code='" + second + "';") == [["2"]]
        query("CREATE TABLE unique_numeric(id INT PRIMARY KEY,n NUMERIC UNIQUE);")
        query("INSERT INTO unique_numeric VALUES(1,123456789012345678901),(2,123456789012345678902);")
        duplicate("INSERT INTO unique_numeric VALUES(3,123456789012345678901.0);")
        assert query("SELECT id,n FROM unique_numeric ORDER BY id;") == [
            ["1", "123456789012345678901"], ["2", "123456789012345678902"]]
        assert query("SELECT 42;") == [["42"]]
        print("[UNIQUE PREFIX COLLISION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
