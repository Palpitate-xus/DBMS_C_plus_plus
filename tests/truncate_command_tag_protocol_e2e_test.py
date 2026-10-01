#!/usr/bin/env python3
"""TRUNCATE's command tag is independent of the optional TABLE keyword."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "truncate_tag_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows, tag

    try:
        query("CREATE TABLE truncate_tag_a(id INT);")
        query("CREATE TABLE truncate_tag_b(id INT);")
        for sql in ["TRUNCATE truncate_tag_a;", "truncate truncate_tag_a;",
                    "TRUNCATE TABLE truncate_tag_a;",
                    "TRUNCATE truncate_tag_a,truncate_tag_b;",
                    "TRUNCATE truncate_tag_a RESTART IDENTITY;"]:
            query("INSERT INTO truncate_tag_a VALUES(1);")
            rows, tag = query(sql)
            assert rows == [] and tag == "TRUNCATE TABLE", (sql, rows, tag)
            assert query("SELECT count(*) FROM truncate_tag_a;")[0] == [["0"]]
        print("[TRUNCATE COMMAND TAG PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
