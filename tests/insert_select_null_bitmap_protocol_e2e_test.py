#!/usr/bin/env python3
"""INSERT SELECT keeps bitmap NULL, empty text and NULL-looking text separate."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "insert_select_null_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None, tag=None, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, command_tag = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert command_tag == tag, (sql, command_tag, tag)

    def error(sql, expected, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])

    try:
        query("CREATE TABLE insert_null_source(id INT,pid INT,content TEXT);")
        query("CREATE TABLE insert_null_target(id INT PRIMARY KEY,pid INT,content TEXT DEFAULT 'fallback');")
        query("INSERT INTO insert_null_source VALUES(1,2,'NULL'),(2,NULL,NULL),(3,NULL,'');")
        expected = [["1", "2", "NULL"], ["2", None, None], ["3", None, ""]]
        query("INSERT INTO insert_null_target SELECT id,pid,content FROM insert_null_source RETURNING id,pid,content;", expected, "INSERT 0 3")
        query("SELECT id,pid,content FROM insert_null_target ORDER BY id;", expected)
        query("CREATE TABLE insert_null_star(id INT,pid INT,content TEXT);")
        query("INSERT INTO insert_null_star SELECT * FROM insert_null_source RETURNING id,pid,content;", expected, "INSERT 0 3")
        query("CREATE TABLE insert_null_projected(id INT,pid INT,content TEXT);")
        query("INSERT INTO insert_null_projected SELECT id+10,pid*2,upper(content) FROM insert_null_source WHERE pid IS NULL RETURNING id,pid,content;", [["12", None, None], ["13", None, ""]], "INSERT 0 2")
        query("INSERT INTO insert_null_projected SELECT id+20,pid,content FROM insert_null_source WHERE content='' RETURNING id,pid,content;", [["23", None, ""]], "INSERT 0 1")
        query("INSERT INTO insert_null_projected SELECT 30,NULL,'NULL' RETURNING pid,content;", [[None, "NULL"]], "INSERT 0 1")
        large_value = "NULL " + "A" * 4000
        query(f"INSERT INTO insert_null_source VALUES(4,NULL,'{large_value}');")
        query("INSERT INTO insert_null_target SELECT id,pid,content FROM insert_null_source WHERE id=4 RETURNING id,pid,content;", [["4", None, large_value]], "INSERT 0 1")
        query("SELECT id,pid,content FROM insert_null_target WHERE id=4;", [["4", None, large_value]])
        query("CREATE TABLE insert_nonnull_target(id INT,pid INT NOT NULL,content TEXT);")
        error("INSERT INTO insert_nonnull_target SELECT id,pid,content FROM insert_null_source;", "23502")
        query("SELECT id FROM insert_nonnull_target;", [])
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT insert_null_sp;", ready=b"T")
        error("INSERT INTO insert_nonnull_target SELECT id,pid,content FROM insert_null_source;", "23502", b"E")
        error("SELECT id FROM insert_nonnull_target;", "25P02", b"E")
        query("ROLLBACK TO SAVEPOINT insert_null_sp;", ready=b"T")
        query("SELECT id FROM insert_nonnull_target;", [], ready=b"T")
        query("COMMIT;")
        print("[INSERT SELECT NULL BITMAP PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
