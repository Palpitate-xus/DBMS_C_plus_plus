#!/usr/bin/env python3
"""Unfixed function clause execution/errors: retain real PostgreSQL expectations.

This diagnostic is intentionally not registered as a supported green E2E.
Run it directly with DBMS_MAIN; it exits nonzero until every control passes.
"""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "function_clause_gap_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def run(sql):
        messages = client.simple_query(server["sock"], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        return result

    try:
        setup = [
            "CREATE TABLE function_gap_sink(id INT PRIMARY KEY);",
            "CREATE TABLE function_gap_driver(id INT);",
            "INSERT INTO function_gap_driver VALUES(1),(2);",
            "CREATE FUNCTION function_gap_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_gap_sink VALUES(arg) RETURNING id) SELECT id INTO STRICT n FROM function_gap_sink WHERE id=-1; RETURN n; END; $$;",
            "CREATE FUNCTION function_gap_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_gap_sink VALUES(arg) RETURNING id) SELECT 'bad' INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION function_gap_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO function_gap_sink VALUES(arg); RETURN -arg; END; $$;",
        ]
        for sql in setup:
            result = run(sql)
            assert result[1] is None, (sql, result)
        controls = [
            ("SELECT d.id FROM function_gap_driver d WHERE function_gap_strict(50+d.id)>0;", "P0002", [], []),
            ("SELECT d.id FROM function_gap_driver d ORDER BY function_gap_cast(60+d.id);", "22P02", [], []),
            ("SELECT id FROM function_gap_driver WHERE (SELECT function_gap_strict(51))>0;", "P0002", [], []),
            ("SELECT id FROM function_gap_driver ORDER BY (SELECT function_gap_cast(61));", "22P02", [], []),
            ("SELECT id FROM function_gap_driver WHERE function_gap_writer(id)<0 ORDER BY id;", None, [["1"], ["2"]], [["1"], ["2"]]),
            ("SELECT id FROM function_gap_driver ORDER BY function_gap_writer(id);", None, [["2"], ["1"]], [["1"], ["2"]]),
            ("EXPLAIN ANALYZE SELECT function_gap_strict(93);", "P0002", [], []),
            ("EXPLAIN ANALYZE SELECT function_gap_cast(id) FROM function_gap_driver;", "22P02", [], []),
        ]
        for sql, state, rows, writes in controls:
            result = run(sql)
            surviving = run("SELECT id FROM function_gap_sink ORDER BY id;")
            if result[1] != state or result[0] != rows or surviving[0] != writes:
                failure = (sql, "expected", state, rows, writes,
                           "actual", result, "surviving writes", surviving)
                failures.append(failure)
                print(failure, flush=True)
            cleanup = run("DELETE FROM function_gap_sink;")
            assert cleanup[1] is None, cleanup
        sql = "UPDATE function_gap_driver SET id=id+10 WHERE id=CAST('1' AS integer);"
        result = run(sql)
        driver = run("SELECT id FROM function_gap_driver ORDER BY id;")
        if result[1] is not None or driver[0] != [["2"], ["11"]]:
            failure = (sql, "expected updated driver", [["2"], ["11"]],
                       "actual", result, driver)
            failures.append(failure)
            print(failure, flush=True)
        assert not failures, f"{len(failures)} function clause controls remain unfixed"
        print("[STORED FUNCTION CLAUSE EXECUTION] all controls passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
