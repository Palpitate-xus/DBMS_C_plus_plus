#!/usr/bin/env python3
"""Indexed residual writers run once for matching, rejecting and NULL results."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("index_residual_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        decoded = runner.decode_wire_result(messages, include_types=True)
        assert decoded[1] is None and messages[-1] == (b"Z", ready), (sql, decoded, messages[-1])
        if expected is not None:
            assert decoded[0] == expected, (sql, decoded, expected)

    try:
        query("CREATE TABLE residual_input(id INT PRIMARY KEY,n INT);")
        query("CREATE TABLE residual_effects(id INT);")
        query("INSERT INTO residual_input VALUES(1,NULL);")
        for suffix, value in (("match", "p"), ("reject", "p+1"), ("null", "NULL")):
            query(f"CREATE FUNCTION residual_{suffix}(p INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO residual_effects VALUES(p); RETURN {value}; END; $$;")
            query("BEGIN;", ready=b"T")
            query(f"SELECT id FROM residual_input WHERE id=1 AND residual_{suffix}(id)=1 AND n IS NULL;",
                  [["1"]] if suffix == "match" else [], b"T")
            query("SELECT id FROM residual_effects;", [["1"]], b"T")
            query("ROLLBACK;")
            query("SELECT id FROM residual_effects;", [])
        query("CREATE TABLE residual_multi(id INT,bucket INT,n INT);")
        query("INSERT INTO residual_multi VALUES(1,1,NULL),(2,1,NULL);")
        query("CREATE INDEX residual_bucket ON residual_multi(bucket);")
        query("CREATE FUNCTION residual_mutating(p INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO residual_effects VALUES(p); UPDATE residual_multi SET n=0 WHERE id=2; RETURN p; END; $$;")
        query("BEGIN;", ready=b"T")
        query("SELECT id FROM residual_multi WHERE bucket=1 AND residual_mutating(id)>0 AND n IS NULL ORDER BY id;", [["1"], ["2"]], b"T")
        query("SELECT id FROM residual_effects ORDER BY id;", [["1"], ["2"]], b"T")
        query("ROLLBACK;")
        query("SELECT n FROM residual_multi ORDER BY id;", [[None], [None]])
        print("[INDEX RESIDUAL EXECUTION ONCE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
