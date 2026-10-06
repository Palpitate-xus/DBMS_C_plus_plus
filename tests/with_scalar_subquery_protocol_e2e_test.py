#!/usr/bin/env python3
"""Parenthesized WITH scalar queries retain grammar and preparation errors."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("with_scalar_runner", root/"tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        messages = client.simple_query(server["sock"], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        return result

    def ok(sql, rows=None):
        result = query(sql)
        assert result[1] is None, (sql, result)
        if rows is not None:
            assert result[0] == rows, (sql, result)
        return result

    try:
        for sql in (
            "CREATE TABLE with_scalar_rows(id INT);",
            "INSERT INTO with_scalar_rows VALUES(1),(2);",
            "CREATE TABLE with_scalar_sink(id INT);",
            "CREATE SEQUENCE with_scalar_seq START 1;",
            "CREATE FUNCTION with_scalar_local() RETURNS BIGINT LANGUAGE plpgsql AS $$ DECLARE wanted BIGINT:=2147483648;n BIGINT; BEGIN SELECT(WITH c AS(SELECT wanted)SELECT c.wanted FROM c) INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION with_scalar_text(arg TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE n TEXT; BEGIN SELECT(WITH c AS(SELECT arg AS v)SELECT c.v FROM c) INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION with_scalar_read(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN SELECT(WITH c AS(SELECT id FROM with_scalar_rows WHERE id=arg)SELECT c.id FROM c) INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION with_scalar_multiple() RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN SELECT(WITH c AS(SELECT id FROM with_scalar_rows)SELECT c.id FROM c) INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION with_scalar_invalid() RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO with_scalar_sink VALUES(nextval('with_scalar_seq')) RETURNING id) SELECT(WITH c AS(SELECT 1 AS id)SELECT c.missing FROM c) INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION with_scalar_width() RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN SELECT(WITH c AS(SELECT 1 AS id)SELECT c.id,c.id FROM c) INTO n; RETURN n; END; $$;",
        ):
            ok(sql)
        result = ok("SELECT with_scalar_local();", [["2147483648"]])
        assert result[5] == [20], result
        for text in ("", "null", "  a b  ", "O'Brien"):
            quoted = "'" + text.replace("'", "''") + "'"
            result = ok(f"SELECT with_scalar_text({quoted});", [[text]])
            assert result[5] == [25], result
        ok("SELECT with_scalar_text(NULL);", [[None]])
        ok("SELECT with_scalar_read(1);", [["1"]])
        ok("SELECT with_scalar_read(99);", [[None]])
        for sql, state in (
            ("SELECT with_scalar_multiple();", "21000"),
            ("SELECT with_scalar_invalid();", "42703"),
            ("SELECT with_scalar_width();", "42601"),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
        ok("SELECT id FROM with_scalar_sink;", [])
        # Nontransactional sequence state proves preparation rejected the
        # invalid nested name before the writing CTE ever executed.
        result = query("SELECT currval('with_scalar_seq');")
        assert result[1] == "55000", result
        ok("SELECT nextval('with_scalar_seq');", [["1"]])
        print("[WITH SCALAR SUBQUERY PROTOCOL] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
