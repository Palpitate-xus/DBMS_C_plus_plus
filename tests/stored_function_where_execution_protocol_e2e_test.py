#!/usr/bin/env python3
"""Stored scalar calls in WHERE: execution, preparation, NULLs and rollback."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "function_where_runner", root / "tests/compat/pg_diff_runner.py")
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

    def empty_sink():
        ok("SELECT id FROM function_where_sink ORDER BY id;", [])

    try:
        for sql in (
            "CREATE TABLE function_where_sink(id INT PRIMARY KEY);",
            "CREATE TABLE function_where_driver(id INT,payload TEXT);",
            "CREATE TABLE function_where_empty(id INT);",
            "INSERT INTO function_where_driver VALUES(1,NULL),(2,''),(3,'null'),(4,'a b');",
            "CREATE FUNCTION function_where_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_where_sink VALUES(arg) RETURNING id) SELECT id INTO STRICT n FROM function_where_sink WHERE id=-1; RETURN n; END; $$;",
            "CREATE FUNCTION function_where_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO function_where_sink VALUES(arg); SELECT 'bad' INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION function_where_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO function_where_sink VALUES(arg); RETURN -arg; END; $$;",
            'CREATE FUNCTION "WhereCase"(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT -arg $$;',
            "CREATE FUNCTION wherecase(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT arg $$;",
            "CREATE FUNCTION function_where_isnull(arg TEXT) RETURNS BOOLEAN LANGUAGE sql AS $$ SELECT arg IS NULL $$;",
            "CREATE FUNCTION function_where_nullstrict(arg TEXT) RETURNS BOOLEAN RETURNS NULL ON NULL INPUT LANGUAGE sql AS $$ SELECT true $$;",
            "CREATE FUNCTION function_where_text(arg TEXT) RETURNS TEXT LANGUAGE sql AS $$ SELECT arg $$;",
        ):
            ok(sql)
        for sql, state in (
            ("SELECT d.id FROM function_where_driver d WHERE function_where_strict(50+d.id)>0;", "P0002"),
            ("SELECT id FROM function_where_driver WHERE function_where_cast(id)>0;", "22P02"),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            empty_sink()
        ok("SELECT id FROM function_where_driver WHERE function_where_writer(id)<0 ORDER BY id;",
           [["1"], ["2"], ["3"], ["4"]])
        ok("SELECT id FROM function_where_sink ORDER BY id;", [["1"], ["2"], ["3"], ["4"]])
        ok("DELETE FROM function_where_sink;")
        ok("SELECT id FROM function_where_driver WHERE CASE WHEN false THEN function_where_writer(id)>0 ELSE true END ORDER BY id;",
           [["1"], ["2"], ["3"], ["4"]])
        ok("SELECT id FROM function_where_driver WHERE coalesce(id,function_where_writer(id))>0 ORDER BY id;",
           [["1"], ["2"], ["3"], ["4"]])
        empty_sink()
        # Preparation rejects unknown calls in unused arms before any writer
        # executes, even when the input relation has no rows.
        for sql in (
            "SELECT id FROM function_where_driver WHERE coalesce(function_where_writer(id),function_where_missing(id))<0;",
            "SELECT id FROM function_where_driver WHERE CASE WHEN false THEN function_where_missing(id)>0 ELSE true END;",
            "SELECT id FROM function_where_empty WHERE function_where_missing(id)>0;",
            "SELECT id FROM function_where_driver WHERE wrong_schema.wherecase(id)>0;",
            "SELECT id FROM function_where_driver WHERE pg_catalog.wherecase(id)>0;",
            "SELECT id FROM function_where_driver WHERE wherecase()>0;",
        ):
            result = query(sql)
            assert result[1] == "42883" and result[0] == [], (sql, result)
            empty_sink()
        ok("SELECT id FROM function_where_empty WHERE function_where_writer(id)<0;", [])
        empty_sink()
        ok('SELECT id FROM function_where_driver WHERE "WhereCase"(id)<0 AND public.wherecase(id)>0 ORDER BY id;',
           [["1"], ["2"], ["3"], ["4"]])
        ok("SELECT id FROM function_where_driver WHERE function_where_isnull(payload) ORDER BY id;", [["1"]])
        ok("SELECT id FROM function_where_driver WHERE function_where_nullstrict(payload) IS NULL ORDER BY id;", [["1"]])
        for literal, expected in (("''", "2"), ("'null'", "3"), ("'a b'", "4")):
            ok(f"SELECT id FROM function_where_driver WHERE function_where_text(payload)={literal};", [[expected]])
        # The same predicate host is used by UPDATE/DELETE. A failed writer
        # must roll back both body effects and the outer mutation.
        changed = ok("UPDATE function_where_driver SET payload='changed' WHERE function_where_writer(id)<0;")
        assert changed[4] == "UPDATE 4", changed
        ok("SELECT id FROM function_where_sink ORDER BY id;", [["1"], ["2"], ["3"], ["4"]])
        ok("DELETE FROM function_where_sink;")
        for sql, state in (
            ("UPDATE function_where_driver SET payload='bad' WHERE function_where_strict(id)>0;", "P0002"),
            ("DELETE FROM function_where_driver WHERE function_where_cast(id)>0;", "22P02"),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            empty_sink()
            ok("SELECT id,payload FROM function_where_driver ORDER BY id;",
               [[str(i), "changed"] for i in range(1, 5)])
        removed = ok("DELETE FROM function_where_driver WHERE function_where_writer(id)<0;")
        assert removed[4] == "DELETE 4", removed
        ok("SELECT id FROM function_where_driver;", [])
        ok("SELECT id FROM function_where_sink ORDER BY id;", [["1"], ["2"], ["3"], ["4"]])
        print("[STORED FUNCTION WHERE EXECUTION] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
