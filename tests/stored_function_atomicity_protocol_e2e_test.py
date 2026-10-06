#!/usr/bin/env python3
"""Stored-function side effects belong to the calling SQL statement."""
import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "function_atomicity_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def extended(sql):
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
        execute = b"\0" + struct.pack("!I", 0)
        server["sock"].sendall(client.typed(b"P", parse) +
                               client.typed(b"B", bind) +
                               client.typed(b"E", execute) + client.typed(b"S"))
        return client.read_until_ready(server["sock"])

    def query(sql, state=None, rows=None, ready=b"I", transport=None, deferred_error=False):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result, state)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1], ready)
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)
        if deferred_error:
            kinds = [kind for kind, _ in messages]
            assert b"D" in kinds and b"C" not in kinds and result[4] is None, (sql, messages, result)
            assert kinds.index(b"D") < kinds.index(b"E"), (sql, kinds)
        return result

    def function(name, body, volatility="VOLATILE", language="plpgsql"):
        query(f"CREATE FUNCTION {name}(arg INT) RETURNS INT {volatility} LANGUAGE {language} AS $body$ {body} $body$;")

    def empty():
        query("SELECT id FROM function_atomic_rows ORDER BY id;", rows=[])

    try:
        query("CREATE TABLE function_atomic_rows(id INT PRIMARY KEY);")
        query("CREATE TABLE function_atomic_driver(id INT);")
        query("INSERT INTO function_atomic_driver VALUES(1),(2);")
        function("atomic_strict", "DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_atomic_rows VALUES(arg) RETURNING id) SELECT id INTO STRICT n FROM function_atomic_rows WHERE id=-1; RETURN n; END;")
        function("atomic_cast", "DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_atomic_rows VALUES(arg) RETURNING id) SELECT 'bad' INTO n; RETURN n; END;")
        # Both failures occur after a successful modifying CTE. The outer
        # SELECT must undo the write, not just the last body SELECT.
        for sql, state in (("SELECT atomic_strict(1);", "P0002"),
                           ("SELECT atomic_cast(2);", "22P02")):
            query(sql, state=state, rows=[])
            empty()

        function("atomic_multi", "DECLARE n INT; BEGIN INSERT INTO function_atomic_rows VALUES(arg); WITH ins AS(INSERT INTO function_atomic_rows VALUES(arg+1) RETURNING id) SELECT id INTO n FROM ins; WITH ins AS(INSERT INTO function_atomic_rows VALUES(arg+2) RETURNING id) SELECT id INTO STRICT n FROM function_atomic_rows WHERE id=-1; RETURN n; END;")
        query("SELECT atomic_multi(10);", state="P0002", rows=[])
        empty()
        function("atomic_read_write", "DECLARE n INT; BEGIN INSERT INTO function_atomic_rows VALUES(arg); SELECT id INTO STRICT n FROM function_atomic_rows WHERE id=arg; RETURN n; END;")
        query("SELECT atomic_read_write(30) AS n;", rows=[["30"]])
        query("SELECT id FROM function_atomic_rows ORDER BY id;", rows=[["30"]])
        query("DELETE FROM function_atomic_rows;")
        # A successful first target expression must be undone when a later
        # target expression in the same calling statement raises an error.
        query("SELECT atomic_read_write(40),atomic_cast(41);", state="22P02", rows=[])
        empty()
        query("CREATE TABLE function_atomic_fk_parent(id INT PRIMARY KEY);")
        query("CREATE TABLE function_atomic_fk_child(id INT PRIMARY KEY,pid INT,CONSTRAINT function_atomic_fk FOREIGN KEY(pid) REFERENCES function_atomic_fk_parent(id) DEFERRABLE INITIALLY DEFERRED);")
        function("atomic_deferred_fk", "BEGIN INSERT INTO function_atomic_fk_child VALUES(arg,999); RETURN arg; END;")
        # PostgreSQL streams the SELECT row before implicit COMMIT checks
        # deferred constraints: DataRow, ErrorResponse, Ready I, no success
        # CommandComplete. The result row is not proof of durable success.
        query("SELECT atomic_deferred_fk(42);", state="23503", rows=[["42"]], deferred_error=True)
        query("SELECT id FROM function_atomic_fk_child;", rows=[])
        for sql, state in (
            ("SELECT q.n FROM(SELECT atomic_strict(70) AS n)q;", "P0002"),
            ("WITH q AS(SELECT atomic_cast(80) AS n) SELECT n FROM q;", "22P02"),
        ):
            query(sql, state=state, rows=[])
            empty()

        query("PREPARE atomic_prepared AS SELECT atomic_strict(90);")
        query("EXECUTE atomic_prepared;", state="P0002", rows=[])
        empty()
        query("SELECT atomic_cast(91);", state="22P02", rows=[], transport=extended)
        empty()
        function("atomic_sql", "SELECT atomic_strict(arg);", language="sql")
        query("SELECT atomic_sql(92);", state="P0002", rows=[])
        empty()
        query("CREATE FUNCTION atomic_sql_user() RETURNS TEXT LANGUAGE sql AS $$ SELECT current_user $$;")
        query("SELECT atomic_sql_user();", rows=[["alice"]])
        query('CREATE FUNCTION atomic_sql_quoted("Value Name" TEXT) RETURNS TEXT LANGUAGE sql AS $$ SELECT "Value Name" $$;')
        query("SELECT atomic_sql_quoted('00123');", rows=[["00123"]])
        query("SELECT atomic_sql_quoted(NULL);", rows=[[None]])
        query('CREATE FUNCTION atomic_sql_case("ID" TEXT,id TEXT) RETURNS TEXT LANGUAGE sql AS $$ SELECT "ID" || id $$;')
        query("SELECT atomic_sql_case('UP','low');", rows=[["UPlow"]])
        query("CREATE FUNCTION atomic_sql_found(found INT) RETURNS INT LANGUAGE sql AS $$ SELECT found $$;")
        query("SELECT atomic_sql_found(7);", rows=[["7"]])
        query("CREATE FUNCTION atomic_sql_unbound() RETURNS INT LANGUAGE sql AS $$ SELECT found $$;")
        query("SELECT atomic_sql_unbound();", state="42703", rows=[])
        query('CREATE FUNCTION "AtomicCase"() RETURNS TEXT LANGUAGE sql AS $$ SELECT \'UP\' $$;')
        query("CREATE FUNCTION atomiccase() RETURNS TEXT LANGUAGE sql AS $$ SELECT 'low' $$;")
        query('CREATE FUNCTION atomic_sql_calls() RETURNS TEXT LANGUAGE sql AS $$ SELECT "AtomicCase"() || atomiccase() $$;')
        query("SELECT atomic_sql_calls();", rows=[["UPlow"]])
        query('CREATE FUNCTION "LENGTH"() RETURNS INT LANGUAGE sql AS $$ SELECT 8 $$;')
        query('CREATE FUNCTION atomic_sql_builtin_case() RETURNS INT LANGUAGE sql AS $$ SELECT "LENGTH"() + length(\'ab\') $$;')
        query("SELECT atomic_sql_builtin_case();", rows=[["10"]])
        function("atomic_sql_public", "SELECT public.atomic_read_write(arg)+atomic_cast(arg+1);", language="sql")
        query("SELECT atomic_sql_public(144);", state="22P02", rows=[])
        empty()
        query("CREATE FUNCTION atomic_sql_prepare_error() RETURNS INT LANGUAGE sql AS $$ SELECT atomic_read_write(145)+no_such_atomic_function() $$;")
        query("SELECT atomic_sql_prepare_error();", state="42883", rows=[])
        empty()
        query("CREATE FUNCTION atomic_sql_short_error() RETURNS INT LANGUAGE sql AS $$ SELECT coalesce(1,no_such_atomic_function()) $$;")
        query("SELECT atomic_sql_short_error();", state="42883", rows=[])

        # Direct UPDATE/DELETE use the full DML host, including their undo
        # records; a later procedural conversion error undoes both.
        query("INSERT INTO function_atomic_rows VALUES(1),(2);")
        function("atomic_change", "DECLARE n INT; BEGIN UPDATE function_atomic_rows SET id=id+10 WHERE id=1; DELETE FROM function_atomic_rows WHERE id=2; SELECT 'bad' INTO n; RETURN n; END;")
        query("SELECT atomic_change(0);", state="22P02", rows=[])
        query("SELECT id FROM function_atomic_rows ORDER BY id;", rows=[["1"], ["2"]])
        query("DELETE FROM function_atomic_rows;")

        for volatility in ("STABLE", "IMMUTABLE"):
            function("atomic_readonly_" + volatility.lower(),
                     "BEGIN INSERT INTO function_atomic_rows VALUES(arg); RETURN arg; END;", volatility)
            query("SELECT atomic_readonly_" + volatility.lower() + "(98);", state="0A000", rows=[])
            empty()
        function("atomic_readonly_cte", "DECLARE n INT; BEGIN WITH ins AS(INSERT INTO function_atomic_rows VALUES(arg) RETURNING id) SELECT id INTO n FROM ins; RETURN n; END;", "STABLE")
        query("SELECT atomic_readonly_cte(99);", state="0A000", rows=[])
        empty()
        query("CREATE TEMP TABLE function_atomic_temp(id INT);")
        function("atomic_readonly_temp", "BEGIN INSERT INTO function_atomic_temp VALUES(arg); RETURN arg; END;", "STABLE")
        query("SELECT atomic_readonly_temp(99);", state="0A000", rows=[])
        query("SELECT id FROM function_atomic_temp;", rows=[])
        function("atomic_readonly_lock", "DECLARE n INT; BEGIN SELECT id INTO n FROM function_atomic_driver FOR UPDATE; RETURN n; END;", "STABLE")
        query("SELECT atomic_readonly_lock(0);", state="0A000", rows=[])

        # STABLE may call a VOLATILE writer, but its later body query still
        # sees its calling statement's snapshot, not the child's insertion.
        function("atomic_stable_wrapper", "DECLARE n INT; BEGIN SELECT atomic_read_write(arg) INTO n; SELECT id INTO n FROM function_atomic_rows WHERE id=arg; IF n IS NULL THEN RETURN -1; END IF; RETURN n; END;", "STABLE")
        query("SELECT atomic_stable_wrapper(140);", rows=[["-1"]])
        query("SELECT id FROM function_atomic_rows;", rows=[["140"]])
        query("DELETE FROM function_atomic_rows;")

        function("atomic_ddl_error", "DECLARE n INT; BEGIN CREATE TABLE function_atomic_created(id INT); INSERT INTO function_atomic_created VALUES(arg); SELECT 'bad' INTO n; RETURN n; END;")
        query("SELECT atomic_ddl_error(150);", state="22P02", rows=[])
        query("SELECT id FROM function_atomic_created;", state="42P01", rows=[])
        query("SELECT 1;", rows=[["1"]])

        function("atomic_control", "BEGIN INSERT INTO function_atomic_rows VALUES(arg); COMMIT; RETURN arg; END;")
        query("SELECT atomic_control(141);", state="2D000", rows=[])
        empty()

        # Both driver tuples were inserted by this transaction. Updating
        # them in a child command creates Combo CIDs, while the outer scan
        # must retain its old command snapshot and return each old id once.
        function("atomic_update_driver", "DECLARE n INT; BEGIN IF arg=1 THEN UPDATE function_atomic_driver SET id=id+10 WHERE id=1; ELSE UPDATE function_atomic_driver SET id=id+10 WHERE id=2; END IF; SELECT id INTO STRICT n FROM function_atomic_driver WHERE id=arg+10; RETURN n; END;")
        query("BEGIN;", ready=b"T")
        query("DELETE FROM function_atomic_driver;", ready=b"T")
        query("INSERT INTO function_atomic_driver VALUES(1),(2);", ready=b"T")
        query("SELECT id,atomic_update_driver(id) FROM function_atomic_driver ORDER BY id;", rows=[["1", "11"], ["2", "12"]], ready=b"T")
        query("SELECT id FROM function_atomic_driver ORDER BY id;", rows=[["11"], ["12"]], ready=b"T")
        query("ROLLBACK;")

        # Existing user transactions retain their pre-savepoint work while
        # failed subtransactions reject subsequent commands until recovery.
        query("BEGIN;", ready=b"T")
        query("INSERT INTO function_atomic_rows VALUES(100);", ready=b"T")
        query("SAVEPOINT keep_prior;", ready=b"T")
        query("INSERT INTO function_atomic_rows VALUES(101);", ready=b"T")
        query("SELECT atomic_ddl_error(151);", state="22P02", rows=[], ready=b"E")
        query("ROLLBACK TO SAVEPOINT keep_prior;", ready=b"T")
        query("SELECT id FROM function_atomic_created;", state="42P01", rows=[], ready=b"E")
        query("ROLLBACK TO SAVEPOINT keep_prior;", ready=b"T")
        query("SELECT id FROM function_atomic_rows;", rows=[["100"]], ready=b"T")
        query("INSERT INTO function_atomic_rows VALUES(101);", ready=b"T")
        query("SELECT atomic_multi(110);", state="P0002", rows=[], ready=b"E")
        query("SELECT id FROM function_atomic_rows;", state="25P02", rows=[], ready=b"E")
        query("ROLLBACK TO SAVEPOINT keep_prior;", ready=b"T")
        query("SELECT id FROM function_atomic_rows;", rows=[["100"]], ready=b"T")
        query("SELECT atomic_read_write(120);", rows=[["120"]], ready=b"T")
        query("COMMIT;")
        query("SELECT id FROM function_atomic_rows ORDER BY id;", rows=[["100"], ["120"]])
        query("DELETE FROM function_atomic_rows;")
        query("BEGIN;", ready=b"T")
        query("INSERT INTO function_atomic_rows VALUES(130);", ready=b"T")
        query("SELECT atomic_cast(131);", state="22P02", rows=[], ready=b"E")
        query("SELECT 1;", state="25P02", rows=[], ready=b"E")
        query("ROLLBACK;")
        empty()
        print("[STORED FUNCTION ATOMICITY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
