#!/usr/bin/env python3
"""Typed aggregate expression scope, nulls, filters and volatile input demand.

--reference is an explicitly PG17.2 diagnostic, not the PG18 compatibility oracle.
"""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("aggregate_expression_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        version = runner.decode_wire_result(client.simple_query(sock, "SHOW server_version_num"))
        assert version[1] is None and version[0] == [["170002"]], version
        server = {"sock": sock}
        print("PG17.2 diagnostic reference", version[0])
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    prefix = "ae_" + uuid.uuid4().hex[:16] + "_"
    def query(sql):
        sql = sql.replace("ae_", prefix)
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    def expect(sql, rows, oids):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == oids, (sql, result)
    try:
        assert query("BEGIN;")[1] is None
        for sql in (
            "CREATE TABLE ae_rows(v INT);",
            "INSERT INTO ae_rows VALUES(1),(NULL),(2);",
            "CREATE TABLE ae_empty(v INT);",
            "CREATE TABLE ae_null(v INT);",
            "INSERT INTO ae_null VALUES(NULL);",
            'CREATE TABLE ae_wide("V" BIGINT,v INT);',
            "INSERT INTO ae_wide VALUES(9223372036854775807,1),(1,2);",
        ):
            assert query(sql)[1] is None, sql
        # These are new expanded cases, not a relabeling of the earlier
        # historical 26-query old/new comparison or its permanent seven cases.
        for relation, count, total in (("ae_rows", "1", "4"),
                                       ("ae_empty", "0", None),
                                       ("ae_null", "1", None)):
            for expression, expected in (("count(*)-count(v)", count),
                                         ("sum(v)+1", total),
                                         ("sum(v)+CAST(1 AS BIGINT)", total),
                                         ("CAST(sum(v) AS BIGINT)+1", total)):
                expect("SELECT " + expression + " FROM " + relation + ";", [[expected]], [20])
        expect("SELECT sum(v)+1 FROM ae_rows WHERE v=1;", [["2"]], [20])
        expect("SELECT count(*)-count(v) FROM ae_rows WHERE false;", [["0"]], [20])
        expect("SELECT sum(v)+1 FROM ae_rows WHERE false;", [[None]], [20])
        expect("SELECT count(v)+sum(v),CAST(sum(v) AS BIGINT)+count(*) FROM ae_rows;", [["5", "6"]], [20, 20])
        expect("SELECT (sum(v)+1)*2,count(*)-(count(v)+1) FROM ae_rows;", [["8", "0"]], [20, 20])
        expect("SELECT coalesce(sum(v),0)+1 FROM ae_empty;", [["1"]], [20])
        expect("SELECT sum(DISTINCT v)+1 FROM ae_rows;", [["4"]], [20])
        expect("SELECT sum(v) FILTER (WHERE v=1)+1 FROM ae_rows;", [["2"]], [20])
        expect("SELECT count(*) FILTER (WHERE NULL)+1 FROM ae_rows;", [["1"]], [20])
        expect("SELECT count(*) FILTER (WHERE 't')+1 FROM ae_rows;", [["4"]], [20])
        expect("SELECT count(*) FILTER (WHERE v IS NULL)-count(v) FILTER (WHERE v=2) FROM ae_rows;", [["0"]], [20])
        expect('SELECT sum("V")+sum(v) FROM ae_wide;', [["9223372036854775811"]], [1700])
        expect("SELECT min(v)+max(v),avg(v)+0 FROM ae_rows;", [["3", "1.5000000000000000"]], [23, 1700])
        expect("SELECT min(v)+max(v),avg(v)+0 FROM ae_empty;", [[None, None]], [23, 1700])
        for sql in (
            "CREATE SEQUENCE ae_seq;",
            "CREATE TABLE ae_effect(n BIGINT DEFAULT nextval('ae_seq'),kind TEXT,v BIGINT);",
            "CREATE FUNCTION ae_inner(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO ae_effect(kind,v) VALUES('inner',i); RETURN i; END;$$;",
            "CREATE FUNCTION ae_outer(i BIGINT) RETURNS BIGINT LANGUAGE plpgsql AS $$BEGIN INSERT INTO ae_effect(kind,v) VALUES('outer',i); RETURN i; END;$$;",
        ):
            assert query(sql)[1] is None, sql
        for sql, state in (
            ("SELECT sum(ae_inner(v)) FILTER (WHERE 1)+1 FROM ae_rows;", "42804"),
            ("SELECT sum(sum(v))+1 FROM ae_empty;", "42803"),
            ("SELECT sum(v)+v FROM ae_empty;", "42803"),
            ("SELECT sum(CAST(v AS TEXT))+1 FROM ae_empty;", "42883"),
            ("SELECT sum('abc')+1 FROM ae_empty;", "42725"),
            ("SELECT avg(NULL)+1 FROM ae_empty;", "42725"),
            ("SELECT sum(CAST($1 AS INT))+1 FROM ae_empty;", "42P02"),
            ("SELECT sum($1)+1 FROM ae_empty;", "42P02"),
        ):
            assert query("SAVEPOINT ae_prepare_error;")[1] is None
            result = query(sql)
            assert result[1] == state, (sql, result, state)
            assert query("ROLLBACK TO SAVEPOINT ae_prepare_error;")[1] is None
            assert query("RELEASE SAVEPOINT ae_prepare_error;")[1] is None
            expect("SELECT kind,v FROM ae_effect;", [], [25, 20])
        expect("SELECT sum(ae_inner(v))+1 FROM ae_rows WHERE v=1;", [["2"]], [20])
        expect("SELECT kind,v FROM ae_effect ORDER BY n;", [["inner", "1"]], [25, 20])
        assert query("DELETE FROM ae_effect;")[1] is None
        expect("SELECT sum(ae_inner(v))+1 FROM ae_empty;", [[None]], [20])
        expect("SELECT kind,v FROM ae_effect;", [], [25, 20])
        expect("SELECT ae_outer(count(*)) FROM ae_empty;", [["0"]], [20])
        expect("SELECT kind,v FROM ae_effect ORDER BY n;", [["outer", "0"]], [25, 20])
        assert query("DELETE FROM ae_effect;")[1] is None
        expect("SELECT sum(ae_inner(v))+sum(ae_inner(v)) FROM ae_rows;", [["6"]], [20])
        expect("SELECT kind,v FROM ae_effect ORDER BY n;", [["inner", "1"], ["inner", "1"],
               ["inner", None], ["inner", None], ["inner", "2"], ["inner", "2"]], [25, 20])
        assert query("DELETE FROM ae_effect;")[1] is None
        expect("SELECT count(ae_inner(v))+1 FROM ae_rows LIMIT 1;", [["3"]], [20])
        expect("SELECT kind,v FROM ae_effect ORDER BY n;", [["inner", "1"], ["inner", None], ["inner", "2"]], [25, 20])
        assert query("DELETE FROM ae_effect;")[1] is None
        expect("SELECT sum(ae_inner(v))+1 FROM ae_rows LIMIT 0;", [], [20])
        expect("SELECT kind,v FROM ae_effect;", [], [25, 20])
        print("[AGGREGATE EXPRESSION SCOPE " + ("PG17.2 DIAGNOSTIC" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            query("ROLLBACK;")
        finally:
            if reference: sock.close()
            else: runner.stop_ours(server)


if __name__ == "__main__":
    main()
