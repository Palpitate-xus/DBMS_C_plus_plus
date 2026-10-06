#!/usr/bin/env python3
"""Strict PG18 oracle for the execution-owned, side-effect-free planning phase."""
import importlib.util
import socket
from pathlib import Path


CASES = [
    ("SELECT 1/0", "22012"),
    ("SELECT 1/0 WHERE false", "22012"),
    ("SELECT 1/0 LIMIT 0", "22012"),
    ("SELECT (SELECT 1/0) LIMIT 0", "22012"),
    ("SELECT NULL::INT/0", None),
    ("SELECT CAST(2147483648 AS INT)", "22003"),
    ("SELECT CASE WHEN false THEN 1/0 ELSE 2 END", None),
    ("SELECT CASE WHEN true THEN 2 ELSE 1/0 END", None),
    ("SELECT CASE WHEN id=1 THEN 1/0 ELSE 2 END FROM plan_constant_rows", "22012"),
    ("SELECT CASE WHEN false THEN(SELECT 1/0) ELSE 2 END", None),
    ("SELECT CASE WHEN true THEN(SELECT 1/0) ELSE 2 END", "22012"),
    ("SELECT 1 WHERE false AND 1/0=0", None),
    ("SELECT 1 WHERE true OR 1/0=0", None),
    ("SELECT 1 FROM plan_constant_rows WHERE id=1 AND 1/0=0", "22012"),
    ("SELECT plan_constant_writer(1) WHERE false", None),
    ("SELECT plan_constant_writer(1/0) WHERE false", "22012"),
    ("SELECT(SELECT plan_constant_writer(1))", None),
    ("SELECT CASE WHEN false THEN(SELECT plan_constant_writer(1)) ELSE 2 END", None),
    ("WITH unused AS(SELECT 1/0) SELECT 2", None),
    ("WITH reached AS(SELECT 1/0 AS n) SELECT n FROM reached", "22012"),
    ("WITH a AS(SELECT 1/0 AS n), b AS(SELECT n FROM a) SELECT n FROM b", "22012"),
    ("WITH unused AS(SELECT plan_constant_writer(1)) SELECT 2", None),
    ("WITH ins AS(INSERT INTO plan_constant_rows VALUES(1/0) RETURNING id) SELECT 2", "22012"),
    ("WITH ins AS(INSERT INTO plan_constant_rows VALUES(plan_constant_writer(1)) RETURNING id) SELECT 2", None),
    ("UPDATE plan_constant_rows SET id=1/0 WHERE false", "22012"),
    ("UPDATE plan_constant_rows SET id=id/0 WHERE false", None),
    ("UPDATE plan_constant_rows SET id=(SELECT 1/0) WHERE false", "22012"),
    ("UPDATE plan_constant_rows SET id=CASE WHEN false THEN(SELECT 1/0) ELSE 2 END", None),
    ("DELETE FROM plan_constant_rows WHERE 1/0=0", "22012"),
    ("INSERT INTO plan_constant_rows SELECT 1/0 WHERE false", "22012"),
    ("SELECT 1 FROM(SELECT 1/0 AS n)q", None),
    ("SELECT n FROM(SELECT 1/0 AS n)q", "22012"),
    ("WITH c AS(SELECT 1/0 AS n) SELECT 1 FROM c", None),
    ("WITH c AS MATERIALIZED(SELECT 1/0 AS n) SELECT 1 FROM c", "22012"),
    ("WITH c AS NOT MATERIALIZED(SELECT 1/0 AS n) SELECT 1 FROM c", None),
    ("SELECT 1 FROM(SELECT 1/0 AS n LIMIT 0)q", None),
    ("SELECT 1 FROM(SELECT DISTINCT 1/0 AS n)q", "22012"),
    ("SELECT 1 FROM(SELECT plan_constant_writer(1/0) AS n)q", "22012"),
    ("SELECT 1 FROM(SELECT CASE WHEN false THEN plan_constant_writer(1/0) ELSE 2 END AS n)q", None),
    ("SELECT 1 FROM(SELECT(SELECT 1/0) AS n)q", None),
    ("SELECT 1 FROM(SELECT abs(1/0) AS n)q", None),
    ("SELECT 1=ANY(SELECT 1/0)", "22012"),
    ("SELECT CASE WHEN false THEN 1=ANY(SELECT 1/0) ELSE true END", None),
    ("SELECT 1=ANY(ARRAY[1,1/0])", "22012"),
    ("SELECT unnest(ARRAY[1/0]) LIMIT 0", "22012"),
    ("SELECT count(1/0) FILTER(WHERE false) FROM plan_constant_rows", "22012"),
    ("SELECT COALESCE(1,1/0)", None),
    ("SELECT COALESCE(NULL,1,1/0)", None),
    ("SELECT COALESCE(id,1/0) FROM plan_constant_rows", "22012"),
    ("SELECT COALESCE(true,false) OR 1/0=0", None),
    ("SELECT CASE WHEN true THEN COALESCE(1,1/0) ELSE 2 END", None),
    ("SELECT COALESCE(1,(SELECT 1/0))", None),
    ("SELECT(SELECT q.n) FROM(SELECT 1/0 AS n)q", "22012"),
    ("WITH c AS(SELECT 1/0 AS n) SELECT(SELECT c.n) FROM c", "22012"),
    ("SELECT CASE WHEN false THEN(SELECT q.n) ELSE 1 END FROM(SELECT 1/0 AS n)q", None),
    ("SELECT 1 FROM(SELECT 1/0 AS n)a JOIN(SELECT 1 AS n)b USING(n)", "22012"),
    ("SELECT 1 FROM(SELECT 1/0 AS n)a NATURAL JOIN(SELECT 1 AS n)b", "22012"),
    ("SELECT 1 FROM(SELECT 1/0 AS n ORDER BY n)q", "22012"),
    ("SELECT 1 FROM(SELECT 1/0 AS n ORDER BY 1)q", "22012"),
    ("WITH c AS(SELECT 1/0 AS n) SELECT 1 FROM c a,c b", "22012"),
    ("WITH c AS NOT MATERIALIZED(SELECT 1/0 AS bad,1 AS good) SELECT a.good,b.bad FROM c a,c b", "22012"),
    ("SELECT good FROM(SELECT * FROM(SELECT 1/0 AS bad,1 AS good)t)q", None),
    ("SELECT bad FROM(SELECT * FROM(SELECT 1/0 AS bad,1 AS good)t)q", "22012"),
    ('SELECT "good" FROM(SELECT d.* FROM(SELECT 1/0 AS "bad.name",1 AS "good")d)q', None),
    ("WITH c AS(SELECT CASE WHEN false THEN plan_constant_writer(1) ELSE 1 END AS v,1/0 AS n) SELECT v FROM c", "22012"),
]


def main():
    spec = importlib.util.spec_from_file_location("constant_plan_runner", Path(__file__).with_name("pg_diff_runner.py"))
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    host, port, user, database, password = runner._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
    client.startup_reference(sock, user, database, password=password)
    runner.verify_reference_version(client, sock)
    failures = []

    def query(sql, expected=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("PURE PLAN REFERENCE", sql, result, flush=True)
        if result[1] != expected:
            failures.append((sql, result[1], expected))
        return result

    try:
        query("BEGIN;")
        query("CREATE TEMP TABLE plan_constant_rows(id INT);")
        query("INSERT INTO plan_constant_rows VALUES(1);")
        query("CREATE TEMP SEQUENCE plan_constant_effects;")
        query("CREATE FUNCTION plan_constant_writer(p INT) RETURNS INT LANGUAGE plpgsql VOLATILE "
              "AS $$BEGIN PERFORM nextval('plan_constant_effects'); RETURN p; END$$;")
        for index, (sql, state) in enumerate(CASES, 1):
            query("SAVEPOINT pure_plan;")
            query("EXPLAIN (FORMAT JSON) " + sql + ";", state)
            query("ROLLBACK TO pure_plan;")
            result = query("SELECT nextval('plan_constant_effects');")
            if result[0] != [[str(index)]] or result[5] != [20]:
                failures.append((sql, "unexpected planning effect", result))
        assert query("SELECT id FROM plan_constant_rows;")[0] == [["1"]]
        print("PURE PLAN REFERENCE FAILURES", failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
        finally:
            sock.close()


if __name__ == "__main__":
    main()
