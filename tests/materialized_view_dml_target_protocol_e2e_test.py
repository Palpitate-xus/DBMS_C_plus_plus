#!/usr/bin/env python3
"""Materialized targets reject writes after pure binding/planning, without effects."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("mv_target_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []

    def query(sql, state=None, rows=None, setup=False):
        actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("MV TARGET", sql, actual, flush=True)
        problems = []
        if actual[1] != state:
            problems.append(f"SQLSTATE actual={actual[1]!r} expected={state!r}")
        if rows is not None and actual[0] != rows:
            problems.append(f"rows actual={actual[0]!r} expected={rows!r}")
        if state is not None and (actual[0] or actual[4] is not None):
            problems.append("failed DML published partial rows/command tag")
        if problems:
            failures.append((sql, problems))
            if setup:
                raise AssertionError(failures[-1])
        return actual

    schema = "mv_target_contract"
    mv = f"{schema}.mv"
    empty = f"{schema}.empty_mv"
    writer = "mv_target_contract_writer(2)"
    cases = [
        (f"INSERT INTO {mv} VALUES(2) RETURNING id", "42809"),
        (f"UPDATE {mv} SET id=2 RETURNING id", "42809"),
        (f"DELETE FROM {mv} WHERE id=1 RETURNING id", "42809"),
        (f"INSERT INTO {empty} VALUES(2) RETURNING id", "42809"),
        (f"UPDATE {empty} SET id=2 RETURNING id", "42809"),
        (f"DELETE FROM {empty} RETURNING id", "42809"),
        (f'INSERT INTO {schema}."MV Name" VALUES(2)', "42809"),
        (f'UPDATE {schema}."MV Name" AS "M" SET id=2 WHERE "M".id=1', "42809"),
        (f'WITH p AS(SELECT 1) DELETE FROM {schema}."MV Name" AS "M" RETURNING "M".*', "42809"),
        (f"WITH p AS(SELECT 1) INSERT INTO {mv} VALUES(3) RETURNING id", "42809"),
        (f"WITH p AS(SELECT 1) UPDATE {mv} SET id=3 RETURNING id", "42809"),
        (f"WITH p AS(SELECT 1) DELETE FROM {mv} RETURNING id", "42809"),
        (f"INSERT INTO {mv} VALUES({writer})", "42809"),
        (f"INSERT INTO {mv} SELECT {writer} FROM {schema}.base", "42809"),
        (f"UPDATE {mv} SET id={writer} WHERE false", "42809"),
        (f"DELETE FROM {mv} WHERE mv_target_contract_writer(id)>0", "42809"),
        (f"UPDATE {mv} SET id=(SELECT {writer})", "42809"),
        (f"WITH ins AS(INSERT INTO {schema}.base VALUES({writer}) RETURNING id) "
         f"INSERT INTO {mv} SELECT id FROM ins", "42809"),
        (f"WITH ins AS(INSERT INTO {schema}.base VALUES({writer}) RETURNING id) "
         f"UPDATE {mv} SET id=3 RETURNING id", "42809"),
        (f"WITH ins AS(INSERT INTO {schema}.base VALUES({writer}) RETURNING id) "
         f"DELETE FROM {mv} RETURNING id", "42809"),
        (f"INSERT INTO {mv}(bad) VALUES(3)", "42703"),
        (f"INSERT INTO {mv} VALUES(missing_mv_function(1))", "42883"),
        (f"INSERT INTO {mv} VALUES('bad')", "22P02"),
        (f"WITH p AS(SELECT 1) INSERT INTO {mv} VALUES('bad')", "22P02"),
        (f"UPDATE {mv} SET bad=3", "42703"),
        (f"UPDATE {mv} SET id=missing_mv_function(1)", "42883"),
        (f"UPDATE {mv} SET id='bad'", "22P02"),
        (f"UPDATE {mv} SET id='bad' WHERE missing_mv_function(1)=1", "42883"),
        (f"UPDATE {mv} SET id='bad' RETURNING missing_mv_function(1)", "42883"),
        (f"INSERT INTO {mv} VALUES('bad') RETURNING missing_mv_function(1)", "22P02"),
        (f"INSERT INTO {mv} SELECT 'bad' WHERE missing_mv_function(1)=1", "42883"),
        (f"INSERT INTO {mv} SELECT CAST('bad' AS INT) WHERE missing_mv_function(1)=1", "22P02"),
        (f"DELETE FROM {mv} WHERE missing_mv_column=1", "42703"),
        (f"DELETE FROM {mv} WHERE missing_mv_function(id)=1", "42883"),
        (f"UPDATE {mv} SET id=1/0", "22012"),
        (f"UPDATE {empty} SET id=1/0", "22012"),
        (f"WITH p AS(SELECT 1) UPDATE {mv} SET id=1/0", "22012"),
        (f"UPDATE {mv} SET id=1/0 WHERE false", "22012"),
        (f"UPDATE {mv} SET id=CAST(2147483648 AS INT)", "22003"),
        (f"UPDATE {mv} SET id=CAST('bad' AS INT)", "22P02"),
        (f"UPDATE {mv} SET id=id/0", "42809"),
        (f"UPDATE {mv} SET id=NULL::INT/0", "42809"),
        (f"UPDATE {mv} SET id=CASE WHEN false THEN 1/0 ELSE 2 END", "42809"),
        (f"UPDATE {mv} SET id=CASE WHEN false THEN CAST('bad' AS INT) ELSE 2 END", "22P02"),
        (f"UPDATE {mv} SET id=CASE WHEN id=1 THEN 1/0 ELSE 2 END", "22012"),
        (f"UPDATE {mv} SET id=CASE WHEN false THEN missing_mv_function(1) ELSE 2 END", "42883"),
        (f"UPDATE {mv} SET id=(SELECT 1/0)", "22012"),
        (f"UPDATE {mv} SET id=CASE WHEN false THEN (SELECT 1/0) ELSE 2 END", "42809"),
        (f"DELETE FROM {mv} WHERE 1/0=0", "22012"),
        (f"DELETE FROM {mv} WHERE false AND 1/0=0", "42809"),
        (f"DELETE FROM {mv} WHERE true OR 1/0=0", "42809"),
        (f"UPDATE {mv} SET id=1/0 RETURNING missing_mv_function(1)", "42883"),
        (f"WITH dead AS(SELECT 1/0) UPDATE {mv} SET id=2", "42809"),
        (f"WITH dead AS(SELECT CAST('bad' AS INT)) UPDATE {mv} SET id=2", "22P02"),
        (f"WITH ins AS(INSERT INTO {schema}.base VALUES(1/0) RETURNING id) "
         f"UPDATE {mv} SET id=2", "22012"),
        (f"INSERT INTO {empty} SELECT id FROM {empty}", "42809"),
        (f"UPDATE {mv} SET id=1/0,id=2", "42601"),
        (f"WITH p AS(SELECT 1) UPDATE {mv} SET id=1/0,id=2", "42601"),
        (f"UPDATE {mv} SET id=CAST('bad' AS INT),id=2", "22P02"),
        (f"UPDATE {mv} SET id=1 WHERE 1", "42804"),
        (f"DELETE FROM {mv} WHERE 1", "42804"),
        (f"DELETE FROM {mv} WHERE 'bad'", "22P02"),
        (f"WITH p AS(SELECT 1) DELETE FROM {mv} WHERE 1", "42804"),
        (f"UPDATE {mv} AS m SET id=b.id FROM {schema}.base AS b WHERE m.id=b.id", "42809"),
        (f"DELETE FROM {mv} AS m USING {schema}.base AS b WHERE m.id=b.id", "42809"),
        (f"WITH p AS(SELECT 1) UPDATE {mv} AS m SET id=b.id FROM {schema}.base AS b WHERE m.id=b.id", "42809"),
        (f"WITH p AS(SELECT 1) DELETE FROM {mv} AS m USING {schema}.base AS b WHERE m.id=b.id", "42809"),
        ("INSERT INTO mv VALUES(2)", "42809"),
        ("UPDATE mv SET id=2", "42809"),
        ("DELETE FROM mv", "42809"),
        ("WITH mv AS(SELECT 7 AS id) INSERT INTO mv VALUES(2)", "42809"),
    ]
    try:
        for sql in (
            "BEGIN", f"CREATE SCHEMA {schema}", f"CREATE TABLE {schema}.base(id INT)",
            f"INSERT INTO {schema}.base VALUES(1)",
            f"CREATE SEQUENCE {schema}.effects",
            "CREATE FUNCTION mv_target_contract_writer(p INT) RETURNS INT LANGUAGE plpgsql "
            f"AS $$BEGIN PERFORM nextval('{schema}.effects'); RETURN p; END$$",
            f"CREATE MATERIALIZED VIEW {mv} AS SELECT id FROM {schema}.base",
            f"CREATE MATERIALIZED VIEW {empty} AS SELECT id FROM {schema}.base WITH NO DATA",
            f'CREATE MATERIALIZED VIEW {schema}."MV Name" AS SELECT id FROM {schema}.base',
            f"SET search_path={schema},public",
        ):
            query(sql+";", setup=True)
        expected_value = 1
        for sql, state in cases:
            query("SAVEPOINT mv_case;", setup=True)
            query(sql+";", state=state, rows=[])
            query("ROLLBACK TO mv_case;", setup=True)
            query(f"SELECT id FROM {schema}.base ORDER BY id;", rows=[["1"]])
            query(f"SELECT id FROM {mv} ORDER BY id;", rows=[["1"]])
            # Never reset the sequence to mask effects; rollback does not undo
            # nextval. Each sentinel proves no routine/CTE was opened.
            actual = query(f"SELECT nextval('{schema}.effects');", rows=[[str(expected_value)]])
            if actual[5] != [20]:
                failures.append((sql, ["sequence sentinel lost BIGINT OID"]))
            expected_value += 1
        query("SAVEPOINT mv_source;")
        query(f"SELECT id FROM {empty};", state="55000", rows=[])
        query("ROLLBACK TO mv_source;")
        query(f"INSERT INTO {schema}.base SELECT id FROM {empty};", state="55000", rows=[])
        query("ROLLBACK TO mv_source;")
        query(f"SELECT id FROM {schema}.base;", rows=[["1"]])
        # A physical TEMP relation wins over the same-named materialized view.
        query("CREATE TEMP TABLE mv(id INT);", setup=True)
        query("INSERT INTO mv VALUES(2) RETURNING id;", rows=[["2"]])
        query("UPDATE mv SET id=3 RETURNING id;", rows=[["3"]])
        query("DELETE FROM mv RETURNING id;", rows=[["3"]])
        query(f"SELECT id FROM {schema}.mv;", rows=[["1"]])
        query("DROP TABLE mv;", setup=True)
        # The target-kind guard must not deny ordinary table mutations.
        query(f"INSERT INTO {schema}.base VALUES(2) RETURNING id;", rows=[["2"]])
        query(f"UPDATE {schema}.base SET id=id+1 WHERE id=2 RETURNING id;", rows=[["3"]])
        query(f"DELETE FROM {schema}.base WHERE id=3 RETURNING id;", rows=[["3"]])
        query(f"WITH p AS(SELECT 4 AS id) INSERT INTO {schema}.base SELECT id FROM p RETURNING id;", rows=[["4"]])
        query(f"WITH p AS(SELECT 1) UPDATE {schema}.base SET id=id+1 WHERE id=4 RETURNING id;", rows=[["5"]])
        query(f"WITH p AS(SELECT 1) DELETE FROM {schema}.base WHERE id=5 RETURNING id;", rows=[["5"]])
        query(f"SELECT id FROM {schema}.base;", rows=[["1"]])
        query("ROLLBACK;", setup=True)
        print("MV TARGET FAILURES", failures, flush=True)
        assert not failures, failures
        print(f"[MV TARGET PROTOCOL] passed {len(cases)} target controls plus source guards", flush=True)
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
