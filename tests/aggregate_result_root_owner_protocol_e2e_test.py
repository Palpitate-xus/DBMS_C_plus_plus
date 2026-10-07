#!/usr/bin/env python3
"""An aggregate descriptor belongs to its outer AST, not nested predicate text."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("aggregate_root_runner", repo / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = "--reference18" in sys.argv
    collect = "--collect-errors" in sys.argv; failures = []
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password); runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server["sock"]

    def query(sql, rows=None, types=None, headers=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("AGGREGATE_RESULT_ROOT", sql, result, flush=True)
        try:
            assert result[1] is None, (sql, result)
            if rows is not None: assert result[0] == rows and result[4] == "SELECT " + str(len(rows)), (sql, result, rows)
            if types is not None: assert result[5] == types, (sql, result, types)
            if headers is not None: assert result[3] == headers, (sql, result, headers)
        except AssertionError as failure:
            if not collect: raise
            failures.append(failure.args); print("AGGREGATE_RESULT_ROOT_STRONG_FAILURE", failure.args, flush=True)

    def describe(sql, types, headers):
        name = ("aggregate_root_" + uuid.uuid4().hex[:12]).encode()
        sock.sendall(client.typed(b'P', name+b'\0'+sql.encode()+b'\0\0\0') +
                     client.typed(b'D', b'S'+name+b'\0') + client.typed(b'S'))
        messages = client.read_until_ready(sock); fields = client.row_description_fields(messages)
        print("AGGREGATE_RESULT_ROOT_DESCRIBE", sql, fields, flush=True)
        try:
            assert not any(kind in (b'E', b'D') for kind, _ in messages), (sql, messages)
            assert [field[3] for field in fields] == types, (sql, fields, types)
            assert [field[0].decode() for field in fields] == headers, (sql, fields, headers)
        except AssertionError as failure:
            if not collect: raise
            failures.append(failure.args); print("AGGREGATE_RESULT_ROOT_STRONG_FAILURE", failure.args, flush=True)

    try:
        schema = "aggregate_root_" + uuid.uuid4().hex[:18]
        if reference: query("BEGIN")
        query('CREATE SCHEMA "'+schema+'"')
        query(('SET LOCAL' if reference else 'SET')+' search_path TO "'+schema+'",pg_catalog')
        query("CREATE TABLE inputs(i INT,t TEXT)")
        query("INSERT INTO inputs VALUES(1,'alpha'),(2,''),(NULL,NULL)")
        query("CREATE TABLE empty_inputs(i INT,t TEXT)")
        for predicate, total in [("i IS DISTINCT FROM 1", "2"), ("i IS NOT DISTINCT FROM 1", "1"),
                                 ("i BETWEEN 1 AND 2", "2"), ("t LIKE 'a%'", "1"), ("t ILIKE 'A%'", "1")]:
            expr = "CASE WHEN "+predicate+" THEN 1 ELSE 0 END"
            query("SELECT sum("+expr+"),min("+expr+"),max("+expr+"),count("+expr+") FROM inputs",
                  [[total, "0", "1", "3"]], [20, 23, 23, 20], ["sum", "min", "max", "count"])
            for source in ["empty_inputs", "inputs WHERE false"]:
                query("SELECT sum("+expr+") FROM "+source, [[None]], [20])
            query("SELECT sum("+expr+") FROM inputs LIMIT 0", [], [20])
            describe("SELECT sum("+expr+") AS total,avg("+expr+") AS mean FROM inputs", [20, 1700], ["total", "mean"])
        query("SELECT sum(CASE WHEN i IS DISTINCT FROM 1 THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) FROM inputs", [["2"]], [1700])
        query("SELECT sum(CASE WHEN i IS DISTINCT FROM 1 THEN CAST(NULL AS SMALLINT) ELSE CAST(NULL AS SMALLINT) END) FROM inputs", [[None]], [20])
        query("SELECT sum(CASE WHEN t=' is distinct from ' THEN 1 ELSE 0 END) FROM inputs", [["0"]], [20])
        query("SELECT sum(CASE WHEN t=' between ' THEN 1 ELSE 0 END) FROM inputs", [["0"]], [20])
        query("SELECT sum(CASE WHEN t=' like ' THEN 1 ELSE 0 END) FROM inputs", [["0"]], [20])
        query("SELECT sum(i) FILTER(WHERE t LIKE 'a%') FROM inputs", [["1"]], [20])
        query("SELECT sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)>1 FROM inputs", [["t"]], [16])
        query("SELECT i IS DISTINCT FROM 1,t LIKE 'a%' FROM inputs WHERE i=1", [["f", "t"]], [16, 16])
        query("CREATE TABLE effects(v INT)")
        query("CREATE FUNCTION writer(v INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO effects VALUES(v); RETURN v; END;$$")
        writer = "SELECT sum(CASE WHEN i IS DISTINCT FROM 1 THEN writer(i) ELSE 0 END) AS total FROM inputs WHERE i IS NOT NULL"
        describe(writer, [20], ["total"])
        query("SELECT v FROM effects", [], [23])
        query(writer, [["2"]], [20], ["total"])
        query("SELECT v FROM effects", [["2"]], [23])
        other = schema + "_other"
        query('CREATE SCHEMA "'+other+'"')
        query('CREATE FUNCTION "'+other+'".sum(v INT) RETURNS TEXT LANGUAGE plpgsql AS $$BEGIN RETURN \'chosen\'; END;$$')
        scalar = 'SELECT "'+other+'".sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END) AS chosen FROM inputs WHERE i=2'
        describe(scalar, [25], ["chosen"])
        query(scalar, [["chosen"]], [25], ["chosen"])
        assert not failures, ("complete aggregate result root matrix failed", failures)
        print("[AGGREGATE RESULT ROOT OWNER] complete ordinary/NULL/empty/filter/cast/describe/once matrix passed", flush=True)
    finally:
        if reference:
            try: client.simple_query(sock, "ROLLBACK")
            finally: sock.close()
        else: runner.stop_ours(server)


if __name__ == "__main__": main()
