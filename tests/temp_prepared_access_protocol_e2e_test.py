#!/usr/bin/env python3
"""PREPARE rejects every TEMP access, including reads and aborted children."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "temp_prepare_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    suffix = uuid.uuid4().hex[:12]
    persistent = "prep_rows_" + suffix
    temporary = "prep_temp_" + suffix
    empty = "prep_empty_" + suffix
    sequence = temporary + "_id_seq"
    created = "prep_child_" + suffix
    deleted_on_commit = "prep_commit_delete_" + suffix
    dropped_on_commit = "prep_commit_drop_" + suffix
    index = 0

    def query(sql, extended=False, state=None, ready=b"I", rows=None, tag=None):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        if tag is not None:
            assert result[4] == tag, (sql, result, tag)

    try:
        query(f"CREATE TABLE {persistent}(id INT PRIMARY KEY);")
        query(f"CREATE TEMP TABLE {temporary}(id SERIAL PRIMARY KEY);")
        query(f"CREATE TEMP TABLE {empty}(id INT);")
        query(f"CREATE TEMP TABLE {deleted_on_commit}(id INT) ON COMMIT DELETE ROWS;")
        query(f"INSERT INTO {temporary} DEFAULT VALUES RETURNING id;", rows=[["1"]])
        for extended in (False, True):
            # Merely owning TEMP objects from a previous transaction must not
            # make an unrelated permanent-table transaction unpreparable.
            positive = "temp_prepare_positive_" + suffix + str(extended)
            query("BEGIN;", extended, ready=b"T")
            query(f"INSERT INTO {persistent} VALUES(100);", extended, ready=b"T")
            query(f"LOCK TABLE {persistent} IN SHARE MODE;", extended,
                  ready=b"T", rows=[], tag="LOCK TABLE")
            query(f"PREPARE TRANSACTION '{positive}';", extended,
                  tag="PREPARE TRANSACTION")
            query(f"ROLLBACK PREPARED '{positive}';", extended,
                  tag="ROLLBACK PREPARED")
            query(f"SELECT id FROM {persistent};", extended, rows=[])

            accesses = [
                (f"SELECT id FROM {temporary};", [["1"]]),
                (f"SELECT id FROM pg_temp.{temporary} WHERE FALSE;", []),
                (f"SELECT id FROM {empty};", []),
                (f"SELECT count(*) FROM {empty};", [["0"]]),
                (f"SELECT id FROM {temporary} WHERE id=999;", []),
                (f"SELECT nextval('pg_temp.{sequence}');", [["20" if extended else "2"]]),
                (f"SELECT currval('{sequence}');", [["20" if extended else "2"]]),
                ("SELECT lastval();", [["20" if extended else "2"]]),
                (f"SELECT setval('{sequence}', 20, FALSE);", [["20"]]),
                (f"UPDATE {empty} SET id=2 WHERE id=999;", []),
                (f"DELETE FROM {empty} WHERE id=999;", []),
                (f"INSERT INTO {empty} VALUES(10);", []),
                (f"LOCK TABLE {temporary} IN SHARE MODE;", []),
            ]
            for access, expected in accesses:
                index += 1
                xid = "temp_prepare_access_" + suffix + str(index)
                query("BEGIN;", extended, ready=b"T")
                query(f"INSERT INTO {persistent} VALUES({index});", extended,
                      ready=b"T")
                query(access, extended, ready=b"T", rows=expected,
                      tag="LOCK TABLE" if access.startswith("LOCK TABLE") else None)
                query(f"PREPARE TRANSACTION '{xid}';", extended, "0A000")
                # PostgreSQL PREPARE failure terminates the whole transaction,
                # rather than leaving an E block holding temporary ownership.
                query(f"SELECT id FROM {persistent};", extended, rows=[])
                query(f"SELECT id FROM {empty};", extended, rows=[])

            # TEMP access remains disqualifying after a user subabort; the
            # access flag is top-level transaction state, not a row undo log.
            for access in (f"SELECT id FROM {empty};",
                           f"INSERT INTO {empty} VALUES(9);",
                           f"CREATE TEMP TABLE {created}(id INT);"):
                index += 1
                query("BEGIN;", extended, ready=b"T")
                query(f"INSERT INTO {persistent} VALUES({index});", extended,
                      ready=b"T")
                query("SAVEPOINT child;", extended, ready=b"T")
                query(access, extended, ready=b"T")
                query("ROLLBACK TO child;", extended, ready=b"T")
                query(f"PREPARE TRANSACTION 'temp_prepare_child_{suffix}{index}';",
                      extended, "0A000")
                query(f"SELECT id FROM {persistent};", extended, rows=[])
                query(f"SELECT id FROM {empty};", extended, rows=[])

            query("BEGIN;", extended, ready=b"T")
            query(f"CREATE TEMP TABLE {dropped_on_commit}(id INT) ON COMMIT DROP;",
                  extended, ready=b"T")
            query(f"PREPARE TRANSACTION 'temp_prepare_on_commit_{suffix}{extended}';",
                  extended, "0A000")
            query(f"SELECT id FROM {dropped_on_commit};", extended, "42P01")

            # PostgreSQL opens referenced relations during Parse analysis,
            # before Bind, Describe or Execute. Holding that TEMP access is
            # enough to prohibit PREPARE even though no SELECT rows were read.
            for parsed_sql in (f"SELECT id FROM {empty};",
                               f"SELECT (SELECT id FROM {empty} LIMIT 1);",
                               f"SELECT 1 WHERE EXISTS(SELECT id FROM {empty});",
                               f"SELECT 1 WHERE 1 IN (SELECT id FROM {empty});",
                               f"SELECT id FROM (SELECT id FROM {empty}) AS nested;",
                               f"WITH child AS (SELECT id FROM {empty}) SELECT id FROM child;",
                               f"WITH {empty} AS (SELECT id FROM {empty}) SELECT id FROM {empty};",
                               f"SELECT id FROM {empty} UNION ALL SELECT id FROM {persistent};",
                               f"UPDATE {empty} SET id=3;",
                               f"DELETE FROM {empty};",
                               f"INSERT INTO {empty} VALUES(3);"):
                index += 1
                query("BEGIN;", extended, ready=b"T")
                statement = ("temp_parse_" + suffix + str(index)).encode()
                sock.sendall(client.typed(b"P", statement + b"\0" +
                                         parsed_sql.encode() + b"\0\0\0") +
                             client.typed(b"H", b""))
                assert client.read_message(sock) == (b"1", b""), parsed_sql
                print("[TEMP PREPARE PARSE-ONLY] " + parsed_sql, flush=True)
                query(f"PREPARE TRANSACTION 'temp_prepare_parse_{suffix}{index}';",
                      extended, "0A000")
                query(f"SELECT id FROM {empty};", extended, rows=[])

            # Analysis must neither execute a volatile sequence function nor
            # confuse a CTE alias with an actual TEMP relation of that name.
            for parsed_sql in (f"SELECT nextval('{sequence}');",
                               f"SELECT '(SELECT id FROM {empty})' AS data;",
                               f"WITH {empty} AS (SELECT 7 AS id) SELECT id FROM {empty};"):
                index += 1
                query("BEGIN;", extended, ready=b"T")
                statement = ("temp_parse_safe_" + suffix + str(index)).encode()
                sock.sendall(client.typed(b"P", statement + b"\0" +
                                         parsed_sql.encode() + b"\0\0\0") +
                             client.typed(b"H", b""))
                assert client.read_message(sock) == (b"1", b""), parsed_sql
                xid = f"temp_prepare_parse_safe_{suffix}{index}"
                query(f"PREPARE TRANSACTION '{xid}';", extended,
                      tag="PREPARE TRANSACTION")
                query(f"ROLLBACK PREPARED '{xid}';", extended,
                      tag="ROLLBACK PREPARED")
                query(f"SELECT currval('{sequence}');", extended,
                      rows=[["20" if extended else "2"]])

            index += 1
            query("BEGIN;", extended, ready=b"T")
            query(f"PREPARE temp_sql_{suffix}{index} AS SELECT id FROM {empty};",
                  extended, ready=b"T")
            query(f"PREPARE TRANSACTION 'temp_prepare_sql_{suffix}{index}';",
                  extended, "0A000")
            query(f"SELECT id FROM {empty};", extended, rows=[])

            query("BEGIN READ ONLY;", extended, ready=b"T")
            query(f"SELECT id FROM {temporary};", extended, ready=b"T", rows=[["1"]])
            query(f"PREPARE TRANSACTION 'temp_prepare_read_only_{suffix}{extended}';",
                  extended, "0A000")

            # A subsequent top-level transaction resets the sticky TEMP flag.
            positive += "_reset"
            query("BEGIN;", extended, ready=b"T")
            query(f"INSERT INTO {persistent} VALUES(200);", extended, ready=b"T")
            query(f"PREPARE TRANSACTION '{positive}';", extended,
                  tag="PREPARE TRANSACTION")
            query(f"COMMIT PREPARED '{positive}';", extended,
                  tag="COMMIT PREPARED")
            query(f"SELECT id FROM {persistent};", extended, rows=[["200"]])
            query(f"DELETE FROM {persistent};", extended)
        query(f"DROP TABLE {temporary};")
        query(f"DROP TABLE {empty};")
        query(f"DROP TABLE {deleted_on_commit};")
        query(f"DROP TABLE {persistent};")
        print("[TEMP PREPARED ACCESS " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
