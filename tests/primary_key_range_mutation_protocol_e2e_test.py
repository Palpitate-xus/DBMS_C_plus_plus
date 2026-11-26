#!/usr/bin/env python3
"""Check target selection for ordinary updates and deletes by integer key range."""

from contextlib import closing
import socket
import sys
import uuid

from session_setting_functions_protocol_e2e_test import Wire, runner


def index_counts(wire, table):
    rows, state, message, _, _ = runner.decode_wire_result(
        wire.send_statements("SELECT * FROM pg_stat_tables"))
    assert state is None, message
    row = next(row for row in rows if row[0] == table)
    return tuple(map(int, row[3:5]))


def exercise(wire, peer, schema, reference):
    name = "range_changes"
    table = f"{schema}.{name}"
    storage_name = f"{schema}__{name}"
    wire.command(f"CREATE SCHEMA {schema}", "CREATE SCHEMA")
    wire.command(f"CREATE TABLE {table}(k int PRIMARY KEY, qty int CHECK(qty<5000), note text)",
                 "CREATE TABLE")
    wire.command("BEGIN", "BEGIN", b"T")
    note = "x" * 60
    for start in range(1, 3073, 128):
        batch = ",".join(f"({k},{k},'{note}')" for k in range(start, start + 128))
        wire.command(f"INSERT INTO {table} VALUES {batch}", "INSERT 0 128", b"T")
    wire.command("COMMIT", "COMMIT")

    def mutate(sql, keys, reader=wire, ready=b"I", indexed=True):
        before = index_counts(wire, storage_name) if not reference else None
        frames = reader.send_statements(sql)
        rows, state, message, _, tag = runner.decode_wire_result(frames)
        assert state is None, (sql, state, message)
        assert sorted(int(row[0]) for row in rows) == sorted(keys), (sql, rows)
        assert tag == f"{sql.split()[0]} {len(keys)}" and frames[-1] == (b"Z", ready), (sql, frames)
        if before is not None:
            reads, fetched = index_counts(wire, storage_name)
            assert (reads - before[0], fetched - before[1]) == ((1, len(keys)) if indexed else (0, 0)), sql

    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 41 AND 48 RETURNING k", list(range(41, 49)))
    mutate(f"UPDATE {table} AS t SET qty=qty+10 WHERE 49<=t.k AND t.k<53 RETURNING k",
           list(range(49, 53)), Wire(wire.client, wire.sock, extended=True))
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE (k BETWEEN 61 AND 64 OR k BETWEEN 63 AND 66) "
           "OR k BETWEEN 1001 AND 1002 RETURNING k", [61, 62, 63, 64, 65, 66, 1001, 1002])
    mutate(f"DELETE FROM {table} WHERE k BETWEEN SYMMETRIC 201 AND 198 RETURNING k", list(range(198, 202)))
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 9000 AND 9008 RETURNING k", [])
    mutate(f"DELETE FROM {table} WHERE k BETWEEN NULL AND 80 RETURNING k", [], indexed=False)
    wire.query(f"SELECT qty FROM {table} WHERE k=63", rows=[("73",)], fields=[("qty", 23)])

    wire.query(f"UPDATE {table} SET qty=CASE WHEN k=11 THEN 5000 ELSE qty+10 END "
               "WHERE k BETWEEN 10 AND 12 RETURNING k", tag=None, state="23514")
    wire.query(f"SELECT qty FROM {table} WHERE k BETWEEN 10 AND 12 ORDER BY k",
               rows=[("10",), ("11",), ("12",)], tag="SELECT 3", fields=[("qty", 23)])

    wire.command("BEGIN", "BEGIN", b"T")
    wire.command(f"UPDATE {table} SET k=9011 WHERE k=1", "UPDATE 1", b"T")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9009 AND 9012 RETURNING k", [9011], ready=b"T", indexed=False)
    wire.command("ROLLBACK", "ROLLBACK")

    wire.command("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T")
    wire.query(f"SELECT qty FROM {table} WHERE k=81", rows=[("81",)], fields=[("qty", 23)], ready=b"T")
    peer.command(f"UPDATE {table} SET k=9001 WHERE k=2", "UPDATE 1")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9000 AND 9008 RETURNING k", [], ready=b"T", indexed=False)
    wire.command("ROLLBACK", "ROLLBACK")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9000 AND 9008 RETURNING k", [9001])

    wire.command("BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN", b"T")
    mutate(f"DELETE FROM {table} WHERE k BETWEEN 21 AND 24 RETURNING k", list(range(21, 25)), ready=b"T", indexed=False)
    wire.command("ROLLBACK", "ROLLBACK")
    wire.command("SET work_mem='64kB'", "SET")
    mutate(f"UPDATE {table} SET qty=qty+1 WHERE k BETWEEN 401 AND 1200 RETURNING k",
           list(range(401, 1201)), indexed=False)


def main():
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        startup = client.startup_reference
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        startup(sock, user, database, password)
    else:
        server = runner.start_ours(client)
        host, port, user, database, password = "127.0.0.1", server["port"], "alice", "info", "secret"
        startup, sock = client.startup, server["sock"]
    schema = "change_keys_" + uuid.uuid4().hex[:12]
    try:
        if reference:
            runner.verify_reference_version(client, sock)
        with closing(socket.create_connection((host, port), timeout=runner.wire_timeout())) as other:
            startup(other, user, database, password)
            exercise(Wire(client, sock), Wire(client, other), schema, reference)
        print("[PRIMARY KEY RANGE MUTATIONS] passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
                client.simple_query(sock, "DROP SCHEMA " + schema + " CASCADE")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
