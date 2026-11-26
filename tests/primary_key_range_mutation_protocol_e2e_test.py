#!/usr/bin/env python3
"""Check target selection for ordinary updates and deletes by integer key range."""

from contextlib import closing
import socket
import sys
import uuid

from session_setting_functions_protocol_e2e_test import Wire, runner


def scan_counts(wire, table):
    rows, state, message, _, _ = runner.decode_wire_result(
        wire.send_statements("SELECT * FROM pg_stat_tables"))
    assert state is None, message
    row = next(row for row in rows if row[0] == table)
    return tuple(map(int, row[1:5]))


def exercise(wire, peer, schema, reference):
    name = "range_changes"
    table = f"{schema}.{name}"
    storage_name = f"{schema}__{name}"
    wire.command(f"CREATE SCHEMA {schema}", "CREATE SCHEMA")
    wire.command(f"CREATE TABLE {table}(note text, qty int CHECK(qty<5000), k int PRIMARY KEY)",
                 "CREATE TABLE")
    wire.command("BEGIN", "BEGIN", b"T")
    note = "x" * 60
    note_sql = f"'{note}'"
    for start in range(1, 3073, 128):
        batch = ",".join(f"({k},{k},{'NULL' if k % 2 else note_sql})" for k in range(start, start + 128))
        wire.command(f"INSERT INTO {table}(k,qty,note) VALUES {batch}", "INSERT 0 128", b"T")
    wire.command("COMMIT", "COMMIT")

    def mutate(sql, keys, reader=wire, ready=b"I", indexed=True, fetched=None,
               heap_rows=None, storage=storage_name):
        before = scan_counts(wire, storage) if not reference else None
        frames = reader.send_statements(sql)
        rows, state, message, _, tag = runner.decode_wire_result(frames)
        assert state is None, (sql, state, message)
        assert sorted(int(row[0]) for row in rows) == sorted(keys), (sql, rows)
        assert tag == f"{sql.split()[0]} {len(keys)}" and frames[-1] == (b"Z", ready), (sql, frames)
        if before is not None:
            after = scan_counts(wire, storage)
            changes = tuple(a - b for a, b in zip(after, before))
            amount = len(keys) if fetched is None else fetched
            expected = (1, amount) if indexed else (0, 0)
            assert changes[2:] == expected, (sql, changes, expected)
            if heap_rows is not None:
                assert changes[:2] == (1, heap_rows), (sql, changes)

    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 41 AND 48 RETURNING k", list(range(41, 49)))
    mutate(f"UPDATE {table} AS t SET qty=qty+10 WHERE 49<=t.k AND t.k<53 RETURNING k",
           list(range(49, 53)), Wire(wire.client, wire.sock, extended=True))
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE (k BETWEEN 61 AND 64 OR k BETWEEN 63 AND 66) "
           "OR k BETWEEN 1001 AND 1002 RETURNING k", [61, 62, 63, 64, 65, 66, 1001, 1002])
    mutate(f"DELETE FROM {table} WHERE k BETWEEN SYMMETRIC 201 AND 198 RETURNING k", list(range(198, 202)))
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 9000 AND 9008 RETURNING k", [])
    mutate(f"DELETE FROM {table} WHERE k BETWEEN NULL AND 80 RETURNING k", [], indexed=False)
    wire.query(f"SELECT qty FROM {table} WHERE k=63", rows=[("73",)], fields=[("qty", 23)])

    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 301 AND 308 AND note='{note}' RETURNING k",
           [302, 304, 306, 308], fetched=8)
    wire.command(f"UPDATE {table} SET qty=NULL WHERE k=315", "UPDATE 1")
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE qty>=315 AND k BETWEEN 312 AND 319 RETURNING k",
           [316, 317, 318, 319], fetched=8)
    mutate(f"DELETE FROM {table} WHERE (k BETWEEN 321 AND 324 AND note='{note}') "
           "OR (k BETWEEN 335 AND 338 AND note IS NULL) RETURNING k", [322, 324, 335, 337], fetched=8)
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k>=340 AND qty>341 AND k<=343 RETURNING k",
           [342, 343], fetched=4)
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE (k BETWEEN 351 AND 354 AND note IS NOT NULL) "
           "OR qty=390 RETURNING k", [352, 354, 390], indexed=False)
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 361 AND 364 AND qty>6000 RETURNING k", [], fetched=4)

    calls = f"{schema}.calls"
    wire.command(f"CREATE SEQUENCE {calls}", "CREATE SEQUENCE")
    mutate(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 371 AND 374 AND nextval('{calls}')>0 RETURNING k",
           [371, 372, 373, 374])
    wire.query(f"SELECT currval('{calls}') AS n", rows=[("4",)], fields=[("n", 20)])
    wire.query(f"UPDATE {table} SET qty=qty+10 WHERE k BETWEEN 20 AND 22 AND 99/(k-20)>1 RETURNING k",
               tag=None, state="22012")
    if not reference:
        # The local executor evaluates the left conjunct first. A late
        # key condition cannot suppress errors from that conjunct.
        wire.query(f"UPDATE {table} SET qty=qty+10 WHERE 99/(k-20)>1 AND k BETWEEN 31 AND 34 RETURNING k",
                   tag=None, state="22012")

    wire.query(f"UPDATE {table} SET qty=CASE WHEN k=11 THEN 5000 ELSE qty+10 END "
               "WHERE k BETWEEN 10 AND 12 RETURNING k", tag=None, state="23514")
    wire.query(f"SELECT qty FROM {table} WHERE k BETWEEN 10 AND 12 ORDER BY k",
               rows=[("10",), ("11",), ("12",)], tag="SELECT 3", fields=[("qty", 23)])

    wire.command("BEGIN", "BEGIN", b"T")
    wire.command(f"UPDATE {table} SET k=9011 WHERE k=1", "UPDATE 1", b"T")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9009 AND 9012 OR "
           "(k BETWEEN 22 AND 23 AND note IS NOT NULL) RETURNING k",
           [22, 9011], ready=b"T", indexed=False, heap_rows=3064)
    mutate(f"DELETE FROM {table} WHERE k BETWEEN 9010 AND 9015 AND qty>=75 RETURNING k",
           [9011], ready=b"T", indexed=False, heap_rows=3064)
    mutate(f"UPDATE {table} SET qty=qty+1 WHERE k BETWEEN 371 AND 374 AND nextval('{calls}')>0 RETURNING k",
           [371, 372, 373, 374], ready=b"T", indexed=False, heap_rows=3063)
    wire.query(f"SELECT currval('{calls}') AS n", rows=[("8",)], fields=[("n", 20)], ready=b"T")
    if not reference:
        # A sequential scan cannot move a later key condition ahead of a
        # division that must still be evaluated on an out-of-range row.
        wire.query(f"UPDATE {table} SET qty=qty+10 WHERE 99/(k-20)>1 AND k BETWEEN 31 AND 34 RETURNING k",
                   tag=None, state="22012", ready=b"E")
    wire.command("ROLLBACK", "ROLLBACK")

    wire.command("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T")
    wire.query(f"SELECT qty FROM {table} WHERE k=81", rows=[("81",)], fields=[("qty", 23)], ready=b"T")
    peer.command(f"UPDATE {table} SET k=9001 WHERE k=2", "UPDATE 1")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9000 AND 9008 RETURNING k",
           [], ready=b"T", indexed=False, heap_rows=3064)
    wire.command("ROLLBACK", "ROLLBACK")
    mutate(f"UPDATE {table} SET qty=75 WHERE k BETWEEN 9000 AND 9008 RETURNING k", [9001])

    wire.command("BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN", b"T")
    mutate(f"DELETE FROM {table} WHERE k BETWEEN 21 AND 24 RETURNING k", list(range(21, 25)), ready=b"T", indexed=False)
    wire.command("ROLLBACK", "ROLLBACK")
    wire.command("SET work_mem='64kB'", "SET")
    mutate(f"UPDATE {table} SET qty=qty+1 WHERE k BETWEEN 401 AND 1200 RETURNING k",
           list(range(401, 1201)), indexed=False, heap_rows=3064)

    wide = f"{schema}.wide_changes"
    wire.command(f"CREATE TABLE {wide}(note text, qty int, k bigint PRIMARY KEY)", "CREATE TABLE")
    wire.command(f"INSERT INTO {wide} VALUES (NULL,NULL,-9223372036854775808),"
                 "('two',2,-9223372036854775807),('three',3,0),(NULL,4,9223372036854775807)", "INSERT 0 4")
    wire.command("BEGIN", "BEGIN", b"T")
    wire.command(f"UPDATE {wide} SET qty=2 WHERE k=0", "UPDATE 1", b"T")
    mutate(f"DELETE FROM {wide} WHERE (k BETWEEN -9223372036854775808 AND -9223372036854775807 AND qty IS NULL) "
           "OR k>=9223372036854775807 RETURNING k", [-9223372036854775808, 9223372036854775807],
           ready=b"T", indexed=False, heap_rows=4, storage=f"{schema}__wide_changes")
    wire.command("ROLLBACK", "ROLLBACK")


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
