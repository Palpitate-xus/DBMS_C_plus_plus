#!/usr/bin/env python3
"""Physical INTEGER ranges validate pure inputs before Parse/Describe/open."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("integer_range_parse_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    schema = "owned_integer_parse_" + uuid.uuid4().hex[:12]; created = False; failures = []; controls = 0
    def fail(condition, detail):
        if condition: failures.append(detail); print("[INTEGER BETWEEN PARSE FAIL] " + str(detail), flush=True)
    def ok(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result); return result
    def check(sql, state, rows, oid, size):
        nonlocal controls
        controls += 1; statement = ("owned_integer_parse_" + uuid.uuid4().hex[:12]).encode(); portal = statement + b"p"
        ok("BEGIN")
        sock.sendall(client.typed(b"P", statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)) +
                     client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
        messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        mismatch = result[1] != state or any(kind == b"D" for kind, _ in messages)
        if state: mismatch = mismatch or any(kind in (b"1", b"T") for kind, _ in messages)
        else: mismatch = mismatch or sum(kind == b"1" for kind, _ in messages) != 1 or len(fields) != 1 or fields[0][0] != b"value" or fields[0][3:] != (oid, size, -1, 0)
        fail(mismatch, (sql, "Parse/Describe", result, fields, state, oid, size))
        if result[1] is not None:
            ok("ROLLBACK"); return
        sock.sendall(client.typed(b"B", portal + b"\0" + statement + b"\0" + struct.pack("!HHH", 0, 0, 0)) +
                     client.typed(b"D", b"P" + portal + b"\0") + client.typed(b"E", portal + b"\0" + struct.pack("!I", 1)) + client.typed(b"S"))
        messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
        # One-row Execute may suspend exactly after its first row; the descriptor
        # and actual data must still match, and a resumed Execute must exhaust it.
        suspended = any(kind == b"s" for kind, _ in messages)
        fail(result[:2] != (rows, None) or result[3] != ["value"] or result[5] != [oid] or not suspended and result[4] != "SELECT " + str(len(rows)),
             (sql, "Bind/portal Describe/Execute cap1", result, rows, suspended))
        if suspended:
            sock.sendall(client.typed(b"E", portal + b"\0" + struct.pack("!I", 1)) + client.typed(b"S"))
            exhausted = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
            fail(exhausted[:2] != ([], None) or exhausted[4] != "SELECT 0", (sql, "resumed portal exhaustion", exhausted))
        sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
        assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        ok("ROLLBACK")
    try:
        ok("CREATE SCHEMA " + schema); created = True
        ok("CREATE SEQUENCE " + schema + ".effects")
        ok("CREATE FUNCTION " + schema + ".writer(p integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('" + schema + ".effects'); RETURN p; END; $$")
        ok("SELECT nextval('" + schema + ".effects')")
        def counter(): return int(ok("SELECT currval('" + schema + ".effects')")[0][0][0])
        for kind in ("smallint", "integer", "bigint"):
            for population in ("populated", "empty", "nulls"):
                table = schema + "." + kind + "_" + population
                ok("CREATE TABLE " + table + "(id integer PRIMARY KEY,i " + kind + ")")
                if population != "empty": ok("INSERT INTO " + table + " VALUES(1," + ("NULL" if population == "nulls" else "1") + ")")
                for lower, upper, state in (("'1'", "2", None), ("0", "'b01'", "22P02"), ("'b01'", "2", "22P02"),
                        ("NULL", "'b01'", "22P02"), ("'b01'", "NULL", "22P02"), ("''", "NULL", "22P02"),
                        ("NULL", "''", "22P02"), ("'1'::text", "2", "42883")):
                    for operation in (" BETWEEN ", " NOT BETWEEN "):
                        condition = "i" + operation + lower + " AND " + upper
                        for projection in (True, False):
                            sql = "SELECT " + (condition if projection else "id") + " AS value FROM " + table
                            if not projection: sql += " WHERE " + condition
                            sql += " ORDER BY id"
                            rows = []
                            if not state and population != "empty":
                                if projection: rows = [[None if population == "nulls" else "t" if operation == " BETWEEN " else "f"]]
                                elif population != "nulls" and operation == " BETWEEN ": rows = [["1"]]
                            check(sql, state, rows, 16 if projection else 23, 1 if projection else 4)
                if kind == "integer":
                    for range_sql in ("'b01' AND 2", "NULL AND 'b01'", "'b01' AND NULL", "NULL AND ''"):
                        for operation in (" BETWEEN ", " NOT BETWEEN "):
                            before = counter(); sql = "SELECT " + schema + ".writer(i)" + operation + range_sql + " AS value FROM " + table
                            check(sql, "22P02", [], 16, 1)
                            after = counter(); fail(after != before, (sql, "Parse admission executed actual volatile body", before, after))
        assert controls == 312, controls
        assert not failures, "%d differences in all %d physical Parse/Describe/input/open controls" % (len(failures), controls)
        print("[INTEGER BETWEEN PARSE PROTOCOL] all %d actual physical/empty/NULL/Parse/Describe/cap1/pure-writer controls passed" % controls)
    finally:
        if created: ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
