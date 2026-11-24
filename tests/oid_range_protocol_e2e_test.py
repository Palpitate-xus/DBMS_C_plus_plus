#!/usr/bin/env python3
"""Check unsigned OID range predicates, wire inputs, and index access."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("OID_RANGE")
    client, sock = review.client, review.sock
    packet = client.typed
    plan_checks = 0

    def command(sql, tag, superuser=None):
        frames = [frame for frame in client.simple_query(sock, sql) if frame[0] != b"N"]
        expected = [(b"C", tag.encode() + b"\0")]
        if superuser is not None:
            expected.append((b"S", b"is_superuser\0" + superuser + b"\0"))
        expected.append((b"Z", b"I"))
        review.record(sql, frames == expected, frames, expected)

    def nodes(node):
        yield node
        for child in node["children"]:
            yield from nodes(child)

    def plan(sql, candidates, bounds=()):
        nonlocal plan_checks
        if review.server is None:
            return
        plan_checks += 1
        explain = "EXPLAIN (FORMAT JSON, ANALYZE TRUE, TIMING FALSE) " + sql
        if bounds:
            parse = b"\0" + explain.encode() + b"\0" + struct.pack("!HII", 2, 26, 26)
            bind = b"\0\0" + struct.pack("!HHH", 1, 1, 2)
            bind += b"".join(struct.pack("!iI", 4, value) for value in bounds) + struct.pack("!H", 0)
            sock.sendall(packet(b"P", parse) + packet(b"B", bind) + packet(b"E", b"\0" + struct.pack("!I", 0))
                         + packet(b"S"))
            frames = client.read_until_ready(sock)
        else:
            frames = client.simple_query(sock, explain)
        reply = review.runner.decode_wire_result(frames)
        tree = list(nodes(json.loads(reply[0][0][0])["plan"])) if not reply[1] else []
        scans = [node for node in tree if node["nodeType"] == "OidRangeScan"]
        reads_heap = any(node["nodeType"] == "TableScan" for node in tree)
        if candidates is None:
            valid = reads_heap and not any(node.get("usedIndex") == "true" for node in scans)
        else:
            valid = (len(scans) == 1 and not reads_heap and scans[0].get("usedIndex") == "true"
                     and scans[0].get("actualRows") == candidates and scans[0].get("column") == "k")
        seen = [(node["nodeType"], node.get("actualRows"), node.get("usedIndex")) for node in tree]
        review.record(explain, valid, seen, candidates)

    def select(predicate, ids, *, table="oid_range_primary", candidates=None):
        sql = f"SELECT id FROM {table} WHERE {predicate} ORDER BY id"
        review.query(sql, rows=[[str(i)] for i in ids], oids=[23])
        plan(sql, candidates)

    def wire_bounds(name, sql, inputs):
        statement = name + b"\0"
        parse = statement + sql.encode() + b"\0" + struct.pack("!HII", 2, 0, 0)
        sock.sendall(packet(b"P", parse) + packet(b"D", b"S" + statement) + packet(b"S"))
        frames = client.read_until_ready(sock)
        param_description = struct.pack("!HII", 2, 26, 26)
        shape = b"".join(kind for kind, _ in frames)
        valid = shape == b"1tTZ" and frames[1] == (b"t", param_description)
        review.record("infer OID bounds", valid, frames, param_description)
        if not valid:
            for lower, upper, expected in inputs:
                review.record(("binary bounds", lower, upper), False, "OID parameter inference failed", expected)
            return
        for lower, upper, expected in inputs:
            bind = b"\0" + statement + struct.pack("!HHH", 1, 1, 2)
            for value in (lower, upper):
                bind += struct.pack("!i", -1 if value is None else 4)
                if value is not None:
                    bind += struct.pack("!I", value)
            bind += struct.pack("!HH", 1, 1)
            sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0") +
                         packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
            frames = client.read_until_ready(sock)
            data = client.data_row_values(frames)
            fields = client.row_description_fields(frames) if any(kind == b"T" for kind, _ in frames) else []
            shape = b"2T" + b"D" * len(expected) + b"CZ"
            wanted = [[struct.pack("!I", oid)] for oid in expected]
            valid = (b"".join(kind for kind, _ in frames) == shape and data == wanted and len(fields) == 1
                     and fields[0][3:] == (26, 4, -1, 1)
                     and frames[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")])
            review.record(("binary bounds", lower, upper), valid, (data, fields, frames[-2:]), wanted)

    try:
        command("CREATE TABLE oid_range_primary(k oid PRIMARY KEY,id integer,note text)", "CREATE TABLE")
        keys = [0, 8, 9, 10, 2147483647, 2147483648, 2147483649, 4294967294, 4294967295]
        values = ",".join(f"({key},{i},'seed')" for i, key in enumerate(keys))
        command("INSERT INTO oid_range_primary VALUES " + values, "INSERT 0 9")
        select("k >= '0xffffffff'", [8], candidates=1)
        select("k < '0'", [], candidates=0)
        select("k > '4294967295'", [], candidates=0)
        select("k > '2147483647' AND k < '2147483649'", [5], candidates=1)
        select("'2147483647' < k AND '2147483649' > k", [5], candidates=1)
        select("k > CAST('0x7fffffff' AS pg_catalog.oid) AND k < 2147483649::oid", [5], candidates=1)
        select("k >= -1", [8], candidates=1)
        select("k > '0x7fffffff' AND k <= '2147483647'", [], candidates=0)
        select("k BETWEEN '0x80000000' AND '0x80000000'", [5], candidates=1)
        select("k BETWEEN SYMMETRIC '2147483649' AND '0x80000000'", [5, 6])
        select("k > '010' AND k < '10'", [2], candidates=1)
        review.query("SELECT id FROM oid_range_primary WHERE k > 'not an oid'", state="22P02")
        review.query("SELECT id FROM oid_range_primary WHERE k <= '4294967296'", state="22003")
        review.query("SELECT id FROM oid_range_primary WHERE k >= CAST(-1 AS bigint)", state="22003")
        review.query("SELECT id FROM oid_range_primary WHERE k >= '10'::text", state="42883")
        review.query("SELECT id FROM oid_range_primary WHERE k > NULL::oid", rows=[], oids=[23])

        query = "SELECT k FROM oid_range_primary WHERE k BETWEEN $1 AND $2 ORDER BY k"
        wire_bounds(b"between_bounds", query, [(2147483648, 2147483649, [2147483648, 2147483649]),
                                              (4294967294, 4294967295, [4294967294, 4294967295]),
                                              (4294967295, 0, []), (None, 4294967295, []), (0, 0, [0])])
        wire_bounds(b"ordinary_bounds", "SELECT k FROM oid_range_primary WHERE $1 <= k AND k < $2 ORDER BY k",
                    [(2147483647, 2147483649, [2147483647, 2147483648]), (4294967294, 4294967295, [4294967294])])
        plan(query, 1, bounds=(2147483648, 2147483648))
        plan("SELECT k FROM oid_range_primary WHERE $1 < k AND k < $2 ORDER BY k", 1,
             bounds=(2147483647, 2147483649))
        review.parameter(query, ["010", "0xA"], [0, 0], [["8"], ["9"], ["10"]], [26])
        for value, state in ((b"4294967296", b"22003"), (b"bad", b"22P02")):
            bind = (b"\0between_bounds\0" + struct.pack("!HHi", 0, 2, len(value)) + value
                    + struct.pack("!i", 10) + b"4294967295" + struct.pack("!H", 0))
            sock.sendall(packet(b"B", bind) + packet(b"S"))
            frames = client.read_until_ready(sock)
            valid = (b"".join(kind for kind, _ in frames) == b"EZ"
                     and client.diagnostic_fields(frames[0][1]).get(b"C") == state
                     and frames[1] == (b"Z", b"I"))
            review.record(("invalid text bound", value), valid, frames, state)

        command("CREATE TABLE oid_range_log(id integer PRIMARY KEY,k oid,note text)", "CREATE TABLE")
        first = 2147483500
        for begin in range(0, 640, 128):
            values = ",".join(f"({i},{first + i},'event')" for i in range(begin, begin + 128))
            command("INSERT INTO oid_range_log VALUES " + values, "INSERT 0 128")
        command("INSERT INTO oid_range_log VALUES (640,2147483651,'duplicate'),(641,NULL,'missing')", "INSERT 0 2")
        command("CREATE INDEX oid_range_partial ON oid_range_log(k) WHERE id=640", "CREATE INDEX")
        interval = "k >= '2147483650' AND k < '2147483653'"
        select(interval, [150, 151, 152, 640], table="oid_range_log")
        command("DROP INDEX oid_range_partial", "DROP INDEX")
        command("CREATE INDEX oid_range_full ON oid_range_log(k)", "CREATE INDEX")
        select(interval, [150, 151, 152, 640], table="oid_range_log", candidates=4)
        select("k > '2147483650' AND k < '2147483652'", [151, 640], table="oid_range_log", candidates=2)
        select(interval + " AND note='duplicate'", [640], table="oid_range_log")
        select("k >= '0'", list(range(641)), table="oid_range_log")
        select("k < '2147483501' OR k > '2147484138'", [0, 639], table="oid_range_log")
        command("REINDEX TABLE oid_range_log", "REINDEX")
        select(interval, [150, 151, 152, 640], table="oid_range_log", candidates=4)
        command("CREATE ROLE oid_range_reader", "CREATE ROLE")
        command("GRANT SELECT ON oid_range_log TO oid_range_reader", "GRANT")
        command("CREATE POLICY visible_oids ON oid_range_log FOR SELECT TO oid_range_reader USING (id=640)", "CREATE POLICY")
        command("ALTER TABLE oid_range_log ENABLE ROW LEVEL SECURITY", "ALTER TABLE")
        command("SET ROLE oid_range_reader", "SET", superuser=b"off")
        select(interval, [640], table="oid_range_log")
        command("RESET ROLE", "RESET", superuser=b"on")
        command("ALTER TABLE oid_range_log DISABLE ROW LEVEL SECURITY", "ALTER TABLE")
        command("DROP INDEX oid_range_full", "DROP INDEX")
        select(interval, [150, 151, 152, 640], table="oid_range_log")

        assert plan_checks == (22 if review.server else 0), plan_checks
        review.complete(58 + plan_checks)
    finally:
        review.close()


if __name__ == "__main__":
    main()
