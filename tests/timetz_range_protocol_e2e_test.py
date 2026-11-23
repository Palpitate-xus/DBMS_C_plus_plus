#!/usr/bin/env python3
"""TIMETZ range predicates preserve SQL comparison and index boundaries."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("TIMETZ_RANGE")
    client, sock = review.client, review.sock
    reference = review.server is None
    plans = 0

    def command(sql, tag):
        frames = client.simple_query(sock, sql)
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record(sql, frames == expected, frames, expected)

    def rows(sql, expected):
        review.query(sql, rows=[[str(item)] for item in expected], oids=[23])

    def nodes(node):
        yield node
        for child in node["children"]:
            yield from nodes(child)

    def plan(sql, candidates):
        nonlocal plans
        if reference:
            return
        plans += 1
        label = "EXPLAIN (ANALYZE, FORMAT JSON) " + sql
        reply = review.runner.decode_wire_result(client.simple_query(sock, label))
        tree = list(nodes(json.loads(reply[0][0][0])["plan"])) if not reply[1] else []
        scans = [node for node in tree if node["nodeType"] == "ZonedClockRangeScan"]
        if candidates is None:
            valid = any(node["nodeType"] == "TableScan" for node in tree)
            valid = valid and all(node.get("usedIndex") == "false" for node in scans)
        else:
            valid = len(scans) == 1 and scans[0].get("usedIndex") == "true"
            valid = valid and scans[0].get("actualRows") == candidates and scans[0].get("column") == "t"
            valid = valid and not any(node["nodeType"] == "TableScan" for node in tree)
        seen = [(node["nodeType"], node.get("actualRows"), node.get("usedIndex")) for node in tree]
        review.record(label, valid, seen, candidates)

    def point(predicate, expected, *, table="rc_pk", projection="id+0", candidates=None):
        sql = f"SELECT {projection} AS v FROM {table} WHERE {predicate} ORDER BY id"
        rows(sql, expected)
        plan(sql, candidates)

    def binary_bounds(sql, payloads):
        statement = b"clock_range\0"
        parse = statement + sql.encode() + b"\0" + struct.pack("!HII", 2, 1266, 1266)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S" + statement) + client.typed(b"S"))
        frames = client.read_until_ready(sock)
        parameters = (b"1tTZ", [struct.pack("!HII", 2, 1266, 1266)])
        actual = (b"".join(kind for kind, _ in frames), [data for kind, data in frames if kind == b"t"])
        review.record("describe range bounds", actual == parameters, actual, parameters)
        for lower, upper, expected in payloads:
            bind = b"\0" + statement + struct.pack("!HHH", 1, 1, 2)
            for value in (lower, upper):
                bind += struct.pack("!i", -1 if value is None else len(value)) + (value or b"")
            bind += struct.pack("!HH", 1, 1)
            sock.sendall(client.typed(b"B", bind) + client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
            frames = client.read_until_ready(sock)
            fields = client.row_description_fields(frames) if any(k == b"T" for k, _ in frames) else []
            actual = (b"".join(kind for kind, _ in frames), client.data_row_values(frames),
                      [(field[0], field[3], field[-1]) for field in fields], frames[-2:])
            expected_wire = (b"2T" + b"D" * len(expected) + b"CZ",
                             [[struct.pack("!i", item)] for item in expected], [(b"v", 23, 1)],
                             [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")])
            review.record(("bind", lower, upper), actual == expected_wire, actual, expected_wire)

    try:
        command("CREATE TABLE rc_pk(t timetz(6) PRIMARY KEY, id integer)", "CREATE TABLE")
        command("INSERT INTO rc_pk VALUES "
                "('07:59:59+00',1),('09:00:00.123001+01',2),('10:00:00.124001+02',3),"
                "('09:00:00.124002+01',4),('10:00:00.124002+02',5),"
                "('24:00:00-15:59:59',6),('00:00:00+15:59:59',7)", "INSERT 0 7")
        first = "'10:00:00.124001+02'"
        second = "'10:00:00.124002+02'"
        third = "'09:00:00.124002+01'"
        narrow = f"t > {first} AND t < {third}"
        point(narrow, [5], candidates=1)
        point(narrow, [5], projection="id", candidates=1)
        point(f"{first} < t AND {third} > t", [5], candidates=1)
        point("t > CAST('10:00:00.1240014+0200' AS timetz(6)) AND "
              "t < CAST('09:00:00.1240024+0100' AS timetz(6))", [5], candidates=1)
        point(f"t BETWEEN {second} AND {second}", [5], candidates=1)
        point(f"t BETWEEN SYMMETRIC {third} AND {first}", [3, 4, 5])
        point(f"t NOT BETWEEN {first} AND {second}", [1, 2, 4, 6, 7])
        point(f"t >= {third} AND t <= {second}", [], candidates=0)
        point("t >= '24:00:00-15:59:59'", [6], candidates=1)
        point("t > '24:00:00-15:59:59'", [], candidates=0)
        point("t <= '00:00:00+15:59:59'", [7], candidates=1)
        point("t > NULL::timetz", [])
        point("t BETWEEN '00:00:00-00:00:01' AND NULL", [])
        point("t >= '00:00:00+15:59:59'", [1, 2, 3, 4, 5, 6, 7])
        point("t >= '00:00:00+15:59:59' AND id>4", [5, 6, 7])
        review.query("SELECT id+0 AS v FROM rc_pk WHERE t < 'bad+05'", state="22007")
        review.query("SELECT id AS v FROM rc_pk WHERE t >= '08:00:00+16'", state="22009")

        query = "SELECT id+0 AS v FROM rc_pk WHERE t > $1 AND t < $2 ORDER BY id"
        begin = struct.pack("!qi", 36_000_124_001, -7200)
        end = struct.pack("!qi", 32_400_124_002, -3600)
        binary_bounds(query, [(begin, end, [5]), (end, begin, []), (begin, None, [])])
        review.parameter(query, ["10:00:00.124001+02", "09:00:00.124002+01"],
                         [1266, 1266], [["5"]], [23])

        command("CREATE TABLE rc_log(id integer PRIMARY KEY,t timetz(6))", "CREATE TABLE")
        command("INSERT INTO rc_log VALUES (1,'10:00:00.124001+02'),"
                "(2,'10:00:00.124001+02'),(3,'09:00:00.124002+01'),(4,NULL)", "INSERT 0 4")
        command("CREATE INDEX rc_partial ON rc_log(t) WHERE id=1", "CREATE INDEX")
        point(f"t > {second}", [3], table="rc_log")
        command("DROP INDEX rc_partial", "DROP INDEX")
        command("CREATE INDEX rc_all ON rc_log(t)", "CREATE INDEX")
        point(f"t > {second}", [3], table="rc_log", candidates=1)
        point(f"t BETWEEN {first} AND {second}", [1, 2], table="rc_log")
        command("REINDEX TABLE rc_log", "REINDEX")
        point(f"t > {second}", [3], table="rc_log", candidates=1)
        command("DROP INDEX rc_all", "DROP INDEX")
        point(f"t > {second}", [3], table="rc_log")
        assert plans == (0 if reference else 20), plans
        review.complete(36 + plans)
    finally:
        review.close()


if __name__ == "__main__":
    main()
