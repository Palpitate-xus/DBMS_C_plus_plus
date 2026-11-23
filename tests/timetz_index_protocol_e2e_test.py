#!/usr/bin/env python3
"""TIMETZ equality uses complete indexes and preserves SQL comparisons."""

import json
import struct

from compat.numeric_type_review import Review


TIMETZ = 1266


def main():
    review = Review("TIMETZ_INDEX")
    client, sock = review.client, review.sock
    reference = review.server is None
    name = b"zoned_point\0"
    plans = 0

    def command(sql, tag):
        messages = client.simple_query(sock, sql)
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record(sql, messages == expected, messages, expected)

    def rows(query, values):
        review.query(query, rows=[[str(value)] for value in values], oids=[23])

    def nodes(plan):
        yield plan
        for child in plan["children"]:
            yield from nodes(child)

    def plan(query, indexed):
        nonlocal plans
        if reference:
            return
        plans += 1
        sql = "EXPLAIN (ANALYZE, FORMAT JSON) " + query
        reply = review.runner.decode_wire_result(client.simple_query(sock, sql))
        actual = list(nodes(json.loads(reply[0][0][0])["plan"])) if not reply[1] else []
        index = [node for node in actual if node["nodeType"] == "IndexScan"]
        marked = [node for node in actual if node["nodeType"].startswith("TypedEqualityScan")]
        good = bool(index) == indexed and (not indexed or (
            len(marked) == 1 and marked[0].get("usedIndex") == "true" and index[0]["column"] == "t"))
        review.record(sql, bool(actual) and good, [node["nodeType"] for node in actual], indexed)

    def describe(query, statement):
        parse = client.typed(b"P", statement + query.encode() + b"\0" + struct.pack("!HI", 1, TIMETZ))
        sock.sendall(parse + client.typed(b"D", b"S" + statement) + client.typed(b"S", b""))
        messages = client.read_until_ready(sock)
        actual = (b"".join(kind for kind, _ in messages), [body for kind, body in messages if kind == b"t"])
        expected = (b"1tTZ", [struct.pack("!HI", 1, TIMETZ)])
        review.record("describe " + query, actual == expected, actual, expected)

    def bind(raw, input_format, result, statement=name):
        body = b"\0" + statement + struct.pack("!HHHi", 1, input_format, 1, -1 if raw is None else len(raw))
        body += (raw or b"") + struct.pack("!HH", 1, 1)
        request = client.typed(b"B", body) + client.typed(b"D", b"P\0")
        request += client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S", b"")
        sock.sendall(request)
        messages = client.read_until_ready(sock)
        expected = (b"2T" + b"D" * len(result) + b"CZ",
                    [[struct.pack("!i", number)] for number in result],
                    [(b"v", 23, 1)], [(b"C", f"SELECT {len(result)}\0".encode()), (b"Z", b"I")])
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        actual = (b"".join(kind for kind, _ in messages), client.data_row_values(messages),
                  [(field[0], field[3], field[-1]) for field in fields], messages[-2:])
        review.record(("bind", statement, raw), actual == expected, actual, expected)

    try:
        command("CREATE TABLE clock_pk(t timetz(6) PRIMARY KEY,id integer,note text)", "CREATE TABLE")
        command("INSERT INTO clock_pk VALUES "
                "('10:00:00.124001+02',1,'first'),('09:00:00.124001+01',2,'tied in UTC'),"
                "('10:00:00.124002+02',3,'next microsecond'),('23:59:59.999999-15:59:59',4,NULL)",
                "INSERT 0 4")
        point = "SELECT id+0 AS v FROM clock_pk WHERE t='10:00:00.124001+0200'"
        rows(point, [1])
        plan(point, True)
        reverse = "SELECT id+0 AS v FROM clock_pk WHERE '09:00:00.124001+01'=t"
        rows(reverse, [2])
        plan(reverse, True)
        cast = "SELECT id+0 AS v FROM clock_pk WHERE t=CAST('10:00:00.1240014+02' AS pg_catalog.timetz(6))"
        rows(cast, [1])
        plan(cast, True)
        miss = "SELECT id+0 AS v FROM clock_pk WHERE t='10:00:00.124003+02'"
        rows(miss, [])
        plan(miss, True)
        rows("SELECT id AS v FROM clock_pk WHERE t='10:00:00.124001+0200'", [1])
        rows("SELECT id AS v FROM clock_pk WHERE t='10:00:00.124003+02'", [])
        review.query("SELECT id AS v FROM clock_pk WHERE t='12:00:00+16'", state="22009")
        rows("SELECT id+0 AS v FROM clock_pk WHERE t=NULL::timetz", [])
        review.query("SELECT id+0 AS v FROM clock_pk WHERE t='bad+05'", state="22007")
        review.query("SELECT id+0 AS v FROM clock_pk WHERE t='12:00:00+16'", state="22009")

        describe("SELECT id+0 AS v FROM clock_pk WHERE t=$1", name)
        bind(b"10:00:00.124002+02", 0, [3])
        bind(struct.pack("!qi", 32_400_124_001, -3_600), 1, [2])
        bind(struct.pack("!qi", 32_400_124_001, 3_600), 1, [])
        bind(None, 1, [])

        command("CREATE TABLE clock_log(id integer PRIMARY KEY,t timetz(3),payload text)", "CREATE TABLE")
        command("INSERT INTO clock_log VALUES (1,'00:00:00.123456-00:00:07','first'),"
                "(2,'00:00:00.123499-00:00:07','second'),(3,NULL,'null'),"
                "(4,'00:00:00.123456+00:00:07','other offset')", "INSERT 0 4")
        command("CREATE INDEX clock_part ON clock_log(t) WHERE id=1", "CREATE INDEX")
        match = "SELECT id+0 AS v FROM clock_log WHERE t='00:00:00.123000-00:00:07' ORDER BY id"
        rows(match, [1, 2])
        plan(match, False)
        command("DROP INDEX clock_part", "DROP INDEX")
        command("CREATE INDEX clock_all ON clock_log(t)", "CREATE INDEX")
        rows(match, [1, 2])
        plan(match, True)
        secondary_name = b"zoned_secondary\0"
        describe("SELECT id+0 AS v FROM clock_log WHERE t=$1 ORDER BY id", secondary_name)
        bind(b"00:00:00.123-00:00:07", 0, [1, 2], secondary_name)
        bind(struct.pack("!qi", 123_000, 7), 1, [1, 2], secondary_name)
        unrounded = "SELECT id+0 AS v FROM clock_log WHERE t='00:00:00.1234-00:00:07'"
        rows(unrounded, [])
        plan(unrounded, True)
        typed = "SELECT id+0 AS v FROM clock_log WHERE t=CAST('00:00:00.1234-00:00:07' AS timetz(3)) ORDER BY id"
        rows(typed, [1, 2])
        plan(typed, True)
        command("REINDEX TABLE clock_log", "REINDEX")
        rows(match, [1, 2])
        plan(match, True)
        command("DROP INDEX clock_all", "DROP INDEX")
        rows(match, [1, 2])
        plan(match, False)
        rows(typed, [1, 2])
        assert plans == (0 if reference else 10), plans
        review.complete(34 + plans)
    finally:
        review.close()


if __name__ == "__main__":
    main()
