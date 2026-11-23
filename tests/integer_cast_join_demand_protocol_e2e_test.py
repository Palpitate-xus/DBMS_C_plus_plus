#!/usr/bin/env python3
"""Verify that joining converted integer keys preserves row demand and SQL results."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("INTEGER_CAST_JOIN_DEMAND")
    client, sock = review.client, review.sock

    def command(sql, tag, ready=b"I"):
        reply = client.simple_query(sock, sql)
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", ready)]
        review.record(sql, reply == expected, reply, expected)

    def ids(suffix, rows, source):
        review.query(f"SELECT l.id,r.id FROM {source} {suffix}", rows=rows, oids=[23, 23])

    def plan(sql, visits):
        result = review.runner.decode_wire_result(client.simple_query(
            sock, "EXPLAIN (FORMAT JSON, ANALYZE TRUE, TIMING FALSE) " + sql), include_types=True)
        if result[1] or not result[0]:
            review.record("EXPLAIN " + sql, False, result, visits)
            return

        def joins(node):
            if node["nodeType"] == "MaterializedNestedLoopJoin":
                yield node
            for child in node.get("children", []):
                yield from joins(child)

        document = json.loads("\n".join(row[0] for row in result[0]))
        nodes = list(joins(document["plan"]))
        observed = [(join.get("integerKeyLookup"), join.get("integerKeyLookupBuilt"),
                     [child.get("actualRows", 0) for child in join["children"]]) for join in nodes]
        expected = [("true", None, visits)]
        review.record("EXPLAIN " + sql, observed == expected, observed, expected)

    try:
        for side in ("left", "right", "late"):
            command(f"CREATE TABLE cast_join_{side}(id integer, k bigint)", "CREATE TABLE")
        command("INSERT INTO cast_join_left VALUES (1,0),(2,2147483648),(3,4294967295),(4,NULL),(5,17)",
                "INSERT 0 5")
        command("INSERT INTO cast_join_right VALUES (10,0),(11,2147483648),(12,2147483648),"
                "(13,4294967295),(14,NULL),(15,23)", "INSERT 0 6")
        command("INSERT INTO cast_join_late VALUES (21,0),(22,4294967296)", "INSERT 0 2")
        left = "(SELECT id,k::oid AS key FROM cast_join_left) l"
        right = "(SELECT id,CAST(k AS pg_catalog.oid) AS key FROM cast_join_right) r"
        source = left + " JOIN " + right + " ON l.key=r.key"
        order = "ORDER BY l.id,r.id"
        matched = [["1", "10"], ["2", "11"], ["2", "12"], ["3", "13"]]
        ids(order, matched, source)
        ids("LIMIT 0", [], source)
        ids("WHERE l.id=3 LIMIT 1", [["3", "13"]], source)
        ids(order + " LIMIT 2 OFFSET 1", [["2", "11"], ["2", "12"]], source)
        ids("ORDER BY l.id DESC,r.id DESC FETCH FIRST ROW ONLY", [["3", "13"]], source)
        ids("WHERE r.id=404 LIMIT 1", [], source)
        ids("ORDER BY l.id NULLS LAST,r.id NULLS LAST",
            matched + [["4", None], ["5", None], [None, "14"], [None, "15"]],
            left + " FULL JOIN " + right + " ON r.key=l.key")
        ids(order + " LIMIT 2", [["2", "11"], ["2", "12"]],
            left.replace("cast_join_left)", "cast_join_left WHERE id>=2)")
            + " JOIN " + right + " ON l.key=r.key")
        native_right = "(SELECT id,id::bigint AS key FROM cast_join_left) r"
        ids(order + " LIMIT 2", [["1", "1"], ["2", "2"]],
            "(SELECT id,CAST(id AS bigint) AS key FROM cast_join_left) l JOIN " + native_right + " ON r.key=l.key")
        review.query(f"SELECT l.key,r.key FROM {source} WHERE l.id=3 LIMIT 1",
                     rows=[["4294967295", "4294967295"]], oids=[26, 26])

        late = ("(SELECT id,k::oid AS key FROM cast_join_late) l JOIN "
                "(SELECT id,k::oid AS key FROM cast_join_right WHERE id=10) r ON l.key=r.key")
        ids("LIMIT 0", [], late)
        ids("LIMIT 1", [["21", "10"]], late)
        review.query("SELECT l.id,r.id FROM " + late + " ORDER BY l.id,r.id", state="22003")
        review.query("SELECT CAST(CAST(-1 AS integer) AS pg_catalog.oid),"
                     " CAST(CAST(-1 AS smallint) AS oid)",
                     rows=[["4294967295", "4294967295"]], oids=[26, 26])
        review.query("SELECT CAST(CAST(4294967295::bigint AS oid) AS pg_catalog.oid)",
                     rows=[["4294967295"]], oids=[26])
        review.query("SELECT CAST(NULL::bigint AS pg_catalog.oid)", rows=[[None]], oids=[26])
        review.query("SELECT CAST(CAST(-1 AS bigint) AS pg_catalog.oid)", state="22003")
        review.query("SELECT CAST(4294967296::bigint AS pg_catalog.oid)", state="22003")
        review.query("SELECT 42", rows=[["42"]], oids=[23])
        command("BEGIN READ ONLY", "BEGIN", b"T")
        ids("WHERE l.id=3 LIMIT 1", [["3", "13"]], source)
        command("ROLLBACK", "ROLLBACK")

        origins = []
        for name in ("cast_join_left", "cast_join_right"):
            catalog = client.simple_query(sock, f"SELECT oid FROM pg_catalog.pg_class WHERE relname='{name}'")
            matches = client.data_row_values(catalog)
            origins.append(int(matches[0][0]) if len(matches) == 1 else 0)
        sql = f"SELECT l.id AS lhs,r.id AS rhs FROM {source} {order} LIMIT 2"
        packet = client.typed
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0\0" + struct.pack("!HHHH", 0, 0, 1, 1)
        sock.sendall(packet(b"P", parse) + packet(b"D", b"S\0") + packet(b"S"))
        declaration = client.read_until_ready(sock)
        declared = (client.row_description_fields(declaration)
                    if any(kind == b"T" for kind, _ in declaration) else [])
        sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0")
                     + packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
        frames = client.read_until_ready(sock)
        fields = client.row_description_fields(frames) if any(kind == b"T" for kind, _ in frames) else []
        rows = client.data_row_values(frames)
        expected_rows = [[struct.pack("!i", int(value)) for value in pair] for pair in matched[:2]]
        format_types = [(name, oid, 1, 23, 4, -1, 1) for name, oid in zip((b"lhs", b"rhs"), origins)]
        kinds = b"".join(kind for kind, _ in frames)
        valid = (all(origins) and b"".join(kind for kind, _ in declaration) == b"1tTZ"
                 and declaration[1] == (b"t", b"\0\0") and declaration[-1] == (b"Z", b"I")
                 and declared == [(*field[:-1], 0) for field in format_types]
                 and kinds == b"2TDDCZ" and rows == expected_rows and fields == format_types
                 and frames[-2:] == [(b"C", b"SELECT 2\0"), (b"Z", b"I")])
        review.record("binary limited join", valid, (declared, kinds, rows, fields), (expected_rows, format_types))

        if review.server:
            plan("SELECT l.id,r.id FROM " + source + " LIMIT 0", [0, 0])
            plan("SELECT l.id,r.id FROM " + source + " LIMIT 1", [1, 1])
            plan("SELECT l.id,r.id FROM " + late + " LIMIT 1", [1, 1])
        review.complete(32 if review.server else 29)
    finally:
        review.close()


if __name__ == "__main__":
    main()
