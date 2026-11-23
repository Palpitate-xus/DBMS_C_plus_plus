#!/usr/bin/env python3
"""Compare OID joins with PostgreSQL and measure visits to the right input."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("OID_EQUALITY_JOIN")
    client, sock = review.client, review.sock

    def command(sql, tag):
        frames = [(kind, data) for kind, data in client.simple_query(sock, sql) if kind != b"N"]
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record(sql, frames == expected, frames, expected)

    def plan(sql, mode, visits):
        result = review.runner.decode_wire_result(client.simple_query(
            sock, "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql), include_types=True)

        def joins(node):
            if node["nodeType"] == "MaterializedNestedLoopJoin":
                yield node
            for child in node.get("children", []):
                yield from joins(child)

        if result[1] is not None or not result[0]:
            review.record("EXPLAIN " + sql, False, result, (mode, visits))
            return
        nodes = list(joins(json.loads("\n".join(row[0] for row in result[0]))["plan"]))
        modes = [node.get("integerKeyLookupMode") for node in nodes]
        rows = [node["children"][1].get("actualRows", 0) for node in nodes]
        valid = (len(nodes) == 1 and modes == [mode]
                 and nodes[0].get("integerKeyLookup") == "true"
                 and rows == [visits])
        review.record("EXPLAIN " + sql, valid, (modes, rows), (mode, visits))

    try:
        for side in ("left", "right"):
            command(f"CREATE TABLE oid_join_{side}(id integer, k bigint)", "CREATE TABLE")
        command("INSERT INTO oid_join_left VALUES "
                "(1,0),(2,2147483648),(3,4294967295),(4,NULL),(5,2147483647)", "INSERT 0 5")
        command("INSERT INTO oid_join_right VALUES "
                "(10,4294967295),(11,2147483648),(12,2147483648),(13,0),(14,NULL),(15,2147483646)",
                "INSERT 0 6")
        left = "(SELECT id,k::oid AS k FROM oid_join_left) l"
        right = "(SELECT id,k::oid AS k FROM oid_join_right) r"
        sources = left + " {kind} JOIN " + right + " ON {equality}"
        matched = [["1", "13"], ["2", "11"], ["2", "12"], ["3", "10"]]
        left_only = [["4", None], ["5", None]]
        right_only = [[None, "14"], [None, "15"]]
        for kind, extra in (("INNER", []), ("LEFT", left_only), ("RIGHT", right_only),
                            ("FULL", left_only + right_only)):
            for equality in ("l.k=r.k", "r.k=l.k"):
                joined = sources.format(kind=kind, equality=equality)
                review.query(f"SELECT l.id,r.id FROM {joined} ORDER BY l.id NULLS LAST,r.id NULLS LAST",
                             rows=matched + extra, oids=[23, 23])
        joined = sources.format(kind="INNER", equality="l.k=r.k")
        review.query(f"SELECT l.k,r.k FROM {joined} ORDER BY l.id,r.id", rows=[
            ["0", "0"], ["2147483648", "2147483648"], ["2147483648", "2147483648"],
            ["4294967295", "4294967295"]], oids=[26, 26])
        review.query("SELECT l.id,r.id FROM "
                     + sources.format(kind="INNER", equality="l.k IS NOT DISTINCT FROM r.k")
                     + " ORDER BY l.id,r.id", rows=matched + [["4", "14"]], oids=[23, 23])
        review.query(f"SELECT count(*) FROM {joined} WHERE r.k=4294967295::oid", rows=[["1"]], oids=[20])
        review.query(f"SELECT l.id FROM {joined} ORDER BY l.id,r.id LIMIT 0", rows=[], oids=[23])
        review.query(f"SELECT l.id FROM {joined} ORDER BY l.id,r.id LIMIT 1", rows=[["1"]], oids=[23])

        sql = f"SELECT r.k FROM (SELECT $1 AS k) l JOIN {right} ON l.k=r.k ORDER BY r.id"
        packet = client.typed
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 26)
        sock.sendall(packet(b"P", parse) + packet(b"S"))
        frames = client.read_until_ready(sock)
        review.record("parse OID join", frames == [(b"1", b""), (b"Z", b"I")], frames, "ParseComplete")
        for value, count in ((2147483648, 2), (4294967295, 1), (2147483647, 0), (0, 1)):
            payload = struct.pack("!I", value)
            bind = b"\0\0" + struct.pack("!HHHi", 1, 1, 1, 4) + payload + struct.pack("!HH", 1, 1)
            sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0")
                         + packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
            frames = client.read_until_ready(sock)
            rows = client.data_row_values(frames)
            fields = client.row_description_fields(frames) if any(kind == b"T" for kind, _ in frames) else []
            valid = (rows == [[payload]] * count and len(fields) == 1 and fields[0][3:] == (26, 4, -1, 1)
                     and b"".join(kind for kind, _ in frames) == b"2T" + b"D" * count + b"CZ"
                     and frames[-2:] == [(b"C", f"SELECT {count}\0".encode()), (b"Z", b"I")])
            review.record(f"binary join for {value}", valid, (rows, fields), [[payload]] * count)

        command("CREATE TABLE oid_join_dense(k bigint)", "CREATE TABLE")
        command("INSERT INTO oid_join_dense VALUES (2147483646),(2147483647),(2147483648),(2147483649)",
                "INSERT 0 4")
        dense = " FROM " + left + " JOIN (SELECT k::oid AS k FROM oid_join_dense) r ON l.k=r.k"
        review.query("SELECT l.k" + dense + " ORDER BY l.id", rows=[["2147483648"], ["2147483647"]], oids=[26])
        command("CREATE TABLE oid_join_parent(id integer)", "CREATE TABLE")
        for number in range(8):
            command(f"CREATE TABLE oid_join_child_{number}() INHERITS(oid_join_parent)", "CREATE TABLE")
        catalog_join = ("SELECT count(*) FROM pg_catalog.pg_inherits i "
                        "JOIN pg_catalog.pg_class c ON c.oid=i.inhparent "
                        "WHERE c.relname='oid_join_parent'")
        review.query(catalog_join, rows=[["8"]], oids=[20])

        if review.server:
            plan("SELECT count(*) FROM " + joined, "sorted", 9)
            plan("SELECT count(*)" + dense, "direct", 6)
            plan("SELECT l.id AS selected FROM " + joined + " LIMIT 0", None, 0)
            plan("SELECT l.id AS selected FROM " + joined + " LIMIT 1", None, 4)
            count = client.data_row_values(client.simple_query(sock, "SELECT count(*) FROM pg_catalog.pg_class"))
            plan(catalog_join, "sorted", int(count[0][0]) + 7)
        review.complete(40 if review.server else 35)
    finally:
        review.close()


if __name__ == "__main__":
    main()
