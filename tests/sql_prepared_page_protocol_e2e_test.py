#!/usr/bin/env python3
"""SQL PREPARE can reuse typed row counts, range bounds, and ordered pages."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("SQL_PREPARED_PAGE")
    client, sock = review.client, review.sock
    packet = client.typed

    def command(sql, tag, ready=b"I"):
        frames = client.simple_query(sock, sql)
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", ready)]
        review.record(sql, frames == expected, frames, expected)

    def rows(sql, expected, field):
        frames = client.simple_query(sock, sql)
        kinds = b"".join(kind for kind, _ in frames)
        description = client.row_description_fields(frames) if b"T" in kinds else []
        values = [[None if value is None else str(value).encode()] for value in expected]
        valid = (kinds == b"T" + b"D" * len(values) + b"CZ" and description == [field]
                 and client.data_row_values(frames) == values
                 and frames[-2:] == [(b"C", f"SELECT {len(values)}\0".encode()), (b"Z", b"I")])
        review.record(sql, valid, (description, frames), (field, values))

    def relation(name):
        reply = client.simple_query(sock, f"SELECT oid FROM pg_catalog.pg_class WHERE relname='{name}'")
        return int(client.data_row_values(reply)[0][0])

    def explain(invocation, fetched, *, analyze=True):
        sql = "EXPLAIN (FORMAT JSON" + (", ANALYZE TRUE, TIMING FALSE" if analyze else "") + ") " + invocation
        frames = client.simple_query(sock, sql)
        kinds = b"".join(kind for kind, _ in frames)
        valid = kinds == b"TDCZ"
        summary = []
        if valid:
            document = json.loads(client.data_row_values(frames)[0][0])
            valid = (client.row_description_fields(frames) == [(b"QUERY PLAN", 0, 0, 114, -1, -1, 0)]
                     and frames[-2:] == [(b"C", b"EXPLAIN\0"), (b"Z", b"I")])
            if review.server is None:
                summary = [document[0]["Plan"]["Node Type"]]
            else:
                pending = [document["plan"]]
                while pending:
                    node = pending.pop()
                    summary.append((node["nodeType"], node.get("actualRows"), node.get("heapRowsFetched")))
                    pending.extend(node["children"])
                if fetched:
                    valid = valid and ("OidIndexPage", fetched, str(fetched)) in summary
                    valid = valid and all(node != "TableScan" for node, _, _ in summary)
                elif fetched == 0:
                    valid = valid and all(actual in (None, 0) for _, actual, _ in summary)
        review.record(sql, valid, (kinds, summary), fetched)

    try:
        command("CREATE TABLE sql_page(k oid PRIMARY KEY, id integer, note text)", "CREATE TABLE")
        command("INSERT INTO sql_page VALUES (0,1,'zero'),(2147483648,2,'high'),(4294967295,3,'max')",
                "INSERT 0 3")
        oid = relation("sql_page")
        object_field = (b"object_number", oid, 1, 26, 4, -1, 0)
        page_query = "SELECT k AS object_number FROM sql_page ORDER BY k LIMIT ($1) OFFSET $2"
        command("PREPARE page AS " + page_query, "PREPARE")
        for args, expected in (
            ("2,1", [2147483648, 4294967295]),
            ("1,2", [4294967295]),
            ("NULL,1", [2147483648, 4294967295]),
            ("2,NULL", [0, 2147483648]),
            ("0,9223372036854775807", []),
            ("9223372036854775807,9223372036854775807", []),
        ):
            rows(f"EXECUTE page({args})", expected, object_field)
        review.query("EXECUTE page(-1,0)", state="2201W")
        review.query("EXECUTE page(-1,-1)", state="2201X")
        review.query("EXECUTE page(1)", state="42601")
        review.query("EXECUTE page('bad',0)", state="22P02")
        review.query("EXECUTE page('9223372036854775808',0)", state="22003")
        review.query("PREPARE rejected(text) AS SELECT k FROM sql_page LIMIT $1", state="42804")
        command("PREPARE rejected(integer) AS SELECT k AS object_number FROM sql_page ORDER BY k LIMIT $1",
                "PREPARE")
        rows("EXECUTE rejected(1)", [0], object_field)
        command("DEALLOCATE rejected", "DEALLOCATE")
        review.query("EXECUTE rejected(1)", state="26000")

        filtered = "SELECT k AS object_number FROM sql_page WHERE k >= $1 ORDER BY 1"
        command("PREPARE bounded AS " + filtered + " OFFSET $2 ROWS FETCH NEXT ($3) ROWS WITH TIES", "PREPARE")
        rows("EXECUTE bounded('2147483648',0,1)", [2147483648], object_field)
        rows("EXECUTE bounded('2147483648',0,NULL)", [2147483648, 4294967295], object_field)
        rows("EXECUTE bounded(NULL,0,2)", [], object_field)
        command("PREPARE interval_page AS SELECT k AS object_number FROM sql_page "
                "WHERE k BETWEEN $1 AND $2 ORDER BY k DESC LIMIT $3", "PREPARE")
        rows("EXECUTE interval_page('2147483648','4294967295',1)", [4294967295], object_field)
        command("PREPARE decimal_page(numeric) AS " + page_query.replace("OFFSET $2", ""), "PREPARE")
        rows("EXECUTE decimal_page(1.5)", [0, 2147483648], object_field)
        review.query("EXECUTE decimal_page(-0.5)", state="2201W")

        explain("EXECUTE page(1,1)", 2)
        explain("EXECUTE page(0,1)", 0)
        explain("EXECUTE bounded('2147483648',0,1)", 2)
        explain("EXECUTE interval_page('2147483648','4294967295',1)", 1)
        explain("EXECUTE page(-1,0)", None, analyze=False)
        review.query("EXPLAIN (ANALYZE) EXECUTE page(-1,0)", state="2201W")
        review.query("EXPLAIN EXECUTE absent_page(1)", state="26000")

        stmt = b"bounded\0"
        request = packet(b"D", b"S" + stmt) + packet(b"S")
        sock.sendall(request)
        frames = client.read_until_ready(sock)
        types = struct.pack("!HIII", 3, 26, 20, 20)
        valid = (b"".join(k for k, _ in frames) == b"tTZ" and frames[0] == (b"t", types)
                 and client.row_description_fields(frames) == [object_field] and frames[-1] == (b"Z", b"I"))
        review.record("Describe SQL statement over the wire", valid, frames, types)
        inputs = (struct.pack("!I", 2147483648), struct.pack("!q", 0), struct.pack("!q", 1))
        bind = b"\0" + stmt + struct.pack("!HHH", 1, 1, len(inputs))
        for value in inputs:
            bind += struct.pack("!i", len(value)) + value
        bind += struct.pack("!HH", 1, 1)
        sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0") +
                     packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
        frames = client.read_until_ready(sock)
        kinds = b"".join(k for k, _ in frames)
        valid = (kinds == b"2TDCZ" and client.row_description_fields(frames) == [(*object_field[:-1], 1)]
                 and client.data_row_values(frames) == [[struct.pack("!I", 2147483648)]]
                 and frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")])
        review.record("Bind SQL statement over the wire", valid, frames, b"2TDCZ")
        name = b"from_wire\0"
        sock.sendall(packet(b"P", name + page_query.encode() + b"\0" + struct.pack("!H", 0)) + packet(b"S"))
        frames = client.read_until_ready(sock)
        review.record("Parse wire statement", frames == [(b"1", b""), (b"Z", b"I")], frames, b"1Z")
        rows("EXECUTE from_wire(1,1)", [2147483648], object_field)

        command("CREATE TABLE sql_peers(id integer PRIMARY KEY,k oid NOT NULL)", "CREATE TABLE")
        command("INSERT INTO sql_peers VALUES (1,0),(2,2147483648),(3,2147483648),"
                "(4,2147483648),(5,4294967295)", "INSERT 0 5")
        command("CREATE INDEX sql_peers_key ON sql_peers(k)", "CREATE INDEX")
        peer_field = (b"k", relation("sql_peers"), 2, 26, 4, -1, 0)
        command("PREPARE peers AS SELECT k FROM sql_peers ORDER BY k OFFSET $1 ROWS FETCH NEXT $2 ROWS WITH TIES",
                "PREPARE")
        rows("EXECUTE peers(1,1)", [2147483648] * 3, peer_field)
        explain("EXECUTE peers(1,1)", 5)
        rows("EXECUTE peers(2,NULL)", [2147483648, 2147483648, 4294967295], peer_field)

        command("CREATE SEQUENCE sql_page_seq", "CREATE SEQUENCE")
        counter = (b"page_size", 0, 0, 20, 8, -1, 0)
        command("PREPARE page_effect(bigint) AS SELECT $1 AS page_size FROM sql_page ORDER BY k LIMIT $1",
                "PREPARE")
        rows("EXECUTE page_effect(nextval('sql_page_seq'))", [1], counter)
        rows("EXECUTE page_effect(nextval('sql_page_seq'))", [2, 2], counter)
        review.query("SELECT currval('sql_page_seq')", [["2"]], [20])
        command("PREPARE zero_row AS SELECT nextval('sql_page_seq') FROM sql_page ORDER BY k LIMIT $1",
                "PREPARE")
        rows("EXECUTE zero_row(0)", [], (b"nextval", 0, 0, 20, 8, -1, 0))
        review.query("SELECT currval('sql_page_seq')", [["2"]], [20])
        explain("EXECUTE zero_row(nextval('sql_page_seq'))", None, analyze=False)
        review.query("SELECT currval('sql_page_seq')", [["3"]], [20])
        if review.server is not None:
            review.query("EXPLAIN EXECUTE page USING 1,0", state="42601")
        review.complete(57 if review.server is not None else 56)
    finally:
        review.close()


if __name__ == "__main__":
    main()
