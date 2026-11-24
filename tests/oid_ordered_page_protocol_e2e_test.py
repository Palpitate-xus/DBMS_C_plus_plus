#!/usr/bin/env python3
"""Check early ordered OID pages and peer boundaries through SQL and the wire."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("OID_ORDERED_PAGE")
    client, sock = review.client, review.sock
    packet = client.typed
    plans = 0

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

    def plan(sql, fetched, parameter=None):
        nonlocal plans
        if review.server is None:
            return
        plans += 1
        label = "EXPLAIN (ANALYZE, FORMAT JSON, TIMING FALSE) " + sql
        if parameter is None:
            frames = client.simple_query(sock, label)
        else:
            parse = b"\0" + label.encode() + b"\0" + struct.pack("!HI", 1, 26)
            bind = b"\0\0" + struct.pack("!HHHiIH", 1, 1, 1, 4, parameter, 0)
            sock.sendall(packet(b"P", parse) + packet(b"B", bind) +
                         packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
            frames = client.read_until_ready(sock)
        reply = review.runner.decode_wire_result(frames)
        tree = list(nodes(json.loads(reply[0][0][0])["plan"])) if not reply[1] else []
        pages = [n for n in tree if n["nodeType"] == "OidIndexPage" and n.get("usedIndex") == "true"]
        table = any(n["nodeType"] == "TableScan" for n in tree)
        if fetched is None:
            valid = table and not pages
        else:
            valid = (not table and len(pages) == 1 and pages[0].get("column") == "k"
                     and pages[0].get("heapRowsFetched") == str(fetched) and pages[0]["actualRows"] == fetched
                     and (fetched == 0 or int(pages[0].get("indexNodesVisited", "0")) > 0))
        summary = [(n["nodeType"], n.get("actualRows"), n.get("heapRowsFetched")) for n in tree]
        review.record(label, valid, summary, fetched)

    def page(sql, expected, fetched=None, oid=23):
        review.query(sql, rows=[[None if value is None else str(value)] for value in expected], oids=[oid])
        plan(sql, fetched)

    try:
        command("CREATE TABLE oid_page_pk(k oid PRIMARY KEY,id integer)", "CREATE TABLE")
        command("INSERT INTO oid_page_pk VALUES (4294967295,9),(0,1),(10,2),(2147483648,5),"
                "(2147483647,4),(4294967294,8),(2147483649,6)", "INSERT 0 7")
        base = "SELECT id AS v FROM oid_page_pk"
        page(base + " ORDER BY k LIMIT 2", [1, 2], 2)
        page(base + " ORDER BY k DESC LIMIT 3", [9, 8, 6], 3)
        page(base + " ORDER BY k LIMIT 2 OFFSET 2", [4, 5], 4)
        page(base + " ORDER BY k ASC NULLS FIRST FETCH FIRST 2 ROWS ONLY", [1, 2], 2)
        page(base + " WHERE k >= '0' ORDER BY k LIMIT 3", [1, 2, 4], 3)
        page(base + " WHERE k >= '0x80000000' ORDER BY k DESC LIMIT 2 OFFSET 1", [8, 6], 3)
        page(base + " WHERE k > '4294967295' ORDER BY k LIMIT 3", [], 0)
        page(base + " WHERE k BETWEEN '0xffffffff' AND '0xffffffff' ORDER BY k LIMIT 2", [9], 1)
        page(base + " ORDER BY k FETCH FIRST 2 ROWS WITH TIES", [1, 2], 3)
        page(base + " ORDER BY k,id LIMIT 2", [1, 2])
        review.query(base + " ORDER BY k LIMIT 0", rows=[], oids=[23])
        for sort in ("object_number", "1"):
            page(f"SELECT k AS object_number FROM oid_page_pk ORDER BY {sort} DESC LIMIT 2",
                 [4294967295, 4294967294], 2, oid=26)

        bound = "SELECT k AS object_number FROM oid_page_pk WHERE k >= $1 ORDER BY 1 LIMIT 2"
        review.parameter(bound, ["0x80000000"], [0], [["2147483648"], ["2147483649"]], [26])
        plan(bound, 2, parameter=2147483648)
        relation = int(client.data_row_values(client.simple_query(
            sock, "SELECT oid FROM pg_catalog.pg_class WHERE relname='oid_page_pk'"))[0][0])
        stmt = b"page_bound\0"
        parse = stmt + bound.encode() + b"\0" + struct.pack("!HI", 1, 0)
        sock.sendall(packet(b"P", parse) + packet(b"D", b"S" + stmt) + packet(b"S"))
        metadata = client.read_until_ready(sock)
        fields = (b"object_number", relation, 1, 26, 4, -1, 0)
        review.record("page parameter and output type",
                      b"".join(kind for kind, _ in metadata) == b"1tTZ"
                      and metadata[1] == (b"t", struct.pack("!HI", 1, 26))
                      and client.row_description_fields(metadata) == [fields], metadata, fields)
        for bound_value, expected in ((2147483648, (2147483648, 2147483649)), (4294967295, (4294967295,))):
            bind = b"\0" + stmt + struct.pack("!HHHiIHH", 1, 1, 1, 4, bound_value, 1, 1)
            sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0") +
                         packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
            frames = client.read_until_ready(sock)
            desc = client.row_description_fields(frames) if any(k == b"T" for k, _ in frames) else []
            rows = client.data_row_values(frames)
            payload = [[struct.pack("!I", value)] for value in expected]
            review.record(("binary ordered page", bound_value),
                          b"".join(k for k, _ in frames) == b"2T" + b"D" * len(expected) + b"CZ"
                          and desc == [(*fields[:-1], 1)] and rows == payload
                          and frames[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")],
                          (desc, rows, frames[-2:]), payload)

        command("CREATE TABLE oid_page_log(id integer PRIMARY KEY,k oid,note text)", "CREATE TABLE")
        command("INSERT INTO oid_page_log VALUES (1,0,'first'),(2,2147483648,'peer'),"
                "(3,2147483648,'peer'),(4,2147483648,'peer'),(5,2147483649,'next'),"
                "(6,4294967295,'high'),(7,NULL,'missing'),(8,NULL,'missing')", "INSERT 0 8")
        command("CREATE INDEX oid_page_part ON oid_page_log(k) WHERE id=6", "CREATE INDEX")
        base = "SELECT k AS v FROM oid_page_log"
        page(base + " ORDER BY k LIMIT 2", [0, 2147483648], oid=26)
        command("DROP INDEX oid_page_part", "DROP INDEX")
        command("CREATE INDEX oid_page_complete ON oid_page_log(k)", "CREATE INDEX")
        page(base + " ORDER BY k LIMIT 2", [0, 2147483648], 2, oid=26)
        page(base + " ORDER BY k DESC LIMIT 2", [None, None], oid=26)
        page(base + " ORDER BY k DESC NULLS LAST LIMIT 2", [4294967295, 2147483649], 2, oid=26)
        page(base + " ORDER BY k OFFSET 5 LIMIT 2", [4294967295, None], oid=26)
        page(base + " WHERE k IS NOT NULL ORDER BY k DESC LIMIT 12",
             [4294967295, 2147483649, 2147483648, 2147483648, 2147483648, 0], 6, oid=26)
        page(base + " ORDER BY v FETCH FIRST 2 ROWS WITH TIES", [0] + [2147483648] * 3, 5, oid=26)
        page(base + " ORDER BY k DESC NULLS LAST OFFSET 2 ROWS FETCH NEXT ROW WITH TIES",
             [2147483648] * 3, 6, oid=26)
        page(base + " ORDER BY k OFFSET 2 ROWS FETCH NEXT ROW WITH TIES", [2147483648] * 2, 5, oid=26)
        page(base + " ORDER BY k OFFSET 6 ROWS FETCH NEXT ROW WITH TIES", [None, None], oid=26)
        page(base + " WHERE k>'0' AND note='next' ORDER BY k LIMIT 1", [2147483649], oid=26)
        command("REINDEX TABLE oid_page_log", "REINDEX")
        page(base + " ORDER BY k ASC LIMIT 2", [0, 2147483648], 2, oid=26)
        command("CREATE ROLE oid_page_reader", "CREATE ROLE")
        command("GRANT SELECT ON oid_page_log TO oid_page_reader", "GRANT")
        command("CREATE POLICY oid_page_visible ON oid_page_log FOR SELECT TO oid_page_reader USING (id>=5)",
                "CREATE POLICY")
        command("ALTER TABLE oid_page_log ENABLE ROW LEVEL SECURITY", "ALTER TABLE")
        command("SET ROLE oid_page_reader", "SET", superuser=b"off")
        page(base + " ORDER BY k LIMIT 2", [2147483649, 4294967295], oid=26)
        command("RESET ROLE", "RESET", superuser=b"on")
        command("ALTER TABLE oid_page_log DISABLE ROW LEVEL SECURITY", "ALTER TABLE")
        command("DROP INDEX oid_page_complete", "DROP INDEX")
        page(base + " ORDER BY k LIMIT 2", [0, 2147483648], oid=26)

        command("CREATE TABLE oid_page_dense(id integer PRIMARY KEY,k oid NOT NULL)", "CREATE TABLE")
        command("INSERT INTO oid_page_dense VALUES " +
                ",".join(f"({1024+i},4294967294)" for i in range(160)), "INSERT 0 160")
        for start in range(0, 1024, 128):
            values = ",".join(f"({i},{2147483500+i})" for i in range(start, start+128))
            command("INSERT INTO oid_page_dense VALUES " + values, "INSERT 0 128")
        command("CREATE INDEX oid_page_dense_key ON oid_page_dense(k)", "CREATE INDEX")
        page("SELECT id FROM oid_page_dense ORDER BY k LIMIT 3 OFFSET 13", [13, 14, 15], 16)
        page("SELECT k FROM oid_page_dense WHERE k>='4294967294' ORDER BY k DESC FETCH FIRST ROW WITH TIES",
             [4294967294] * 160, 160, oid=26)
        command("INSERT INTO oid_page_dense VALUES " +
                ",".join(f"({1184+i},4294967295)" for i in range(513)), "INSERT 0 513")
        page("SELECT k FROM oid_page_dense ORDER BY k DESC FETCH FIRST ROW WITH TIES", [4294967295] * 513, oid=26)

        assert plans == (0 if review.server is None else 30), plans
        review.complete(62 + plans)
    finally:
        review.close()


if __name__ == "__main__":
    main()
