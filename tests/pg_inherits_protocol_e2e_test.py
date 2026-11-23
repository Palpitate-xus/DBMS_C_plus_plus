#!/usr/bin/env python3
"""Verify direct inheritance links, durable parent order and wire types."""

import struct

from compat.numeric_type_review import Review


def main():
    review = Review("PG_INHERITS")
    client, sock = review.client, review.sock

    def command(sql, tag, ready=b"I"):
        ignored = {b"N", b"S"} if tag == "SET" else {b"N"}
        frames = [frame for frame in client.simple_query(sock, sql) if frame[0] not in ignored]
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", ready)]
        review.record(sql, frames == expected, frames, expected)

    def table_oid(schema, name):
        sql = ("SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
               f"ON c.relnamespace=n.oid WHERE n.nspname='{schema}' AND c.relname='{name}'")
        frames = client.simple_query(sock, sql)
        data = client.data_row_values(frames)
        fields = client.row_description_fields(frames) if data else []
        valid = (len(data) == 1 and len(data[0]) == 1 and int(data[0][0]) > 0
                 and fields[0][3] == 26 and frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")])
        review.record(sql, valid, data, "a real relation OID")
        return int(data[0][0]) if valid else 0

    def parents(child, edges):
        sql = f"SELECT * FROM pg_catalog.pg_inherits WHERE inhrelid={child}::oid ORDER BY inhseqno"
        review.query(sql, rows=[[str(child), str(parent), str(sequence), "f"]
                               for parent, sequence in edges], oids=[26, 26, 23, 16])

    try:
        command("CREATE SCHEMA inh_graph", "CREATE SCHEMA")
        command("CREATE TABLE public.inh_a(id integer)", "CREATE TABLE")
        command("CREATE TABLE inh_graph.inh_a(id integer)", "CREATE TABLE")
        command("CREATE TABLE inh_graph.inh_c(id integer)", "CREATE TABLE")
        command("CREATE TABLE inh_graph.inh_child() INHERITS "
                "(public.inh_a, inh_graph.inh_a, inh_graph.inh_c)", "CREATE TABLE")
        command("CREATE TABLE inh_graph.inh_grand() INHERITS(inh_graph.inh_child)", "CREATE TABLE")
        root = table_oid("public", "inh_a")
        second = table_oid("inh_graph", "inh_a")
        third = table_oid("inh_graph", "inh_c")
        child = table_oid("inh_graph", "inh_child")
        grandchild = table_oid("inh_graph", "inh_grand")
        catalog = table_oid("pg_catalog", "pg_inherits")
        review.record("system relation identity", catalog == 2611, catalog, 2611)
        parents(child, [(root, 1), (second, 2), (third, 3)])
        parents(grandchild, [(child, 1)])
        parents(root, [])
        review.query("SELECT p.relname,c.relname,i.inhseqno FROM pg_catalog.pg_inherits i "
                     "JOIN pg_catalog.pg_class p ON p.oid=i.inhparent "
                     "JOIN pg_catalog.pg_class c ON c.oid=i.inhrelid "
                     f"WHERE i.inhrelid={child}::oid ORDER BY i.inhseqno",
                     rows=[["inh_a", "inh_child", "1"], ["inh_a", "inh_child", "2"], ["inh_c", "inh_child", "3"]],
                     oids=[19, 19, 23])
        command("ALTER TABLE inh_graph.inh_child NO INHERIT inh_graph.inh_a", "ALTER TABLE")
        parents(child, [(root, 1), (third, 3)])
        command("ALTER TABLE inh_graph.inh_child INHERIT inh_graph.inh_a", "ALTER TABLE")
        parents(child, [(root, 1), (third, 3), (second, 4)])
        command("ALTER TABLE inh_graph.inh_a RENAME TO inh_other", "ALTER TABLE")
        parents(child, [(root, 1), (third, 3), (second, 4)])

        sql = "SELECT inhrelid,inhparent,inhseqno,inhdetachpending FROM pg_catalog.pg_inherits " \
              "WHERE inhrelid=$1 ORDER BY inhseqno"
        statement = b"inherits_by_child"
        parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 26)
        packet = client.typed
        sync = packet(b"S", b"")
        sock.sendall(packet(b"P", parse) + packet(b"D", b"S" + statement + b"\0") + sync)
        frames = client.read_until_ready(sock)
        declaration = client.row_description_fields(frames) if any(k == b"T" for k, _ in frames) else []
        names = [b"inhrelid", b"inhparent", b"inhseqno", b"inhdetachpending"]
        expected = [(fields, catalog, i, oid, length, -1, 0) for i, (fields, oid, length) in
                    enumerate(zip(names, (26, 26, 23, 16), (4, 4, 4, 1)), 1)]
        review.record("Describe statement", declaration == expected
                      and [kind for kind, _ in frames] == [b"1", b"t", b"T", b"Z"]
                      and frames[1][1] == struct.pack("!HI", 1, 26),
                      declaration, expected)

        for key, edges in ((child, [(root, 1), (third, 3), (second, 4)]), (grandchild, [(child, 1)]), (root, [])):
            bind = b"\0" + statement + b"\0" + struct.pack("!HHHiIHH", 1, 1, 1, 4, key, 1, 1)
            sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0")
                         + packet(b"E", b"\0" + struct.pack("!I", 0)) + sync)
            frames = client.read_until_ready(sock)
            data = client.data_row_values(frames)
            desc = client.row_description_fields(frames) if any(k == b"T" for k, _ in frames) else []
            wanted = [[struct.pack("!I", key), struct.pack("!I", parent), struct.pack("!i", ordinal), b"\0"]
                      for parent, ordinal in edges]
            expected_desc = [(*field[:-1], 1) for field in expected]
            review.record(f"binary portal for {key}", data == wanted and desc == expected_desc
                          and b"".join(k for k, _ in frames) == b"2T" + b"D" * len(edges) + b"CZ"
                          and frames[-2:] == [(b"C", f"SELECT {len(edges)}\0".encode()), (b"Z", b"I")],
                          (desc, data), (expected_desc, wanted))

        command("DROP TABLE inh_graph.inh_grand", "DROP TABLE")
        parents(grandchild, [])
        command("DROP TABLE inh_graph.inh_child", "DROP TABLE")
        parents(child, [])
        command("CREATE TABLE public.pg_inherits(marker integer)", "CREATE TABLE")
        command("INSERT INTO public.pg_inherits VALUES(91)", "INSERT 0 1")
        review.query("SELECT count(*) FROM pg_inherits", rows=[["0"]], oids=[20])
        command("SET search_path=public,pg_catalog", "SET")
        review.query("SELECT marker FROM pg_inherits", rows=[["91"]], oids=[23])
        review.query("SELECT count(*) FROM pg_catalog.pg_inherits", rows=[["0"]], oids=[20])
        review.complete(37)
    finally:
        review.close()


if __name__ == "__main__":
    main()
