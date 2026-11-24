#!/usr/bin/env python3
"""Exercise access method metadata and the table listing issued by psql."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


METHODS = [
    ("2", "heap", "t"), ("403", "btree", "i"), ("405", "hash", "i"),
    ("783", "gist", "i"), ("2742", "gin", "i"), ("3580", "brin", "i"),
    ("4000", "spgist", "i"),
]
METHOD_QUERY = "SELECT oid,amname,amtype FROM pg_catalog.pg_am WHERE oid < 9000 ORDER BY oid"
METHOD_FIELDS = [("oid", 26), ("amname", 19), ("amtype", 18)]
RELATION_METHODS = (
    "SELECT c.relname, am.amname FROM pg_catalog.pg_class c "
    "LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam "
    "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
    "WHERE n.nspname='method_probe' ORDER BY 1"
)
TABLE_LIST = """
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view'
       WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table'
       WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index'
  END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','t','s','')
  AND c.relname OPERATOR(pg_catalog.~) '^(access_probe)$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2
"""


def main():
    review = Review("PG_ACCESS_METHOD_CATALOG")
    client = review.client
    wire = Wire(client, review.sock)
    owns_schema = False
    owns_role = False

    def expect_methods(rows, ready=b"I"):
        for extended in (False, True):
            Wire(client, review.sock, extended).query(
                RELATION_METHODS, rows=rows, ready=ready,
                fields=[("relname", 19), ("amname", 19)], tag=f"SELECT {len(rows)}")

    try:
        for extended in (False, True):
            mode = Wire(client, review.sock, extended)
            frames = mode.send_statements(METHOD_QUERY)
            mode.check(METHOD_QUERY, frames, rows=METHODS, fields=METHOD_FIELDS, tag="SELECT 7")
            assert client.row_description_fields(frames) == [
                (b"oid", 2601, 1, 26, 4, -1, 0), (b"amname", 2601, 2, 19, 64, -1, 0),
                (b"amtype", 2601, 4, 18, 1, -1, 0)], frames

        sql = "SELECT oid,amname,amtype FROM pg_catalog.pg_am WHERE oid=$1"
        frames = wire.exchange(wire.parse(sql, oids=(26,)) +
                               wire.bind(arguments=(b"403",), result_format=1) +
                               wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
        assert client.data_row_values(frames) == [[struct.pack("!I", 403), b"btree", b"i"]], frames
        assert [field[-1] for field in client.row_description_fields(frames)] == [1, 1, 1], frames

        statement = b"char_byte"
        packet = wire.packet
        frames = wire.exchange(wire.parse("SELECT $1 AS v", statement, (18,)) +
                               packet(b"D", b"S" + statement + b"\0") + packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert frames[1][1] == struct.pack("!HI", 1, 18), frames
        assert client.row_description_fields(frames) == [(b"v", 0, 0, 18, 1, -1, 0)], frames
        for value in (b"\x00", b"i", b"'", b"\\", b"\x80", b"\xff", None, b"", b"ab"):
            bind = b"\0" + statement + b"\0" + struct.pack("!HHHi", 1, 1, 1,
                                                             -1 if value is None else len(value))
            bind += (value or b"") + struct.pack("!HH", 1, 1)
            frames = wire.exchange(packet(b"B", bind) + wire.describe_portal() + wire.execute() + packet(b"S"))
            if value in (b"", b"ab"):
                wire.check(f"invalid char length {len(value)}", frames,
                           state="08P01" if value == b"" else "22P03", tag=None)
            else:
                assert b"".join(kind for kind, _ in frames) == b"2TDCZ", frames
                assert client.data_row_values(frames) == [[value]], frames
                assert client.row_description_fields(frames) == [(b"v", 0, 0, 18, 1, -1, 1)], frames

        wire.command("CREATE SCHEMA method_probe", "CREATE SCHEMA")
        owns_schema = True
        wire.command("CREATE TABLE method_probe.access_probe(id integer,k integer,label text)", "CREATE TABLE")
        wire.command("CREATE VIEW method_probe.access_view AS SELECT id FROM method_probe.access_probe", "CREATE VIEW")
        wire.command("CREATE INDEX access_tree ON method_probe.access_probe(id)", "CREATE INDEX")
        wire.command("CREATE INDEX access_composite ON method_probe.access_probe(id,k)", "CREATE INDEX")
        wire.command("CREATE INDEX access_hash ON method_probe.access_probe USING hash(k)", "CREATE INDEX")
        wire.command("CREATE INDEX access_brin ON method_probe.access_probe USING brin(k)", "CREATE INDEX")
        expected = [
            ("access_brin", "brin"), ("access_composite", "btree"), ("access_hash", "hash"),
            ("access_probe", "heap"), ("access_tree", "btree"), ("access_view", None),
        ]
        expect_methods(expected)
        wire.command("SET search_path=method_probe,public", "SET")
        owner = "postgres" if review.server is None else "alice"
        wire.query(TABLE_LIST, rows=[("method_probe", "access_probe", "table", owner)],
                   fields=[("Schema", 19), ("Name", 19), ("Type", 25), ("Owner", 19)])
        wire.query("SELECT pg_catalog.pg_get_userbyid(0::oid) AS missing,"
                   "pg_catalog.pg_get_userbyid(NULL::oid) AS empty",
                   rows=[("unknown (OID=0)", None)], fields=[("missing", 19), ("empty", 19)])
        wire.query("SELECT pg_catalog.pg_get_userbyid('missing'::text)", state="42883", tag=None)
        wire.command("CREATE ROLE method_owner", "CREATE ROLE")
        owns_role = True
        wire.command("ALTER TABLE method_probe.access_probe OWNER TO method_owner", "ALTER TABLE")
        wire.query(TABLE_LIST, rows=[("method_probe", "access_probe", "table", "method_owner")],
                   fields=[("Schema", 19), ("Name", 19), ("Type", 25), ("Owner", 19)])

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("DROP INDEX method_probe.access_tree", "DROP INDEX", ready=b"T")
        expect_methods([row for row in expected if row[0] != "access_tree"], ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        expect_methods(expected)

        if review.server is not None:
            wire.query("SELECT oid,amname,amtype FROM pg_catalog.pg_am WHERE oid >= 9000 ORDER BY oid",
                       rows=[("9000", "bloom", "i"), ("9001", "fulltext", "i")],
                       fields=METHOD_FIELDS, tag="SELECT 2")
            wire.command("CREATE INDEX access_bloom ON method_probe.access_probe USING bloom(label)", "CREATE INDEX")
            wire.command("CREATE INDEX access_fulltext ON method_probe.access_probe USING fulltext(label)", "CREATE INDEX")
            expect_methods(sorted(expected + [("access_bloom", "bloom"), ("access_fulltext", "fulltext")]))
            for suffix in ("*", "amhandler"):
                for extended in (False, True):
                    Wire(client, review.sock, extended).query(
                        f"SELECT {suffix} FROM pg_catalog.pg_am LIMIT 0", state="0A000", tag=None)
            wire.command("CREATE TABLE method_probe.pg_am(marker integer)", "CREATE TABLE")
            wire.command("INSERT INTO method_probe.pg_am VALUES(42)", "INSERT 0 1")
            wire.command("SET search_path=method_probe,pg_catalog,public", "SET")
            wire.query("SELECT marker FROM pg_am", rows=[("42",)], fields=[("marker", 23)])
            wire.query("SELECT amname FROM pg_catalog.pg_am WHERE oid=403",
                       rows=[("btree",)], fields=[("amname", 19)])
        print("PG_ACCESS_METHOD_CATALOG_COMPLETE")
    finally:
        if review.server is None:
            client.simple_query(review.sock, "ROLLBACK")
            if owns_schema:
                client.simple_query(review.sock, "DROP SCHEMA method_probe CASCADE")
            if owns_role:
                client.simple_query(review.sock, "DROP ROLE method_owner")
        review.close()


if __name__ == "__main__":
    main()
