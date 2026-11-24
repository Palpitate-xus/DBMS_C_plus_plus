#!/usr/bin/env python3
"""Check catalog rows and the collation declared by actual table columns."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


CATALOG = "SELECT c.* FROM pg_catalog.pg_collation c WHERE c.oid IN (100,950,951,962) ORDER BY c.oid"
FIELDS = [
    ("oid", 26, 4), ("collname", 19, 64), ("collnamespace", 26, 4), ("collowner", 26, 4),
    ("collprovider", 18, 1), ("collisdeterministic", 16, 1), ("collencoding", 23, 4),
    ("collcollate", 25, -1), ("collctype", 25, -1), ("colllocale", 25, -1),
    ("collicurules", 25, -1), ("collversion", 25, -1),
]
COLLATIONS = [
    ("100", "default", "11", "10", "d", "t", "-1", None, None, None, None, None),
    ("950", "C", "11", "10", "c", "t", "-1", "C", "C", None, None, None),
    ("951", "POSIX", "11", "10", "c", "t", "-1", "POSIX", "POSIX", None, None, None),
    ("962", "ucs_basic", "11", "10", "b", "t", "6", None, None, "C", None, "1"),
]
DDL = ('CREATE TABLE coll_probe.column_probe(id integer DEFAULT 7, plain text, '
       'bytewise text COLLATE "C" DEFAULT \'pending\', portable text COLLATE "POSIX", '
       'unicode_name text COLLATE ucs_basic)')
DECLARATIONS = [
    ("id", "0", None), ("plain", "100", "default"), ("bytewise", "950", "C"),
    ("portable", "951", "POSIX"), ("unicode_name", "962", "ucs_basic"),
]
COLUMN_QUERY = (
    "SELECT a.attname, a.attcollation, c.collname FROM pg_catalog.pg_attribute a "
    "LEFT JOIN pg_catalog.pg_collation c ON c.oid=a.attcollation "
    "WHERE a.attrelid='coll_probe.column_probe'::regclass AND a.attnum>0 "
    "AND NOT a.attisdropped ORDER BY a.attnum"
)
COLUMN_FIELDS = [("attname", 19), ("attcollation", 26), ("collname", 19)]
DETAIL_QUERY = """
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '{oid}' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""
DETAILS = [
    ("id", "integer", "7", "f", None, "", ""), ("plain", "text", None, "f", None, "", ""),
    ("bytewise", "text", "'pending'::text", "f", "C", "", ""),
    ("portable", "text", None, "f", "POSIX", "", ""),
    ("unicode_name", "text", None, "f", "ucs_basic", "", ""),
]


def main():
    review = Review("PG_COLLATION_CATALOG")
    client, sock = review.client, review.sock
    wire = Wire(client, sock)
    owns_schema = False

    def columns(expected, ready=b"I"):
        for extended in (False, True):
            Wire(client, sock, extended).query(COLUMN_QUERY, rows=expected, fields=COLUMN_FIELDS,
                                               tag=f"SELECT {len(expected)}", ready=ready)

    try:
        for extended in (False, True):
            mode = Wire(client, sock, extended)
            frames = mode.send_statements(CATALOG)
            mode.check(CATALOG, frames, rows=COLLATIONS, fields=[(n, oid) for n, oid, _ in FIELDS], tag="SELECT 4")
            assert client.row_description_fields(frames) == [
                (name.encode(), 3456, number, oid, size, -1, 0)
                for number, (name, oid, size) in enumerate(FIELDS, 1)], frames

        sql = ("SELECT oid,collname,collprovider,collisdeterministic,collencoding,collcollate,colllocale "
               "FROM pg_catalog.pg_collation WHERE oid=$1")
        frames = wire.exchange(wire.parse(sql, oids=(26,)) +
                               wire.bind(arguments=(b"950",), result_format=1) +
                               wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
        assert client.data_row_values(frames) == [[struct.pack("!I", 950), b"C", b"c", b"\x01",
                                                   struct.pack("!i", -1), b"C", None]], frames
        assert [field[-1] for field in client.row_description_fields(frames)] == [1] * 7, frames

        wire.command("CREATE SCHEMA coll_probe", "CREATE SCHEMA")
        owns_schema = True
        wire.command(DDL, "CREATE TABLE")
        columns(DECLARATIONS)
        identity = wire.send_statements("SELECT 'coll_probe.column_probe'::regclass::oid AS id")
        oid = int(client.data_row_values(identity)[0][0])
        wire.query(DETAIL_QUERY.format(oid=oid), rows=DETAILS, tag="SELECT 5")
        wire.query("SELECT a.attname FROM pg_catalog.pg_attribute a "
                   f"WHERE ('{oid}'=a.attrelid OR a.attrelid=3456) AND a.attnum>0 ORDER BY a.attrelid,a.attnum",
                   rows=[(column[0],) for column in FIELDS + DECLARATIONS], fields=[("attname", 19)], tag="SELECT 17")
        wire.command("INSERT INTO coll_probe.column_probe(bytewise) VALUES ('b'),('a'),('A')", "INSERT 0 3")
        wire.query("SELECT bytewise FROM coll_probe.column_probe ORDER BY bytewise",
                   rows=[("A",), ("a",), ("b",)], fields=[("bytewise", 25)], tag="SELECT 3")
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command('ALTER TABLE coll_probe.column_probe ADD COLUMN added text COLLATE "C"',
                     "ALTER TABLE", ready=b"T")
        columns([*DECLARATIONS, ("added", "950", "C")], b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        columns(DECLARATIONS)

        wire.command("CREATE TABLE coll_probe.pg_collation(marker integer)", "CREATE TABLE")
        wire.command("INSERT INTO coll_probe.pg_collation VALUES (17)", "INSERT 0 1")
        wire.command("SET search_path=coll_probe,pg_catalog,public", "SET")
        wire.query("SELECT marker FROM pg_collation", rows=[("17",)], fields=[("marker", 23)])
        wire.query("SELECT collname FROM pg_catalog.pg_collation WHERE oid=950",
                   rows=[("C",)], fields=[("collname", 19)])

        if review.server is not None:
            wire.query("SELECT oid,collname,collisdeterministic FROM pg_catalog.pg_collation "
                       "WHERE oid>=9000 ORDER BY oid", rows=[("9002", "nocase", "f"), ("9003", "reverse", "t"),
                                                           ("9004", "C.utf8", "t")],
                       fields=[("oid", 26), ("collname", 19), ("collisdeterministic", 16)], tag="SELECT 3")
            wire.command("ALTER TABLE coll_probe.column_probe ADD COLUMN local text COLLATE nocase", "ALTER TABLE")
            columns([*DECLARATIONS, ("local", "9002", "nocase")])
        print("PG_COLLATION_CATALOG_COMPLETE")
    finally:
        if review.server is None and owns_schema:
            client.simple_query(sock, "ROLLBACK")
            client.simple_query(sock, "DROP SCHEMA coll_probe CASCADE")
        review.close()


if __name__ == "__main__":
    main()
