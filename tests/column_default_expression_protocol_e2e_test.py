#!/usr/bin/env python3
"""Check actual pg_attrdef data, its SQL output and wire descriptions."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


SETUP = (
    "CREATE SCHEMA expression_probe",
    "CREATE TABLE expression_probe.target(id integer)",
    "CREATE TABLE expression_probe.defaults("
    "n integer DEFAULT 7, negative integer DEFAULT -2147483648, "
    "big bigint DEFAULT 9223372036854775807, flag boolean DEFAULT false, "
    "label text DEFAULT 'a''bc', empty text DEFAULT '', "
    "target regclass DEFAULT 'expression_probe.target', "
    "array_target regclass[] DEFAULT '{expression_probe.target,NULL}', "
    "no_default integer, explicit_null integer DEFAULT NULL)",
)
OWNER = "'expression_probe.defaults'::regclass"
DEFAULTS = (
    "SELECT d.adnum, d.adbin, pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS pretty "
    "FROM pg_catalog.pg_attrdef d WHERE d.adrelid=" + OWNER + " ORDER BY d.adnum"
)
TEXT = (
    "SELECT d.adnum, pg_catalog.pg_get_expr(d.adbin,d.adrelid,true) AS pretty "
    "FROM pg_catalog.pg_attrdef d WHERE d.adrelid=" + OWNER + " AND d.adnum>0 ORDER BY d.adnum"
)


def node(type_oid, length, datum, collation=0):
    by_value = length > 0
    encoded = " ".join(str(byte - 256 if byte >= 128 else byte) for byte in datum)
    return (f"{{CONST :consttype {type_oid} :consttypmod -1 :constcollid {collation} "
            f":constlen {length} :constbyval {str(by_value).lower()} "
            f":constisnull false :location -1 :constvalue {length if by_value else len(datum)} [ {encoded} ]}}")


def varlena(value):
    return struct.pack("=I", (len(value) + 4) << 2) + value


def main():
    review = Review("COLUMN_DEFAULT_EXPRESSION_PROTOCOL")
    client, sock = review.client, review.sock
    wire = Wire(client, sock)
    owns_schema = False
    try:
        wire.command(SETUP[0], "CREATE SCHEMA")
        owns_schema = True
        for sql in SETUP[1:]:
            wire.command(sql, "CREATE TABLE")
        # An unencoded expression on another row must not poison a query for
        # a genuine constant, or the identity-only catalog projection.
        wire.command("CREATE TABLE expression_probe.computed(id integer DEFAULT 1+2)", "CREATE TABLE")
        lookup = wire.send_statements("SELECT 'expression_probe.target'::regclass::oid AS id")
        target = int(client.data_row_values(lookup)[0][0])
        metadata = [
            (23, 4, struct.pack("=q", 7), 0, "7"),
            (23, 4, struct.pack("=q", -2147483648), 0, "'-2147483648'::integer"),
            (20, 8, struct.pack("=q", 9223372036854775807), 0, "'9223372036854775807'::bigint"),
            (16, 1, struct.pack("=q", 0), 0, "false"),
            (25, -1, varlena(b"a'bc"), 100, "'a''bc'::text"),
            (25, -1, varlena(b""), 100, "''::text"),
            (2205, 4, struct.pack("=Q", target), 0, "'expression_probe.target'::regclass"),
            (2210, -1, varlena(struct.pack("=iiiiiiiI", 1, 32, 2205, 2, 1, 1, 0, target)),
             0, "'{expression_probe.target,NULL}'::regclass[]"),
        ]
        raw = [(str(i), node(oid, length, datum, collation), display)
               for i, (oid, length, datum, collation, display) in enumerate(metadata, 1)]
        displays = [(i, display) for i, _, display in raw]
        for extended in (False, True):
            mode = Wire(client, sock, extended)
            mode.query(DEFAULTS, rows=raw, fields=[("adnum", 21), ("adbin", 194), ("pretty", 25)], tag="SELECT 8")
            mode.query(TEXT, rows=displays, fields=[("adnum", 21), ("pretty", 25)], tag="SELECT 8")
            unreal = DEFAULTS.replace("d.adrelid) AS pretty", "4294967295::oid) AS pretty")
            mode.query(unreal, rows=[(i, value, None) for i, value, _ in raw],
                       fields=[("adnum", 21), ("adbin", 194), ("pretty", 25)], tag="SELECT 8")
            mode.query("SELECT pg_catalog.pg_get_expr(d.adbin,0,NULL) AS v FROM pg_catalog.pg_attrdef d "
                       "WHERE d.adrelid=" + OWNER + " AND d.adnum=1",
                       rows=[(None,)], fields=[("v", 25)])

        sql = "SELECT d.adbin FROM pg_catalog.pg_attrdef d WHERE d.adrelid=" + OWNER + " AND d.adnum=2"
        frames = wire.exchange(wire.parse(sql) + wire.bind(result_format=1) + wire.describe_portal() +
                               wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
        assert client.data_row_values(frames) == [[raw[1][1].encode()]], frames
        assert client.row_description_fields(frames) == [(b"adbin", 2604, 4, 194, -1, -1, 1)], frames
        wire.query("SELECT pg_catalog.pg_get_expr('raw text'::text,0)", state="42883", tag=None)
        wire.query("SELECT pg_catalog.pg_get_expr(NULL,0) AS v", rows=[(None,)], fields=[("v", 25)])
        wire.query("SELECT d.node::text AS v FROM pg_catalog.pg_attrdef AS d(id,rel,num,node) "
                   "WHERE d.rel=" + OWNER + " AND d.num=2", rows=[(raw[1][1],)], fields=[("v", 25)])
        wire.query("SELECT d.adnum FROM pg_catalog.pg_attrdef d WHERE d.adrelid=" + OWNER +
                   " AND pg_catalog.pg_get_expr(d.adbin,0)='7'", rows=[("1",)], fields=[("adnum", 21)])

        wire.command("ALTER TABLE expression_probe.target RENAME TO renamed", "ALTER TABLE")
        for offset in (6, 7):
            displays[offset] = (displays[offset][0], displays[offset][1].replace(".target", ".renamed"))
        wire.query(TEXT, rows=displays, fields=[("adnum", 21), ("pretty", 25)], tag="SELECT 8")
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("ALTER TABLE expression_probe.defaults ALTER COLUMN n SET DEFAULT 19", "ALTER TABLE", ready=b"T")
        wire.query(TEXT, rows=[("1", "19"), *displays[1:]], fields=[("adnum", 21), ("pretty", 25)], tag="SELECT 8", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.query(TEXT, rows=displays, fields=[("adnum", 21), ("pretty", 25)], tag="SELECT 8")

        identity_sql = ("SELECT d.adnum FROM pg_catalog.pg_attrdef d WHERE "
                        "d.adrelid='expression_probe.computed'::regclass")
        wire.query(identity_sql, rows=[("1",)], fields=[("adnum", 21)])
        if review.server is not None:
            wire.query(identity_sql.replace("d.adnum", "d.adbin"), state="0A000", tag=None)
            wire.query(identity_sql.replace("d.adnum", "*"), state="0A000", tag=None)
            wire.query(identity_sql.replace("d.adnum", "d.*"), state="0A000", tag=None)
        print("COLUMN_DEFAULT_EXPRESSION_PROTOCOL_COMPLETE")
    finally:
        if review.server is None and owns_schema:
            client.simple_query(sock, "ROLLBACK")
            client.simple_query(sock, "DROP SCHEMA expression_probe CASCADE")
        review.close()


if __name__ == "__main__":
    main()
