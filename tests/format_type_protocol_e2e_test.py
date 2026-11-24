#!/usr/bin/env python3
"""Verify format_type against real column declarations and PostgreSQL 18."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


COLUMN_QUERY = (
    "SELECT a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod) AS declared "
    "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
    "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
    "WHERE n.nspname='format_a' AND c.relname='format_probe' AND a.attnum>0 "
    "AND NOT a.attisdropped ORDER BY a.attnum"
)
COLUMNS = ("id integer, name character varying(12), initial character(3), "
           "price numeric(10,-2), clock time(3) with time zone, received timestamp(4), "
           "elapsed interval day to second(2), flags bit(3), tags integer[]")
DECLARATIONS = [
    ("id", "integer"), ("name", "character varying(12)"), ("initial", "character(3)"),
    ("price", "numeric(10,-2)"), ("clock", "time(3) with time zone"),
    ("received", "timestamp(4) without time zone"), ("elapsed", "interval day to second(2)"),
    ("flags", "bit(3)"), ("tags", "integer[]"),
]
BASIC_CASES = [
    ("NULL::oid", "0", None), ("0::oid", "NULL", "-"), ("4294967295::oid", "-1", "???"),
    ("16", "6", "boolean"), ("20", "NULL", "bigint"), ("21", "NULL", "smallint"),
    ("23", "7", "integer"), ("700", "-1", "real"), ("701", "0", "double precision"),
    ("114", "7", "json"), ("18", "-1", '"char"'), ("25", "0", "text(0)"),
    ("30", "NULL", "oidvector"), ("1043", "16", "character varying(12)"),
    ("1043", "4", "character varying"), ("1042", "NULL", "character"),
    ("1042", "-1", "bpchar"), ("1042", "7", "character(3)"),
    ("1560", "NULL", "bit"), ("1560", "-1", '"bit"'), ("1560", "0", "bit(0)"),
    ("1562", "8", "bit varying(8)"), ("1114", "NULL", "timestamp without time zone"),
    ("1184", "2", "timestamp(2) with time zone"), ("1083", "6", "time(6) without time zone"),
    ("1266", "3", "time(3) with time zone"), ("1186", str((7176 << 16) | 2), "interval day to second(2)"),
    ("1186", str(32767 << 16), "interval(0)"), ("1186", "NULL", "interval"),
    ("1700", "3", "numeric"), ("1700", "4", "numeric(0,0)"),
    ("1700", str((10 << 16) + 2046 + 4), "numeric(10,-2)"),
    ("1231", str((8 << 16) + 2 + 4), "numeric(8,2)[]"),
    ("1015", "11", "character varying(7)[]"), ("1014", "-1", "bpchar[]"),
    ("1007", "NULL", "integer[]"),
    ("83", "NULL", "pg_class"),
]


def main():
    review = Review("FORMAT_TYPE_PROTOCOL")
    client, sock = review.client, review.sock
    wire = Wire(client, sock)
    owned = []

    def display(oid, expected, modifier="NULL", ready=b"I"):
        wire.query(f"SELECT pg_catalog.format_type({oid},{modifier}) AS v",
                   rows=[(expected,)], fields=[("v", 25)], ready=ready)

    try:
        for extended in (False, True):
            mode = Wire(client, sock, extended)
            for oid, modifier, expected in BASIC_CASES:
                mode.query(f"SELECT pg_catalog.format_type({oid},{modifier}) AS v",
                           rows=[(expected,)], fields=[("v", 25)])
            mode.query("SELECT pg_catalog.format_type(1186,0)", state="XX000", tag=None)
            mode.query("SELECT pg_catalog.format_type(23::text,NULL)", state="42883", tag=None)
            mode.query("SELECT pg_catalog.format_type(23)", state="42883", tag=None)
            mode.query("SELECT pg_catalog.format_type(23,NULL) OVER ()", state="42809", tag=None)

        name = b"display"
        packet = wire.packet
        frames = wire.exchange(wire.parse("SELECT pg_catalog.format_type($1,$2) AS v", name) +
                               packet(b"D", b"S" + name + b"\0") + packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert frames[1][1] == struct.pack("!HII", 2, 26, 23), frames
        assert client.row_description_fields(frames) == [(b"v", 0, 0, 25, -1, -1, 0)], frames
        for oid, modifier, expected in ((1043, 16, b"character varying(12)"),
                                        (1560, None, b"bit"), (1560, -1, b'"bit"'),
                                        (None, 0, None), (4294967295, -1, b"???")):
            bind = b"\0" + name + b"\0" + struct.pack("!HHH", 1, 1, 2)
            bind += struct.pack("!iI", 4, oid) if oid is not None else struct.pack("!i", -1)
            bind += struct.pack("!ii", 4, modifier) if modifier is not None else struct.pack("!i", -1)
            bind += struct.pack("!HH", 1, 1)
            frames = wire.exchange(packet(b"B", bind) + wire.describe_portal() + wire.execute() + packet(b"S"))
            assert b"".join(kind for kind, _ in frames) == b"2TDCZ", frames
            assert client.data_row_values(frames) == [[expected]], frames
            assert client.row_description_fields(frames) == [(b"v", 0, 0, 25, -1, -1, 1)], frames

        for schema in ("format_a", "format_b"):
            wire.command(f"CREATE SCHEMA {schema}", "CREATE SCHEMA")
            owned.append(schema)
            wire.command(f"CREATE TYPE {schema}.status AS ENUM ('a','b')", "CREATE TYPE")
        wire.command("CREATE DOMAIN format_a.labels AS varchar(7)[]", "CREATE DOMAIN")
        wire.command("""CREATE TYPE format_a."Case.Row" AS ENUM ('x')""", "CREATE TYPE")
        wire.command("CREATE TABLE format_a.format_probe(" + COLUMNS + ")", "CREATE TABLE")
        for extended in (False, True):
            Wire(client, sock, extended).query(COLUMN_QUERY, rows=DECLARATIONS,
                fields=[("attname", 19), ("declared", 25)], tag=f"SELECT {len(DECLARATIONS)}")

        display("'format_a.status'::regtype", "format_a.status")
        display("'format_a.status[]'::regtype", "format_a.status[]")
        display("'format_a.labels'::regtype", "format_a.labels")
        display("""'format_a."Case.Row"'::regtype""", 'format_a."Case.Row"')
        wire.command("SET search_path=format_b,format_a,public", "SET")
        display("'format_b.status'::regtype", "status")
        display("'format_a.status[]'::regtype", "format_a.status[]")
        display("""'format_a."Case.Row"[]'::regtype""", '"Case.Row"[]')
        display("'format_a.labels'::regtype", "labels")
        wire.command("CREATE TYPE format_b.int4 AS ENUM ('x')", "CREATE TYPE")
        wire.command("CREATE TYPE format_b.text AS ENUM ('x')", "CREATE TYPE")
        wire.command("SET search_path=format_b,pg_catalog,format_a,public", "SET")
        display("23", "integer")
        display("25", "pg_catalog.text")
        display("1009", "pg_catalog.text[]")
        display("'format_b.text'::regtype", "text")
        Wire(client, sock, extended=True).query("SELECT format_type(25,NULL) AS v",
            rows=[("pg_catalog.text",)], fields=[("v", 25)])
        wire.query("SELECT pg_typeof(pg_catalog.format_type(25,NULL))::oid AS v",
                   rows=[("25",)], fields=[("v", 26)])
        wire.command("SET search_path=pg_catalog,format_b,format_a,public", "SET")
        display("25", "text")
        display("1009", "text[]")
        display("'format_b.text'::regtype", "format_b.text")
        wire.command("CREATE TEMP TABLE format_temp_probe(id integer)", "CREATE TABLE")
        display("25", "text")

        identity = wire.send_statements("""SELECT 'format_a."Case.Row"'::regtype::oid AS v""")
        oid = client.data_row_values(identity)[0][0].decode()
        assert int(oid) > 0
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command('DROP TYPE format_a."Case.Row"', "DROP TYPE", ready=b"T")
        display(oid, "???", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        display("""'format_a."Case.Row"'::regtype""", '"Case.Row"')
        print("FORMAT_TYPE_PROTOCOL_COMPLETE")
    finally:
        if review.server is None:
            client.simple_query(sock, "ROLLBACK")
            for schema in reversed(owned):
                client.simple_query(sock, f"DROP SCHEMA {schema} CASCADE")
        review.close()


if __name__ == "__main__":
    main()
