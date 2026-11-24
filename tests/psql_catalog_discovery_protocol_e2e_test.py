#!/usr/bin/env python3
"""Verify the relation lookup issued by psql and its SQL building blocks."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


RELATIONS = (
    "SELECT c.oid, n.nspname, c.relname "
    "FROM pg_catalog.pg_class c "
    "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
)
PATTERN = "c.relname OPERATOR(pg_catalog.~) '^(catalog_probe)$' COLLATE pg_catalog.default"
VISIBLE = "pg_catalog.pg_table_is_visible(c.oid)"
DISCOVERY = RELATIONS + "WHERE " + PATTERN + " AND " + VISIBLE + " ORDER BY 2, 3"
FIELDS = [("oid", 26), ("nspname", 19), ("relname", 19)]


def main():
    review = Review("PSQL_CATALOG_DISCOVERY")
    wire = Wire(review.client, review.sock)
    owns_schema = False
    owns_public_table = False

    def table_id(schema, name):
        sql = RELATIONS + f"WHERE n.nspname = '{schema}' AND c.relname = '{name}'"
        frames = wire.send_statements(sql)
        rows = review.client.data_row_values(frames)
        assert len(rows) == 1 and int(rows[0][0]) > 0, (sql, frames)
        oid = rows[0][0].decode()
        wire.check(sql, frames, rows=[(oid, schema, name)], fields=FIELDS)
        return oid

    def discovery(schema=None, oid=None):
        rows = [] if schema is None else [(oid, schema, "catalog_probe")]
        for extended in (False, True):
            Wire(review.client, review.sock, extended).query(
                DISCOVERY, rows=rows, tag=f"SELECT {len(rows)}", fields=FIELDS)

    def value(expression, expected, ready=b"I"):
        wire.query("SELECT " + expression + " AS v", rows=[(expected,)],
                   fields=[("v", 16)], ready=ready)

    def visibility(oid, expected, ready=b"I"):
        value(f"pg_catalog.pg_table_is_visible({oid}::oid)", expected, ready)

    try:
        wire.command("CREATE SCHEMA psql_discovery", "CREATE SCHEMA")
        owns_schema = True
        wire.command("CREATE TABLE public.catalog_probe(k integer, label text)", "CREATE TABLE")
        owns_public_table = True
        wire.command("INSERT INTO public.catalog_probe VALUES (1,'Abc'),(2,'B'),(3,NULL)", "INSERT 0 3")
        wire.command("CREATE TABLE psql_discovery.catalog_probe(k integer)", "CREATE TABLE")
        public = table_id("public", "catalog_probe")
        private = table_id("psql_discovery", "catalog_probe")
        assert public != private
        visibility("1259", "t")
        visibility("0", None)
        visibility("NULL", None)
        visibility(public, "t")
        visibility(private, "f")
        discovery("public", public)

        wire.command("SET search_path=psql_discovery,public", "SET")
        discovery("psql_discovery", private)
        visibility(public, "f")
        visibility(private, "t")
        value("pg_table_is_visible('catalog_probe'::regclass)", "t")
        wire.command("SET search_path=pg_catalog", "SET")
        discovery()

        wire.command("SET search_path=public", "SET")
        wire.command("CREATE TEMP TABLE catalog_probe(k integer)", "CREATE TABLE")
        temporary = wire.send_statements("SELECT 'pg_temp.catalog_probe'::regclass::oid AS oid")
        temporary_oid = review.client.data_row_values(temporary)[0][0].decode()
        wire.check("temporary table identity", temporary, rows=[(temporary_oid,)], fields=[("oid", 26)])
        assert temporary_oid not in (public, private)
        visibility(temporary_oid, "t")
        visibility(public, "f")
        wire.command("SET search_path=public,pg_temp", "SET")
        visibility(temporary_oid, "f")
        visibility(public, "t")
        discovery("public", public)
        wire.command("DROP TABLE pg_temp.catalog_probe", "DROP TABLE")
        visibility(temporary_oid, None)

        wire.command("SET search_path=psql_discovery,public", "SET")
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("ALTER TABLE psql_discovery.catalog_probe RENAME TO other_probe",
                     "ALTER TABLE", ready=b"T")
        visibility(public, "t", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        visibility(public, "f")
        discovery("psql_discovery", private)
        wire.command("DROP TABLE psql_discovery.catalog_probe", "DROP TABLE")
        visibility(private, None)
        discovery("public", public)

        for extended in (False, True):
            mode = Wire(review.client, review.sock, extended)
            mode.query("SELECT k FROM public.catalog_probe "
                       "WHERE label OPERATOR(pg_catalog.~) '^A' COLLATE pg_catalog.default ORDER BY k",
                       rows=[("1",)], fields=[("k", 23)])
            mode.query("SELECT label ~ '^A' AS v FROM public.catalog_probe ORDER BY k",
                       rows=[("t",), ("f",), (None,)], tag="SELECT 3", fields=[("v", 16)])
            mode.query("SELECT k FROM public.catalog_probe WHERE k ~ '1' LIMIT 0",
                       state="42883", tag=None)

        value("'Abc' OPERATOR(\"pg_catalog\".~) '^A' COLLATE pg_catalog.default", "t")
        value("'Abc' OPERATOR(pg_catalog.~*) '^a' COLLATE pg_catalog.default", "t")
        value("'Abc' OPERATOR(pg_catalog.!~) '^a'", "t")
        value("'Abc' OPERATOR(pg_catalog.!~*) '^a'", "f")
        value("NULL::text OPERATOR(pg_catalog.~) '('", None)
        value("2 OPERATOR(pg_catalog.*) 3 + 4 = 14", "t")
        errors = (
            ("SELECT 'a' OPERATOR(pg_catalog.~) '('", "2201B"),
            ("SELECT 'a' OPERATOR(public.~) 'a'", "42883"),
            ("SELECT 'a' OPERATOR(pg_catalog.) 'a'", "42601"),
            ("SELECT 1 OPERATOR(pg_catalog.~) '1'", "42883"),
            ("SELECT pg_catalog.pg_table_is_visible('pg_class'::text)", "42883"),
        )
        for extended in (False, True):
            mode = Wire(review.client, review.sock, extended)
            for sql, state in errors:
                mode.query(sql, state=state, tag=None)
            mode.query("SELECT true AS v", rows=[("t",)], fields=[("v", 16)])

        wire.command("BEGIN", "BEGIN", ready=b"T")
        extended = Wire(review.client, review.sock, True)
        invalid_operator, state = errors[1]
        extended.query(invalid_operator, state=state, tag=None, ready=b"E")
        extended.query(invalid_operator, state="25P02", tag=None, ready=b"E")
        wire.command("ROLLBACK", "ROLLBACK")

        name = b"psql_visibility"
        sql = "SELECT pg_catalog.pg_table_is_visible($1) AS v"
        packet = wire.packet
        frames = wire.exchange(wire.parse(sql, name, (0,)) +
                               packet(b"D", b"S" + name + b"\0") + packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert frames[1][1] == struct.pack("!HI", 1, 26), frames
        assert review.client.row_description_fields(frames) == [(b"v", 0, 0, 16, 1, -1, 0)], frames
        binary = b"\0" + name + b"\0" + struct.pack("!HHHiIHH", 1, 1, 1, 4, int(public), 1, 1)
        frames = wire.exchange(packet(b"B", binary) + wire.describe_portal() +
                               wire.execute() + packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"2TDCZ", frames
        assert review.client.row_description_fields(frames) == [(b"v", 0, 0, 16, 1, -1, 1)], frames
        assert review.client.data_row_values(frames) == [[b"\1"]], frames
        assert frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")], frames
        regex_sql = "SELECT $1 OPERATOR(pg_catalog.~) $2 COLLATE pg_catalog.default AS v"
        frames = wire.exchange(wire.parse(regex_sql, oids=(0, 0)) +
                               packet(b"D", b"S\0") + packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert frames[1][1] == struct.pack("!HII", 2, 25, 25), frames
        review.parameter(regex_sql, ["Abc", "^A"], [25, 25], [["t"]], [16])
        assert not review.failures, review.failures
        print("PSQL_CATALOG_DISCOVERY_COMPLETE")
    finally:
        if review.server is None:
            review.client.simple_query(review.sock, "ROLLBACK")
            if owns_public_table:
                review.client.simple_query(review.sock, "DROP TABLE public.catalog_probe")
            if owns_schema:
                review.client.simple_query(review.sock, "DROP SCHEMA psql_discovery CASCADE")
        review.close()


if __name__ == "__main__":
    main()
