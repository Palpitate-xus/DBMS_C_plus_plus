#!/usr/bin/env python3
"""Verify format_type against real column declarations and PostgreSQL 18."""

import json
import struct

from compat.numeric_type_review import Review
from catalog_oid_join_filter_protocol_e2e_test import plan_nodes
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


def check_prepared_regclass(wire, is_postgres):
    sql = ("SELECT attname FROM pg_catalog.pg_attribute "
           "WHERE attrelid='path_probe'::regclass AND attnum>0")
    wire.command("CREATE TABLE format_a.path_probe(first_column integer)", "CREATE TABLE")
    wire.command("CREATE TABLE format_b.path_probe(second_column integer)", "CREATE TABLE")
    wire.command("SET search_path=format_a,format_b,public", "SET")
    name = b"format_relation_reference"
    sync = wire.packet(b"S")
    frames = wire.exchange(wire.parse(sql, name) + sync)
    assert b"".join(kind for kind, _ in frames) == b"1Z", frames

    def check(column, ready=b"I"):
        frames = wire.exchange(wire.bind(name) + wire.describe_portal() + wire.execute() + sync)
        wire.check(sql, frames, rows=[(column,)], fields=[("attname", 19)], ready=ready)

    check("first_column")
    wire.command("SET search_path=format_b,format_a,public", "SET")
    check("second_column")
    wire.command("SET search_path=format_a,format_b,public", "SET")
    check("first_column")
    wire.command("BEGIN", "BEGIN", ready=b"T")
    wire.command("ALTER TABLE format_a.path_probe RENAME TO moved_probe", "ALTER TABLE", ready=b"T")
    check("second_column", ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    # PostgreSQL can retain the second object's binding after the first
    # object's name is restored. The local statement binds again at execution.
    check("second_column" if is_postgres else "first_column")
    wire.query(sql, rows=[("first_column",)], fields=[("attname", 19)])
    wire.command("SET search_path=format_b,format_a,public", "SET")
    check("second_column")
    wire.command("SET search_path=format_a,format_b,public", "SET")
    check("first_column")
    wire.command("SET search_path=public", "SET")
    frames = wire.exchange(wire.packet(b"C", b"S" + name + b"\0") + sync)
    assert b"".join(kind for kind, _ in frames) == b"3Z", frames


def check_runtime_catalog_key(wire, is_postgres):
    sql = ("SELECT attname FROM pg_catalog.pg_attribute "
           "WHERE attrelid=$1::pg_catalog.text::pg_catalog.regclass AND attnum>0")
    name = b"runtime_catalog_key"
    sync = wire.packet(b"S")
    assert b"".join(kind for kind, _ in wire.exchange(wire.parse(sql, name, (25,)) + sync)) == b"1Z"

    def check(argument, rows=(), tag="SELECT 1", ready=b"I", state=None):
        frames = wire.exchange(wire.bind(name, (argument,)) + wire.describe_portal() + wire.execute() + sync)
        fields = [("attname", 19)] if state is None else None
        wire.check(sql, frames, rows=rows, tag=tag, ready=ready, state=state, fields=fields)

    wire.command("SET search_path=format_a,format_b,public", "SET")
    check(b"path_probe", [("first_column",)])
    check(b"format_b.path_probe", [("second_column",)])
    check(None, tag="SELECT 0")
    check(b"format_a.gone", tag=None, state="42P01")
    wire.command("BEGIN", "BEGIN", ready=b"T")
    wire.command("ALTER TABLE format_a.path_probe RENAME TO moved_probe", "ALTER TABLE", ready=b"T")
    check(b"path_probe", [("second_column",)], ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    check(b"path_probe", [("first_column",)])
    frames = wire.exchange(wire.packet(b"C", b"S" + name + b"\0") + sync)
    assert b"".join(kind for kind, _ in frames) == b"3Z", frames

    if not is_postgres:
        base = "SELECT attname FROM pg_catalog.pg_attribute WHERE "
        wire.query(base + "false AND attrelid='format_a.gone'::text::regclass",
                   fields=[("attname", 19)], tag="SELECT 0")
        wire.query(base + "attrelid='format_a.gone'::text::regclass AND false", state="42P01", tag=None)
        wire.query(base + "attrelid=NULL::text::regclass AND 1/0=0", state="22012", tag=None)
        wire.query(base + "attrelid='format_a.path_probe'::text::regclass LIMIT 0",
                   fields=[("attname", 19)], tag="SELECT 0")
        wire.query(base + "attrelid='format_a.gone'::text::regclass LIMIT 0", state="42P01", tag=None)
        wire.command("CREATE TABLE format_a.switch_probe(first_column int, another_column int)", "CREATE TABLE")
        wire.command("CREATE TABLE format_b.switch_probe(second_column int)", "CREATE TABLE")
        wire.command("CREATE FUNCTION format_a.switch_directory() RETURNS boolean VOLATILE LANGUAGE plpgsql "
                     "AS $$BEGIN SET search_path TO format_b,format_a,public; RETURN true; END;$$",
                     "CREATE FUNCTION")
        key = "attrelid='switch_probe'::text::regclass AND attnum>0"
        wire.query(base + key + " AND format_a.switch_directory()",
                   rows=[("first_column",), ("another_column",)], fields=[("attname", 19)], tag="SELECT 2")
        wire.query(base + key, rows=[("second_column",)], fields=[("attname", 19)])
        wire.command("SET search_path=format_a,format_b,public", "SET")
        wire.command("CREATE OR REPLACE FUNCTION format_a.switch_directory() RETURNS boolean VOLATILE LANGUAGE plpgsql "
                     "AS $$BEGIN SET search_path TO public; RETURN true; END;$$", "CREATE FUNCTION")
        wire.query(base + key + " AND format_a.switch_directory()",
                   rows=[("first_column",), ("another_column",)], fields=[("attname", 19)], tag="SELECT 2")
        wire.query(base + key, state="42P01", tag=None)
    wire.command("SET search_path=public", "SET")


def check_qualified_regclass(wire, table_oid):
    frames = wire.send_statements("SELECT current_database()")
    database = wire.client.data_row_values(frames)[0][0].decode()
    db_name = '"' + database.replace('"', '""') + '"'
    target = f"{db_name}.format_a.format_probe"
    for suffix in ("::regclass", "::text::regclass", "::varchar::regclass"):
        wire.query(f"SELECT '{db_name} . format_a . format_probe'{suffix}::oid AS id",
                   rows=[(str(table_oid),)], fields=[("id", 26)])
        for invalid, state in (
            ("other_database.format_a.format_probe", "0A000"),
            (f"{db_name}.format_a.missing", "42P01"),
            (f"{db_name}.no_such_schema.missing", "42P01" if suffix == "::regclass" else "3F000"),
            ("a.b.c.d", "42601"),
            ("a.b.c.", "42602"),
        ):
            wire.query(f"SELECT '{invalid}'{suffix}", state=state, tag=None)

    sql = "SELECT $1::pg_catalog.text::pg_catalog.regclass::oid AS id"
    frames = wire.exchange(wire.parse(sql, oids=(25,)) + wire.bind(arguments=(target.encode(),)) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    wire.check(sql, frames, rows=[(str(table_oid),)], fields=[("id", 26)])
    scan = ("SELECT attname FROM pg_catalog.pg_attribute "
            "WHERE attrelid=$1::text::regclass AND attnum>0 ORDER BY attnum")
    frames = wire.exchange(wire.parse(scan, oids=(25,)) + wire.bind(arguments=(target.encode(),)) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    wire.check(scan, frames, rows=[(column,) for column, _ in DECLARATIONS],
               fields=[("attname", 19)], tag=f"SELECT {len(DECLARATIONS)}")

    wire.command("CREATE TEMP TABLE qualified_temp_probe(id integer)", "CREATE TABLE")
    temp_oid = wire.client.data_row_values(
        wire.send_statements("SELECT 'pg_temp.qualified_temp_probe'::regclass::oid"))[0][0].decode()
    wire.query(f"SELECT '{db_name}.pg_temp.qualified_temp_probe'::text::regclass::oid AS id",
               rows=[(temp_oid,)], fields=[("id", 26)])
    wire.query(f"SELECT '{db_name}.pg_temp.no_table'::text::regclass", state="42P01", tag=None)
    wire.command("DROP TABLE pg_temp.qualified_temp_probe", "DROP TABLE")

    wire.command(f"CREATE TABLE format_a.qualified_reference(id int, ref regclass DEFAULT '{target}')",
                 "CREATE TABLE")
    wire.command(f"INSERT INTO format_a.qualified_reference VALUES (1,DEFAULT),(2,'{target}')", "INSERT 0 2")
    query = "SELECT ref::text AS ref, ref::oid AS number FROM format_a.qualified_reference ORDER BY id"
    fields = [("ref", 25), ("number", 26)]
    original = ("format_a.format_probe", str(table_oid))
    wire.query(query, rows=[original] * 2, fields=fields, tag="SELECT 2")
    wire.command("BEGIN", "BEGIN", ready=b"T")
    wire.command("ALTER TABLE format_a.format_probe RENAME TO qualified_moved", "ALTER TABLE", ready=b"T")
    wire.query(query, rows=[("format_a.qualified_moved", str(table_oid))] * 2,
               fields=fields, tag="SELECT 2", ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    wire.query(query, rows=[original] * 2, fields=fields, tag="SELECT 2")


def check_runtime_regclass(wire):
    wire.command('CREATE TABLE format_a."Case.Table"(id integer)', "CREATE TABLE")
    wire.command('CREATE TABLE format_a."007"(id integer)', "CREATE TABLE")
    wire.command("CREATE TABLE format_a.relation_input(id integer PRIMARY KEY, relation_name text)",
                 "CREATE TABLE")
    wire.command("""INSERT INTO format_a.relation_input VALUES
        (1,'path_probe'),(2,'format_a.path_probe'),(3,' format_a . "Case.Table" '),
        (4,'pg_temp.format_temp_probe'),(5,'007'),(6,NULL)""", "INSERT 0 6")

    def oid(name):
        frames = wire.send_statements(f"SELECT '{name}'::pg_catalog.regclass::oid")
        return wire.client.data_row_values(frames)[0][0].decode()

    first = oid("format_a.path_probe")
    second = oid("format_b.path_probe")
    mixed = oid('format_a."Case.Table"')
    numeric_name = oid('format_a."007"')
    temporary = oid("pg_temp.format_temp_probe")
    projection = (
        "SELECT relation_name::pg_catalog.regclass AS relation, "
        "relation_name::pg_catalog.regclass::oid AS number FROM format_a.relation_input"
    )
    fields = [("relation", 2205), ("number", 26)]
    remaining = [('"Case.Table"', mixed), ("format_temp_probe", temporary),
                 ('"007"', numeric_name), (None, None)]
    wire.command("SET search_path=pg_catalog,format_b,format_a,public", "SET")
    for extended in (False, True):
        mode = Wire(wire.client, wire.sock, extended)
        for type_name in ("regclass", "pg_catalog.regclass", '"pg_catalog"."regclass"'):
            mode.query(projection.replace("pg_catalog.regclass", type_name) + " ORDER BY id",
                       rows=[("path_probe", second), ("format_a.path_probe", first), *remaining],
                       fields=fields, tag="SELECT 6")
    frames = wire.exchange(wire.parse(projection + " WHERE id=5") + wire.bind(result_format=1) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
    encoded = struct.pack("!I", int(numeric_name))
    assert wire.client.data_row_values(frames) == [[encoded, encoded]], frames
    assert [(name.decode(), type_oid, fmt) for name, _, _, type_oid, _, _, fmt in
            wire.client.row_description_fields(frames)] == [("relation", 2205, 1), ("number", 26, 1)], frames
    parameter = "SELECT $1::pg_catalog.text::pg_catalog.regclass::oid AS number"
    frames = wire.exchange(wire.parse(parameter, oids=[25]) + wire.bind(arguments=(b"007",)) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    wire.check(parameter, frames, rows=[(numeric_name,)], fields=[("number", 26)])
    wire.command("SET search_path=pg_catalog,format_a,format_b,public", "SET")
    wire.query(projection + " ORDER BY id",
               rows=[("path_probe", first), ("path_probe", first), *remaining],
               fields=fields, tag="SELECT 6")
    wire.command("BEGIN", "BEGIN", ready=b"T")
    wire.command("ALTER TABLE format_a.path_probe RENAME TO moved_probe", "ALTER TABLE", ready=b"T")
    wire.query(projection + " WHERE id=1", rows=[("path_probe", second)], fields=fields, ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    wire.query(projection + " WHERE id=1", rows=[("path_probe", first)], fields=fields)
    for name, state in (("format_a.gone", "42P01"), ('"unfinished', "42602"),
                        ("-", "42P01"), ("4294967296", "42P01")):
        wire.query(f"SELECT '{name}'::pg_catalog.text::pg_catalog.regclass", state=state, tag=None)
    wire.query("SELECT '-'::varchar::pg_catalog.regclass", state="42P01", tag=None)
    wire.query("SELECT CAST('007' AS varchar)::pg_catalog.regclass::oid AS number",
               rows=[(numeric_name,)], fields=[("number", 26)])
    wire.query("SELECT '4294967296'::pg_catalog.regclass", state="22003", tag=None)
    wire.query("SELECT '-'::pg_catalog.regclass AS relation, '007'::pg_catalog.regclass::oid AS number",
               rows=[("-", "7")], fields=fields)
    wire.command("SET search_path=pg_catalog,format_b,format_a,public", "SET")


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
        probe = wire.send_statements("SELECT 'format_a.format_probe'::regclass::oid AS id")
        table_oid = int(client.data_row_values(probe)[0][0])
        columns_by_oid = (
            "SELECT a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod) AS declared "
            "FROM pg_catalog.pg_attribute a WHERE {equality} AND a.attnum>0 ORDER BY a.attnum"
        )
        fields = [("attname", 19), ("declared", 25)]
        for extended in (False, True):
            mode = Wire(client, sock, extended)
            mode.query(COLUMN_QUERY, rows=DECLARATIONS, fields=fields, tag=f"SELECT {len(DECLARATIONS)}")
            for equality in (
                f"a.attrelid='{table_oid}'::oid",
                f"'{table_oid}'::pg_catalog.oid=a.attrelid",
                f"a.attrelid=CAST('{table_oid}' AS oid)",
                f"a.attrelid=CAST('0{table_oid:o}' AS pg_catalog.oid)",
                f"a.attrelid=OID '0x{table_oid:x}'",
                f"a.attrelid={table_oid}::oid",
                "a.attrelid='format_a.format_probe'::regclass",
                "REGCLASS 'format_a.format_probe'=a.attrelid",
                "a.attrelid=CAST('format_a.format_probe' AS pg_catalog.regclass)::oid",
                "a.attrelid='format_a.format_probe'::text::regclass",
                "CAST(CAST('format_a.format_probe' AS text) AS pg_catalog.regclass)::oid=a.attrelid",
            ):
                mode.query(columns_by_oid.format(equality=equality), rows=DECLARATIONS,
                           fields=fields, tag=f"SELECT {len(DECLARATIONS)}")
            for value in ("NULL::oid", "'0'::oid", "4294967295::pg_catalog.oid"):
                mode.query(columns_by_oid.format(equality=f"a.attrelid={value}"), fields=fields, tag="SELECT 0")
            for value, state in (("CAST('08' AS pg_catalog.oid)", "22P02"),
                                 ("'4294967296'::oid", "22003")):
                mode.query(columns_by_oid.format(equality=f"a.attrelid={value}"), state=state, tag=None)
            mode.query(columns_by_oid.format(equality="false AND a.attrelid='format_a.absent'::regclass"),
                       state="42P01", tag=None)
        review.parameter(columns_by_oid.format(equality="a.attrelid=$1::oid"),
                         [str(table_oid)], [26], [list(row) for row in DECLARATIONS], [19, 25])
        review.parameter(columns_by_oid.format(equality="a.attrelid=$1::text::regclass"),
                         ["format_a.format_probe"], [25], [list(row) for row in DECLARATIONS], [19, 25])
        assert not review.failures
        if review.server:
            # An earlier error still belongs to the unfiltered source rows.
            earlier = "1/(CASE WHEN a.attrelid='1249'::oid THEN 0 ELSE 1 END)=1"
            wire.query(columns_by_oid.format(equality=f"{earlier} AND a.attrelid='{table_oid}'::oid"),
                       state="22012", tag=None)
            wire.query(columns_by_oid.format(equality=f"{earlier} AND a.attrelid='format_a.format_probe'::text::regclass"),
                       state="22012", tag=None)
            for value in (f"'{table_oid}'::oid", "'format_a.format_probe'::regclass",
                          "CAST('format_a.format_probe' AS pg_catalog.regclass)::oid",
                          "'format_a.format_probe'::text::regclass",
                          "CAST('format_a.format_probe'::text AS pg_catalog.regclass)::oid"):
                sql = columns_by_oid.format(equality=f"a.attrelid={value}")
                frames = wire.send_statements("EXPLAIN (FORMAT JSON, ANALYZE TRUE, TIMING FALSE) " + sql)
                assert not any(kind == b"E" for kind, _ in frames), frames
                plan = json.loads("\n".join(row[0].decode() for row in client.data_row_values(frames)))
                sources = [node["actualRows"] for node in plan_nodes(plan["plan"])
                           if node["nodeType"] == "TypedSource"]
                assert plan["actualRows"] == len(DECLARATIONS) and sources == [len(DECLARATIONS) + 6], plan
        check_prepared_regclass(wire, review.server is None)
        check_runtime_catalog_key(wire, review.server is None)
        check_qualified_regclass(wire, table_oid)

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
        wire.command("CREATE DOMAIN format_b.regclass AS pg_catalog.text", "CREATE DOMAIN")
        wire.command("SET search_path=format_b,pg_catalog,format_a,public", "SET")
        wire.query(columns_by_oid.format(equality="a.attrelid='format_a.format_probe'::regclass"),
                   state="42883", tag=None)
        wire.query(columns_by_oid.format(equality="a.attrelid='format_a.format_probe'::pg_catalog.regclass"),
                   rows=DECLARATIONS, fields=fields, tag=f"SELECT {len(DECLARATIONS)}")
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
        wire.query(columns_by_oid.format(equality="a.attrelid='pg_temp.format_temp_probe'::regclass"),
                   rows=[("id", "integer")], fields=fields)
        check_runtime_regclass(wire)

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
