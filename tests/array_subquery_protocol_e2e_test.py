#!/usr/bin/env python3
"""Verify ARRAY(SELECT ...) on ordinary, correlated, empty and array-valued inputs."""

import struct

from compat.numeric_type_review import Review
from pg_foreign_key_catalog_protocol_e2e_test import value
from session_setting_functions_protocol_e2e_test import Wire


def setup(wire):
    wire.command("CREATE TABLE arr_children(k integer PRIMARY KEY, group_id integer, v text)", "CREATE TABLE")
    wire.command("INSERT INTO arr_children VALUES (4,2,'last'), (2,1,NULL), "
                 "(1,1,'a,b'), (3,1,'NULL'), (5,2,'')", "INSERT 0 5")
    wire.command("CREATE TABLE arr_groups(id integer PRIMARY KEY)", "CREATE TABLE")
    wire.command("INSERT INTO arr_groups VALUES (3), (2), (1)", "INSERT 0 3")


CORRELATED = ("SELECT g.id, ARRAY(SELECT c.v FROM arr_children c WHERE c.group_id=g.id ORDER BY c.k) AS a "
              "FROM arr_groups g ORDER BY g.id")
CORRELATED_ROWS = [("1", '{"a,b",NULL,"NULL"}'), ("2", '{last,""}'), ("3", '{}')]


def binary_result(wire, parameter_oids):
    label = b"array_subquery_explicit" if parameter_oids else b"array_subquery_inferred"
    sql = "SELECT ARRAY(SELECT k FROM arr_children WHERE group_id=$1 ORDER BY k DESC) AS a"
    frames = wire.exchange(wire.parse(sql, label, parameter_oids) + wire.packet(b"D", b"S" + label + b"\0") +
                           wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
    assert frames[1] == (b"t", struct.pack("!HI", 1, 23)), frames
    declared = (b"a", 0, 0, 1007, -1, -1)
    assert wire.client.row_description_fields(frames) == [(*declared, 0)], frames
    for group, items in ((b"1", (3, 2, 1)), (b"3", ()), (None, ())):
        frames = wire.exchange(wire.bind(label, (group,), 1) + wire.describe_portal() + wire.execute() +
                               wire.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"2TDCZ", frames
        assert wire.client.row_description_fields(frames) == [(*declared, 1)], frames
        expected = struct.pack("!III", int(bool(items)), 0, 23)
        if items:
            expected += struct.pack("!ii", len(items), 1)
            expected += b"".join(struct.pack("!ii", 4, item) for item in items)
        assert wire.client.data_row_values(frames) == [[expected]], frames
        assert frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")], frames


def main():
    review = Review("ARRAY_SUBQUERY")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        for mode in (wire, extended):
            for expression, expected, oid in (
                ("ARRAY(SELECT 1)", "{1}", 1007),
                ("ARRAY(SELECT 2+3)", "{5}", 1007),
                ("ARRAY /* nested /* ( */ comment */ (SELECT 2 /* ) */)", "{2}", 1007),
                ("ARRAY(SELECT k FROM arr_children ORDER BY k DESC LIMIT 3 OFFSET 1)", "{4,3,2}", 1007),
                ("ARRAY(SELECT k FROM arr_children WHERE false)", "{}", 1007),
                ("ARRAY(SELECT NULL)", "{NULL}", 1009),
                ("ARRAY(SELECT 'hello')", "{hello}", 1009),
                ("ARRAY(VALUES (2), (2), (3))", "{2,2,3}", 1007),
                ("ARRAY((SELECT 1 UNION ALL SELECT 2))", "{1,2}", 1007),
                ("ARRAY(SELECT ARRAY(SELECT k FROM arr_children WHERE group_id=1 ORDER BY k))", "{{1,2,3}}", 1007),
                ("ARRAY(VALUES ('[0:1]={1,2}'::integer[]), ('[0:1]={3,4}'::integer[]))",
                 "[1:2][0:1]={{1,2},{3,4}}", 1007),
                ("ARRAY(SELECT NULL::integer[] WHERE false)", "{}", 1007),
                ("ARRAY(SELECT 1::bigint)", "{1}", 1016),
                ("ARRAY(SELECT ROW(1,'test'::text))", '{"(1,test)"}', 2287),
                ("ARRAY(WITH t(v) AS (VALUES (7), (4)) SELECT v FROM t ORDER BY v)", "{4,7}", 1007),
            ):
                mode.query(f"SELECT {expression}", rows=[(expected,)], fields=[("array", oid)])
            mode.query(CORRELATED, rows=CORRELATED_ROWS, fields=[("id", 23), ("a", 1009)], tag="SELECT 3")
            mode.query("SELECT array_to_string(ARRAY(SELECT v FROM arr_children ORDER BY k), ',', '(missing)') AS joined",
                       rows=[("a,b,(missing),NULL,last,",)], fields=[("joined", 25)])
            mode.query("VALUES (ARRAY(SELECT k FROM arr_children WHERE group_id=1 ORDER BY k))",
                       rows=[("{1,2,3}",)], fields=[("column1", 1007)])
            mode.query("SELECT 3 = ANY(ARRAY(SELECT id FROM arr_groups)) AS found, "
                       "4 > ALL(ARRAY(SELECT id FROM arr_groups)) AS above, "
                       "1 = ANY(ARRAY(SELECT NULL::integer)) AS unknown, "
                       "1 = ANY(SELECT k FROM arr_children WHERE group_id=1) AS direct",
                       rows=[("t", "t", None, "t")],
                       fields=[("found", 16), ("above", 16), ("unknown", 16), ("direct", 16)])
            mode.query("SELECT -5 = ANY(ARRAY(SELECT 10/(k-3) FROM arr_children ORDER BY k))",
                       tag=None, state="22012")
        for parameter_oids in ((23,), ()):
            binary_result(wire, parameter_oids)

        for expression, state in (
            ("ARRAY(1)", "42601"), ("ARRAY(SELECT 1,2)", "42601"),
            ("ARRAY(SELECT no_such_column FROM arr_children)", "42703"),
            ("ARRAY(SELECT NULL::integer[])", "22004"),
            ("ARRAY(SELECT '{}'::integer[])", "2202E"),
            ("ARRAY(VALUES ('{1,2}'::integer[]), ('{3}'::integer[]))", "2202E"),
            ("ARRAY(VALUES ('[0:1]={1,2}'::integer[]), ('{3,4}'::integer[]))", "2202E"),
            ("ARRAY(SELECT 10/(k-3) FROM arr_children ORDER BY k)", "22012"),
            ("ARRAY(SELECT '{{{{{{1}}}}}}'::integer[])", "54000"),
            ("EXISTS(ARRAY(SELECT k FROM arr_children))", "42601"),
        ):
            for mode in (wire, extended):
                mode.query(f"SELECT {expression}", tag=None, state=state)

        wire.command("CREATE ROLE array_reader_a NOLOGIN", "CREATE ROLE")
        wire.command("CREATE ROLE array_reader_z NOLOGIN", "CREATE ROLE")
        first = value(wire, "SELECT oid FROM pg_catalog.pg_roles WHERE rolname='array_reader_a'")
        last = value(wire, "SELECT oid FROM pg_catalog.pg_roles WHERE rolname='array_reader_z'")
        wire.query("SELECT ARRAY(SELECT rolname FROM pg_catalog.pg_roles "
                   f"WHERE oid = ANY(ARRAY['{last}'::oid,'{first}'::oid]) ORDER BY 1) AS a",
                   rows=[("{array_reader_a,array_reader_z}",)], fields=[("a", 1003)])
        for mode in (wire, extended):
            mode.query("SELECT CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string("
                       "ARRAY(SELECT rolname FROM pg_catalog.pg_roles WHERE oid = ANY(pol.polroles) ORDER BY 1), ',') "
                       f"END AS roles FROM (VALUES (1, ARRAY['{last}'::oid,'{first}'::oid]), "
                       "(2, '{0}'::oid[]), (3, '{}'::oid[])) AS pol(id,polroles) ORDER BY pol.id",
                       rows=[("array_reader_a,array_reader_z",), (None,), ("",)],
                       fields=[("roles", 25)], tag="SELECT 3")

        wire.command("CREATE TABLE arr_saved (id integer PRIMARY KEY, a integer[])", "CREATE TABLE")
        wire.query("INSERT INTO arr_saved VALUES (1, ARRAY(SELECT k FROM arr_children WHERE group_id=1 ORDER BY k)) "
                   "RETURNING a", rows=[("{1,2,3}",)], fields=[("a", 1007)], tag="INSERT 0 1")
        wire.query("UPDATE arr_saved SET a=ARRAY(SELECT k FROM arr_children WHERE group_id=arr_saved.id+1 ORDER BY k) "
                   "WHERE id=1 RETURNING a", rows=[("{4,5}",)], fields=[("a", 1007)], tag="UPDATE 1")
        wire.query("SELECT a FROM arr_saved", rows=[("{4,5}",)], fields=[("a", 1007)])

        wire.command("CREATE SEQUENCE array_subquery_seq", "CREATE SEQUENCE")
        repeated = "SELECT ARRAY(SELECT nextval('array_subquery_seq')) AS a FROM arr_groups ORDER BY id"
        for expected in ("{1}", "{2}"):
            wire.query(repeated, rows=[(expected,)] * 3, fields=[("a", 1016)], tag="SELECT 3")
        wire.query("SELECT CASE WHEN false THEN ARRAY(SELECT nextval('array_subquery_seq')) ELSE ARRAY(SELECT 9::bigint) END AS a",
                   rows=[("{9}",)], fields=[("a", 1016)])
        wire.query("SELECT nextval('array_subquery_seq') AS a", rows=[("3",)], fields=[("a", 20)])
        wire.query("SELECT (SELECT k FROM arr_children)", tag=None, state="21000")
        print("ARRAY_SUBQUERY_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
