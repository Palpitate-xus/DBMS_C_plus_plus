#!/usr/bin/env python3
"""Check partition ancestry, relation identity and the psql incoming FK query."""

import struct
import sys

from compat.numeric_type_review import Review
from pg_foreign_key_catalog_protocol_e2e_test import relation_oid, setup as setup_foreign_keys
from session_setting_functions_protocol_e2e_test import Wire


FUNCTION = "pg_catalog.pg_partition_ancestors"
REGCLASS = 2205
INCOMING = ("SELECT conname, conrelid::pg_catalog.regclass AS ontable, "
            "pg_catalog.pg_get_constraintdef(oid, true) AS condef "
            "FROM pg_catalog.pg_constraint c "
            "WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{oid}') "
            "UNION ALL VALUES ('{oid}'::pg_catalog.regclass)) "
            "AND contype = 'f' AND conparentid = 0 ORDER BY conname")


def setup(wire):
    wire.command("CREATE SCHEMA part_n", "CREATE SCHEMA")
    wire.command("CREATE TABLE public.part_root (id integer)", "CREATE TABLE")
    wire.command("CREATE TABLE part_n.part_root (id integer) PARTITION BY RANGE (id)", "CREATE TABLE")
    wire.command("CREATE TABLE part_n.list_root (id integer) PARTITION BY LIST (id)", "CREATE TABLE")
    wire.command("CREATE TABLE part_n.hash_root (id integer) PARTITION BY HASH (id)", "CREATE TABLE")
    wire.command("CREATE TABLE part_n.inherited () INHERITS (public.part_root)", "CREATE TABLE")


def ancestors(wire, argument, expected, ready=b"I"):
    sql = f"SELECT {FUNCTION}({argument}) AS a"
    wire.query(sql, rows=[(name,) for name in expected], fields=[("a", REGCLASS)],
               tag=f"SELECT {len(expected)}", ready=ready)


def incoming_foreign_keys(wire):
    setup_foreign_keys(wire)
    fields = [("conname", 19), ("ontable", REGCLASS), ("condef", 25)]
    parent = relation_oid(wire, "fk_parent.accounts")
    owner = relation_oid(wire, "fk_child.payments")
    expected = [("invoice,fk", "fk_child.payments", "FOREIGN KEY (a, b) REFERENCES "
                 "fk_parent.accounts(a, b) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED")]
    for mode in (wire, Wire(wire.client, wire.sock, extended=True)):
        mode.query(INCOMING.format(oid=parent), rows=expected, fields=fields)
        mode.query(INCOMING.format(oid=owner), fields=fields, tag="SELECT 0")


def binary_ancestry(wire, oid):
    name = b"partition_ancestors_by_oid"
    sql = f"SELECT {FUNCTION}($1) AS a"
    messages = wire.exchange(wire.parse(sql, name, (26,)) +
                             wire.packet(b"D", b"S" + name + b"\0") + wire.packet(b"S"))
    assert b"".join(kind for kind, _ in messages) == b"1tTZ", (sql, messages)
    assert messages[1] == (b"t", struct.pack("!HI", 1, 26)), messages
    description = (b"a", 0, 0, REGCLASS, 4, -1)
    assert wire.client.row_description_fields(messages) == [(*description, 0)], messages
    for argument, expected in ((oid, [[struct.pack("!I", int(oid))]]), ("0", []), (None, [])):
        messages = wire.exchange(wire.bind(name, (None if argument is None else argument.encode(),), 1) +
                                 wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in messages) == b"2T" + b"D" * len(expected) + b"CZ", messages
        assert wire.client.row_description_fields(messages) == [(*description, 1)], messages
        assert wire.client.data_row_values(messages) == expected, messages
        assert messages[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")], messages


def main():
    review = Review("PG_PARTITION_ANCESTORS")
    wire = Wire(review.client, review.sock)
    try:
        setup(wire)
        root = relation_oid(wire, "part_n.part_root")
        for name in ("part_root", "list_root", "hash_root"):
            ancestors(wire, f"'part_n.{name}'", [f"part_n.{name}"])
        for expression in ("'part_n.part_root'::regclass", "'part_n.part_root'::text",
                           "'part_n.part_root'::varchar", f"'{root}'::oid", root,
                           f"{root}::bigint", f"partitionid => '{root}'"):
            ancestors(wire, expression, ["part_n.part_root"])
        for expression in ("'part_root'", "'part_n.inherited'", "'pg_catalog.pg_class'", "NULL",
                           "NULL::regclass", "0", "0::smallint", "-1", "4294967295::oid"):
            ancestors(wire, expression, [])
        for expression, state in (("'missing_relation'", "42P01"), ("'4294967296'", "22003"),
                                  ("true", "42883"), ("0,0", "42883"), ("", "42883"),
                                  ("bad_name => 0", "42883"), ("'pg_class'::name", "42883")):
            wire.query(f"SELECT {FUNCTION}({expression})", tag=None, state=state)

        wire.query(f"SELECT * FROM {FUNCTION}('part_n.part_root') WITH ORDINALITY",
                   fields=[("relid", REGCLASS), ("ordinality", 20)], rows=[("part_n.part_root", "1")])
        wire.query(f"SELECT a FROM ROWS FROM ({FUNCTION}('part_n.part_root')) AS p(a)",
                   fields=[("a", REGCLASS)], rows=[("part_n.part_root",)])
        wire.query(f"SELECT {FUNCTION}('part_n.part_root')", fields=[("pg_partition_ancestors", REGCLASS)],
                   rows=[("part_n.part_root",)])
        union = (f"SELECT {FUNCTION}('part_n.part_root') AS a "
                 "UNION ALL VALUES ('public.part_root'::regclass)")
        wire.query(union, fields=[("a", REGCLASS)], rows=[("part_n.part_root",), ("part_root",)], tag="SELECT 2")
        wire.query(union + " OFFSET 1 LIMIT 1", fields=[("a", REGCLASS)], rows=[("part_root",)])

        wire.command("SET search_path=part_n,public", "SET")
        ancestors(wire, "'part_root'", ["part_root"])
        ancestors(wire, "'public.part_root'", [])
        wire.command("BEGIN", "BEGIN", b"T")
        wire.command("ALTER TABLE part_root RENAME TO renamed_root", "ALTER TABLE", b"T")
        ancestors(wire, f"{root}::oid", ["renamed_root"], b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        ancestors(wire, f"{root}::oid", ["part_root"])
        wire.command("RESET search_path", "RESET")

        wire.command("CREATE FUNCTION public.pg_partition_ancestors(p text) "
                     "RETURNS text LANGUAGE SQL AS $$SELECT p$$", "CREATE FUNCTION")
        wire.query("SELECT pg_partition_ancestors('chosen'::text) AS a", rows=[("chosen",)], fields=[("a", 25)])
        wire.query("SELECT pg_partition_ancestors('part_n.part_root'::regclass) AS a",
                   rows=[("part_n.part_root",)], fields=[("a", REGCLASS)])

        binary_ancestry(wire, root)
        incoming_foreign_keys(wire)

        if sys.argv[1:] == ["--reference18"]:
            wire.command("CREATE TABLE part_n.part_mid PARTITION OF part_n.part_root "
                         "FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id)", "CREATE TABLE")
            wire.command("CREATE TABLE part_n.part_leaf PARTITION OF part_n.part_mid "
                         "FOR VALUES FROM (0) TO (10)", "CREATE TABLE")
            ancestors(wire, "'part_n.part_leaf'", ["part_n.part_leaf", "part_n.part_mid", "part_n.part_root"])
        else:
            wire.query("CREATE TABLE part_n.part_leaf PARTITION OF part_n.part_root "
                       "FOR VALUES FROM (0) TO (10)", state="0A000", tag=None)
            wire.command("CREATE TABLE part_n.part_leaf (id integer)", "CREATE TABLE")
            wire.command("SET search_path=part_n,public", "SET")
            wire.query("ALTER TABLE part_root ATTACH PARTITION part_leaf "
                       "FOR VALUES FROM (0) TO (10)", state="0A000", tag=None)
            ancestors(wire, "'part_n.part_leaf'", [])
            wire.command("RESET search_path", "RESET")
            ancestors(wire, f"{root}::oid", ["part_n.part_root"])
        wire.command("DROP TABLE part_n.list_root", "DROP TABLE")
        wire.query(f"SELECT {FUNCTION}('part_n.list_root')", state="42P01", tag=None)
        print("PG_PARTITION_ANCESTORS_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
