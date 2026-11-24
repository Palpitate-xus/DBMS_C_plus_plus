#!/usr/bin/env python3
"""Verify named index columns and the SQL used to describe them in psql."""

import struct

from compat.numeric_type_review import Review
from pg_get_indexdef_protocol_e2e_test import relation_oid
from session_setting_functions_protocol_e2e_test import Wire


TABLE = "index_columns.items"
COVER = "index_columns.cover_idx"
HASH = "index_columns.hash_idx"
SINGLE = "index_columns.single_idx"
ATTRIBUTE_FIELDS = [
    ("attname", 19), ("attnum", 21), ("atttypid", 26), ("atttypmod", 23),
    ("attlen", 21), ("attndims", 21), ("attbyval", 16), ("attalign", 18),
    ("attstorage", 18), ("attnotnull", 16), ("atthasdef", 16), ("attidentity", 18),
    ("attgenerated", 18), ("attisdropped", 16), ("attislocal", 16), ("attinhcount", 21),
    ("attcollation", 26), ("attstattarget", 21),
]
ATTRIBUTE_SQL = "SELECT " + ", ".join(field for field, _ in ATTRIBUTE_FIELDS) + (
    " FROM pg_catalog.pg_attribute WHERE attrelid='{index}' ORDER BY attnum")
DESCRIBE_SQL = (
    "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), "
    "CASE WHEN a.attnum <= (SELECT i.indnkeyatts FROM pg_catalog.pg_index i "
    "WHERE i.indexrelid = '{index}') THEN 'yes' ELSE 'no' END AS is_key, "
    "pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef "
    "FROM pg_catalog.pg_attribute a WHERE a.attrelid='{index}' AND a.attnum>0 "
    "AND NOT a.attisdropped ORDER BY a.attnum"
)
DESCRIBE_FIELDS = [("attname", 19), ("format_type", 25), ("is_key", 25), ("indexdef", 25)]
COVER_DESCRIPTION = (
    ("label", "character varying(12)", "yes", "label"),
    ("qty", "bigint", "yes", "qty"),
    ("note", "text", "no", "note"),
    ("item", "integer", "no", "item"),
    ("codes", "integer[]", "no", "codes"),
)


def setup(wire):
    wire.command("CREATE SCHEMA index_columns", "CREATE SCHEMA")
    wire.command(f'CREATE TABLE {TABLE}(item integer NOT NULL DEFAULT 1, qty bigint, '
                 'label varchar(12) COLLATE "C", note text COLLATE "C", codes integer[], flag integer)',
                 "CREATE TABLE")
    wire.command(f"INSERT INTO {TABLE} VALUES (3,7,'sample','present',ARRAY[2,4],1)", "INSERT 0 1")
    wire.command(f"CREATE INDEX cover_idx ON {TABLE}(label,qty) INCLUDE(note,item,codes) WHERE item>0",
                 "CREATE INDEX")
    wire.command(f"CREATE INDEX hash_idx ON {TABLE} USING hash(label)", "CREATE INDEX")
    wire.command(f"CREATE INDEX single_idx ON {TABLE}(flag) INCLUDE(flag,flag)", "CREATE INDEX")


def attr(name, ordinal, typ, length, byval, align, storage, *, mod=-1, collation=0, dims=0):
    return tuple(map(str, (name, ordinal, typ, mod, length, dims, byval, align, storage,
                           "f", "f", "", "", "f", "t", 0, collation))) + (None,)


COVER_ATTRIBUTES = (
    attr("label", 1, 1043, -1, "f", "i", "x", mod=16, collation=950),
    attr("qty", 2, 20, 8, "t", "d", "p"),
    attr("note", 3, 25, -1, "f", "i", "x"),
    attr("item", 4, 23, 4, "t", "i", "p"),
    attr("codes", 5, 1007, -1, "f", "i", "x", dims=1),
)


def check_attributes(wire, index, rows, ready=b"I"):
    wire.query(ATTRIBUTE_SQL.format(index=index), rows=rows, fields=ATTRIBUTE_FIELDS,
               tag=f"SELECT {len(rows)}", ready=ready)


def describe(wire, index, rows, ready=b"I"):
    wire.query(DESCRIBE_SQL.format(index=index), rows=rows, fields=DESCRIBE_FIELDS,
               tag=f"SELECT {len(rows)}", ready=ready)


def main():
    review = Review("INDEX_ATTRIBUTE")
    wire = Wire(review.client, review.sock)
    extended = Wire(review.client, review.sock, extended=True)
    try:
        setup(wire)
        cover, hashed, single = (relation_oid(wire, name) for name in (COVER, HASH, SINGLE))
        for mode in (wire, extended):
            check_attributes(mode, cover, COVER_ATTRIBUTES)
            describe(mode, cover, COVER_DESCRIPTION)
            check_attributes(mode, hashed, [attr("label", 1, 23, 4, "t", "i", "p", collation=950)])
            describe(mode, hashed, [("label", "integer", "yes", "label")])
            check_attributes(mode, single, [attr(name, ordinal, 23, 4, "t", "i", "p")
                                           for ordinal, name in enumerate(("flag", "flag1", "flag2"), 1)])
            mode.query(f"SELECT relnatts FROM pg_catalog.pg_class WHERE oid='{cover}'",
                       rows=[("5",)], fields=[("relnatts", 21)])
            mode.query(f"SELECT indnatts, indnkeyatts FROM pg_catalog.pg_index WHERE indexrelid='{cover}'",
                       rows=[("5", "2")], fields=[("indnatts", 21), ("indnkeyatts", 21)])

        sql = "SELECT attnum, atttypid FROM pg_catalog.pg_attribute WHERE attrelid=$1 ORDER BY attnum"
        frames = extended.exchange(extended.parse(sql, oids=(26,)) +
                                   extended.bind(arguments=(cover.encode(),), result_format=1) +
                                   extended.describe_portal() + extended.execute() + extended.packet(b"S"))
        assert wire.client.row_description_fields(frames) == [
            (b"attnum", 1249, 5, 21, 2, -1, 1), (b"atttypid", 1249, 3, 26, 4, -1, 1)], frames
        assert wire.client.data_row_values(frames) == [
            [struct.pack("!h", i), struct.pack("!I", oid)]
            for i, oid in enumerate((1043, 20, 25, 23, 1007), 1)], frames
        assert frames[-2:] == [(b"C", b"SELECT 5\0"), (b"Z", b"I")], frames

        wire.command(f"ALTER TABLE {TABLE} RENAME COLUMN qty TO stock", "ALTER TABLE")
        assert relation_oid(wire, COVER) == cover
        check_attributes(extended, cover, COVER_ATTRIBUTES)
        renamed = ("qty", "bigint", "yes", "stock")
        describe(extended, cover, (COVER_DESCRIPTION[0], renamed, *COVER_DESCRIPTION[2:]))

        wire.command(f"ALTER TABLE {TABLE} ALTER COLUMN stock TYPE smallint", "ALTER TABLE")
        rebuilt = relation_oid(wire, COVER)
        assert rebuilt != cover and relation_oid(wire, HASH) == hashed
        check_attributes(extended, cover, ())
        after_type = (COVER_ATTRIBUTES[0], attr("stock", 2, 21, 2, "t", "s", "p"), *COVER_ATTRIBUTES[2:])
        check_attributes(extended, rebuilt, after_type)
        describe(wire, rebuilt, (COVER_DESCRIPTION[0], ("stock", "smallint", "yes", "stock"),
                                 *COVER_DESCRIPTION[2:]))

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f"ALTER TABLE {TABLE} DROP COLUMN note", "ALTER TABLE", b"T")
        check_attributes(wire, rebuilt, (), ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        check_attributes(extended, rebuilt, after_type)
        wire.command(f"ALTER TABLE {TABLE} DROP COLUMN note", "ALTER TABLE")
        check_attributes(extended, rebuilt, ())
        assert relation_oid(wire, HASH) == hashed
        wire.query(f"SELECT stock, label FROM {TABLE}", rows=[("7", "sample")],
                   fields=[("stock", 21), ("label", 1043)])
        wire.command(f"DROP INDEX {HASH}", "DROP INDEX")
        check_attributes(extended, hashed, ())

        wire.command("CREATE TABLE index_columns.gated(k integer, gate integer)", "CREATE TABLE")
        wire.command("CREATE INDEX gated_idx ON index_columns.gated(k) WHERE gate > 0", "CREATE INDEX")
        gated = relation_oid(wire, "index_columns.gated_idx")
        wire.command("ALTER TABLE index_columns.gated ALTER COLUMN gate TYPE bigint", "ALTER TABLE")
        gate_rebuilt = relation_oid(wire, "index_columns.gated_idx")
        assert gate_rebuilt != gated
        check_attributes(extended, gated, ())
        check_attributes(extended, gate_rebuilt, [attr("k", 1, 23, 4, "t", "i", "p")])
        wire.command("ALTER TABLE index_columns.gated DROP COLUMN gate", "ALTER TABLE")
        check_attributes(extended, gate_rebuilt, ())

        wire.command("CREATE TEMP TABLE temp_index_items(k integer, payload text)", "CREATE TABLE")
        wire.command("CREATE INDEX temp_cover ON temp_index_items(k) INCLUDE(payload)", "CREATE INDEX")
        temp = relation_oid(wire, "pg_temp.temp_cover")
        describe(extended, temp, [("k", "integer", "yes", "k"), ("payload", "text", "no", "payload")])
        wire.command("DROP TABLE temp_index_items", "DROP TABLE")
        check_attributes(extended, temp, ())
        print("INDEX_ATTRIBUTE_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
