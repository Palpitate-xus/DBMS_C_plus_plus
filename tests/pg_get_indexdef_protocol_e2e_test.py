#!/usr/bin/env python3
"""Check index definitions against real relation identities and stored index metadata."""

import os
from pathlib import Path

from compat.numeric_type_review import Review
from pg_foreign_key_catalog_protocol_e2e_test import relation_oid
from session_setting_functions_protocol_e2e_test import Wire


TABLE = 'idx_catalog."Audit Data"'
COMPOSITE = 'idx_catalog."Audit composite"'
BTREE = 'idx_catalog.payload_idx'
HASH = 'idx_catalog.flags_idx'


def setup(wire):
    for sql, tag in (
        ("CREATE SCHEMA idx_catalog", "CREATE SCHEMA"),
        ("CREATE SCHEMA idx_shadow", "CREATE SCHEMA"),
        (f'CREATE TABLE {TABLE} (k integer, "Other Field" integer, payload text, flag integer)', "CREATE TABLE"),
        ('CREATE TABLE idx_shadow."Audit Data" (k integer)', "CREATE TABLE"),
        (f'CREATE UNIQUE INDEX "Audit composite" ON {TABLE} (k, "Other Field") INCLUDE (payload)', "CREATE INDEX"),
        (f"CREATE INDEX payload_idx ON {TABLE} (payload) INCLUDE (k)", "CREATE INDEX"),
        (f"CREATE INDEX flags_idx ON {TABLE} USING hash (flag)", "CREATE INDEX"),
        (f"INSERT INTO {TABLE} VALUES (2,12,'other',3), (1,11,'first',5)", "INSERT 0 2"),
    ):
        wire.command(sql, tag)


def definition(wire, oid, expected, arguments="", ready=b"I"):
    wire.query(f"SELECT pg_catalog.pg_get_indexdef('{oid}'::oid{arguments}) AS v",
               rows=[(expected,)], fields=[("v", 25)], ready=ready)


def check_shadowed_catalog_functions(wire, extended, index):
    wire.command("CREATE FUNCTION idx_shadow.pg_get_indexdef(k oid, n integer, pretty boolean) "
                 "RETURNS integer[] LANGUAGE plpgsql AS $$BEGIN RETURN ARRAY[47,n]; END$$",
                 "CREATE FUNCTION")
    wire.command("CREATE FUNCTION idx_shadow.format_type(k oid, m integer) RETURNS integer[] "
                 "LANGUAGE plpgsql AS $$BEGIN RETURN ARRAY[53,m]; END$$", "CREATE FUNCTION")
    wire.command("SET search_path TO idx_shadow, pg_catalog, idx_catalog, public", "SET")

    for mode in (wire, extended):
        mode.query(
            "SELECT a.attnum, pg_get_indexdef(a.attrelid,a.attnum::integer,true) AS shadow, "
            "pg_catalog.pg_get_indexdef(a.attrelid,a.attnum,true) AS builtin "
            f"FROM pg_catalog.pg_attribute a WHERE a.attrelid='{index}' AND a.attnum>0 ORDER BY a.attnum",
            rows=[("1", "{47,1}", "k"), ("2", "{47,2}", '"Other Field"'),
                  ("3", "{47,3}", "payload")],
            fields=[("attnum", 21), ("shadow", 1007), ("builtin", 25)], tag="SELECT 3")
        mode.query("SELECT format_type(1043::oid,16) AS shadow, "
                   "pg_catalog.format_type(1043::oid,16) AS builtin",
                   rows=[("{53,16}", "character varying(12)")],
                   fields=[("shadow", 1007), ("builtin", 25)])
        mode.query(f"SELECT pg_catalog.pg_get_indexdef('{index}'::oid,NULL::integer,true) AS definition, "
                   "pg_catalog.format_type(23::oid,NULL::integer) AS type_name",
                   rows=[(None, "integer")], fields=[("definition", 25), ("type_name", 25)])
    wire.run_prepared(b"index_def", (index.encode(), b"2", b"t"), '"Other Field"')
    wire.command("SET search_path TO idx_shadow, idx_catalog, public", "SET")


def check_metadata_file_changes(wire, server):
    wire.command("CREATE TABLE idx_catalog.file_check(k integer)", "CREATE TABLE")
    wire.command("CREATE TABLE idx_catalog.file_donor(x integer)", "CREATE TABLE")
    wire.command("CREATE INDEX file_idx ON idx_catalog.file_check(k)", "CREATE INDEX")
    index = relation_oid(wire, "idx_catalog.file_idx")
    database = Path(server["dir"]) / "info"
    schema = database / "idx_catalog__file_check.stc"
    names = database / "idx_catalog__file_check.idxnames"
    sql = f"SELECT pg_catalog.pg_get_indexdef('{index}'::oid,1,true) AS v"

    definition(wire, index, "k", ", 1, true")
    definition(wire, index, "k", ", 1, true")

    original = schema.read_bytes()
    different = (database / "idx_catalog__file_donor.stc").read_bytes()
    assert len(original) == len(different) and original != different
    replacement = schema.with_suffix(".test-replacement")
    stat = schema.stat()
    replacement.write_bytes(different)
    os.utime(replacement, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    replacement.replace(schema)
    try:
        replaced = schema.stat()
        assert replaced.st_ino != stat.st_ino and replaced.st_mtime_ns == stat.st_mtime_ns
        wire.query(sql, tag=None, state="XX001")
    finally:
        schema.write_bytes(original)
    definition(wire, index, "k", ", 1, true")

    original = names.read_bytes()
    assert original == b"file_idx\tbtree\tk\n", original
    stat = names.stat()
    try:
        names.write_bytes(b"file_idx\tbtree\tx\n")
        os.utime(names, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        changed = names.stat()
        assert changed.st_ino == stat.st_ino and changed.st_mtime_ns == stat.st_mtime_ns
        assert changed.st_ctime_ns != stat.st_ctime_ns
        wire.query(sql, tag=None, state="XX001")
        names.unlink()
        wire.query(sql, tag=None, state="XX001")
        names.write_bytes(original + b"file_idx\tbtree\tx\n")
        definition(wire, index, "k", ", 1, true")
        definition(wire, index, "k", ", 1, true")
        # The first matching record still works in a large names file.
        names.write_bytes(original + b"x" * (256 * 1024))
        definition(wire, index, "k", ", 1, true")
    finally:
        names.write_bytes(original)
    definition(wire, index, "k", ", 1, true")


def main():
    review = Review("INDEX_DEFINITION")
    wire = Wire(review.client, review.sock)
    extended = Wire(review.client, review.sock, extended=True)
    try:
        setup(wire)
        composite = relation_oid(wire, COMPOSITE)
        btree = relation_oid(wire, BTREE)
        hashed = relation_oid(wire, HASH)
        table = relation_oid(wire, TABLE)
        full = f'CREATE UNIQUE INDEX "Audit composite" ON {TABLE} USING btree (k, "Other Field") INCLUDE (payload)'
        for mode in (wire, extended):
            definition(mode, composite, full)
            definition(mode, composite, full, ", 0, false")
            definition(mode, composite, '"Other Field"', ", 2, true")
            definition(mode, composite, "payload", ", 3, true")
            definition(mode, composite, "", ", 4, true")
            definition(mode, composite, "", ", -1, true")
            definition(mode, btree, f"CREATE INDEX payload_idx ON {TABLE} USING btree (payload) INCLUDE (k)")
            definition(mode, btree, "k", ", 2, false")
            definition(mode, hashed, f"CREATE INDEX flags_idx ON {TABLE} USING hash (flag)")
            definition(mode, table, None)
            definition(mode, 0, None)
            definition(mode, composite, None, ", 1, NULL")
            mode.query("SELECT pg_catalog.pg_get_indexdef(NULL) AS v", rows=[(None,)], fields=[("v", 25)])
            mode.query("SELECT pg_catalog.pg_get_indexdef('1'::text)", tag=None, state="42883")
            mode.query("SELECT pg_catalog.pg_get_indexdef(c.oid, 0, false) AS v FROM pg_catalog.pg_class c "
                       f"WHERE c.oid='{composite}'", rows=[(full,)], fields=[("v", 25)])

        wire.command("SET search_path TO idx_catalog, public", "SET")
        visible = full.replace(f" ON {TABLE} ", ' ON "Audit Data" ')
        definition(extended, composite, visible, ", 0, true")
        definition(wire, composite, full)
        wire.prepare(b"index_def", "SELECT pg_catalog.pg_get_indexdef($1, $2, $3) AS v",
                     (26, 23, 16), (26, 23, 16))
        wire.run_prepared(b"index_def", (composite.encode(), b"0", b"t"), visible)
        wire.run_prepared(b"index_def", (btree.encode(), b"2", b"f"), "k")
        wire.run_prepared(b"index_def", (b"0", b"0", b"t"), None)
        messages = wire.exchange(wire.bind(b"index_def", (composite.encode(), b"0", b"t"), 1) +
                                 wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert wire.client.row_description_fields(messages) == [(b"v", 0, 0, 25, -1, -1, 1)], messages
        assert wire.client.data_row_values(messages) == [[visible.encode()]], messages
        assert messages[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")], messages
        wire.command("SET search_path TO idx_shadow, idx_catalog, public", "SET")
        definition(extended, composite, full, ", 0, true")
        check_shadowed_catalog_functions(wire, extended, composite)

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f'ALTER TABLE {TABLE} RENAME COLUMN "Other Field" TO "Current Field"', "ALTER TABLE", b"T")
        definition(wire, composite, full.replace('"Other Field"', '"Current Field"'), ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        definition(wire, composite, full)

        wire.command(f'ALTER TABLE {TABLE} RENAME TO "Current Data"', "ALTER TABLE")
        assert relation_oid(wire, BTREE) == btree
        definition(extended, btree, 'CREATE INDEX payload_idx ON idx_catalog."Current Data" USING btree (payload) INCLUDE (k)')
        wire.command(f"DROP INDEX {BTREE}", "DROP INDEX")
        definition(wire, btree, None)

        wire.command("CREATE TABLE idx_catalog.marker_values (a integer, b integer, label text)", "CREATE TABLE")
        wire.command("CREATE INDEX marker_idx ON idx_catalog.marker_values (a,b) WHERE label=':INCLUDE:label'",
                     "CREATE INDEX")
        marker = relation_oid(wire, "idx_catalog.marker_idx")
        definition(wire, marker, "a", ", 1, true")
        definition(extended, marker, "", ", 3, true")
        if review.server is not None:
            wire.query(f"SELECT pg_catalog.pg_get_indexdef('{marker}'::oid)", tag=None, state="0A000")
            check_metadata_file_changes(wire, review.server)
        print("INDEX_DEFINITION_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
