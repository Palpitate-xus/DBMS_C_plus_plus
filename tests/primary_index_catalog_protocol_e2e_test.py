#!/usr/bin/env python3
"""Check durable catalog identities for the indexes maintained by ordinary primary keys."""

from compat.numeric_type_review import Review
from pg_get_indexdef_protocol_e2e_test import definition, relation_oid
from pg_index_catalog_protocol_e2e_test import FIELDS, SELECT, data
from session_setting_functions_protocol_e2e_test import Wire


TABLE = "pk_catalog.accounts"
INDEX = "pk_catalog.accounts_key"


def setup(wire):
    wire.command("CREATE SCHEMA pk_catalog", "CREATE SCHEMA")
    wire.command(f"CREATE TABLE {TABLE} (a integer, b integer, payload text, "
                 "CONSTRAINT accounts_key PRIMARY KEY (b,a))", "CREATE TABLE")
    wire.command(f"INSERT INTO {TABLE} VALUES (1,2,'one'), (3,2,'two')", "INSERT 0 2")


def primary_data(index, owner, key_count):
    row = data(index, owner, key_count, key_count, "t")
    return (*row[:6], "t", *row[7:])


def check_index(wire, index, owner, key_count, ready=b"I"):
    wire.query(f"{SELECT} WHERE indexrelid='{index}'", rows=[primary_data(index, owner, key_count)],
               fields=FIELDS, ready=ready)


def no_index(wire, index, ready=b"I"):
    wire.query(f"{SELECT} WHERE indexrelid='{index}'", fields=FIELDS, tag="SELECT 0", ready=ready)


def check_columns(wire, index, rows):
    wire.query("SELECT attname, attnum, atttypid FROM pg_catalog.pg_attribute "
               f"WHERE attrelid='{index}' AND attnum>0 ORDER BY attnum", rows=rows,
               fields=[("attname", 19), ("attnum", 21), ("atttypid", 26)], tag=f"SELECT {len(rows)}")


def main():
    review = Review("PRIMARY_INDEX")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        owner = relation_oid(wire, TABLE)
        index = relation_oid(wire, INDEX)
        assert index != owner
        for mode in (wire, extended):
            check_index(mode, index, owner, 2)
            definition(mode, index, f"CREATE UNIQUE INDEX accounts_key ON {TABLE} USING btree (b, a)")
            definition(mode, index, "b", ", 1, true")
            check_columns(mode, index, [("b", "1", "23"), ("a", "2", "23")])
        wire.query(f"DROP INDEX {INDEX}", tag=None, state="2BP01")
        check_index(wire, index, owner, 2)

        wire.command(f"ALTER TABLE {TABLE} RENAME COLUMN b TO changed", "ALTER TABLE")
        wire.command(f"ALTER TABLE {TABLE} RENAME CONSTRAINT accounts_key TO renamed_key", "ALTER TABLE")
        assert relation_oid(wire, "pk_catalog.renamed_key") == index
        definition(extended, index, f"CREATE UNIQUE INDEX renamed_key ON {TABLE} USING btree (changed, a)")
        wire.command(f"ALTER TABLE {TABLE} RENAME TO new_accounts", "ALTER TABLE")
        wire.command("CREATE SCHEMA pk_moved", "CREATE SCHEMA")
        wire.command("ALTER TABLE pk_catalog.new_accounts SET SCHEMA pk_moved", "ALTER TABLE")
        moved = "pk_moved.new_accounts"
        assert relation_oid(wire, moved) == owner
        assert relation_oid(wire, "pk_moved.renamed_key") == index
        definition(wire, index, f"CREATE UNIQUE INDEX renamed_key ON {moved} USING btree (changed, a)")

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f"ALTER TABLE {moved} DROP CONSTRAINT renamed_key", "ALTER TABLE", b"T")
        no_index(wire, index, b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        check_index(extended, index, owner, 2)

        wire.command(f"ALTER TABLE {moved} ALTER COLUMN changed TYPE bigint", "ALTER TABLE")
        rebuilt = relation_oid(wire, "pk_moved.renamed_key")
        assert rebuilt not in (owner, index)
        no_index(wire, index)
        check_index(extended, rebuilt, owner, 2)
        check_columns(extended, rebuilt, [("changed", "1", "20"), ("a", "2", "23")])
        wire.query(f"SELECT a, payload FROM {moved} WHERE changed=2 AND a=3",
                   rows=[("3", "two")], fields=[("a", 23), ("payload", 25)])

        wire.command(f"ALTER TABLE {moved} DROP CONSTRAINT renamed_key", "ALTER TABLE")
        no_index(wire, rebuilt)
        wire.command(f"ALTER TABLE {moved} ADD CONSTRAINT renamed_key PRIMARY KEY (a)", "ALTER TABLE")
        replacement = relation_oid(wire, "pk_moved.renamed_key")
        assert replacement not in (owner, index, rebuilt)
        check_index(extended, replacement, owner, 1)
        wire.command(f"DROP TABLE {moved}", "DROP TABLE")
        no_index(wire, replacement)

        wire.command("CREATE TABLE pk_catalog.inline_key (a integer CONSTRAINT declared_key PRIMARY KEY)", "CREATE TABLE")
        inline_owner = relation_oid(wire, "pk_catalog.inline_key")
        inline_index = relation_oid(wire, "pk_catalog.declared_key")
        check_index(wire, inline_index, inline_owner, 1)
        wire.query("CREATE TABLE pk_catalog.conflict (a integer, CONSTRAINT declared_key PRIMARY KEY (a))",
                   tag=None, state="42P07")
        wire.query("SELECT * FROM pk_catalog.conflict", tag=None, state="42P01")
        wire.command("CREATE TABLE pk_catalog.conflict (a integer)", "CREATE TABLE")
        check_index(wire, inline_index, inline_owner, 1)

        wire.command("CREATE TABLE pk_catalog.numbered_pkey (a integer)", "CREATE TABLE")
        wire.command("CREATE TABLE pk_catalog.numbered (a integer PRIMARY KEY)", "CREATE TABLE")
        generated = relation_oid(wire, "pk_catalog.numbered_pkey1")
        check_index(wire, generated, relation_oid(wire, "pk_catalog.numbered"), 1)
        statements = wire.send_statements("DROP SCHEMA pk_catalog CASCADE")
        wire.check("DROP SCHEMA pk_catalog CASCADE", [(kind, payload) for kind, payload in statements if kind != b"N"],
                   tag="DROP SCHEMA")
        no_index(wire, generated)
        print("PRIMARY_INDEX_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
