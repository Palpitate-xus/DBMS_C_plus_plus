#!/usr/bin/env python3
"""Verify the primary key constraint's own identity and its backing index."""

import struct

from compat.numeric_type_review import Review
from pg_foreign_key_catalog_protocol_e2e_test import assert_definition, catalog_oid, relation_oid
from primary_index_catalog_protocol_e2e_test import INDEX, TABLE, setup
from session_setting_functions_protocol_e2e_test import Wire


PRIMARY_FIELDS = [("conname", 19), ("contype", 18), ("conrelid", 26), ("conindid", 26), ("conkey", 1005),
                  ("confrelid", 26), ("confkey", 1005), ("confupdtype", 18), ("confdeltype", 18)]
PRIMARY_SQL = ("SELECT " + ", ".join(name for name, _ in PRIMARY_FIELDS) +
               " FROM pg_catalog.pg_constraint WHERE oid='{key}'")
PSQL_INDEX_LIST = (
    "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, "
    "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), "
    "pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, "
    "i.indisreplident, c2.reltablespace, con.conperiod "
    "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i "
    "LEFT JOIN pg_catalog.pg_constraint con ON (conrelid=i.indrelid AND conindid=i.indexrelid "
    "AND contype IN ('p','u','x')) "
    "WHERE c.oid='{owner}' AND c.oid=i.indrelid AND i.indexrelid=c2.oid "
    "ORDER BY i.indisprimary DESC, c2.relname")
PSQL_FIELDS = [("relname", 19), ("indisprimary", 16), ("indisunique", 16), ("indisclustered", 16),
               ("indisvalid", 16), ("pg_get_indexdef", 25), ("pg_get_constraintdef", 25), ("contype", 18),
               ("condeferrable", 16), ("condeferred", 16), ("indisreplident", 16), ("reltablespace", 26), ("conperiod", 16)]


def check_primary(wire, constraint, owner, index, name, keys, ready=b"I"):
    sql = PRIMARY_SQL.format(key=constraint)
    frames = wire.send_statements(sql)
    wire.check(sql, frames, rows=[(name, "p", owner, index, keys, "0", None, " ", " ")],
               fields=PRIMARY_FIELDS, ready=ready)
    assert wire.client.row_description_fields(frames)[3] == (b"conindid", 2606, 11, 26, 4, -1, 0), frames


def binary_primary(wire, constraint, index):
    sql = "SELECT conindid,conkey,confkey FROM pg_catalog.pg_constraint WHERE oid=$1"
    frames = wire.exchange(wire.parse(sql, oids=(26,)) + wire.bind(arguments=(constraint.encode(),), result_format=1) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
    assert wire.client.row_description_fields(frames) == [
        (b"conindid", 2606, 11, 26, 4, -1, 1), (b"conkey", 2606, 21, 1005, -1, -1, 1),
        (b"confkey", 2606, 22, 1005, -1, -1, 1)], frames
    key = struct.pack("!IIIII", 1, 0, 21, 2, 1) + struct.pack("!IhIh", 2, 2, 2, 1)
    assert wire.client.data_row_values(frames) == [[struct.pack("!I", int(index)), key, None]], frames


def main():
    review = Review("PRIMARY_KEY_CATALOG")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        owner, index = relation_oid(wire, TABLE), relation_oid(wire, INDEX)
        constraint = catalog_oid(wire, owner, "accounts_key")
        assert len({owner, index, constraint}) == 3
        for mode in (wire, extended):
            check_primary(mode, constraint, owner, index, "accounts_key", "{2,1}")
            assert_definition(mode, constraint, "PRIMARY KEY (b, a)")
            mode.query(PSQL_INDEX_LIST.format(owner=owner), fields=PSQL_FIELDS,
                       rows=[("accounts_key", "t", "t", "f", "t",
                              f"CREATE UNIQUE INDEX accounts_key ON {TABLE} USING btree (b, a)",
                              "PRIMARY KEY (b, a)", "p", "f", "f", "f", "0", "f")])
        binary_primary(wire, constraint, index)

        wire.command("CREATE TABLE pk_catalog.child(a integer, b integer, "
                     f"CONSTRAINT child_key FOREIGN KEY (a,b) REFERENCES {TABLE}(a,b))", "CREATE TABLE")
        child = relation_oid(wire, "pk_catalog.child")
        fk = catalog_oid(wire, child, "child_key")
        for mode in (wire, extended):
            mode.query("SELECT conname,conindid,confkey FROM pg_catalog.pg_constraint "
                       f"WHERE confrelid='{owner}' AND conrelid='{child}'", rows=[("child_key", index, "{1,2}")],
                       fields=[("conname", 19), ("conindid", 26), ("confkey", 1005)])
        assert_definition(wire, fk, "FOREIGN KEY (a, b) REFERENCES pk_catalog.accounts(a, b)")
        wire.command("DROP TABLE pk_catalog.child", "DROP TABLE")

        wire.command(f"ALTER TABLE {TABLE} RENAME COLUMN b TO changed", "ALTER TABLE")
        wire.command(f"ALTER TABLE {TABLE} RENAME CONSTRAINT accounts_key TO renamed_key", "ALTER TABLE")
        wire.command(f"ALTER TABLE {TABLE} RENAME TO moved", "ALTER TABLE")
        wire.command("CREATE SCHEMA pk_other", "CREATE SCHEMA")
        wire.command("ALTER TABLE pk_catalog.moved SET SCHEMA pk_other", "ALTER TABLE")
        assert catalog_oid(wire, owner, "renamed_key") == constraint
        check_primary(extended, constraint, owner, index, "renamed_key", "{2,1}")
        assert_definition(wire, constraint, "PRIMARY KEY (changed, a)")
        moved = "pk_other.moved"

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f"ALTER TABLE {moved} DROP CONSTRAINT renamed_key", "ALTER TABLE", b"T")
        wire.query(PRIMARY_SQL.format(key=constraint), fields=PRIMARY_FIELDS, tag="SELECT 0", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        check_primary(wire, constraint, owner, index, "renamed_key", "{2,1}")

        wire.command(f"ALTER TABLE {moved} ALTER COLUMN changed TYPE bigint", "ALTER TABLE")
        rebuilt_index = relation_oid(wire, "pk_other.renamed_key")
        rebuilt = catalog_oid(wire, owner, "renamed_key")
        assert len({owner, index, constraint, rebuilt_index, rebuilt}) == 5
        wire.query(PRIMARY_SQL.format(key=constraint), fields=PRIMARY_FIELDS, tag="SELECT 0")
        check_primary(wire, rebuilt, owner, rebuilt_index, "renamed_key", "{2,1}")

        wire.command(f"DROP TABLE {moved}", "DROP TABLE")
        wire.query(PRIMARY_SQL.format(key=rebuilt), fields=PRIMARY_FIELDS, tag="SELECT 0")
        print("PRIMARY_KEY_CATALOG_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
