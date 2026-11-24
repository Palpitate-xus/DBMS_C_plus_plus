#!/usr/bin/env python3
"""Catalog join filters preserve matches, outer nulls and predicate demand."""

import json

from compat.numeric_type_review import Review
from primary_index_catalog_protocol_e2e_test import TABLE, setup
from primary_key_catalog_protocol_e2e_test import PSQL_FIELDS, PSQL_INDEX_LIST, relation_oid
from session_setting_functions_protocol_e2e_test import Wire


OTHER = "pk_catalog.other"
JOIN = ("FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid=i.indrelid "
        "LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid=i.indrelid AND con.conindid=i.indexrelid)")
KEY_FIELDS = [("indrelid", 26), ("conname", 19)]


def plan_nodes(node):
    yield node
    for child in node.get("children", []):
        yield from plan_nodes(child)


def explicit_index(table, name, column):
    return (name, "f", "f", "f", "t", f"CREATE INDEX {name} ON {table} USING btree ({column})",
            None, None, None, None, "f", "0", None)


def main():
    review = Review("CATALOG_OID_JOIN_FILTER")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        wire.command(f"CREATE INDEX payload_idx ON {TABLE}(payload)", "CREATE INDEX")
        wire.command(f"CREATE TABLE {OTHER}(k integer, v integer)", "CREATE TABLE")
        wire.command(f"CREATE INDEX other_idx ON {OTHER}(v)", "CREATE INDEX")
        wire.command("CREATE TABLE pk_catalog.elsewhere(k integer PRIMARY KEY)", "CREATE TABLE")
        table, other = relation_oid(wire, TABLE), relation_oid(wire, OTHER)
        primary = ("accounts_key", "t", "t", "f", "t",
                   f"CREATE UNIQUE INDEX accounts_key ON {TABLE} USING btree (b, a)",
                   "PRIMARY KEY (b, a)", "p", "f", "f", "f", "0", "f")
        for mode in (wire, extended):
            mode.query(PSQL_INDEX_LIST.format(owner=table),
                       rows=[primary, explicit_index(TABLE, "payload_idx", "payload")], fields=PSQL_FIELDS, tag="SELECT 2")
            mode.query(PSQL_INDEX_LIST.format(owner=other),
                       rows=[explicit_index(OTHER, "other_idx", "v")], fields=PSQL_FIELDS)
            mode.query(PSQL_INDEX_LIST.format(owner="4294967295"), tag="SELECT 0", fields=PSQL_FIELDS)

            mode.query(f"SELECT i.indrelid, con.conname {JOIN} WHERE c.oid='{table}' "
                       "AND (con.contype='p' OR con.oid IS NULL) ORDER BY con.conname NULLS LAST",
                       rows=[(table, "accounts_key"), (table, None)], fields=KEY_FIELDS, tag="SELECT 2")
            mode.query(f"SELECT i.indrelid, con.conname {JOIN} WHERE c.oid='{table}' OR c.oid='{other}' "
                       "ORDER BY c.relname, con.conname NULLS LAST",
                       rows=[(table, "accounts_key"), (table, None), (other, None)], fields=KEY_FIELDS, tag="SELECT 3")
            mode.query(f"SELECT i.indrelid, con.conname {JOIN} WHERE (con.conrelid='{other}' OR con.oid IS NULL) "
                       f"AND c.oid IN ('{table}','{other}') ORDER BY c.relname",
                       rows=[(table, None), (other, None)], fields=KEY_FIELDS, tag="SELECT 2")

        review.parameter(PSQL_INDEX_LIST.format(owner="placeholder").replace("'placeholder'", "$1"),
                         [other], [26], [list(explicit_index(OTHER, "other_idx", "v"))],
                         [oid for _, oid in PSQL_FIELDS])
        assert not review.failures

        if review.server:
            # Preserve errors that are reached before the selective WHERE
            # term, and errors in ON before WHERE can discard unrelated rows.
            fail_on_other = f"1 / (CASE WHEN i.indrelid='{other}' THEN 0 ELSE 1 END) = 1"
            wire.query(f"SELECT i.indrelid {JOIN} WHERE {fail_on_other} AND c.oid='{table}' ORDER BY i.indexrelid",
                       tag=None, state="22012")
            complex_join = JOIN.replace("ON (con.conrelid", f"ON ({fail_on_other} AND con.conrelid")
            wire.query(f"SELECT i.indrelid {complex_join} WHERE c.oid='{table}' ORDER BY i.indexrelid",
                       tag=None, state="22012")
            wire.query(f"SELECT i.indrelid {JOIN} WHERE con.conrelid='{table}' AND "
                       f"{fail_on_other} AND c.oid='{table}' ORDER BY i.indexrelid", tag=None, state="22012")

            class_rows = wire.client.data_row_values(wire.send_statements("SELECT oid FROM pg_catalog.pg_class"))
            statement = "EXPLAIN (FORMAT JSON, ANALYZE TRUE, TIMING FALSE) " + PSQL_INDEX_LIST.format(owner=table)
            frames = wire.send_statements(statement)
            assert not any(kind == b"E" for kind, _ in frames), frames
            plan = json.loads("\n".join(row[0].decode() for row in wire.client.data_row_values(frames)))
            join = next(node for node in plan_nodes(plan["plan"]) if node["nodeType"].endswith("Join"))
            assert plan["actualRows"] == 2 and 2 <= join["actualRows"] <= 2 * len(class_rows), plan
        print("CATALOG_OID_JOIN_FILTER_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
