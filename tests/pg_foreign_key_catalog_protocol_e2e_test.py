#!/usr/bin/env python3
"""Check durable foreign key identities and typed catalog rows via the SQL protocol."""

import struct

from compat.numeric_type_review import Review
from session_setting_functions_protocol_e2e_test import Wire


PARENT = "CREATE TABLE fk_parent.accounts(a integer, b integer, PRIMARY KEY (a,b))"
CHILD = ('CREATE TABLE fk_child.payments(id integer, b integer, a integer, '
         'CONSTRAINT "invoice,fk" FOREIGN KEY (a,b) REFERENCES fk_parent.accounts(a,b) '
         'ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)')
EXTRA = ("ALTER TABLE fk_child.payments ADD CONSTRAINT late_fk FOREIGN KEY (a,b) "
         "REFERENCES fk_parent.accounts(a,b) NOT VALID")
FIELDS = [("oid", 26), ("conname", 19), ("connamespace", 26), ("contype", 18),
          ("condeferrable", 16), ("condeferred", 16), ("conenforced", 16), ("convalidated", 16),
          ("conrelid", 26), ("contypid", 26), ("conparentid", 26), ("confrelid", 26),
          ("conislocal", 16), ("coninhcount", 21), ("conperiod", 16), ("conkey", 1005), ("confkey", 1005)]
FIELD_ORDINALS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 17, 18, 20, 21, 22)
FIELD_SIZES = (4, 64, 4, 1, 1, 1, 1, 1, 4, 4, 4, 4, 1, 2, 1, -1, -1)
QUERY = ("SELECT " + ",".join("r." + name for name, _ in FIELDS) +
         " FROM pg_catalog.pg_constraint r WHERE {filter} AND r.contype='f' ORDER BY r.conname")


def setup(wire):
    for statement, tag in (("CREATE SCHEMA fk_parent", "CREATE SCHEMA"),
                           ("CREATE SCHEMA fk_child", "CREATE SCHEMA"),
                           (PARENT, "CREATE TABLE"), (CHILD, "CREATE TABLE")):
        wire.command(statement, tag)


def value(wire, sql):
    messages = wire.send_statements(sql)
    rows = wire.client.data_row_values(messages)
    assert len(rows) == 1 and len(rows[0]) == 1, (sql, messages)
    return rows[0][0].decode()


def relation_oid(wire, qualified):
    return value(wire, f"SELECT '{qualified}'::regclass::oid")


def catalog_rows(wire, expression, expected, ready=b"I"):
    sql = QUERY.format(filter=expression)
    frames = wire.send_statements(sql)
    wire.check(sql, frames, rows=expected, fields=FIELDS, tag=f"SELECT {len(expected)}", ready=ready)
    assert wire.client.row_description_fields(frames) == [
        (name.encode(), 2606, ordinal, oid, size, -1, 0)
        for (name, oid), ordinal, size in zip(FIELDS, FIELD_ORDINALS, FIELD_SIZES, strict=True)], frames


def expected_row(oid, name, namespace, owner, parent, *, deferred=False, validated=True):
    immediate = "t" if deferred else "f"
    return (oid, name, namespace, "f", immediate, immediate, "t", "t" if validated else "f",
            owner, "0", "0", parent, "t", "0", "f", "{3,2}", "{1,2}")


def catalog_oid(wire, owner, name):
    identity = value(wire, "SELECT r.oid FROM pg_catalog.pg_constraint r "
                     f"WHERE r.conrelid='{owner}' AND r.conname='{name}'")
    assert 0 < int(identity) <= 4294967295, identity
    return identity


def binary_keys(wire, owner):
    sql = ("SELECT r.conkey,r.confkey FROM pg_catalog.pg_constraint r "
           "WHERE r.conrelid=$1 AND r.conname='invoice,fk'")
    frames = wire.exchange(wire.parse(sql, oids=(26,)) + wire.bind(arguments=(owner.encode(),), result_format=1) +
                           wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"12TDCZ", frames
    assert wire.client.row_description_fields(frames) == [
        (b"conkey", 2606, 21, 1005, -1, -1, 1), (b"confkey", 2606, 22, 1005, -1, -1, 1)], frames
    def encoded(first, second):
        return struct.pack("!IIIII", 1, 0, 21, 2, 1) + struct.pack("!IhIh", 2, first, 2, second)
    assert wire.client.data_row_values(frames) == [[encoded(3, 2), encoded(1, 2)]], frames


def main():
    review = Review("PG_FOREIGN_KEY_CATALOG")
    wire = Wire(review.client, review.sock)
    try:
        setup(wire)
        wire.command(EXTRA, "ALTER TABLE")
        namespace = value(wire, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='fk_child'")
        owner = relation_oid(wire, "fk_child.payments")
        parent = relation_oid(wire, "fk_parent.accounts")
        invoice = catalog_oid(wire, owner, "invoice,fk")
        late = catalog_oid(wire, owner, "late_fk")
        assert invoice != late, (invoice, late)
        rows = [expected_row(invoice, "invoice,fk", namespace, owner, parent, deferred=True),
                expected_row(late, "late_fk", namespace, owner, parent, validated=False)]
        for extended in (False, True):
            mode = Wire(wire.client, wire.sock, extended)
            catalog_rows(mode, f"r.conrelid='{owner}'", rows)
            catalog_rows(mode, f"'{parent}'=r.confrelid", rows)
            catalog_rows(mode, f"(r.conrelid='{parent}' OR r.confrelid='{parent}')", rows)
            catalog_rows(mode, "r.conrelid='4294967295'", [])
        binary_keys(wire, owner)

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command('ALTER TABLE fk_child.payments RENAME CONSTRAINT "invoice,fk" TO current_fk',
                     "ALTER TABLE", ready=b"T")
        renamed = expected_row(invoice, "current_fk", namespace, owner, parent, deferred=True)
        catalog_rows(wire, f"r.conrelid='{owner}'", [renamed, rows[1]], b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        catalog_rows(wire, f"r.conrelid='{owner}'", rows)
        wire.command('ALTER TABLE fk_child.payments RENAME CONSTRAINT "invoice,fk" TO current_fk', "ALTER TABLE")
        wire.command("ALTER TABLE fk_child.payments VALIDATE CONSTRAINT late_fk", "ALTER TABLE")
        validated = expected_row(late, "late_fk", namespace, owner, parent)
        wire.command("ALTER TABLE fk_parent.accounts RENAME COLUMN b TO second_key", "ALTER TABLE")
        wire.command("ALTER TABLE fk_parent.accounts RENAME TO active_accounts", "ALTER TABLE")
        wire.command("ALTER TABLE fk_child.payments RENAME TO paid_invoices", "ALTER TABLE")
        assert relation_oid(wire, "fk_parent.active_accounts") == parent
        assert relation_oid(wire, "fk_child.paid_invoices") == owner
        catalog_rows(wire, f"r.confrelid='{parent}'", [renamed, validated])

        wire.command("ALTER TABLE fk_child.paid_invoices DROP CONSTRAINT late_fk", "ALTER TABLE")
        catalog_rows(wire, f"r.conrelid='{owner}'", [renamed])
        wire.command("ALTER TABLE fk_child.paid_invoices ADD CONSTRAINT late_fk FOREIGN KEY (a,b) "
                     "REFERENCES fk_parent.active_accounts(a,second_key)", "ALTER TABLE")
        replacement = catalog_oid(wire, owner, "late_fk")
        assert replacement not in (invoice, late), (invoice, late, replacement)
        catalog_rows(wire, f"r.conrelid='{owner}'",
                     [renamed, expected_row(replacement, "late_fk", namespace, owner, parent)])
        wire.command("DROP TABLE fk_child.paid_invoices", "DROP TABLE")
        catalog_rows(wire, f"r.confrelid='{parent}'", [])

        wire.command("CREATE TABLE fk_child.pg_constraint(k integer)", "CREATE TABLE")
        wire.command("INSERT INTO fk_child.pg_constraint VALUES (71)", "INSERT 0 1")
        wire.command("SET search_path=fk_child,pg_catalog,public", "SET")
        wire.query("SELECT k FROM pg_constraint", rows=[("71",)], fields=[("k", 23)])
        print("PG_FOREIGN_KEY_CATALOG_COMPLETE")
    finally:
        if review.server is None:
            review.client.simple_query(review.sock, "ROLLBACK")
            review.client.simple_query(review.sock, "DROP SCHEMA IF EXISTS fk_child CASCADE")
            review.client.simple_query(review.sock, "DROP SCHEMA IF EXISTS fk_parent CASCADE")
        review.close()


if __name__ == "__main__":
    main()
