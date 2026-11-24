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
OUTGOING = ("SELECT true AS sametable, conname, pg_catalog.pg_get_constraintdef(r.oid, true) AS condef, "
            "conrelid::pg_catalog.regclass AS ontable FROM pg_catalog.pg_constraint r "
            "WHERE r.conrelid='{owner}' AND r.contype='f' AND conparentid=0 ORDER BY conname")
DEFERRED_CASCADE = " ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED"


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


def assert_definition(wire, oid, expected, ready=b"I"):
    for pretty in ("", ", false", ", true"):
        wire.query(f"SELECT pg_catalog.pg_get_constraintdef('{oid}'::oid{pretty}) AS v",
                   rows=[(expected,)], fields=[("v", 25)], ready=ready)


def actions(wire, owner, expected):
    sql = ("SELECT conname,confupdtype,confdeltype FROM pg_catalog.pg_constraint "
           f"WHERE conrelid='{owner}' AND contype='f' ORDER BY conname")
    frames = wire.send_statements(sql)
    wire.check(sql, frames, rows=expected, fields=[("conname", 19), ("confupdtype", 18), ("confdeltype", 18)],
               tag=f"SELECT {len(expected)}")
    assert wire.client.row_description_fields(frames) == [
        (b"conname", 2606, 2, 19, 64, -1, 0),
        (b"confupdtype", 2606, 14, 18, 1, -1, 0),
        (b"confdeltype", 2606, 15, 18, 1, -1, 0)], frames


def definition_lookup(wire, owner, invoice, late):
    base = "FOREIGN KEY (a, b) REFERENCES fk_parent.accounts(a, b)"
    for extended in (False, True):
        mode = Wire(wire.client, wire.sock, extended)
        mode.query(OUTGOING.format(owner=owner),
                   rows=[("t", "invoice,fk", base + DEFERRED_CASCADE, "fk_child.payments"),
                         ("t", "late_fk", base + " NOT VALID", "fk_child.payments")],
                   fields=[("sametable", 16), ("conname", 19), ("condef", 25), ("ontable", 2205)], tag="SELECT 2")
        actions(mode, owner, [("invoice,fk", "c", "a"), ("late_fk", "a", "a")])
        assert_definition(mode, invoice, base + DEFERRED_CASCADE)
        assert_definition(mode, late, base + " NOT VALID")
        mode.query("SELECT pg_catalog.pg_get_constraintdef(NULL, true) AS v", rows=[(None,)], fields=[("v", 25)])
        mode.query(f"SELECT pg_catalog.pg_get_constraintdef('{invoice}', NULL) AS v", rows=[(None,)], fields=[("v", 25)])
        mode.query("SELECT pg_catalog.pg_get_constraintdef('1'::text)", tag=None, state="42883")
    wire.prepare(b"fk_definition", "SELECT pg_catalog.pg_get_constraintdef($1, true) AS v", (26,), (26,))
    wire.run_prepared(b"fk_definition", (invoice.encode(),), base + DEFERRED_CASCADE)
    wire.run_prepared(b"fk_definition", (b"4294967295",), None)

    wire.command("SET search_path=fk_parent,public", "SET")
    assert_definition(wire, invoice, "FOREIGN KEY (a, b) REFERENCES accounts(a, b)" + DEFERRED_CASCADE)
    wire.command("CREATE TABLE fk_child.accounts(id integer)", "CREATE TABLE")
    wire.command("SET search_path=fk_child,fk_parent,public", "SET")
    assert_definition(wire, invoice, base + DEFERRED_CASCADE)
    wire.command("RESET search_path", "RESET")

    wire.command("CREATE FUNCTION fk_child.pg_get_constraintdef(oid) RETURNS text "
                 "LANGUAGE SQL AS $$ SELECT 'local'::text $$", "CREATE FUNCTION")
    wire.command("SET search_path=fk_child,pg_catalog,public", "SET")
    wire.query(f"SELECT pg_get_constraintdef('{invoice}'::oid) AS v", rows=[("local",)], fields=[("v", 25)])
    assert_definition(wire, invoice, base + DEFERRED_CASCADE)
    wire.command("RESET search_path", "RESET")

    wire.command('CREATE TABLE fk_parent."Target.Table"("Key ID" integer PRIMARY KEY)', "CREATE TABLE")
    wire.command('CREATE TABLE fk_child.action_demo("Key Ref" integer, '
                 'CONSTRAINT inline_action FOREIGN KEY ("Key Ref") REFERENCES fk_parent."Target.Table"("Key ID") '
                 'ON UPDATE RESTRICT ON DELETE SET NULL)', "CREATE TABLE")
    wire.command('ALTER TABLE fk_child.action_demo ADD CONSTRAINT explicit_action FOREIGN KEY ("Key Ref") '
                 'REFERENCES fk_parent."Target.Table"("Key ID") ON UPDATE NO ACTION ON DELETE RESTRICT', "ALTER TABLE")
    action_owner = relation_oid(wire, "fk_child.action_demo")
    actions(wire, action_owner, [("explicit_action", "a", "r"), ("inline_action", "r", "n")])
    quoted = 'FOREIGN KEY ("Key Ref") REFERENCES fk_parent."Target.Table"("Key ID")'
    assert_definition(wire, catalog_oid(wire, action_owner, "explicit_action"), quoted + " ON DELETE RESTRICT")
    assert_definition(wire, catalog_oid(wire, action_owner, "inline_action"), quoted + " ON UPDATE RESTRICT ON DELETE SET NULL")


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
        definition_lookup(wire, owner, invoice, late)

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
        renamed_definition = "FOREIGN KEY (a, b) REFERENCES fk_parent.active_accounts(a, second_key)"
        assert_definition(wire, invoice, renamed_definition + DEFERRED_CASCADE)
        assert_definition(wire, late, renamed_definition)

        wire.command("ALTER TABLE fk_child.paid_invoices DROP CONSTRAINT late_fk", "ALTER TABLE")
        catalog_rows(wire, f"r.conrelid='{owner}'", [renamed])
        assert_definition(wire, late, None)
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
