#!/usr/bin/env python3
"""Check the public role catalog, password masking and typed identifier lookup."""

from datetime import datetime, timezone
import struct

from compat.numeric_type_review import Review
from pg_foreign_key_catalog_protocol_e2e_test import relation_oid, value
from session_setting_functions_protocol_e2e_test import Wire


COLUMNS = [("rolname", 19, 64), ("rolsuper", 16, 1), ("rolinherit", 16, 1),
           ("rolcreaterole", 16, 1), ("rolcreatedb", 16, 1), ("rolcanlogin", 16, 1),
           ("rolreplication", 16, 1), ("rolconnlimit", 23, 4), ("rolpassword", 25, -1),
           ("rolvaliduntil", 1184, 8), ("rolbypassrls", 16, 1), ("rolconfig", 1009, -1),
           ("oid", 26, 4)]
CREATE = (
    "CREATE ROLE role_catalog_options LOGIN NOINHERIT CREATEROLE CREATEDB REPLICATION BYPASSRLS "
    "CONNECTION LIMIT 12 VALID UNTIL '2034-01-02 03:04:05.123456+04' PASSWORD 'catalog_dummy_password'",
    "CREATE ROLE role_catalog_limited NOLOGIN",
    "CREATE ROLE role_catalog_infinite VALID UNTIL 'infinity'",
)
DESCRIBE_ROLES = (
    "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, "
    "r.rolconnlimit, r.rolvaliduntil, r.rolreplication, r.rolbypassrls FROM pg_catalog.pg_roles r "
    "WHERE r.rolname OPERATOR(pg_catalog.~) '^(role_catalog.*)$' COLLATE pg_catalog.default ORDER BY 1"
)


def role_oid(wire, name):
    return value(wire, f"SELECT oid FROM pg_catalog.pg_roles WHERE rolname='{name}'")


def binary_result(wire, oid, relation, *, reference):
    name = b"owned_role_metadata"
    sql = ("SELECT oid,rolname,rolcanlogin,rolpassword,rolconnlimit,rolvaliduntil,rolconfig "
           "FROM pg_catalog.pg_roles WHERE oid=$1")
    frames = wire.exchange(wire.parse(sql, name, (26,)) + wire.packet(b"D", b"S" + name + b"\0") +
                           wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
    assert frames[1] == (b"t", struct.pack("!HI", 1, 26)), frames
    fields = wire.client.row_description_fields(frames)
    ordinals = (13, 1, 6, 9, 8, 10, 12)
    for number, (label, origin, attr, data_type, size, modifier, fmt) in zip(ordinals, fields, strict=True):
        expected_name, expected_type, expected_size = COLUMNS[number - 1]
        assert (label, data_type, size, modifier, fmt) == (expected_name.encode(), expected_type, expected_size, -1, 0), fields
        if not reference:
            assert (origin, attr) == (int(relation), number), fields
    epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
    expires = datetime(2034, 1, 1, 23, 4, 5, 123456, tzinfo=timezone.utc) - epoch
    micros = (expires.days * 86400 + expires.seconds) * 1000000 + expires.microseconds
    expected = [struct.pack("!I", int(oid)), b"role_catalog_options", b"\x01", b"********",
                struct.pack("!i", 12), struct.pack("!q", micros), None]
    for key, data in ((oid.encode(), [expected]), (b"0", []), (None, [])):
        reply = wire.exchange(wire.bind(name, (key,), 1) + wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in reply) == b"2T" + b"D" * len(data) + b"CZ", reply
        assert wire.client.row_description_fields(reply) == [(*field[:-1], 1) for field in fields], reply
        assert wire.client.data_row_values(reply) == data, reply
        assert reply[-2:] == [(b"C", f"SELECT {len(data)}\0".encode()), (b"Z", b"I")], reply


def main():
    review = Review("PG_ROLES_CATALOG")
    wire = Wire(review.client, review.sock)
    try:
        wire.command("SET TIME ZONE 'UTC'", "SET")
        for sql in CREATE:
            wire.command(sql, "CREATE ROLE")
        relation = relation_oid(wire, "pg_catalog.pg_roles")
        oid = role_oid(wire, "role_catalog_options")
        assert int(oid) > 0 and role_oid(wire, "role_catalog_limited") != oid
        labels = [(name, data_type) for name, data_type, _ in COLUMNS]
        opts = ("role_catalog_options", "f", "f", "t", "t", "t", "t", "12", "********",
                "2034-01-01 23:04:05.123456+00", "t", None, oid)
        for mode in (wire, Wire(wire.client, wire.sock, extended=True)):
            mode.query(f"SELECT * FROM pg_catalog.pg_roles WHERE oid='{oid}'", rows=[opts], fields=labels)
            mode.query("SELECT rolname,rolpassword,rolvaliduntil,rolconfig FROM pg_catalog.pg_roles "
                       "WHERE rolname IN ('role_catalog_limited','role_catalog_infinite') ORDER BY rolname",
                       fields=[labels[i] for i in (0, 8, 9, 11)], tag="SELECT 2",
                       rows=[("role_catalog_infinite", "********", "infinity", None),
                             ("role_catalog_limited", "********", None, None)])
            mode.query(DESCRIBE_ROLES,
                       fields=[labels[i] for i in (0, 1, 2, 3, 4, 5, 7, 9, 6, 10)], tag="SELECT 3",
                       rows=[("role_catalog_infinite", "f", "t", "f", "f", "f", "-1", "infinity", "f", "f"),
                             ("role_catalog_limited", "f", "t", "f", "f", "f", "-1", None, "f", "f"),
                             tuple(opts[i] for i in (0, 1, 2, 3, 4, 5, 7, 9, 6, 10))])

        wire.query(f"SELECT attnum,attname,atttypid,attlen,attnotnull FROM pg_catalog.pg_attribute "
                   f"WHERE attrelid='{relation}' AND attnum>0 ORDER BY attnum",
                   fields=[("attnum", 21), ("attname", 19), ("atttypid", 26), ("attlen", 21), ("attnotnull", 16)],
                   rows=[(str(i), name, str(type_oid), str(length), "f")
                         for i, (name, type_oid, length) in enumerate(COLUMNS, 1)], tag="SELECT 13")
        binary_result(wire, oid, relation, reference=review.server is None)

        wire.command("SET ROLE role_catalog_limited", "SET")
        wire.query(f"SELECT rolpassword FROM pg_catalog.pg_roles WHERE oid='{oid}'", rows=[("********",)],
                   fields=[("rolpassword", 25)])
        wire.command("RESET ROLE", "RESET")

        wire.command("CREATE TABLE public.pg_roles(marker integer)", "CREATE TABLE")
        wire.command("INSERT INTO public.pg_roles VALUES (17)", "INSERT 0 1")
        wire.query(f"SELECT rolname FROM pg_roles WHERE oid='{oid}'", rows=[("role_catalog_options",)],
                   fields=[("rolname", 19)])
        wire.command("SET search_path=public,pg_catalog", "SET")
        wire.query("SELECT marker FROM pg_roles", rows=[("17",)], fields=[("marker", 23)])
        wire.query(f"SELECT rolname FROM pg_catalog.pg_roles WHERE oid='{oid}'", rows=[("role_catalog_options",)],
                   fields=[("rolname", 19)])
        wire.command("RESET search_path", "RESET")

        wire.command("ALTER ROLE role_catalog_options RENAME TO role_catalog_renamed", "ALTER ROLE")
        assert role_oid(wire, "role_catalog_renamed") == oid
        wire.command("ALTER ROLE role_catalog_renamed CONNECTION LIMIT 8", "ALTER ROLE")
        wire.query(f"SELECT rolname,rolconnlimit FROM pg_catalog.pg_roles WHERE oid='{oid}'",
                   fields=[("rolname", 19), ("rolconnlimit", 23)], rows=[("role_catalog_renamed", "8")])
        wire.command("DROP ROLE role_catalog_renamed", "DROP ROLE")
        wire.query(f"SELECT rolname FROM pg_catalog.pg_roles WHERE oid='{oid}'", tag="SELECT 0", fields=[("rolname", 19)])
        print("PG_ROLES_CATALOG_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
