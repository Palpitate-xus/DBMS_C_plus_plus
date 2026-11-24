#!/usr/bin/env python3
"""Exercise real explicit index identities, column counts and catalog state."""

import struct

from compat.numeric_type_review import Review
from pg_get_indexdef_protocol_e2e_test import BTREE, COMPOSITE, HASH, TABLE, relation_oid, setup
from session_setting_functions_protocol_e2e_test import Wire


FIELDS = [("indexrelid", 26), ("indrelid", 26), ("indnatts", 21), ("indnkeyatts", 21),
          ("indisunique", 16), ("indnullsnotdistinct", 16), ("indisprimary", 16),
          ("indisexclusion", 16), ("indimmediate", 16), ("indisclustered", 16), ("indisvalid", 16),
          ("indcheckxmin", 16), ("indisready", 16), ("indislive", 16), ("indisreplident", 16)]
SELECT = "SELECT " + ", ".join(name for name, _ in FIELDS) + " FROM pg_catalog.pg_index"
INDEX_LIST = ("SELECT c.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, "
              "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), i.indisreplident, c.reltablespace "
              "FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
              "WHERE i.indrelid = '{owner}' ORDER BY c.relname")
INDEX_LIST_FIELDS = [("relname", 19), ("indisprimary", 16), ("indisunique", 16), ("indisclustered", 16),
                     ("indisvalid", 16), ("pg_get_indexdef", 25), ("indisreplident", 16), ("reltablespace", 26)]


def data(index, owner, total, keys, unique, clustered="f", replica="f"):
    return (index, owner, str(total), str(keys), unique, "f", "f", "f", "t", clustered,
            "t", "f", "t", "t", replica)


def binary_result(wire, index, owner):
    name = b"index_catalog_point"
    sql = ("SELECT indexrelid, indrelid, indnkeyatts, indisunique FROM pg_catalog.pg_index "
           "WHERE indexrelid=$1")
    frames = wire.exchange(wire.parse(sql, name, (26,)) + wire.packet(b"D", b"S" + name + b"\0") +
                           wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
    assert frames[1] == (b"t", struct.pack("!HI", 1, 26)), frames
    fields = [(b"indexrelid", 2610, 1, 26, 4, -1), (b"indrelid", 2610, 2, 26, 4, -1),
              (b"indnkeyatts", 2610, 4, 21, 2, -1), (b"indisunique", 2610, 5, 16, 1, -1)]
    assert wire.client.row_description_fields(frames) == [(*field, 0) for field in fields], frames
    frames = wire.exchange(wire.bind(name, (index.encode(),), 1) + wire.describe_portal() +
                           wire.execute() + wire.packet(b"S"))
    assert b"".join(kind for kind, _ in frames) == b"2TDCZ", frames
    assert wire.client.row_description_fields(frames) == [(*field, 1) for field in fields], frames
    assert wire.client.data_row_values(frames) == [[struct.pack("!I", int(index)), struct.pack("!I", int(owner)),
                                                   b"\0\2", b"\1"]], frames
    assert frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")], frames


def main():
    review = Review("PG_INDEX")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        owner = relation_oid(wire, TABLE)
        composite = relation_oid(wire, COMPOSITE)
        btree = relation_oid(wire, BTREE)
        hashed = relation_oid(wire, HASH)
        expected = [data(composite, owner, 3, 2, "t"), data(btree, owner, 2, 1, "f"), data(hashed, owner, 1, 1, "f")]
        expected.sort(key=lambda row: int(row[0]))
        for mode in (wire, extended):
            mode.query(f"{SELECT} WHERE indrelid='{owner}' ORDER BY indexrelid", rows=expected,
                       fields=FIELDS, tag="SELECT 3")
            mode.query(f"{SELECT} WHERE indexrelid='{composite}' AND indrelid='{owner}'",
                       rows=[data(composite, owner, 3, 2, "t")], fields=FIELDS)
            mode.query(f"{SELECT} WHERE indexrelid='{composite}' AND indrelid='0'",
                       fields=FIELDS, tag="SELECT 0")
            mode.query(f"{SELECT} WHERE indexrelid='0'", fields=FIELDS, tag="SELECT 0")
            mode.query("SELECT count(*) AS n FROM pg_index WHERE indrelid='" + owner + "'",
                       rows=[("3",)], fields=[("n", 20)])
            mode.query(INDEX_LIST.format(owner=owner), rows=[
                ("Audit composite", "f", "t", "f", "t",
                 f'CREATE UNIQUE INDEX "Audit composite" ON {TABLE} USING btree (k, "Other Field") INCLUDE (payload)',
                 "f", "0"),
                ("flags_idx", "f", "f", "f", "t", f"CREATE INDEX flags_idx ON {TABLE} USING hash (flag)",
                 "f", "0"),
                ("payload_idx", "f", "f", "f", "t", f"CREATE INDEX payload_idx ON {TABLE} USING btree (payload) INCLUDE (k)",
                 "f", "0"),
            ], fields=INDEX_LIST_FIELDS, tag="SELECT 3")
        binary_result(wire, composite, owner)

        wire.command(f"ALTER TABLE {TABLE} CLUSTER ON payload_idx", "ALTER TABLE")
        wire.query(f"{SELECT} WHERE indexrelid='{btree}'", rows=[data(btree, owner, 2, 1, "f", clustered="t")],
                   fields=FIELDS)
        wire.command("CREATE TABLE idx_catalog.replica_rows(k integer NOT NULL)", "CREATE TABLE")
        wire.command("CREATE UNIQUE INDEX replica_idx ON idx_catalog.replica_rows(k)", "CREATE INDEX")
        wire.command("ALTER TABLE idx_catalog.replica_rows REPLICA IDENTITY USING INDEX replica_idx", "ALTER TABLE")
        replica = relation_oid(wire, "idx_catalog.replica_idx")
        replica_owner = relation_oid(wire, "idx_catalog.replica_rows")
        extended.query(f"{SELECT} WHERE indexrelid='{replica}'",
                       rows=[data(replica, replica_owner, 1, 1, "t", replica="t")], fields=FIELDS)

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f"DROP INDEX {HASH}", "DROP INDEX", b"T")
        wire.query(f"{SELECT} WHERE indexrelid='{hashed}'", fields=FIELDS, tag="SELECT 0", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.query(f"{SELECT} WHERE indexrelid='{hashed}'", rows=[data(hashed, owner, 1, 1, "f")], fields=FIELDS)
        wire.command("CREATE TABLE public.pg_index (v integer)", "CREATE TABLE")
        wire.command("INSERT INTO public.pg_index VALUES (42)", "INSERT 0 1")
        wire.command("SET search_path TO public, pg_catalog", "SET")
        extended.query("SELECT * FROM pg_index", rows=[("42",)], fields=[("v", 23)])
        wire.query(f"{SELECT} WHERE indexrelid='{hashed}'", rows=[data(hashed, owner, 1, 1, "f")], fields=FIELDS)
        if review.server is not None:
            for mode in (wire, extended):
                mode.query("SELECT * FROM pg_catalog.pg_index WHERE false", tag=None, state="0A000")
                mode.query("SELECT indkey FROM pg_catalog.pg_index WHERE false", tag=None, state="0A000")
        print("PG_INDEX_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
