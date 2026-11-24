#!/usr/bin/env python3
"""Check partial index expressions in pg_index and in index definitions."""

from compat.numeric_type_review import Review
from pg_get_indexdef_protocol_e2e_test import definition, relation_oid
from session_setting_functions_protocol_e2e_test import Wire


TABLE = "predicates.events"
SINGLE = "predicates.n_idx"
COMPOSITE = "predicates.combo_idx"
CONDITION = '"Mixed Key" IS NOT NULL AND (n >= \'-3\'::integer OR s <> 0)'
FULL_CONDITION = '("Mixed Key" IS NOT NULL) AND ((n >= \'-3\'::integer) OR (s <> 0))'
N_NODE = (
    "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 "
    ":args ({VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) "
    ":varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1} "
    "{CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false "
    ":location -1 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) :location -1}"
)
FOOTER_SQL = (
    "SELECT pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) AS predicate "
    "FROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am a "
    "WHERE i.indexrelid = c.oid AND c.oid = '{index}' AND c.relam = a.oid AND i.indrelid = c2.oid"
)


def setup(wire):
    wire.command("CREATE SCHEMA predicates", "CREATE SCHEMA")
    wire.command(f'CREATE TABLE {TABLE} (n integer, h bigint, s smallint, active boolean, "Mixed Key" integer)',
                 "CREATE TABLE")
    wire.command(f"INSERT INTO {TABLE} VALUES (1, 24, 0, false, 4), (-5, 2, 3, true, NULL)",
                 "INSERT 0 2")
    wire.command(f"CREATE INDEX n_idx ON {TABLE}(n) WHERE n > 0", "CREATE INDEX")
    wire.command(f"CREATE INDEX h_idx ON {TABLE}(h) WHERE h > 17 OR NOT active", "CREATE INDEX")
    wire.command(f'CREATE INDEX combo_idx ON {TABLE}(s,"Mixed Key") '
                 'WHERE "Mixed Key" IS NOT NULL AND (n >= -3 OR s <> 0)', "CREATE INDEX")
    wire.command(f"CREATE INDEX active_idx ON {TABLE}(active) WHERE TRUE", "CREATE INDEX")


def predicate(wire, index, expected, pretty=True, ready=b"I"):
    sql = ("SELECT pg_catalog.pg_get_expr(indpred, indrelid, " + ("true" if pretty else "false") +
           f") AS predicate FROM pg_catalog.pg_index WHERE indexrelid='{index}'")
    wire.query(sql, rows=[(expected,)], fields=[("predicate", 25)], ready=ready)


def main():
    review = Review("INDEX_PREDICATE")
    wire = Wire(review.client, review.sock)
    extended = Wire(wire.client, wire.sock, extended=True)
    try:
        setup(wire)
        single = relation_oid(wire, SINGLE)
        composite = relation_oid(wire, COMPOSITE)
        bigint = relation_oid(wire, "predicates.h_idx")
        whole = relation_oid(wire, "predicates.active_idx")
        for mode in (wire, extended):
            sql = f"SELECT indpred FROM pg_catalog.pg_index WHERE indexrelid='{single}'"
            frames = mode.send_statements(sql)
            mode.check(sql, frames, rows=[(N_NODE,)], fields=[("indpred", 194)])
            assert mode.client.row_description_fields(frames) == [(b"indpred", 2610, 21, 194, -1, -1, 0)], frames
            predicate(mode, single, "n > 0")
            predicate(mode, single, "(n > 0)", pretty=False)
            predicate(mode, bigint, "h > 17 OR NOT active")
            predicate(mode, composite, CONDITION)
            predicate(mode, whole, None)
            definition(mode, single, f"CREATE INDEX n_idx ON {TABLE} USING btree (n) WHERE (n > 0)")
            definition(mode, composite, f'CREATE INDEX combo_idx ON {TABLE} USING btree (s, "Mixed Key") WHERE ({FULL_CONDITION})')
            definition(mode, whole, f"CREATE INDEX active_idx ON {TABLE} USING btree (active)")
            mode.query(FOOTER_SQL.format(index=single), rows=[("n > 0",)], fields=[("predicate", 25)])

        parse = extended.parse("SELECT indpred FROM pg_catalog.pg_index WHERE indexrelid=$1", oids=(26,))
        bind = extended.bind(arguments=(single.encode(),), result_format=1)
        frames = extended.exchange(parse + bind + extended.describe_portal() + extended.execute() + extended.packet(b"S"))
        assert extended.client.row_description_fields(frames) == [(b"indpred", 2610, 21, 194, -1, -1, 1)], frames
        assert extended.client.data_row_values(frames) == [[N_NODE.encode()]], frames
        assert frames[-2:] == [(b"C", b"SELECT 1\0"), (b"Z", b"I")], frames

        wire.command("BEGIN", "BEGIN", b"T")
        wire.command(f'ALTER TABLE {TABLE} RENAME COLUMN "Mixed Key" TO "Current Key"', "ALTER TABLE", b"T")
        predicate(wire, composite, CONDITION.replace("Mixed Key", "Current Key"), ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        predicate(wire, composite, CONDITION)
        wire.command(f"ALTER TABLE {TABLE} RENAME COLUMN n TO new_n", "ALTER TABLE")
        assert relation_oid(wire, SINGLE) == single
        predicate(extended, single, "new_n > 0")
        wire.command(f"DROP INDEX {SINGLE}", "DROP INDEX")
        definition(wire, single, None)

        if review.server is not None:
            # Unavailable encodings may fail when selected. This index must
            # not block a lookup on an unrelated index.
            wire.command("CREATE TABLE predicates.other(k integer)", "CREATE TABLE")
            wire.command("CREATE INDEX other_idx ON predicates.other(k) WHERE abs(k) > 1", "CREATE INDEX")
            other = relation_oid(wire, "predicates.other_idx")
            wire.query(f"SELECT indpred FROM pg_catalog.pg_index WHERE indexrelid='{other}'", tag=None, state="0A000")
            wire.query(FOOTER_SQL.format(index=bigint), rows=[("h > 17 OR NOT active",)], fields=[("predicate", 25)])
        print("INDEX_PREDICATE_COMPLETE", flush=True)
    finally:
        review.close()


if __name__ == "__main__":
    main()
