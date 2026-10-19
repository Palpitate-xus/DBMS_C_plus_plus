#!/usr/bin/env python3
"""R175 direct aggregate identity: an UNRUN PostgreSQL-common candidate.

Uses only the unchanged R170 strict Wire helpers, not its 791-control runner.
Integer in-call ORDER controls prove input-column identity only: they do not
prove sort-key demand, ordering or floating accumulation (separate R176 Open).
Native decoded AggItem/SelectExpr APIs are not changed or exercised by wire SQL.
"""

from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import MODES, Wire, identifier
import uuid


EXPECTED_CONTROLS = 906


class AggregateReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("CONTROL INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


def relation(schema, name):
    return schema + "." + identifier(name)


def run(wire, token, schema):
    actual_name, empty_name = "Agg.Rows", "Empty.Rows"
    actual, empty = relation(schema, actual_name), relation(schema, empty_name)
    definition = '(id INTEGER PRIMARY KEY,n INTEGER,"N" INTEGER,"n.dot" INTEGER,"n""q" INTEGER)'
    wire.query("simple", "SELECT 1 AS v", [1], 23)
    wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
    wire.command("CREATE TABLE " + actual + definition, "CREATE TABLE")
    wire.command("CREATE TABLE " + empty + definition, "CREATE TABLE")
    # Duplicate values are intentional: SUM(DISTINCT n)=-3, SUM(n)=-5.
    # The original 791 fixture has no duplicates and cannot prove this role.
    wire.command("INSERT INTO " + actual +
                 " VALUES(1,-2,-7,-11,-13),(2,3,8,12,14),(3,NULL,NULL,NULL,NULL),"
                 "(4,-4,-9,-15,-16),(5,-2,-7,-11,-13)", "INSERT 0 5")
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)

    # (SQL column token, qualification style, alias, SUM, DISTINCT SUM, FILTER SUM)
    cases = (
        ("n", "bare", "", -5, -3, -3),
        (identifier("n"), "bare", "", -5, -3, -3),
        (identifier("n"), "table", "", -5, -3, -3),
        (identifier("n"), "schema", "", -5, -3, -3),
        (identifier("n"), "alias", identifier("r.dot"), -5, -3, -3),
        (identifier("N"), "alias", "r", -15, -8, -8),
        (identifier("n.dot"), "alias", identifier("r.dot"), -25, -14, -14),
        (identifier('n"q'), "alias", identifier('r"q'), -28, -15, -15),
    )

    def parts(case, name):
        column, style, alias = case[:3]
        table_token = identifier(name)
        qualifier = (alias if style == "alias" else table_token if style == "table" else
                     schema + "." + table_token if style == "schema" else "")
        argument = (qualifier + "." if qualifier else "") + column
        source = " FROM " + relation(schema, name) + (" AS " + alias if alias else "")
        row_id = (alias or table_token) + ".id"
        return argument, source, row_id

    def projection(kind, argument, row_id):
        if kind == "sum":
            return "sum(" + argument + ")"
        if kind == "sum_distinct":
            return "sum(DISTINCT " + argument + ")"
        if kind == "sum_filter":
            return "sum(" + argument + ") FILTER(WHERE " + row_id + ">1)"
        if kind == "count":
            return "count(" + argument + ")"
        return "count(DISTINCT " + argument + ")"

    for index, case in enumerate(cases):
        argument, source, row_id = parts(case, actual_name)
        empty_argument, empty_source, empty_id = parts(case, empty_name)
        reductions = (("sum", case[3], None), ("sum_distinct", case[4], None),
                      ("sum_filter", case[5], None), ("count", 4, 0), ("count_distinct", 3, 0))
        for kind, value, absent in reductions:
            prefix = "SELECT " + projection(kind, argument, row_id) + " AS v"
            empty_prefix = "SELECT " + projection(kind, empty_argument, empty_id) + " AS v"
            statement = ("r175_" + token + "_" + str(index) + "_" + kind).encode("ascii")
            wire.describe(statement, prefix + source, 20)
            for mode in MODES:
                wire.query(mode, prefix + source, [value], 20)
                wire.query(mode, empty_prefix + empty_source, [absent], 20)
                wire.query(mode, prefix + source + " WHERE false", [absent], 20)
                wire.query(mode, prefix + source + " WHERE " + row_id + "=3", [absent], 20)
                wire.query(mode, prefix + source + " LIMIT 0", [], 20)
            wire.close(statement)

        # Legacy in-call ORDER is deliberately non-DISTINCT and integer-only.
        # Preserve the whole sort/filter SQL while proving the actual input
        # column survives the adapter; R176 must separately prove sort semantics.
        ordered = "SELECT sum(" + argument + " ORDER BY " + row_id + ") AS v"
        ordered_empty = "SELECT sum(" + empty_argument + " ORDER BY " + empty_id + ") AS v"
        ordered_filter = ("SELECT sum(" + argument + " ORDER BY " + row_id +
                          ") FILTER(WHERE " + row_id + ">1) AS v")
        for mode in MODES:
            wire.query(mode, ordered + source, [case[3]], 20)
            wire.query(mode, ordered_empty + empty_source, [None], 20)
            wire.query(mode, ordered + source + " WHERE false", [None], 20)
            wire.query(mode, ordered + source + " LIMIT 0", [], 20)
            wire.query(mode, ordered_filter + source, [case[5]], 20)

    source = identifier("r.dot")
    tail = " FROM " + actual + " AS " + source
    empty_tail = " FROM " + empty + " AS " + source
    n = source + '."n"'
    fromless = (("SELECT sum(1) AS v", [1]), ("SELECT count(1) AS v", [1]),
                ("SELECT count(NULL::integer) AS v", [0]),
                ("SELECT sum(NULL::integer) AS v", [None]),
                ("SELECT sum(DISTINCT 1) AS v", [1]),
                ("SELECT count(DISTINCT NULL::integer) AS v", [0]),
                ("SELECT sum(1) AS v WHERE false", [None]),
                ("SELECT count(1) AS v WHERE false", [0]),
                ("SELECT sum(1) AS v LIMIT 0", []),
                ("SELECT count(NULL::integer) AS v LIMIT 0", []))
    for sql, values in fromless:
        for mode in MODES:
            wire.query(mode, sql, values, 20)

    repeated = ("r175_" + token + "_repeat").encode("ascii")
    wire.describe(repeated, "SELECT sum(DISTINCT " + n + ") AS v" + tail, 20)
    wire.bind_describe(repeated, 20, (), (), False, False)
    for binary_output in (False, True):
        for repeat in range(2):
            wire.execute(repeated, 20, [-3], binary_output=binary_output, repeat=repeat)
    wire.close(repeated)

    # Simple Query owns these Main analysis errors. This does not add an
    # unquoted aggregate Parse-validation or wire-parameter-inference owner.
    for error_index in range(4):
        for shape in ("rows", "empty", "false", "limit"):
            chosen_tail = empty_tail if shape == "empty" else tail
            arguments = (source + '."missing"', identifier("wrong.range") + '."n"',
                         identifier(actual_name if shape != "empty" else empty_name) + '."n"',
                         "'\"r.dot\".\"n\"'::text")
            states = ("42703", "42P01", "42P01", "42883")
            sql = "SELECT sum(" + arguments[error_index] + ") AS v" + chosen_tail
            sql += " WHERE false" if shape == "false" else " LIMIT 0" if shape == "limit" else ""
            wire.query("simple", sql, state=states[error_index])
            wire.query("simple", "SELECT sum(" + n + ") AS v" + tail, [-5], 20)

    # The true selected scalar declaration wins over an aggregate spelling.
    # This tests stored metadata, not a forged native callback/pg_proc row.
    wire.command("CREATE FUNCTION " + schema +
                 ".sum(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+100$$",
                 "CREATE FUNCTION")
    wire.command("SET search_path=" + schema + ",pg_catalog", "SET", parameter_status=schema + ", pg_catalog")
    shadow = ("r175_" + token + "_shadow").encode("ascii")
    scalar = "SELECT sum(" + n + ") AS v"
    wire.describe(shadow, scalar + tail, 23)
    for mode in MODES:
        wire.query(mode, scalar + tail + " ORDER BY " + source + ".id", [98, 103, None, 96, 98], 23)
        wire.query(mode, scalar + empty_tail, [], 23)
        wire.query(mode, scalar + tail + " WHERE false", [], 23)
        wire.query(mode, scalar + tail + " LIMIT 0", [], 23)
        wire.query(mode, "SELECT sum(1) AS v", [101], 23)
        wire.query(mode, "SELECT sum(NULL::integer) AS v", [None], 23)
        wire.query(mode, "SELECT pg_catalog.sum(" + n + ") AS v" + tail, [-5], 20)
        wire.query(mode, "SELECT pg_catalog.sum(1) AS v", [1], 20)
    wire.close(shadow)
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)


def main():
    label = "ORDINARY_AGGREGATE_IDENTITY_PROTOCOL"
    try:
        review = AggregateReview(label)
    except Exception:
        print(label + "_COMPLETE", 0, "FAILURES", 1, flush=True)
        raise
    wire = Wire(review)
    token = uuid.uuid4().hex
    schema = identifier("R175.Scope_" + token)
    try:
        run(wire, token, schema)
    except Exception as error:
        review.failures.append(("UNEXPECTED COLLECTOR EXCEPTION", type(error).__name__, str(error)))
    finally:
        wire.command("SET search_path=public,pg_catalog", "SET", parameter_status="public, pg_catalog")
        wire.command("DROP SCHEMA " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        try:
            review.close()
        except Exception as error:
            review.failures.append(("CLOSE", type(error).__name__, str(error)))
    # Setup6 +matrix8cases*5reductions*(describe1+3modes*5shapes+close1)=680
    # +8cases*3modes*5integerORDER=120 +10FROMless*3=30
    # +repeat7 +4errors*4shapes*(error1+recovery1)=32 +shadow29 +cleanup2=906.
    review.complete()


if __name__ == "__main__":
    main()
