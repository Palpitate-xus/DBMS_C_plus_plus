#!/usr/bin/env python3
"""R172 finite SQL ABS(ColumnRef) identity controls; standalone inventory.

Requires the unchanged R170 quoted_unqualified_scalar_projection_protocol_e2e_test
module beside this file. Only its pure Wire/identifier helpers are imported;
this fixture does not use its ScalarReview or its 791-control completion rule.
"""

from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import MODES, Wire, identifier
import uuid


EXPECTED_CONTROLS = 314


class AbsColumnReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("CONTROL INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


def relation(schema, name):
    return schema + "." + identifier(name)


def run(wire, token, schema):
    actual_name, empty_name = "Abs.Rows", "Empty.Rows"
    actual, empty = relation(schema, actual_name), relation(schema, empty_name)
    definition = ('(id INTEGER PRIMARY KEY,n INTEGER,"N" INTEGER,"n.dot" INTEGER,'
                  '"n""q" INTEGER,"""edge""" INTEGER,wide BIGINT,t TEXT)')
    wire.query("simple", "SELECT 1 AS v", [1], 23)
    wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
    wire.command("CREATE TABLE " + actual + definition, "CREATE TABLE")
    wire.command("CREATE TABLE " + empty + definition, "CREATE TABLE")
    wire.command("INSERT INTO " + actual +
                 " VALUES(1,-2,-7,-11,-13,-17,-9007199254740993,'n'),"
                 "(2,3,8,12,14,18,9007199254740995,''),"
                 "(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
                 "(4,-4,-9,-15,-16,-19,-9223372036854775807,'NULL')", "INSERT 0 4")
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)

    # Every case is one real single-table, single-target ABS(ColumnRef).
    # Relation-qualified operands are regenerated for the actual empty table;
    # this never string-rewrites a SQL literal or invents a catalog identity.
    ordinary, upper = [2, 3, None, 4], [7, 8, None, 9]
    dotted, embedded, edge = [11, 12, None, 15], [13, 14, None, 16], [17, 18, None, 19]
    wide = [9007199254740993, 9007199254740995, None, 9223372036854775807]
    cases = (
        ("n", "bare", "", "abs", 23, ordinary),
        (identifier("n"), "bare", "", "abs", 23, ordinary),
        ("n", "table", "", "abs", 23, ordinary),
        (identifier("n"), "schema", "", "abs", 23, ordinary),
        ("n", "alias", "r", "abs", 23, ordinary),
        (identifier("n"), "alias", "r", "abs", 23, ordinary),
        (identifier("n"), "alias", identifier("r.dot"), "abs", 23, ordinary),
        (identifier("n"), "alias", identifier('r"q'), "abs", 23, ordinary),
        (identifier("N"), "alias", "r", "abs", 23, upper),
        (identifier("n.dot"), "alias", identifier("r.dot"), "abs", 23, dotted),
        (identifier('n"q'), "alias", identifier('r"q'), "abs", 23, embedded),
        (identifier('"edge"'), "alias", identifier("r.dot"), "abs", 23, edge),
        ("wide", "alias", identifier("r.dot"), "abs", 20, wide),
        (identifier("wide"), "bare", "", "abs", 20, wide),
        (identifier("n"), "alias", identifier("r.dot"), "ABS", 23, ordinary),
    )

    def query_parts(case, name):
        column, style, alias, callee, oid, values = case
        table_token = identifier(name)
        qualifier = (alias if style == "alias" else table_token if style == "table" else
                     schema + "." + table_token if style == "schema" else "")
        argument = (qualifier + "." if qualifier else "") + column
        prefix = "SELECT " + callee + "(" + argument + ") AS v"
        source = " FROM " + relation(schema, name) + (" AS " + alias if alias else "")
        order = " ORDER BY " + (alias or table_token) + ".id"
        return prefix, source, order

    for index, case in enumerate(cases):
        oid, values = case[4:]
        statement = ("r172_" + token + "_case_" + str(index)).encode("ascii")
        prefix, source, order = query_parts(case, actual_name)
        wire.describe(statement, prefix + source, oid)
        for mode in MODES:
            wire.query(mode, prefix + source + order, values, oid)
            empty_prefix, empty_source, empty_order = query_parts(case, empty_name)
            wire.query(mode, empty_prefix + empty_source + empty_order, [], oid)
            wire.query(mode, prefix + source + " WHERE false" + order, [], oid)
            wire.query(mode, prefix + source + order + " LIMIT 0", [], oid)
        wire.close(statement)

    # Simple Query owns this Main execution boundary. These strict analysis
    # errors do not assert a new unquoted Parse owner in Network. The shared
    # full791 retains its own Parse-time missing-callee/retry assertions.
    source = identifier("r.dot")
    for error_index in range(8):
        for shape in ("rows", "false", "limit", "empty"):
            name = empty_name if shape == "empty" else actual_name
            table_token = identifier(name)
            arguments = (identifier("missing"), source + ".missing",
                         identifier("not.range") + ".n", table_token + ".n",
                         schema + "." + table_token + ".n", source + ".t",
                         "'\"r.dot\".\"n\"'::text", source + "." + identifier("N_missing"))
            states = ("42703", "42703", "42P01", "42P01", "42P01", "42883", "42883", "42703")
            sql = "SELECT abs(" + arguments[error_index] + ") AS v FROM " + relation(schema, name) + " AS " + source
            sql += " WHERE false" if shape == "false" else " LIMIT 0" if shape == "limit" else ""
            wire.query("simple", sql, state=states[error_index])
            wire.query("simple", "SELECT abs(" + source + '."n") AS v FROM ' + actual +
                       " AS " + source + " WHERE " + source + ".id=1", [2], 23)

    # Literal calls are not newly admitted by the direct-ColumnRef gate. Text
    # containing quoted SQL-looking bytes remains ordinary string data.
    boundaries = (("SELECT abs(-2::integer) AS v", [2], 23),
                  ("SELECT ABS(-9007199254740993::bigint) AS v", [9007199254740993], 20),
                  ("SELECT abs(NULL::integer) AS v", [None], 23),
                  ("SELECT '\"r.dot\".\"n\"'::text AS v", ['"r.dot"."n"'], 25),
                  ("SELECT abs(-2::integer) AS v FROM " + actual + " WHERE id=1", [2], 23))
    for sql, values, oid in boundaries:
        for mode in MODES:
            wire.query(mode, sql, values, oid)

    # Selecting a real user overload still belongs to the actual typed binder;
    # the new SQL entry must not force a builtin by a raw callee-name match.
    wire.command("CREATE FUNCTION " + schema +
                 ".abs(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+100$$",
                 "CREATE FUNCTION")
    wire.command("SET search_path=" + schema + ",pg_catalog", "SET", parameter_status=schema + ", pg_catalog")
    shadow = ("r172_" + token + "_shadow").encode("ascii")
    prefix = 'SELECT abs(' + source + '."n") AS v'
    tail, empty_tail = " FROM " + actual + " AS " + source, " FROM " + empty + " AS " + source
    order = " ORDER BY " + source + ".id"
    wire.describe(shadow, prefix + tail, 23)
    for mode in MODES:
        wire.query(mode, prefix + tail + order, [98, 103, None, 96], 23)
        wire.query(mode, prefix + empty_tail + order, [], 23)
        wire.query(mode, prefix + tail + " WHERE false" + order, [], 23)
        wire.query(mode, prefix + tail + order + " LIMIT 0", [], 23)
    wire.close(shadow)
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)


def main():
    label = "ABS_COLUMN_IDENTITY_PROTOCOL"
    try:
        review = AbsColumnReview(label)
    except Exception:
        print(label + "_COMPLETE", 0, "FAILURES", 1, flush=True)
        raise
    wire = Wire(review)
    token = uuid.uuid4().hex
    schema = identifier("R172.Scope_" + token)
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
    # Setup6 +15*(describe1 +3 modes*4 row shapes +close1)=210
    # +8 errors*4 shapes*(error1+recovery1)=64 +literal-owner5*3=15
    # +shadow(DDL1+SET1+describe1+3*4+close1+restoreSET1)=17 +cleanup2 =314.
    review.complete()


if __name__ == "__main__":
    main()
