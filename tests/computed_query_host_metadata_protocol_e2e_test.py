#!/usr/bin/env python3
"""R174 finite computed-output query-host metadata; independent full inventory.

Requires the unchanged R170 quoted_unqualified_scalar_projection_protocol_e2e_test
module beside this file. Reuses only transport/strict DataRow/identifier helpers,
not its ScalarReview/791 count. This fixture is UNRUN until a complete PostgreSQL
oracle is obtained; no production execution claim follows from this draft.
"""

from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import (
    MODES, Wire, identifier, matches, strict_data_rows,
)
import struct
import uuid


EXPECTED_CONTROLS = 331
WIDTHS = {20: 8, 23: 4, 25: -1, 1043: -1, 1700: -1}


class HostReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("CONTROL INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


def numeric_integer(value):
    """Exact PG binary NUMERIC for these finite nonnegative integer sums."""
    assert isinstance(value, int) and value >= 0
    digits = []
    while value:
        value, digit = divmod(value, 10000)
        digits.append(digit)
    digits.reverse()
    return struct.pack("!hhhh", len(digits), len(digits) - 1 if digits else 0, 0, 0) + (
        struct.pack("!" + "H" * len(digits), *digits) if digits else b"")


class HostWire(Wire):
    """Own strict typmod/origin validator; shared Wire remains byte-unchanged."""
    physical_oid = None

    def description(self, oid, binary=False, typmod=-1, origin=False):
        table, attribute = (self.physical_oid or 0, 2) if origin else (0, 0)
        return struct.pack("!H", 1) + b"v\0" + struct.pack(
            "!IhIhih", table, attribute, oid, WIDTHS[oid], typmod, int(binary))

    def description_ok(self, body, oid, binary=False, typmod=-1, origin=False):
        if not origin:
            return body == self.description(oid, binary, typmod)
        # Cluster-local OIDs differ between PG and product. Require a genuine
        # nonzero table origin, exact physical attnum2 and identical origin in
        # every later descriptor; computed outputs above require exact zero.
        if len(body) != 22 or body[:4] != b"\0\1v\0":
            return False
        table, attribute, actual_oid, width, modifier, format_code = struct.unpack("!IhIhih", body[4:])
        good = (table != 0 and attribute == 2 and actual_oid == oid and
                width == WIDTHS[oid] and modifier == typmod and format_code == int(binary) and
                self.physical_oid in (None, table))
        if good:
            self.physical_oid = table
        return good

    def result_ok(self, messages, values, oid, state=None, binary=False,
                  parsed=False, bound=False, simple=False, inputs=(), typmod=-1,
                  ready=b"I", origin=False):
        rows, valid_rows = strict_data_rows(messages)
        kinds = [kind for kind, _ in messages]
        descriptions = [body for kind, body in messages if kind == b"T"]
        parameters = [body for kind, body in messages if kind == b"t"]
        tags = [body for kind, body in messages if kind == b"C"]
        if state:
            good = (kinds == [b"E", b"Z"] and self.errors(messages) == [state] and
                    not rows and not descriptions and not parameters and not tags)
        else:
            prefix = [b"T"] if simple else [b"1", b"t", b"T"] if parsed else []
            formats = [False] if parsed else []
            if simple or bound:
                formats.append(binary)
            if bound:
                prefix += [b"2", b"T"]

            def cell(raw, expected):
                if raw is None or expected is None:
                    return raw is None and expected is None
                if binary and oid == 1700:
                    return raw == numeric_integer(expected)
                return matches(raw, expected, oid, binary)

            good = (not self.errors(messages) and
                    kinds == prefix + [b"D"] * len(values) + [b"C", b"Z"] and
                    len(descriptions) == len(formats) and
                    all(self.description_ok(body, oid, format_code, typmod, origin)
                        for body, format_code in zip(descriptions, formats)) and
                    parameters == ([self.parameters(inputs)] if parsed else []) and
                    tags == [("SELECT " + str(len(values))).encode("ascii") + b"\0"] and
                    len(rows) == len(values) and
                    all(len(row) == 1 and cell(row[0], value) for row, value in zip(rows, values)))
        return (good and valid_rows and messages[-1:] == [(b"Z", ready)] and
                all(body == b"" for kind, body in messages if kind in (b"1", b"2")))

    def query(self, mode, sql, values=(), oid=23, state=None, typmod=-1, ready=b"I", origin=False):
        binary = mode == "binary"
        producer = (lambda: self.client.simple_query(self.sock, sql)) if mode == "simple" else (
            lambda: self.exchange(self.parse(b"", sql) + self.bind(b"", binary_output=binary)))
        self.collect((mode, sql), producer,
                     lambda messages: self.result_ok(messages, values, oid, state, binary,
                         parsed=mode != "simple", bound=mode != "simple", simple=mode == "simple",
                         typmod=typmod, ready=ready, origin=origin),
                     (values, oid, WIDTHS[oid], typmod, state, binary, ready, origin))

    def describe(self, statement, sql, oid=23, declared=(), resolved=None,
                 state=None, typmod=-1, ready=b"I", origin=False):
        inputs = declared if resolved is None else resolved

        def valid(messages):
            if state:
                return (messages[-1:] == [(b"Z", ready)] and
                        [kind for kind, _ in messages] == [b"E", b"Z"] and
                        self.errors(messages) == [state])
            return (len(messages) == 4 and messages[:2] == [(b"1", b""), (b"t", self.parameters(inputs))] and
                    messages[2][0] == b"T" and
                    self.description_ok(messages[2][1], oid, False, typmod, origin) and
                    messages[3:] == [(b"Z", ready)])

        self.collect(("PARSE/DESCRIBE", statement, sql, declared),
                     lambda: self.exchange(self.parse(statement, sql, declared)), valid,
                     (oid, WIDTHS[oid], typmod, inputs, state, ready, origin, "no execution"))

    def bind_describe(self, statement, oid, inputs=(), values=(), binary_input=False,
                      binary_output=False, typmod=-1, origin=False):
        self.collect(("BIND/DESCRIBE", statement, binary_input, binary_output),
                     lambda: self.exchange(self.bind(statement, inputs, values, binary_input, binary_output, False)),
                     lambda messages: (len(messages) == 3 and messages[0] == (b"2", b"") and
                         messages[1][0] == b"T" and
                         self.description_ok(messages[1][1], oid, binary_output, typmod, origin) and
                         messages[2] == (b"Z", b"I")),
                     (oid, WIDTHS[oid], typmod, binary_output, origin, "no execution"))

    def execute(self, statement, oid, values, inputs=(), parameters=(),
                binary_input=False, binary_output=False, typmod=-1, origin=False):
        self.collect(("BIND/EXECUTE", statement, parameters, binary_input, binary_output),
                     lambda: self.exchange(self.bind(statement, inputs, parameters, binary_input, binary_output)),
                     lambda messages: self.result_ok(messages, values, oid, binary=binary_output,
                         bound=True, typmod=typmod, origin=origin),
                     (values, oid, WIDTHS[oid], typmod, binary_output, origin))

    def utility(self, sql, tag, ready):
        wanted = [(b"C", tag.encode("ascii") + b"\0"), (b"Z", ready)]
        self.collect(sql, lambda: self.client.simple_query(self.sock, sql),
                     lambda messages: messages == wanted, wanted)


def run(wire, token, schema):
    table, empty = schema + ".rows", schema + ".empty_rows"
    sequences = [schema + ".effects_" + suffix for suffix in ("derived", "cte", "sum")]
    statement = lambda group, index: ("r174_" + token + "_" + group + "_" + str(index)).encode("ascii")

    # Fixed setup13: probe/schema/two tables/insert/SET/function +3sequence +3prime.
    wire.query("simple", "SELECT 1 AS v", [1])
    wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
    for relation in (table, empty):
        wire.command("CREATE TABLE " + relation + "(id INTEGER,n INTEGER,t TEXT)", "CREATE TABLE")
    wire.command("INSERT INTO " + table + " VALUES(1,-2,'AbCd'),(2,3,''),(3,NULL,NULL)", "INSERT 0 3")
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)
    echo = schema + ".echo"
    wire.command("CREATE FUNCTION " + echo + "(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT "
                 "AS $$SELECT p$$", "CREATE FUNCTION")
    for sequence in sequences:
        wire.command("CREATE SEQUENCE " + sequence, "CREATE SEQUENCE")
        wire.query("simple", "SELECT nextval('" + sequence + "') AS v", [1], 20)

    def current(index, expected):
        wire.query("simple", "SELECT currval('" + sequences[index] + "') AS v", [expected], 20)

    def effect_sql(index, shape):
        relation = empty if shape == "empty" else table
        predicate = "false" if shape == "false" else "id=1"
        call = '"abs"(nextval(\'' + sequences[index] + "'))"
        if index == 0:
            sql = "SELECT " + call + " AS v FROM (SELECT id FROM " + relation + ") AS d WHERE " + predicate
        elif index == 1:
            sql = "WITH c AS (SELECT id FROM " + relation + ") SELECT " + call + " AS v FROM c WHERE " + predicate
        else:
            # BIGINT input -> real NUMERIC SUM. No cast masks its declaration.
            sql = "SELECT sum(" + call + ") AS v FROM " + relation + " WHERE " + predicate
        return sql + (" LIMIT 0" if shape == "limit" else "")

    # 3*(11 named metadata/execution controls +3modes*3zero-demand*(query+counter))=87.
    for index in range(3):
        oid = 1700 if index == 2 else 20
        name, sql = statement("effects", index), effect_sql(index, "rows")
        wire.describe(name, sql, oid)
        current(index, 1)
        for binary in (False, True):
            wire.bind_describe(name, oid, binary_output=binary)
            current(index, 1)
        for binary, expected in ((False, 2), (True, 3)):
            wire.execute(name, oid, [expected], binary_output=binary)
            current(index, expected)
        wire.close(name)
        for mode in MODES:
            for shape in ("empty", "false", "limit"):
                values = [None] if index == 2 and shape != "limit" else []
                wire.query(mode, effect_sql(index, shape), values, oid)
                current(index, 3)

    # Ten computed/true pass-through cases * (describe +3modes +close)=50.
    cases = [
        ('SELECT CAST("lower"(d.t) AS VARCHAR(3)) AS v FROM (SELECT id,t FROM ' + table + ') AS d ORDER BY d.id',
         ["abc", "", None], 1043, 7),
        ('WITH c AS (SELECT id,t FROM ' + table + ') SELECT CAST("lower"(c.t) AS VARCHAR(3)) AS v FROM c ORDER BY c.id',
         ["abc", "", None], 1043, 7),
        ('SELECT d.v AS v FROM (SELECT CAST("lower"(\'AbCd\'::text) AS VARCHAR(3)) AS v) AS d',
         ["abc"], 1043, 7),
        ("SELECT d.v AS v FROM (SELECT 'a''b café'::text AS v) AS d", ["a'b café"], 25, -1),
        ("SELECT d.v AS v FROM (SELECT ''::text AS v) AS d", [""], 25, -1),
        ("SELECT d.v AS v FROM (SELECT NULL::integer AS v) AS d", [None], 23, -1),
        ('SELECT "d.dot"."v.dot" AS v FROM (SELECT "length"(\'abc\'::text) AS "v.dot") AS "d.dot"', [3], 23, -1),
        ('SELECT "d""q"."v""q" AS v FROM (SELECT "length"(\'abc\'::text) AS "v""q") AS "d""q"', [3], 23, -1),
        ('SELECT d."v.dot" AS v FROM (SELECT "length"(\'abc\'::text) AS original) AS d("v.dot")', [3], 23, -1),
        ('SELECT "d""q"."v""q" AS v FROM (SELECT "length"(\'abc\'::text) AS original) AS "d""q"("v""q")', [3], 23, -1),
    ]
    for index, (sql, values, oid, typmod) in enumerate(cases):
        name = statement("computed", index)
        wire.describe(name, sql, oid, typmod=typmod)
        for mode in MODES:
            wire.query(mode, sql, values, oid, typmod=typmod)
        wire.close(name)

    # 8errors*4source shapes*(failedParse+same-name recovery+execute+close)=128.
    missing = identifier("R174.Missing_" + token)
    recovery = 'SELECT "length"(\'abc\'::text) AS v FROM (SELECT id FROM ' + table + ') AS d WHERE id=1'
    for error_index in range(8):
        for shape_index, shape in enumerate(("rows", "false", "limit", "empty")):
            relation = empty if shape == "empty" else table
            derived = "(SELECT n FROM " + relation + ") AS d"
            bad = (
                'SELECT "abs"(d.missing) AS v FROM ' + derived,
                "SELECT " + missing + "(nextval('" + sequences[0] + "')) AS v FROM " + derived,
                'WITH c AS (SELECT n FROM ' + relation + ') SELECT "abs"(c.missing) AS v FROM c',
                "WITH c AS (SELECT n FROM " + relation + ") SELECT " + missing + "(c.n) AS v FROM c",
                'SELECT sum("abs"(missing)) AS v FROM ' + relation,
                "SELECT sum(" + missing + "(nextval('" + sequences[2] + "'))) AS v FROM " + relation,
                'SELECT "abs"("not.range".n) AS v FROM ' + derived,
                'WITH c AS (SELECT missing AS n FROM ' + relation + ') SELECT "abs"(c.n) AS v FROM c',
            )[error_index]
            bad += " WHERE false" if shape == "false" else " LIMIT 0" if shape == "limit" else ""
            state = ("42703", "42883", "42703", "42883", "42703", "42883", "42P01", "42703")[error_index]
            name = statement("bad", 4 * error_index + shape_index)
            wire.describe(name, bad, state=state)
            wire.describe(name, recovery)
            wire.execute(name, 23, [3])
            wire.close(name)
    for index in range(3):
        current(index, 3)

    # Existing failed-transaction protection precedes this new Parse owner (7).
    name = statement("failed_tx", 0)
    wire.utility("BEGIN", "BEGIN", b"T")
    wire.query("simple", "SELECT n AS v FROM " + schema + ".absent", state="42P01", ready=b"E")
    wire.describe(name, effect_sql(0, "rows"), 20, state="25P02", ready=b"E")
    wire.utility("ROLLBACK", "ROLLBACK", b"I")
    wire.describe(name, effect_sql(0, "rows"), 20)
    current(0, 3)
    wire.close(name)

    # Existing aggregate role owners: Parse/Describe only, not an R175 execution claim (6).
    for index, target in enumerate(("sum(n ORDER BY id)", "sum(DISTINCT n)", "sum(n) FILTER(WHERE id>1)")):
        name = statement("roles", index)
        wire.describe(name, "SELECT " + target + " AS v FROM " + table, 20)
        wire.close(name)

    # A direct physical TABLE column keeps the old nonzero-origin owner (5).
    name, physical = statement("physical", 0), "SELECT n AS v FROM " + table + " ORDER BY id"
    wire.describe(name, physical, origin=True)
    for mode in MODES:
        wire.query(mode, physical, [-2, 3, None], origin=True)
    wire.close(name)

    # Existing R166 unknown source-free inference and already-typed whole-query
    # metadata remain their owners. No unknown-$n derived-source inference is
    # assigned a fabricated product oracle here. 3cases*(describe+4bind+4exec+close)=30.
    parameter_cases = [
        ("SELECT " + echo + "($1) AS v", (), (23,), [-7]),
        ("SELECT " + echo + "($1) AS v", (0,), (23,), [-7]),
        ('SELECT "abs"(d.n)+$1 AS v FROM (SELECT id,n FROM ' + table + ') AS d ORDER BY d.id',
         (23,), (23,), [-5, -4, None]),
    ]
    for index, (sql, declared, resolved, values) in enumerate(parameter_cases):
        name = statement("parameter", index)
        wire.describe(name, sql, declared=declared, resolved=resolved)
        for binary_input in (False, True):
            for binary_output in (False, True):
                wire.bind_describe(name, 23, resolved, (-7,), binary_input, binary_output)
                wire.execute(name, 23, values, resolved, (-7,), binary_input, binary_output)
        wire.close(name)


def main():
    label = "COMPUTED_QUERY_HOST_METADATA_PROTOCOL"
    try:
        review = HostReview(label)
    except Exception:
        print(label + "_COMPLETE", 0, "FAILURES", 1, flush=True)
        raise
    wire, token = HostWire(review), uuid.uuid4().hex
    schema = identifier("R174.Scope_" + token)
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
    # Setup13 +effect87 +computed50 +negative128 +counter3 +transaction7
    # +role6 +physical5 +parameter30 +cleanup2 =331. Every operation collects.
    review.complete()


if __name__ == "__main__":
    main()

