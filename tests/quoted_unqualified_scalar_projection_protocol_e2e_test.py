#!/usr/bin/env python3
"""Regression: quoted-unqualified SQL scalar identity over real table rows.

Compare the entire fixture with PostgreSQL 18 before product acceptance.
This does not change the native SelectExpr API: native callee names are already
decoded identities. Literal dots and embedded quotes below are single SQL
identifier tokens, not namespaces. Quoted aggregate admission is deliberately
not assigned a product-only error oracle; it remains outside this finite fix.
"""

from compat.numeric_type_review import Review
import struct
import uuid


MODES = ("simple", "extended", "binary")
EXPECTED_CONTROLS = 791
WIDTHS = {20: 8, 23: 4, 25: -1, 701: 8}
PACK = {20: "!q", 23: "!i", 701: "!d"}
FACTOR = float.fromhex("0x1.1df46a2529d39p-6")
FLOATS = (0.0, 1.0, None, -1.0)


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def angle(name, value):
    if value is None:
        return None
    if name == "degrees":
        return value / FACTOR
    if name == "radians":
        return value * FACTOR
    return {0.0: value, 1.0: 0.8414709848078965, -1.0: -0.8414709848078965}[value]


def datum(value, oid, binary):
    if value is None:
        return None
    if binary and oid in PACK:
        return struct.pack(PACK[oid], value)
    return str(value).encode("utf-8")


def matches(raw, value, oid, binary):
    if raw is None or value is None:
        return raw is None and value is None
    if binary or oid != 701:
        return raw == datum(value, oid, binary)
    try:
        # Decimal formatting is not this identity fix's owner; require the
        # exact float8 value, and exact IEEE bytes for every binary result.
        return struct.pack("!d", float(raw.decode("ascii"))) == struct.pack("!d", value)
    except (UnicodeError, ValueError, OverflowError, struct.error):
        return False


def strict_data_rows(messages):
    """Reject non-NULL negative lengths, truncation, overrun and trailing bytes."""
    rows = []
    for kind, body in messages:
        if kind != b"D":
            continue
        if len(body) < 2:
            return [], False
        columns = struct.unpack_from("!H", body, 0)[0]
        offset, row = 2, []
        for _ in range(columns):
            if len(body) - offset < 4:
                return [], False
            length = struct.unpack_from("!i", body, offset)[0]
            offset += 4
            if length == -1:
                row.append(None)
            elif length < -1 or length > len(body) - offset:
                return [], False
            else:
                row.append(body[offset:offset + length])
                offset += length
        if offset != len(body):
            return [], False
        rows.append(row)
    return rows, True


class ScalarReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("CONTROL INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock

    def collect(self, label, producer, validator, expected):
        # One control per operation even when transport/decoding fails. Do not
        # stop collecting at the first old-product identity failure.
        try:
            actual = producer()
            good = validator(actual)
        except Exception as error:
            actual, good = (type(error).__name__, str(error)), False
        self.review.record(label, good, actual, expected)

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode("ascii")
                for kind, body in messages if kind == b"E"]

    def description(self, oid, binary=False):
        # Exact expression-field origin, OID, width, typmod and format. Raw
        # body comparison also rejects malformed/trailing RowDescription data.
        return struct.pack("!H", 1) + b"v\0" + struct.pack("!IhIhih", 0, 0, oid, WIDTHS[oid], -1, int(binary))

    def parameters(self, inputs):
        return struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", oid) for oid in inputs)

    def parse(self, statement, sql, inputs=()):
        body = statement + b"\0" + sql.encode("utf-8") + b"\0" + self.parameters(inputs)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind(self, statement, inputs=(), values=(), binary_input=False, binary_output=False, execute=True):
        assert len(inputs) == len(values)
        body = b"\0" + statement + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!H", int(binary_input)) for _ in inputs)
        body += struct.pack("!H", len(values))
        for oid, value in zip(inputs, values):
            raw = datum(value, oid, binary_input)
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        packet = self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0")
        if execute:
            packet += self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        return packet

    def result_ok(self, messages, values, oid, state=None, binary=False,
                  parsed=False, bound=False, simple=False, inputs=()):
        rows, valid_rows = strict_data_rows(messages)
        kinds = [kind for kind, _ in messages]
        descriptions = [body for kind, body in messages if kind == b"T"]
        parameters = [body for kind, body in messages if kind == b"t"]
        tags = [body for kind, body in messages if kind == b"C"]
        if state:
            # Missing quoted identities are analysis errors, including over an
            # actual empty table, WHERE false and LIMIT 0. PG fails Parse here.
            good = (kinds == [b"E", b"Z"] and self.errors(messages) == [state] and
                    not rows and not descriptions and not parameters and not tags)
        else:
            prefix = [b"T"] if simple else [b"1", b"t", b"T"] if parsed else []
            wanted_descriptions = [self.description(oid)] if parsed else []
            if simple or bound:
                wanted_descriptions.append(self.description(oid, binary))
            if bound:
                prefix += [b"2", b"T"]
            good = (not self.errors(messages) and
                    kinds == prefix + [b"D"] * len(values) + [b"C", b"Z"] and
                    descriptions == wanted_descriptions and
                    parameters == ([self.parameters(inputs)] if parsed else []) and
                    tags == [("SELECT " + str(len(values))).encode("ascii") + b"\0"] and
                    len(rows) == len(values) and
                    all(len(row) == 1 and matches(row[0], value, oid, binary)
                        for row, value in zip(rows, values)))
        return (good and valid_rows and messages[-1:] == [(b"Z", b"I")] and
                all(body == b"" for kind, body in messages if kind in (b"1", b"2")))

    def query(self, mode, sql, values=(), oid=701, state=None):
        binary = mode == "binary"
        producer = (lambda: self.client.simple_query(self.sock, sql)) if mode == "simple" else (
            lambda: self.exchange(self.parse(b"", sql) + self.bind(b"", binary_output=binary)))
        self.collect((mode, sql), producer,
                     lambda messages: self.result_ok(messages, values, oid, state, binary,
                         parsed=mode != "simple", bound=mode != "simple", simple=mode == "simple"),
                     (values, None if state else oid, None if state else WIDTHS[oid], state, binary))

    def describe(self, statement, sql, oid, inputs=(), state=None):
        def valid(messages):
            if state:
                return ([kind for kind, _ in messages] == [b"E", b"Z"] and
                        self.errors(messages) == [state] and messages[-1:] == [(b"Z", b"I")])
            return (messages == [(b"1", b""), (b"t", self.parameters(inputs)),
                                 (b"T", self.description(oid)), (b"Z", b"I")])
        self.collect(("PARSE/DESCRIBE", statement, sql, inputs),
                     lambda: self.exchange(self.parse(statement, sql, inputs)), valid,
                     (None if state else oid, inputs, state, "no execution"))

    def bind_describe(self, statement, oid, inputs, values, binary_input, binary_output):
        self.collect(("BIND/DESCRIBE", statement, values, binary_input, binary_output),
                     lambda: self.exchange(self.bind(statement, inputs, values, binary_input, binary_output, False)),
                     lambda messages: messages == [(b"2", b""), (b"T", self.description(oid, binary_output)), (b"Z", b"I")],
                     (oid, "no execution"))

    def execute(self, statement, oid, values, inputs=(), parameters=(), binary_input=False,
                binary_output=False, repeat=None):
        self.collect(("BIND/EXECUTE", statement, parameters, binary_input, binary_output, repeat),
                     lambda: self.exchange(self.bind(statement, inputs, parameters, binary_input, binary_output)),
                     lambda messages: self.result_ok(messages, values, oid, binary=binary_output, bound=True),
                     (values, oid, WIDTHS[oid], binary_output))

    def close(self, statement):
        wanted = [(b"3", b""), (b"Z", b"I")]
        self.collect(("CLOSE", statement), lambda: self.exchange(self.client.typed(b"C", b"S" + statement + b"\0")),
                     lambda messages: messages == wanted, wanted)

    def command(self, sql, tag, notices=False, parameter_status=None):
        def valid(messages):
            kinds = [kind for kind, _ in messages]
            prefix = [b"N"] * sum(kind == b"N" for kind in kinds) if notices else []
            allowed = [prefix + [b"C", b"Z"]]
            statuses = [body for kind, body in messages if kind == b"S"]
            status_ok = not statuses
            if parameter_status is not None:
                # Only these five SET operations may report this one exact
                # search_path value, after CommandComplete and before Ready.
                wanted = b"search_path\0" + parameter_status.encode("utf-8") + b"\0"
                allowed.append(prefix + [b"C", b"S", b"Z"])
                status_ok = statuses in ([], [wanted])
            return (kinds in allowed and status_ok and not self.errors(messages) and
                    [body for kind, body in messages if kind == b"C"] == [tag.encode("ascii") + b"\0"] and
                    messages[-1:] == [(b"Z", b"I")])
        self.collect(sql, lambda: self.client.simple_query(self.sock, sql), valid, tag)


def run(review, wire, token, schema, other):
    table, empty, source = schema + ".projection_rows", schema + ".empty_rows", '"r.dot"'
    x, n, text = (source + "." + identifier(name) for name in ("x", "n", "t"))
    tail, empty_tail = " FROM " + table + " AS " + source, " FROM " + empty + " AS " + source
    ordered = " ORDER BY " + source + ".id"
    statement_name = lambda group, index: ("r170_" + token + "_" + group + "_" + str(index)).encode("ascii")
    dot, quote, mixed = (identifier(name) for name in ("Name.With.Dot", 'Say"Hi', "MiXeD"))
    wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
    wire.command("CREATE SCHEMA " + other, "CREATE SCHEMA")
    definition = "(id INTEGER PRIMARY KEY,x DOUBLE PRECISION,n INTEGER,t TEXT)"
    wire.command("CREATE TABLE " + table + definition, "CREATE TABLE")
    wire.command("CREATE TABLE " + empty + definition, "CREATE TABLE")
    wire.command("INSERT INTO " + table + " VALUES(1,0,-2,'abc'),(2,1,3,''),(3,NULL,NULL,NULL),(4,-1,-4,'a,b')", "INSERT 0 4")
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)

    # V2_NO_SHADOW_BEGIN: fourteen additional controls before any user degrees
    # exists. The old stored-name fallback must not mask the observed builtin.
    no_shadow = statement_name("unshadowed", 0)
    wire.describe(no_shadow, 'SELECT "degrees"(' + x + ") AS v" + tail, 701)
    for mode in MODES:
        for callee in ("degrees", '"degrees"'):
            wire.query(mode, "SELECT " + callee + "(" + x + ") AS v" + tail + ordered,
                       [angle("degrees", value) for value in FLOATS])
            wire.query(mode, "SELECT " + callee + "(1::float8) AS v", [angle("degrees", 1.0)])
    wire.close(no_shadow)
    # V2_NO_SHADOW_END

    wire.command("CREATE FUNCTION " + schema + "." + dot + "(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+10$$", "CREATE FUNCTION")
    wire.command("CREATE FUNCTION " + schema + "." + quote + "(p TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p || ':q'$$", "CREATE FUNCTION")
    wire.command("CREATE FUNCTION " + schema + "." + mixed + "(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+20$$", "CREATE FUNCTION")
    for owner, increment in ((schema, 100), (other, 200)):
        wire.command("CREATE FUNCTION " + owner + ".degrees(p DOUBLE PRECISION) RETURNS DOUBLE PRECISION LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+" + str(increment) + "$$", "CREATE FUNCTION")
    wire.command("CREATE FUNCTION " + other + "." + dot + "(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+30$$", "CREATE FUNCTION")

    cases = []
    for function in ("degrees", "radians", "sin"):
        for callee in (function, identifier(function)):
            cases.append((callee + "(" + x + ")", 701, [angle(function, value) for value in FLOATS]))
    for callee in ("length", '"length"'):
        cases.append((callee + "(" + text + ")", 23, [3, 0, None, 3]))
    for callee in ("abs", '"abs"'):
        cases.append((callee + "(" + n + ")", 23, [2, 3, None, 4]))
    cases.extend(((dot + "(" + n + ")", 23, [8, 13, None, 6]),
                  (quote + "(" + text + ")", 25, ["abc:q", ":q", None, "a,b:q"]),
                  (mixed + "(" + n + ")", 23, [18, 23, None, 16])))
    for index, (expression, oid, values) in enumerate(cases):
        prefix, statement = "SELECT " + expression + " AS v", statement_name("case", index)
        wire.describe(statement, prefix + tail, oid)
        for mode in MODES:
            wire.query(mode, prefix + tail + ordered, values, oid)
            wire.query(mode, prefix + empty_tail + ordered, [], oid)
            wire.query(mode, prefix + tail + " WHERE false", [], oid)
            wire.query(mode, prefix + tail + " LIMIT 0", [], oid)
        wire.close(statement)

    nested = (
        ('"degrees"("radians"(' + x + ")) AS v", 701, list(FLOATS), ordered),
        ('"length"("upper"(' + text + ")) AS v", 23, [3, 0, None, 3], ordered),
        ('"abs"("abs"(' + n + ")) v", 23, [2, 3, None, 4], ordered),
        ('"abs"(' + n + ") AS v", 23, [None, 2, 3, 4], " ORDER BY v NULLS FIRST," + source + ".id"),
        (dot + '("abs"(' + n + ")) AS v", 23, [12, 13, None, 14], ordered),
        (quote + '("lower"(' + text + ")) AS v", 25, ["abc:q", ":q", None, "a,b:q"], ordered),
    )
    for target, oid, values, order in nested:
        for mode in MODES:
            wire.query(mode, "SELECT " + target + tail + order, values, oid)

    # V2_SOURCE_BOUNDARIES_BEGIN: seven finite shapes, all three wire modes.
    # The WHERE/ORDER cases have no quoted callee in the projection. A quoted
    # child with no own FROM still belongs to the root's derived-source probe.
    boundaries = (
        ("SELECT " + source + '.id+0 AS v' + tail + ' WHERE "abs"(' + n + ')>2' + ordered,
         23, [2, 4]),
        ("SELECT " + source + '.id+0 AS v' + tail + ' ORDER BY "abs"(' + n + ') NULLS FIRST,' + source + '.id',
         23, [3, 1, 2, 4]),
        ('SELECT "abs"(d.n) AS v FROM (SELECT n,id FROM ' + table + ') AS d ORDER BY d.id',
         23, [2, 3, None, 4]),
        ('SELECT d.v AS v FROM (SELECT "length"(\'abc\'::text) AS v) AS d', 23, [3]),
        ('WITH c AS (SELECT id,n FROM ' + table + ') SELECT "abs"(c.n) AS v FROM c ORDER BY c.id',
         23, [2, 3, None, 4]),
        ('SELECT sum("abs"(' + n + ')) AS v' + tail, 20, [9]),
        ('SELECT count("abs"(' + n + ')) AS v' + tail, 20, [3]),
    )
    for sql, oid, values in boundaries:
        for mode in MODES:
            wire.query(mode, sql, values, oid)
    # V2_SOURCE_BOUNDARIES_END

    # Existing FROM-less admission and ordinary aggregate/window/SRF owners:
    # no quoted aggregate is assigned an expected product-only failure here.
    guards = (
        ('SELECT "degrees"(1::float8) AS v', 701, [angle("degrees", 1.0)]),
        ('SELECT "length"(\'abc\'::text) AS v', 23, [3]),
        ('SELECT "abs"(-2::integer) AS v', 23, [2]),
        ("SELECT sum(1) AS v", 20, [1]),
        ("SELECT count(1) AS v", 20, [1]),
        ("SELECT row_number() OVER(ORDER BY " + source + ".id) AS v" + tail + ordered, 20, [1, 2, 3, 4]),
        ("SELECT unnest(ARRAY[1,2]) AS v", 23, [1, 2]),
        ("SELECT sum(" + n + ") AS v" + tail, 20, [-3]),
        ("SELECT sum(DISTINCT " + n + ") AS v" + tail, 20, [-3]),
        ("SELECT sum(" + n + ") FILTER(WHERE " + source + ".id>1) AS v" + tail, 20, [-1]),
        ("SELECT sum(" + n + " ORDER BY " + source + ".id) AS v" + tail, 20, [-3]),
    )
    for sql, oid, values in guards:
        for mode in MODES:
            wire.query(mode, sql, values, oid)

    wire.command("SET search_path=" + schema + ",pg_catalog", "SET", parameter_status=schema + ", pg_catalog")
    for mode in MODES:
        for callee in ('"degrees"', "degrees"):
            wire.query(mode, "SELECT " + callee + "(" + x + ") AS v" + tail + ordered, [100.0, 101.0, None, 99.0])
        wire.query(mode, "SELECT pg_catalog.degrees(" + x + ") AS v" + tail + ordered, [angle("degrees", value) for value in FLOATS])
    wire.command("SET search_path=" + other + "," + schema + ",pg_catalog", "SET", parameter_status=other + ", " + schema + ", pg_catalog")
    for mode in MODES:
        wire.query(mode, 'SELECT "degrees"(' + x + ") AS v" + tail + ordered, [200.0, 201.0, None, 199.0])
        wire.query(mode, "SELECT " + dot + "(" + n + ") AS v" + tail + ordered, [28, 33, None, 26], 23)
        wire.query(mode, "SELECT " + schema + "." + dot + "(" + n + ") AS v" + tail + ordered, [8, 13, None, 6], 23)
    wire.command("SET search_path=pg_catalog," + schema, "SET", parameter_status="pg_catalog, " + schema)

    # V2_EFFECT_BOUNDARIES_BEGIN: thirty-four controls, no immutable-error or
    # skipped COALESCE demand policy. Quoted abs owns the new scalar admission;
    # ordinary nextval is VOLATILE, and has its existing bigint declaration.
    # Discovery/empty sources must not advance it; each row must advance once.
    sequence = schema + ".effect_seq"
    sequence_literal = "'" + sequence.replace("'", "''") + "'"
    effect_statement = statement_name("effect", 0)
    effect_sql = 'SELECT "abs"(nextval(' + sequence_literal + ")) AS v"
    current_sql = "SELECT currval(" + sequence_literal + ") AS v"
    wire.command("CREATE SEQUENCE " + sequence, "CREATE SEQUENCE")
    wire.query("simple", "SELECT nextval(" + sequence_literal + ") AS v", [1], 20)
    wire.describe(effect_statement, effect_sql + tail + " WHERE " + source + ".id=1", 20)
    wire.query("simple", current_sql, [1], 20)
    for binary_output in (False, True):
        wire.bind_describe(effect_statement, 20, (), (), False, binary_output)
        wire.query("simple", current_sql, [1], 20)
    for mode in MODES:
        for source_sql in (empty_tail, tail + " WHERE false", tail + " LIMIT 0"):
            wire.query(mode, effect_sql + source_sql, [], 20)
            wire.query("simple", current_sql, [1], 20)
    for expected, mode in enumerate(MODES, 2):
        wire.query(mode, effect_sql + tail + " WHERE " + source + ".id=1", [expected], 20)
        wire.query("simple", current_sql, [expected], 20)
    wire.close(effect_statement)
    wire.command("DROP SEQUENCE " + sequence, "DROP SEQUENCE")
    # V2_EFFECT_BOUNDARIES_END

    float_inputs = [(value,) for value in (0.0, 1.0, None, -1.0, 0.0)]
    integers = [(-2,), (3,), (None,), (-4,), (0,)]
    texts = [(value,) for value in ("abc", "", None, 'O"Brien', "a.b")]
    parameters = []
    for function in ("degrees", "radians", "sin"):
        parameters.append((identifier(function) + "($1)", (701,), 701, float_inputs,
                           [angle(function, value[0]) for value in float_inputs]))
    parameters.extend((('"length"($1)', (25,), 23, texts, [3, 0, None, 7, 3]),
                       ('"abs"($1)', (23,), 23, integers, [2, 3, None, 4, 0]),
                       (dot + "($1)", (23,), 23, integers, [8, 13, None, 6, 10]),
                       (quote + "($1)", (25,), 25, texts, ["abc:q", ":q", None, 'O"Brien:q', "a.b:q"])))
    for index, (expression, inputs, oid, values, outputs) in enumerate(parameters):
        statement = statement_name("bind", index)
        wire.describe(statement, "SELECT " + expression + " AS v" + tail + " WHERE " + source + ".id=1", oid, inputs)
        for binary_input in (False, True):
            for binary_output in (False, True):
                wire.bind_describe(statement, oid, inputs, values[0], binary_input, binary_output)
        pairs = list(zip(values, outputs))
        for repeat in (0, 1):
            for value, output in (pairs if repeat == 0 else list(reversed(pairs))):
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        wire.execute(statement, oid, [output], inputs, value, binary_input, binary_output, repeat)
        wire.close(statement)

    invalid = ('"Degrees"(' + x + ")", '"DEGREES"(' + x + ")", '"Length"(' + text + ")",
               '"ABS"(' + n + ")", '"name.with.dot"(' + n + ")", '"say""hi"(' + text + ")",
               '"mixed"(' + n + ")", '"pg_catalog.degrees"(' + x + ")")
    for index, expression in enumerate(invalid):
        statement, prefix = statement_name("missing", index), "SELECT " + expression + " AS v"
        wire.describe(statement, prefix + tail, 701, state="42883")
        for mode in MODES:
            for suffix in ("", " WHERE false", " LIMIT 0"):
                wire.query(mode, prefix + tail + suffix, state="42883")
            wire.query(mode, prefix + empty_tail, state="42883")
        # Failed named Parse must not reserve that name or poison the socket.
        recovery = 'SELECT "degrees"(' + x + ") AS v" + tail + " WHERE " + source + ".id=2"
        wire.describe(statement, recovery, 701)
        wire.execute(statement, 701, [angle("degrees", 1.0)], binary_output=True)
        wire.close(statement)
    for mode in MODES:
        wire.query(mode, 'SELECT "sin"(' + x + ") AS v" + tail + " WHERE " + source + ".id=1", [0.0])


def main():
    label = "QUOTED_UNQUALIFIED_SCALAR_PROJECTION_PROTOCOL"
    try:
        review = ScalarReview(label)
    except Exception:
        print(label + "_COMPLETE", 0, "FAILURES", 1, flush=True)
        raise
    wire = Wire(review)
    token = uuid.uuid4().hex
    schema, other = identifier("R170.Scope_" + token), identifier("R170.Other_" + token)
    try:
        run(review, wire, token, schema, other)
    except Exception as error:
        review.failures.append(("UNEXPECTED COLLECTOR EXCEPTION", type(error).__name__, str(error)))
    finally:
        wire.command("SET search_path=public,pg_catalog", "SET", parameter_status="public, pg_catalog")
        wire.command("DROP SCHEMA " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        wire.command("DROP SCHEMA " + other + " CASCADE", "DROP SCHEMA", notices=True)
        try:
            review.close()
        except Exception as error:
            review.failures.append(("CLOSE", type(error).__name__, str(error)))
    # Original722 controls all retained; the initial SET moved before UDF DDL.
    # Setup12 + no-shadow14 + core13*(describe1 + modes3*four source shapes + close1)=182
    # + nested6*3=18 + source boundaries7*3=21 + unchanged-owner guards11*3=33 + shadowing21
    # + volatile-effect34
    # + retained7*(describe1+bind-describe4+two passes*five values*four formats+close1)=322
    # + missing8*(describe1+four source shapes*3+same-name retry/execute/close3)=128
    # + recovery3 + cleanup3 = 791. One record per planned wire operation.
    review.complete()


if __name__ == "__main__":
    main()
