#!/usr/bin/env python3
"""R161: qualified routine identity survives real-table projection lowering.

Run the entire fixture on PostgreSQL 18.6 before treating it as acceptance
evidence. Ordinary +/-1, +/-0 and NULL keep this independent of R158 angle
range fixes. The R158 1449-control fixture is separate and remains unchanged.
Qualified grammar is not a decoded identifier to split on a dot: both schema
and routine identifiers below deliberately contain punctuation and quotes.
"""

from compat.numeric_type_review import Review
import struct
import uuid


MODES = ("simple", "extended", "binary")
TYPES = {701: (8, "!d"), 23: (4, "!i"), 20: (8, "!q"), 25: (-1, None)}
EXPECTED_CONTROLS = 637
FACTOR = float.fromhex("0x1.1df46a2529d39p-6")
INPUTS = (0.0, -0.0, 1.0, -1.0, None, 0.0, None)


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def angle(function, value):
    if value is None:
        return None
    if function == "degrees":
        return value / FACTOR
    if function == "radians":
        return value * FACTOR
    # Fixed ordinary sin controls; no platform-dependent broad libm oracle.
    return {0.0: value, 1.0: 0.8414709848078965, -1.0: -0.8414709848078965}[value]


def raw_value(value, oid, binary):
    if value is None:
        return None
    if not binary or oid == 25:
        return str(value).encode("utf-8")
    return struct.pack(TYPES[oid][1], value)


def matches(raw, value, oid, binary):
    if raw is None or value is None:
        return raw is None and value is None
    try:
        if binary or oid == 25:
            return raw == raw_value(value, oid, binary)
        if oid == 701:
            return struct.pack("!d", float(raw.decode("ascii"))) == struct.pack("!d", value)
        return raw == str(value).encode("ascii")
    except (ValueError, UnicodeError, OverflowError, struct.error):
        return False


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                for kind, body in messages if kind == b"E"]

    def descriptions(self, messages):
        return [self.client.row_description_fields([message])
                for message in messages if message[0] == b"T"]

    def fields(self, oid, binary=False):
        return [(b"v", 0, 0, oid, TYPES[oid][0], -1, int(binary))]

    def parse(self, statement, sql, inputs=()):
        body = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!I", oid) for oid in inputs)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind(self, statement, inputs=(), values=(), binary_input=False, binary_output=False, execute=True):
        body = b"\0" + statement + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!H", int(binary_input)) for _ in inputs)
        body += struct.pack("!H", len(values))
        for oid, value in zip(inputs, values):
            raw = raw_value(value, oid, binary_input)
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        packet = self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0")
        if execute:
            packet += self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        return packet

    def result(self, label, messages, values, oid, state=None, binary=False,
               parsed=False, bound=False, simple=False, inputs=()):
        errors, rows = self.errors(messages), self.client.data_row_values(messages)
        descriptions = self.descriptions(messages)
        tags = [body for kind, body in messages if kind == b"C"]
        parameters = [body for kind, body in messages if kind == b"t"]
        if state:
            # Existing binder errors: assert SQLSTATE and no result, without
            # inventing a Parse/Bind/Execute timing requirement for R161.
            good = errors == [state] and not rows and not tags
        else:
            wanted = ([self.fields(oid)] if parsed else [])
            if simple or bound:
                wanted.append(self.fields(oid, binary))
            parameter_body = struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", n) for n in inputs)
            good = (not errors and descriptions == wanted and len(rows) == len(values) and
                    parameters == ([parameter_body] if parsed else []) and
                    tags == [("SELECT " + str(len(values))).encode() + b"\0"] and
                    sum(kind == b"1" for kind, _ in messages) == int(parsed) and
                    sum(kind == b"2" for kind, _ in messages) == int(bound) and
                    all(len(row) == 1 and matches(row[0], value, oid, binary)
                        for row, value in zip(rows, values)))
        good = good and messages[-1:] == [(b"Z", b"I")] and not any(kind in (b"N", b"s") for kind, _ in messages)
        self.review.record(label, good, (errors, rows, descriptions, parameters, tags, messages[-1:]),
                           (values, oid, state, "binary" if binary else "text"))

    def query(self, mode, sql, values=(), oid=701, state=None):
        if mode == "simple":
            self.result((mode, sql), self.client.simple_query(self.sock, sql), values, oid, state, simple=True)
        else:
            binary = mode == "binary"
            messages = self.exchange(self.parse(b"", sql) + self.bind(b"", binary_output=binary))
            self.result((mode, sql), messages, values, oid, state, binary, parsed=True, bound=True)

    def describe(self, sql, oid, statement=b"", inputs=(), state=None):
        messages = self.exchange(self.parse(statement, sql, inputs))
        if state:
            good = self.errors(messages) == [state] and not any(kind in (b"D", b"C", b"1") for kind, _ in messages)
        else:
            parameter_body = struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", n) for n in inputs)
            good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(oid)] and
                    [body for kind, body in messages if kind == b"t"] == [parameter_body] and
                    sum(kind == b"1" for kind, _ in messages) == 1 and
                    not any(kind in (b"D", b"C", b"2", b"N") for kind, _ in messages))
        good = good and messages[-1:] == [(b"Z", b"I")]
        self.review.record(("PARSE/DESCRIBE", sql, inputs), good, messages, (oid, inputs, state, "no execution"))

    def bind_describe(self, statement, oid, inputs=(), values=(), binary_input=False, binary_output=False):
        messages = self.exchange(self.bind(statement, inputs, values, binary_input, binary_output, False))
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(oid, binary_output)] and
                sum(kind == b"2" for kind, _ in messages) == 1 and
                not any(kind in (b"D", b"C", b"1", b"t", b"N") for kind, _ in messages) and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record(("BIND/DESCRIBE", statement, values, binary_input, binary_output), good,
                           messages, (self.fields(oid, binary_output), "no execution"))

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", b"I")]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)

    def command(self, sql, tag, notices=False):
        messages = self.client.simple_query(self.sock, sql)
        forbidden = (b"T", b"D") if notices else (b"T", b"D", b"N")
        good = (not self.errors(messages) and [body for kind, body in messages if kind == b"C"] ==
                [tag.encode() + b"\0"] and not any(kind in forbidden for kind, _ in messages) and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record(sql, good, messages, tag)


def main():
    review = Review("QUALIFIED_PROJECTION_ROUTINE_IDENTITY_PROTOCOL")
    wire = Wire(review)
    schema = identifier("R161.Scope_" + uuid.uuid4().hex[:10])
    table, source = schema + ".projection_rows", '"r.dot"'
    x, text, integer = (source + "." + identifier(name) for name in ("X", "Text", "Count"))
    tail = " FROM " + table + " AS " + source
    ordered = " ORDER BY " + source + ".id"
    mix, dot, quote, mixed = (schema + "." + identifier(name)
                              for name in ("mix", "Name.With.Dot", 'Say"Hi', "MiXeD"))
    text_values = ["alpha:2", ":-3", "a,b:4", "O'Reilly:5", None, "NULL:0", None]
    integer_values = [9, 4, 11, 12, None, 7, 16]
    try:
        wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        wire.command("CREATE TABLE " + table + '(id INTEGER PRIMARY KEY,"X" DOUBLE PRECISION,"Text" TEXT,"Count" INTEGER)', "CREATE TABLE")
        wire.command("INSERT INTO " + table + " VALUES(1,'0'::float8,'alpha',2),(2,'-0'::float8,'',-3),"
                     "(3,1,'a,b',4),(4,-1,'O''Reilly',5),(5,NULL,NULL,NULL),(6,0,'NULL',0),(7,NULL,NULL,9)", "INSERT 0 7")
        for name in (mix, dot, quote):
            wire.command("CREATE FUNCTION " + name + "(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                         "AS $$SELECT t || ':' || n::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + mixed + "(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n+7$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + schema + ".degrees(x DOUBLE PRECISION) RETURNS DOUBLE PRECISION "
                     "LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT x+100$$", "CREATE FUNCTION")
        wire.command("SET search_path=" + schema + ",pg_catalog", "SET")

        cases = []
        for function in ("degrees", "radians", "sin"):
            values = [angle(function, value) for value in INPUTS]
            for name in ("pg_catalog." + function, '"pg_catalog".' + identifier(function)):
                cases.append((name + "(" + x + ")", 701, values))
        for name in (schema + ".mix", mix, dot, quote):
            cases.append((name + "(" + text + "," + integer + ")", 25, text_values))
        cases.append((mixed + "(" + integer + ")", 23, integer_values))
        shadow_values = [None if value is None else value + 100 for value in INPUTS]
        cases.extend(((schema + ".degrees(" + x + ")", 701, shadow_values),
                      ("degrees(" + x + ")", 701, shadow_values)))
        for index, (expression, oid, values) in enumerate(cases):
            sql, statement = "SELECT " + expression + " AS v" + tail, ("r161_case_" + str(index)).encode()
            wire.describe(sql, oid, statement)
            for mode in MODES:
                wire.query(mode, sql + ordered, values, oid)
                wire.query(mode, sql + " WHERE false", [], oid)
                wire.query(mode, sql + " LIMIT 0", [], oid)
            for binary in (False, True):
                wire.bind_describe(statement, oid, binary_output=binary)
            wire.close(statement)

        # Actual argument grammar: two arguments stay separate; casts, commas
        # inside text, arithmetic and qualified field identities survive.
        argument_values = ["alpha:suffix:3", ":suffix:-2", "a,b:suffix:5", "O'Reilly:suffix:6", None, "NULL:suffix:1", None]
        for name in (schema + ".mix", mix, dot, quote):
            sql = "SELECT " + name + "(" + text + " || ':suffix'," + integer + "+1) AS v" + tail + ordered
            for mode in MODES:
                wire.query(mode, sql, argument_values, 25)
        for mode in MODES:
            wire.query(mode, "SELECT pg_catalog.degrees(pg_catalog.radians(" + x + ")) AS v" + tail + ordered, list(INPUTS))

        # Whole qualified grammar must not enter legacy predicate scans of
        # string contents or nested CASE, and implicit aliases are AST roles.
        for mode in MODES:
            wire.query(mode, "SELECT pg_catalog.upper('a is null') AS v" + tail + " LIMIT 1", ["A IS NULL"], 25)
            wire.query(mode, "SELECT pg_catalog.lower(CASE WHEN " + text + " IS NULL THEN 'missing' ELSE " +
                       text + " END) AS v" + tail + ordered,
                       ["alpha", "", "a,b", "o'reilly", "missing", "null", "missing"], 25)
            wire.query(mode, "SELECT pg_catalog.degrees(" + x + ") v" + tail + ordered,
                       [angle("degrees", value) for value in INPUTS])

        # Extending the table receiver must not narrow the pre-existing
        # FROM-less qualified aggregate owner, including DISTINCT and FILTER.
        for expression in ("pg_catalog.sum(1)", "pg_catalog.count(1)",
                           "pg_catalog.sum(DISTINCT 1)",
                           "pg_catalog.sum(1) FILTER(WHERE true)"):
            sql = "SELECT " + expression + " AS v"
            wire.describe(sql, 20)
            for mode in MODES:
                wire.query(mode, sql, [1], 20)

        # Retained statements read new text/binary cells in both directions;
        # each expression has a real table source rather than a FROM-less path.
        parameters = []
        float_inputs = [(value,) for value in (0.0, -0.0, 1.0, -1.0, None, 1.0)]
        for function in ("degrees", "radians", "sin"):
            parameters.append(("pg_catalog." + function + "($1)", (701,), 701, float_inputs,
                               [angle(function, value[0]) for value in float_inputs]))
        text_inputs = [("bind", 2), ("", -3), ("a,b", 4), ("NULL", 0), (None, 5), ("after", None)]
        for name in (mix, dot):
            parameters.append((name + "($1,$2)", (25, 23), 25, text_inputs,
                               ["bind:2", ":-3", "a,b:4", "NULL:0", None, None]))
        parameters.append((mixed + "($1)", (23,), 23, [(0,), (-1,), (2,), (None,), (7,), (0,)], [7, 6, 9, None, 14, 7]))
        for index, (expression, inputs, oid, values, outputs) in enumerate(parameters):
            statement = ("r161_bind_" + str(index)).encode()
            wire.describe("SELECT " + expression + " AS v" + tail + " WHERE " + source + ".id=1", oid, statement, inputs)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, oid, inputs, values[0], binary_input, binary_output)
            for repeat in (0, 1):
                pairs = list(zip(values, outputs))
                for value, output in (pairs if repeat == 0 else list(reversed(pairs))):
                    for binary_input in (False, True):
                        for binary_output in (False, True):
                            messages = wire.exchange(wire.bind(statement, inputs, value, binary_input, binary_output))
                            wire.result(("REEXECUTE", statement, value, binary_input, binary_output, repeat),
                                        messages, [output], oid, binary=binary_output, bound=True)
            wire.close(statement)

        # Existing namespaces only: no guessed missing-schema SQLSTATE.
        invalid = ("pg_catalog.r161_missing(" + x + ")", "pg_catalog.mix(" + text + "," + integer + ")",
                   "pg_catalog.degrees(" + text + ")", "pg_catalog.sin(" + text + ")", "pg_catalog.degrees()",
                   "pg_catalog.sin(" + x + "," + x + ")", mix + "(" + integer + "," + text + ")",
                   schema + ".mixed(" + integer + ")", dot + "(" + text + ")")
        for expression in invalid:
            sql = "SELECT " + expression + " AS v" + tail
            wire.describe(sql, 701, state="42883")
            for suffix in ("", " WHERE false", " LIMIT 0"):
                for mode in MODES:
                    wire.query(mode, sql + suffix, state="42883")
        for mode in MODES:
            wire.query(mode, "SELECT pg_catalog.sin(" + x + ") AS v" + tail + " WHERE " + source + ".id=1", [0.0])
    finally:
        try:
            wire.command("SET search_path=public", "SET")
            wire.command("DROP SCHEMA " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Setup9 + thirteen cases*13=169 + argument12 + nested3 + six retained
    # statements*54=324 + invalid9*10=90 + recovery3 + cleanup2
    # + original-source/CASE/implicit-alias controls9
    # + preserved FROM-less qualified aggregate controls16 = 637.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
