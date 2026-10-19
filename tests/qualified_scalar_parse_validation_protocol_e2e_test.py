#!/usr/bin/env python3
"""R163: qualified scalar signatures fail during Parse, before publication.

Use the complete --reference18 run as the oracle gate. R162 owns declared
stored-argument matching; its swapped-type negative remains mandatory here.
R161's 637 and R158's 1449 controls are separate and are not replaced by this
fixture. No extreme angle values or deferred modules are involved.
"""

from compat.numeric_type_review import Review
import struct
import uuid


EXPECTED_CONTROLS = 1017
TYPES = {701: (8, "!d"), 23: (4, "!i"), 20: (8, "!q"), 25: (-1, None)}
SUFFIXES = ("", " WHERE false", " LIMIT 0")
FACTOR = float.fromhex("0x1.1df46a2529d39p-6")


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


def raw(value, oid, binary):
    if value is None:
        return None
    return (struct.pack(TYPES[oid][1], value) if binary and oid != 25 else str(value).encode())


def matches(value, expected, oid, binary):
    if value is None or expected is None:
        return value is None and expected is None
    try:
        if oid == 701 and not binary:
            return struct.pack("!d", float(value.decode("ascii"))) == struct.pack("!d", expected)
        return value == raw(expected, oid, binary)
    except (ValueError, UnicodeError, OverflowError, struct.error):
        return False


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock
        self.ready = b"I"

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def fields(self, oid, binary=False):
        return [(b"v", 0, 0, oid, TYPES[oid][0], -1, int(binary))]

    def descriptions(self, messages):
        return [self.client.row_description_fields([message]) for message in messages if message[0] == b"T"]

    def parse(self, statement, sql, inputs=()):
        body = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!I", oid) for oid in inputs)
        return self.client.typed(b"P", body)

    def describe(self, target, name):
        return self.client.typed(b"D", target + name + b"\0")

    def bind(self, statement, inputs=(), values=(), binary_input=False, binary_output=False):
        body = b"\0" + statement + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!H", int(binary_input)) for _ in inputs)
        body += struct.pack("!H", len(values))
        for oid, value in zip(inputs, values):
            data = raw(value, oid, binary_input)
            body += struct.pack("!i", -1 if data is None else len(data)) + (data or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        return self.client.typed(b"B", body)

    def error_messages(self, label, messages, state):
        diagnostics = [self.client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        good = ([kind for kind, _ in messages] == [b"E", b"Z"] and len(diagnostics) == 1 and
                diagnostics[0].get(b"C") == state.encode() and diagnostics[0].get(b"S") == b"ERROR" and
                bool(diagnostics[0].get(b"M")) and messages[-1:] == [(b"Z", self.ready)])
        self.review.record(label, good, messages, (state, "one ErrorResponse, no completion or metadata", self.ready))

    def error(self, label, packet, state):
        self.error_messages(label, self.exchange(packet), state)

    def invalid(self, statement, sql, state="42883", inputs=()):
        # Describe is pipelined before Sync. A failed Parse must discard it,
        # not emit ParseComplete/ParameterDescription/RowDescription first.
        self.error(("INVALID PARSE", statement, sql, inputs),
                   self.parse(statement, sql, inputs) + self.describe(b"S", statement), state)

    def prepare(self, statement, sql, oid, inputs=()):
        messages = self.exchange(self.parse(statement, sql, inputs) + self.describe(b"S", statement))
        wanted_parameters = struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", n) for n in inputs)
        good = ([kind for kind, _ in messages] == [b"1", b"t", b"T", b"Z"] and
                self.descriptions(messages) == [self.fields(oid)] and
                [body for kind, body in messages if kind == b"t"] == [wanted_parameters] and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("VALID PARSE/DESCRIBE", statement, sql, inputs), good,
                           messages, (self.fields(oid), inputs, self.ready))

    def parse_only(self, statement, sql):
        # Shape-admission controls must preserve the existing owner's Parse
        # boundary without asking a new scalar path to describe or execute it.
        messages = self.exchange(self.parse(statement, sql))
        wanted = [(b"1", b""), (b"Z", self.ready)]
        self.review.record(("VALID EXISTING-OWNER PARSE", statement, sql),
                           messages == wanted, messages, wanted)

    def bind_describe(self, statement, oid, inputs=(), values=(), binary_input=False, binary_output=False):
        messages = self.exchange(self.bind(statement, inputs, values, binary_input, binary_output) + self.describe(b"P", b""))
        good = ([kind for kind, _ in messages] == [b"2", b"T", b"Z"] and
                self.descriptions(messages) == [self.fields(oid, binary_output)] and messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("BIND/DESCRIBE PORTAL", statement, values, binary_input, binary_output), good,
                           messages, (self.fields(oid, binary_output), "no execution", self.ready))

    def selected(self, label, messages, values, oid, binary=False, bound=False):
        rows = self.client.data_row_values(messages)
        prefix = [b"2", b"T"] if bound else [b"T"]
        good = ([kind for kind, _ in messages] == prefix + [b"D"] * len(values) + [b"C", b"Z"] and
                self.descriptions(messages) == [self.fields(oid, binary)] and len(rows) == len(values) and
                all(len(row) == 1 and matches(row[0], value, oid, binary) for row, value in zip(rows, values)) and
                [body for kind, body in messages if kind == b"C"] == [("SELECT " + str(len(values))).encode() + b"\0"] and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(label, good, messages, (values, self.fields(oid, binary), self.ready))

    def run(self, statement, oid, values, inputs=(), arguments=(), binary_input=False, binary_output=False, repeat=None):
        # Fresh Bind after Sync: an autocommit portal need not survive the
        # earlier metadata-only exchange. Never assume a cross-Sync portal.
        packet = self.bind(statement, inputs, arguments, binary_input, binary_output) + self.describe(b"P", b"")
        packet += self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        self.selected(("EXECUTE", statement, arguments, binary_input, binary_output, repeat),
                      self.exchange(packet), values, oid, binary_output, True)

    def simple(self, sql, values=(), oid=701, state=None):
        messages = self.client.simple_query(self.sock, sql)
        if state:
            self.error_messages(("SIMPLE ERROR", sql), messages, state)
        else:
            self.selected(("SIMPLE", sql), messages, values, oid)

    def command(self, sql, tag, notices=False, parameter_status=None):
        messages = self.client.simple_query(self.sock, sql)
        # SET may report a server-selected GUC through ParameterStatus. It
        # is not a statement-completion error; validate the exact payload
        # when present rather than guessing that every server reports it.
        statuses = [body for kind, body in messages if kind == b"S"]
        valid_status = not statuses
        if parameter_status is not None:
            name, value = parameter_status
            valid_status = statuses in ([], [name + b"\0" + value + b"\0"])
        relevant = [(kind, body) for kind, body in messages
                    if not (notices and kind == b"N") and
                    not (parameter_status is not None and kind == b"S")]
        wanted = [(b"C", tag.encode() + b"\0"), (b"Z", self.ready)]
        self.review.record(sql, valid_status and relevant == wanted, messages, (wanted, parameter_status))

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", self.ready)]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)

    def retry_cycle(self, statement, sql, valid, state="42883", inputs=(), full=False):
        self.invalid(statement, sql, state, inputs)
        self.error(("FAILED PARSE LEFT NO STATEMENT", statement), self.describe(b"S", statement), "26000")
        # SAME name is essential: an invalid prepared object must not turn
        # this valid retry into 42P05. Unnamed replacement is tested too.
        self.prepare(statement, valid, 701)
        if full:
            for binary in (False, True):
                self.bind_describe(statement, 701, binary_output=binary)
            self.run(statement, 701, [0.0], binary_output=True)
        self.close(statement)


def main():
    review = Review("QUALIFIED_SCALAR_PARSE_VALIDATION_PROTOCOL")
    wire = Wire(review)
    schema = identifier("R163.Scope_" + uuid.uuid4().hex[:10])
    table, alias = schema + ".rows", '"r.dot"'
    x, text, integer = (alias + "." + identifier(name) for name in ("X", "Text", "Count"))
    tail, order = " FROM " + table + " AS " + alias, " ORDER BY " + alias + ".id"
    mix, dot, mixed = (schema + "." + identifier(name) for name in ("mix", "Name.With.Dot", "MiXeD"))
    valid = "SELECT pg_catalog.sin(" + x + ") AS v" + tail + " WHERE " + alias + ".id=1"
    sequence = schema + ".effects"
    next_value = "SELECT pg_catalog.nextval('" + sequence.replace("'", "''") + "') AS v"
    try:
        wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        wire.command("CREATE TABLE " + table + '(id INTEGER PRIMARY KEY,"X" DOUBLE PRECISION,"Text" TEXT,"Count" INTEGER)', "CREATE TABLE")
        wire.command("INSERT INTO " + table + " VALUES(1,'0'::float8,'alpha',2),(2,'-0'::float8,'',-3),"
                     "(3,NULL,NULL,NULL),(4,1,'NULL',0)", "INSERT 0 4")
        for name in (mix, dot):
            wire.command("CREATE FUNCTION " + name + "(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                         "AS $$SELECT t || ':' || n::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + mixed + "(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n+7$$", "CREATE FUNCTION")
        wire.command("CREATE SEQUENCE " + sequence, "CREATE SEQUENCE")
        wire.command("CREATE FUNCTION " + schema + ".effect(x DOUBLE PRECISION) RETURNS DOUBLE PRECISION VOLATILE LANGUAGE plpgsql "
                     "AS $$BEGIN PERFORM nextval('" + sequence.replace("'", "''") + "'); RETURN x; END;$$", "CREATE FUNCTION")
        wire.command("SET search_path=" + schema + ",pg_catalog", "SET",
                     parameter_status=(b"search_path", (schema + ", pg_catalog").encode()))

        invalid = ("pg_catalog.r163_missing(" + x + ")", "pg_catalog.mix(" + text + "," + integer + ")",
                   "pg_catalog.degrees(" + text + ")", "pg_catalog.sin(" + text + ")", "pg_catalog.degrees()",
                   "pg_catalog.sin(" + x + "," + x + ")", mix + "(" + integer + "," + text + ")",
                   schema + ".mixed(" + integer + ")", dot + "(" + text + ")")
        for index, expression in enumerate(invalid):
            for suffix_index, suffix in enumerate(SUFFIXES):
                sql = "SELECT " + expression + " AS v" + tail + suffix
                wire.simple(sql, state="42883")
                for named in (False, True):
                    statement = ("r163_bad_" + str(index) + "_" + str(suffix_index)).encode() if named else b""
                    wire.retry_cycle(statement, sql, valid, full=True)

        typed = (("pg_catalog.sin($1)", (25,)), ("pg_catalog.degrees($1)", (25,)),
                 (mix + "($1,$2)", (23, 25)), (dot + "($1)", (25,)),
                 ("pg_catalog.r163_missing($1)", (701,)), (schema + ".mixed($1)", (23,)))
        for index, (expression, inputs) in enumerate(typed):
            for suffix_index, suffix in enumerate(SUFFIXES):
                wire.retry_cycle(("r163_typed_" + str(index) + "_" + str(suffix_index)).encode(),
                                 "SELECT " + expression + " AS v" + tail + suffix, valid, inputs=inputs)
        literals = (("pg_catalog.sin('not-a-float')", "22P02"), ("pg_catalog.degrees('not-a-float')", "22P02"),
                    (mix + "('alpha','not-an-integer')", "22P02"), ("pg_catalog.sin('0'::text)", "42883"),
                    ("pg_catalog.degrees('0'::text)", "42883"), (mix + "('alpha'::text,'2'::text)", "42883"))
        for index, (expression, state) in enumerate(literals):
            for suffix_index, suffix in enumerate(SUFFIXES):
                for named in (False, True):
                    statement = ("r163_literal_" + str(index) + "_" + str(suffix_index)).encode() if named else b""
                    wire.retry_cycle(statement, "SELECT " + expression + " AS v" + tail + suffix, valid, state)

        cases = (("pg_catalog.sin(" + x + ")", 701, [0.0, -0.0, None, 0.8414709848078965]),
                 ('"pg_catalog"."degrees"(' + x + ")", 701, [0.0, -0.0, None, 1.0 / FACTOR]),
                 (mix + "(" + text + "," + integer + ")", 25, ["alpha:2", ":-3", None, "NULL:0"]),
                 (dot + "(" + text + "," + integer + ")", 25, ["alpha:2", ":-3", None, "NULL:0"]),
                 (mixed + "(" + integer + ")", 23, [9, 4, None, 7]))
        for index, (expression, oid, outputs) in enumerate(cases):
            for suffix_index, suffix in enumerate(SUFFIXES):
                sql = "SELECT " + expression + " AS v" + tail + (order if not suffix else suffix)
                wanted = outputs if not suffix else []
                statement = ("r163_valid_" + str(index) + "_" + str(suffix_index)).encode()
                wire.simple(sql, wanted, oid)
                wire.prepare(statement, sql, oid)
                for binary in (False, True):
                    wire.bind_describe(statement, oid, binary_output=binary)
                    wire.run(statement, oid, wanted, binary_output=binary)
                wire.close(statement)

        parameters = []
        values = [(0.0,), (-0.0,), (1.0,), (None,)]
        for function in ("sin", "degrees", "radians"):
            outputs = ([0.0, -0.0, 0.8414709848078965, None] if function == "sin" else
                       [0.0, -0.0, 1.0 / FACTOR if function == "degrees" else FACTOR, None])
            parameters.append(("pg_catalog." + function + "($1)", (701,), 701, values, outputs))
        parameters.extend(((mix + "($1,$2)", (25, 23), 25,
                            [("bind", 2), ("", -3), ("NULL", 0), (None, None)], ["bind:2", ":-3", "NULL:0", None]),
                           (mixed + "($1)", (23,), 23, [(0,), (-1,), (2,), (None,)], [7, 6, 9, None])))
        for index, (expression, inputs, oid, values, outputs) in enumerate(parameters):
            statement = ("r163_parameter_" + str(index)).encode()
            wire.prepare(statement, "SELECT " + expression + " AS v" + tail + " WHERE " + alias + ".id=1", oid, inputs)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, oid, inputs, values[0], binary_input, binary_output)
            for repeat in (0, 1):
                pairs = list(zip(values, outputs))
                for arguments, output in (pairs if not repeat else list(reversed(pairs))):
                    for binary_input in (False, True):
                        for binary_output in (False, True):
                            wire.run(statement, oid, [output], inputs, arguments, binary_input, binary_output, repeat)
            wire.close(statement)

        for index, expression in enumerate(("pg_catalog.sum(1)", "pg_catalog.count(1)",
                                            "pg_catalog.sum(DISTINCT 1)", "pg_catalog.sum(1) FILTER(WHERE true)")):
            sql, statement = "SELECT " + expression + " AS v", ("r163_aggregate_" + str(index)).encode()
            wire.simple(sql, [1], 20)
            wire.prepare(statement, sql, 20)
            for binary in (False, True):
                wire.bind_describe(statement, 20, binary_output=binary)
                wire.run(statement, 20, [1], binary_output=binary)
            wire.close(statement)

        # A qualified call cannot move a GROUP BY output alias into the
        # ordinary scalar binder, which does not own SQL92 grouping aliases.
        # The second query also contains a qualified scalar outside an
        # unqualified aggregate: excluding only qualified aggregates is not
        # enough. Preserve Parse admission for named/unnamed and empty shapes.
        for index, expression in enumerate(("pg_catalog.count(*)", "pg_catalog.sin(count(*))")):
            for suffix_index, suffix in enumerate(SUFFIXES):
                group_tail = tail + (suffix if suffix == " WHERE false" else "") + " GROUP BY a"
                if suffix == " LIMIT 0":
                    group_tail += suffix
                sql = "SELECT " + integer + " AS a," + expression + " AS v" + group_tail
                for named in (False, True):
                    statement = ("r163_group_alias_" + str(index) + "_" + str(suffix_index)).encode() if named else b""
                    wire.parse_only(statement, sql)
                    wire.close(statement)

        effect_sql = "SELECT pg_catalog.sin(" + schema + ".effect(" + x + ")) AS v" + tail
        statement = b"r163_effect_metadata"
        wire.prepare(statement, effect_sql + " WHERE " + alias + ".id=1", 701)
        for binary in (False, True):
            wire.bind_describe(statement, 701, binary_output=binary)
        wire.simple(next_value, [1], 20)  # Parse/Describe/Bind invoked no effect.
        wire.run(statement, 701, [0.0], binary_output=True)
        wire.simple(next_value, [3], 20)  # Exactly one real projection called it.
        wire.close(statement)
        for index, suffix in enumerate(SUFFIXES[1:]):
            statement = ("r163_effect_zero_" + str(index)).encode()
            wire.prepare(statement, effect_sql + suffix, 701)
            for binary in (False, True):
                wire.bind_describe(statement, 701, binary_output=binary)
            wire.run(statement, 701, [], binary_output=True)
            wire.close(statement)
        wire.simple(next_value, [4], 20)

        wire.ready = b"T"
        wire.command("BEGIN", "BEGIN")
        for index, (expression, state) in enumerate(((invalid[0], "42883"), (literals[0][0], "22P02"))):
            statement = ("r163_transaction_" + str(index)).encode()
            wire.command("SAVEPOINT r163_recover", "SAVEPOINT")
            wire.ready = b"E"
            wire.invalid(statement, "SELECT " + expression + " AS v" + tail, state)
            wire.invalid(b"r163_aborted", valid, "25P02")
            wire.ready = b"T"
            wire.command("ROLLBACK TO SAVEPOINT r163_recover", "ROLLBACK")
            wire.prepare(statement, valid, 701)
            wire.bind_describe(statement, 701)
            wire.run(statement, 701, [0.0])
            wire.close(statement)
            wire.command("RELEASE SAVEPOINT r163_recover", "RELEASE")
        wire.ready = b"I"
        wire.command("COMMIT", "COMMIT")
    finally:
        try:
            if wire.ready != b"I":
                wire.client.simple_query(wire.sock, "ROLLBACK")  # Emergency cleanup, not a planned control.
                wire.ready = b"I"
            wire.command("SET search_path=public", "SET", parameter_status=(b"search_path", b"public"))
            wire.command("DROP SCHEMA " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Setup9 + original negatives54*7=378 + Simple negatives27 + typed72
    # + UNKNOWN/typed-literal144 + valid105 + retained parameter190
    # + aggregate28 + metadata effects18 + transaction20 + cleanup2 = 993.
    # Preserve all original 993 controls, plus group-alias Parse/Close24.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
