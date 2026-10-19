#!/usr/bin/env python3
"""R158 degrees/radians: binary64 values, range errors and actual wire owners.

Run this whole pass/fail fixture against PostgreSQL 18.6 before using it as
acceptance evidence. Expected binary64 operations use the independently frozen
PostgreSQL RADIANS_PER_DEGREE constant, not server output or product helpers.
Finite text results are parsed numerically; binary results retain exact bits,
including the sign of zero. NaN may use either signed canonical quiet NaN.

The unchanged external r158-angle-discovery.py collector retains all 20 original
observations. Its ten invalid-constant/zero-row timing differences remain OPEN
under R160; its separate string-codec findings also remain OPEN. This fixture
asserts zero rows only for valid constants and actual variable columns. It is
not a replacement for that collector or a claim that all angle semantics match.
"""

from compat.numeric_type_review import Review
import math
import struct
import uuid


RADIANS_PER_DEGREE = float.fromhex("0x1.1df46a2529d39p-6")
FUNCTIONS = ("degrees", "radians")
MODES = ("simple", "extended", "binary")
INPUTS = (
    "0", "-0", "1", "-1", "0.1", "-0.1", "0.5", "-0.5",
    "3.141592653589793", "-3.141592653589793", "90", "-90",
    "180", "-180", "360", "-360", "1e-100", "-1e-100", "1e100", "-1e100",
    "1e305", "-1e305", "1e306", "-1e306", "3e306", "-3e306",
    "4e306", "-4e306", "1e307", "-1e307", "1e308", "-1e308",
    "1.7976931348623157e308", "-1.7976931348623157e308",
    "2.2250738585072014e-308", "-2.2250738585072014e-308", "1e-300", "-1e-300",
    "5e-324", "-5e-324", "1e-323", "-1e-323", "1.4e-322", "-1.4e-322",
    "1.43e-322", "-1.43e-322", "3e-322", "-3e-322", "1e-320", "-1e-320",
    "Infinity", "-Infinity", "NaN", None,
)
NUMERIC_INPUTS = ("0", "-1", "1", "0.1", "-0.1", "1e306", "-1e306", "3.141592653589793")
NESTED_INPUTS = ("0", "-0", "1", "-1", "3.141592653589793", "-3.141592653589793",
                 "1e306", "-1e306", "Infinity", "-Infinity", "NaN", None)
SAFE_CONSTANTS = ("0", "-0", "1e306", None)
TABLE_INPUTS = ("0", "-0", "1", "-1", "1e306", "-1e306", "NaN", "Infinity", "-Infinity",
                None, "1e-320", "-1e-320", "1e308", "5e-324")
EXPECTED_CONTROLS = 1449


def literal(value, target="pg_catalog.float8"):
    return "NULL::" + target if value is None else "'" + value + "'::" + target


def expected(function, value):
    """One correctly rounded binary64 operation, with PostgreSQL range rules."""
    if value is None:
        return None, None
    argument = float(value)
    result = (argument / RADIANS_PER_DEGREE if function == "degrees" else
              argument * RADIANS_PER_DEGREE)
    if (math.isfinite(argument) and math.isinf(result)) or (result == 0.0 and argument != 0.0):
        return None, "22003"
    return result, None


def matches_float(raw, value, binary):
    if raw is None or value is None:
        return raw is None and value is None
    try:
        if binary:
            if len(raw) != 8:
                return False
            if math.isnan(value):
                bits = struct.unpack("!Q", raw)[0]
                return bits & 0x7fffffffffffffff == 0x7ff8000000000000
            return raw == struct.pack("!d", value)
        parsed = float(raw.decode("ascii"))
        if math.isnan(value):
            return math.isnan(parsed)
        # Numeric equality alone would hide a lost negative zero.
        return struct.pack("!d", parsed) == struct.pack("!d", value)
    except (ValueError, UnicodeError, OverflowError, struct.error):
        return False


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock
        self.ready = b"I"

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                for kind, body in messages if kind == b"E"]

    def fields(self, binary):
        return [(b"v", 0, 0, 701, 8, -1, int(binary))]

    def descriptions(self, messages):
        return [self.client.row_description_fields([message])
                for message in messages if message[0] == b"T"]

    def parse_packet(self, statement, sql, parameter):
        body = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", int(parameter))
        if parameter:
            body += struct.pack("!I", 701)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind_packet(self, statement, parameter, value, binary_input, binary_output, execute=True):
        body = b"\0" + statement + b"\0"
        body += struct.pack("!H", int(parameter))
        if parameter:
            body += struct.pack("!H", int(binary_input))
        body += struct.pack("!H", int(parameter))
        if parameter:
            raw = None if value is None else (struct.pack("!d", float(value)) if binary_input else value.encode())
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        packet = self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0")
        if execute:
            packet += self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        return packet

    def result(self, label, messages, values, state=None, binary=False,
               parsed=False, bound=False, parameter=False, simple=False):
        errors = self.errors(messages)
        rows = self.client.data_row_values(messages)
        descriptions = self.descriptions(messages)
        tags = [body for kind, body in messages if kind == b"C"]
        parameters = [body for kind, body in messages if kind == b"t"]
        if state:
            # The callback range fix does not own the frontend's error phase.
            # Check the actual SQLSTATE/absence of results without guessing
            # whether a particular receiver has already emitted metadata.
            good = errors == [state] and not rows and not tags
        else:
            wanted_descriptions = ([self.fields(False)] if parsed else [])
            if simple or bound:
                wanted_descriptions.append(self.fields(binary))
            wanted_parameters = ([struct.pack("!HI", 1, 701) if parameter else struct.pack("!H", 0)]
                                 if parsed else [])
            good = (not errors and len(rows) == len(values) and descriptions == wanted_descriptions and
                    parameters == wanted_parameters and tags == [("SELECT " + str(len(values))).encode() + b"\0"] and
                    sum(kind == b"1" for kind, _ in messages) == int(parsed) and
                    sum(kind == b"2" for kind, _ in messages) == int(bound))
            good = good and all(len(row) == 1 and matches_float(row[0], value, binary)
                                for row, value in zip(rows, values))
        good = (good and messages[-1:] == [(b"Z", self.ready)] and
                not any(kind in (b"N", b"s") for kind, _ in messages))
        self.review.record(label, good,
                           (errors, rows, descriptions, parameters, tags, messages[-1:]),
                           (values, state, "binary" if binary else "text", self.ready))

    def query(self, mode, sql, values=(), state=None):
        if mode == "simple":
            messages = self.client.simple_query(self.sock, sql)
            self.result((mode, sql), messages, values, state, simple=True)
        else:
            binary = mode == "binary"
            messages = self.exchange(self.parse_packet(b"", sql, False) +
                                     self.bind_packet(b"", False, None, False, binary))
            self.result((mode, sql), messages, values, state, binary, parsed=True, bound=True)

    def describe(self, label, sql, statement=b"", parameter=False):
        messages = self.exchange(self.parse_packet(statement, sql, parameter))
        wanted_parameters = [struct.pack("!HI", 1, 701) if parameter else struct.pack("!H", 0)]
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(False)] and
                [body for kind, body in messages if kind == b"t"] == wanted_parameters and
                sum(kind == b"1" for kind, _ in messages) == 1 and
                not any(kind in (b"D", b"C", b"2", b"N") for kind, _ in messages) and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("PARSE/DESCRIBE", label, sql), good, messages,
                           ([self.fields(False)], wanted_parameters, "no execution"))

    def bind_describe(self, statement, value, binary_input, binary_output):
        messages = self.exchange(self.bind_packet(statement, True, value, binary_input, binary_output, False))
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(binary_output)] and
                sum(kind == b"2" for kind, _ in messages) == 1 and
                not any(kind in (b"D", b"C", b"1", b"t", b"N") for kind, _ in messages) and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("BIND/DESCRIBE", statement, value, binary_input, binary_output), good,
                           messages, (self.fields(binary_output), "no execution"))

    def reexecute(self, statement, value, output, state, binary_input, binary_output, repeat):
        messages = self.exchange(self.bind_packet(statement, True, value, binary_input, binary_output))
        self.result(("REEXECUTE", statement, value, binary_input, binary_output, repeat), messages,
                    [output] if not state else [], state, binary_output, bound=True)

    def command(self, sql, tag):
        messages = self.client.simple_query(self.sock, sql)
        good = (not self.errors(messages) and [body for kind, body in messages if kind == b"C"] ==
                [tag.encode() + b"\0"] and not any(kind in (b"T", b"D", b"N") for kind, _ in messages) and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(sql, good, messages, (tag, self.ready))

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", self.ready)]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)


def main():
    review = Review("ANGLE_CONVERSION_RANGE_PROTOCOL")
    wire = Wire(review)
    table = "r158_angle_rows_" + uuid.uuid4().hex[:10]
    created = False
    try:
        assert len(INPUTS) == 54 and len(NUMERIC_INPUTS) == 8 and len(NESTED_INPUTS) == 12
        assert RADIANS_PER_DEGREE.hex() == "0x1.1df46a2529d39p-6"
        for function in FUNCTIONS:
            for value in INPUTS:
                output, state = expected(function, value)
                for mode in MODES:
                    wire.query(mode, "SELECT pg_catalog." + function + "(" + literal(value) + ") AS v",
                               [output] if not state else [], state)
            for value in NUMERIC_INPUTS:
                output, state = expected(function, value)
                for mode in MODES:
                    wire.query(mode, "SELECT pg_catalog." + function + "(" + literal(value, "pg_catalog.numeric") + ") AS v",
                               [output], state)
            for value in SAFE_CONSTANTS:
                sql = "SELECT pg_catalog." + function + "(" + literal(value) + ") AS v"
                wire.describe((function, "valid constant", value), sql)
                for suffix in (" WHERE false", " LIMIT 0"):
                    for mode in MODES:
                        wire.query(mode, sql + suffix, [])

        for outer, inner in (("degrees", "radians"), ("radians", "degrees")):
            for value in NESTED_INPUTS:
                intermediate, first_state = expected(inner, value)
                output, second_state = expected(outer, intermediate)
                assert first_state is None and second_state is None
                sql = "SELECT pg_catalog." + outer + "(pg_catalog." + inner + "(" + literal(value) + ")) AS v"
                for mode in MODES:
                    wire.query(mode, sql, [output])

        for function in FUNCTIONS:
            statement = ("r158_" + function).encode()
            wire.describe((function, "declared float8 parameter"),
                          "SELECT pg_catalog." + function + "($1) AS v", statement, True)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, "1", binary_input, binary_output)
            for repeat in (0, 1):
                # Reverse the second complete corpus, so a retained error,
                # NULL datum, sign bit or earlier result cannot pass by reuse.
                values = INPUTS if repeat == 0 else tuple(reversed(INPUTS))
                for value in values:
                    output, state = expected(function, value)
                    for binary_input in (False, True):
                        for binary_output in (False, True):
                            wire.reexecute(statement, value, output, state, binary_input, binary_output, repeat)
            wire.close(statement)

        wire.command("CREATE TEMP TABLE " + table + "(id INTEGER PRIMARY KEY,x DOUBLE PRECISION)", "CREATE TABLE")
        created = True
        rows = ",".join("(" + str(index) + "," + literal(value) + ")"
                        for index, value in enumerate(TABLE_INPUTS, 1))
        wire.command("INSERT INTO " + table + " VALUES" + rows, "INSERT 0 14")
        for function, bad_id in (("degrees", 13), ("radians", 14)):
            projection = "SELECT pg_catalog." + function + "(x) AS v FROM " + table
            safe_values = [expected(function, value)[0] for value in TABLE_INPUTS[:12]]
            wire.describe((function, "real variable column"), projection)
            for mode in MODES:
                wire.query(mode, projection + " WHERE id<=12 ORDER BY id", safe_values)
                # The invalid values are real row cells, not function-call
                # constants. Empty demand therefore cannot execute them.
                wire.query(mode, projection + " WHERE false", [])
                wire.query(mode, projection + " LIMIT 0", [])
                wire.query(mode, projection + " WHERE id=" + str(bad_id), [], "22003")
                wire.query(mode, projection + " WHERE id=1", [0.0])
                wire.query(mode, "SELECT CASE WHEN id=" + str(bad_id) + " THEN 0::pg_catalog.float8 ELSE pg_catalog." +
                           function + "(x) END AS v FROM " + table + " WHERE id=" + str(bad_id), [0.0])
                wire.query(mode, "SELECT COALESCE(0::pg_catalog.float8,pg_catalog." + function +
                           "(x)) AS v FROM " + table + " WHERE id=" + str(bad_id), [0.0])
            # Metadata of a genuine source expression stays physical and pure.
            statement = ("r158_column_" + function).encode()
            wire.describe((function, "variable column metadata"), projection, statement)
            for binary_output in (False, True):
                messages = wire.exchange(wire.bind_packet(statement, False, None, False, binary_output, False))
                good = (not wire.errors(messages) and wire.descriptions(messages) == [wire.fields(binary_output)] and
                        sum(kind == b"2" for kind, _ in messages) == 1 and
                        not any(kind in (b"D", b"C", b"1", b"t", b"N") for kind, _ in messages) and
                        messages[-1:] == [(b"Z", wire.ready)])
                review.record(("R158 COLUMN BIND/DESCRIBE", function, binary_output), good,
                              messages, (wire.fields(binary_output), "no rows demanded"))
            wire.close(statement)

        wire.ready = b"T"
        wire.command("BEGIN", "BEGIN")
        for function, value in (("degrees", "1e308"), ("radians", "5e-324")):
            wire.command("SAVEPOINT r158_recover", "SAVEPOINT")
            wire.ready = b"E"
            wire.query("binary", "SELECT pg_catalog." + function + "(" + literal(value) + ") AS v", [], "22003")
            wire.query("simple", "SELECT pg_catalog." + function + "('1'::pg_catalog.float8) AS v", [], "25P02")
            wire.ready = b"T"
            wire.command("ROLLBACK TO SAVEPOINT r158_recover", "ROLLBACK")
            for mode in MODES:
                wire.query(mode, "SELECT pg_catalog." + function + "('-0'::pg_catalog.float8) AS v", [-0.0])
            wire.command("RELEASE SAVEPOINT r158_recover", "RELEASE")
        wire.ready = b"I"
        wire.command("COMMIT", "COMMIT")
    finally:
        try:
            if wire.ready != b"I":
                # Cleanup is deliberately outside the inventory: it is only
                # an emergency recovery if a transport exception interrupted
                # the planned transaction controls.
                wire.client.simple_query(wire.sock, "ROLLBACK")
                wire.ready = b"I"
            if created:
                wire.command("DROP TABLE " + table, "DROP TABLE")
        finally:
            review.close()

    # Fixed complete inventory, independent of error responses or skipped
    # executions: scalar324 + numeric48 + valid constant metadata/zero rows56
    # + nested72 + retained parameter876 + table setup2 + table controls52
    # + transaction recovery18 + cleanup1 = 1449.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
