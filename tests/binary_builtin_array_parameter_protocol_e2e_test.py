#!/usr/bin/env python3
"""R165: finite builtin array binary Bind codecs, independent of OID inference.

The complete --reference18 run is mandatory before accepting the SQLSTATE
oracles below. The fixture covers only int2/int4/int8/text/varchar arrays.
It neither creates catalog rows nor uses automatically generated user arrays.
Array output is cast to TEXT: binary array result encoding is another owner.
"""

from compat.numeric_type_review import Review
import struct


EXPECTED_CONTROLS = 1852
ARRAYS = ((1005, 21, "!h"), (1007, 23, "!i"), (1016, 20, "!q"),
          (1009, 25, None), (1015, 1043, None))


def header(dimensions, element, flags=0):
    return struct.pack("!iiI", len(dimensions), flags, element) + b"".join(
        struct.pack("!ii", length, lower) for length, lower in dimensions)


def cell(value, scalar_format):
    return struct.pack(scalar_format, value) if scalar_format else value.encode("utf-8")


def array_binary(dimensions, values, element, scalar_format, flags=None):
    if flags is None:
        flags = int(any(value is None for value in values))
    output = header(dimensions, element, flags)
    for value in values:
        payload = None if value is None else cell(value, scalar_format)
        output += struct.pack("!i", -1 if payload is None else len(payload)) + (payload or b"")
    return output


def array_text(dimensions, values):
    if not dimensions or any(length == 0 for length, _ in dimensions):
        return "{}"
    iterator = iter(values)

    def render(depth):
        output = []
        for _ in range(dimensions[depth][0]):
            if depth + 1 < len(dimensions):
                output.append(render(depth + 1))
                continue
            value = next(iterator)
            if value is None:
                output.append("NULL")
            elif isinstance(value, str):
                quoted = not value or value.lower() == "null" or any(
                    byte in value for byte in ',{}"\\ \t\n\r\v\f')
                output.append('"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
                              if quoted else value)
            else:
                output.append(str(value))
        return "{" + ",".join(output) + "}"

    prefix = ""
    if any(lower != 1 for _, lower in dimensions):
        prefix = "".join("[" + str(lower) + ":" + str(lower + length - 1) + "]"
                         for length, lower in dimensions) + "="
    return prefix + render(0)


def valid_cases(element, scalar_format):
    if scalar_format:
        width = struct.calcsize(scalar_format) * 8
        edges = [-(1 << (width - 1)), (1 << (width - 1)) - 1, 0, -1, 1]
    else:
        edges = ["", "NULL", "null", "NuLl", "alpha", "a,b", "{brace}", 'a"b', "a\\b",
                 "O'Reilly", " leading", "trailing ", "\t\n\r\v\f", "\x01\x7f",
                 "\u0080\u07ff\u0800\ud7ff\ue000\uffff", "\U00010000\U0010ffff"]
    first, last = edges[0], edges[-1]
    cases = [("sql_null", (), None, None),
             ("empty", (), [], None), ("empty_flags_one", (), [], 1),
             ("zero_length", ((0, 1),), [], None),
             ("zero_lower_min", ((0, -2147483648),), [], None),
             ("zero_lower_max", ((0, 2147483647),), [], None),
             ("zero_second_dimension", ((2, 1), (0, 7)), [], None),
             ("zero_first_dimension", ((0, 1), (2, -3)), [], None),
             ("singleton", ((1, 1),), [last], None),
             ("scalar_edges", ((len(edges), 1),), edges, None),
             ("all_null", ((3, 1),), [None, None, None], None),
             ("flags_zero_with_null", ((3, 1),), [first, None, last], 0),
             ("flags_one_without_null", ((2, 1),), [first, last], 1),
             ("lower_min", ((1, -2147483648),), [first], None),
             ("upper_max_minus_one", ((1, 2147483646),), [last], None)]
    for rank in range(1, 7):
        dimensions = ((1, 1),) * (rank - 1) + ((2, 1),)
        cases.append(("rank_" + str(rank), dimensions, [first, last], None))
        dimensions = tuple((length, index - 3) for index, (length, _) in enumerate(dimensions))
        cases.append(("rank_bounds_" + str(rank), dimensions, [first, last], None))
    assert len(cases) == 27
    output = []
    for name, dimensions, values, flags in cases:
        expected = None if values is None else array_text(dimensions, values)
        binary = None if values is None else array_binary(dimensions, values, element, scalar_format, flags)
        output.append((name, binary, expected))
    return output


def invalid_cases(element, scalar_format):
    base = header(((1, 1),), element)
    good = array_binary(((1, 1),), [1 if scalar_format else "ok"], element, scalar_format)
    cases = [("empty_bytes", b"", "08P01"), ("short_rank", b"\0" * 3, "08P01"),
             ("negative_rank", struct.pack("!i", -1), "22P03"),
             ("rank_seven", struct.pack("!i", 7), "54000"),
             ("missing_flags", struct.pack("!i", 0), "08P01"),
             ("flags_two", struct.pack("!ii", 0, 2), "22P03"),
             ("negative_flags", struct.pack("!ii", 0, -1), "22P03"),
             ("missing_element", struct.pack("!ii", 0, 0), "08P01"),
             ("wrong_element", header((), 23 if element != 23 else 25), "42804"),
             ("short_dimension", struct.pack("!iiI", 1, 0, element) +
              struct.pack("!i", 1), "08P01"),
             ("negative_extent", header(((-1, 1),), element), "54000"),
             ("product_overflow", header(((65536, 1), (65536, 1)), element), "54000"),
             ("bound_overflow", header(((1, 2147483647),), element), "54000"),
             ("array_size_limit", header(((134217728, 1),), element), "54000"),
             ("missing_value", base, "08P01"),
             ("short_item_length", base + b"\0" * 3, "08P01"),
             ("negative_item_length", base + struct.pack("!i", -2), "22P03"),
             ("truncated_item", base + struct.pack("!i", 10) + b"a", "22P03"),
             ("empty_trailing", header((), element) + b"x", "22P03"),
             ("value_trailing", good + b"x", "22P03")]
    if scalar_format:
        width = struct.calcsize(scalar_format)
        cases.extend((("short_scalar", base + struct.pack("!i", width - 1) + b"\0" * (width - 1), "08P01"),
                      ("long_scalar", base + struct.pack("!i", width + 1) + b"\0" * (width + 1), "22P03")))
    else:
        for name, value in (("text_nul", b"a\0b"), ("bad_utf8", b"\xff"),
                            ("short_utf8", b"\xe2\x82"), ("overlong_utf8", b"\xc0\xaf"),
                            ("surrogate_utf8", b"\xed\xa0\x80"),
                            ("beyond_unicode", b"\xf4\x90\x80\x80")):
            cases.append((name, base + struct.pack("!i", len(value)) + value, "22021"))
    assert len(cases) == (22 if scalar_format else 26)
    return cases


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock
        self.ready = b"I"

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    @staticmethod
    def fields(binary=False):
        return [(b"v", 0, 0, 25, -1, -1, int(binary))]

    def descriptions(self, messages):
        return [self.client.row_description_fields([message]) for message in messages if message[0] == b"T"]

    def prepare(self, statement, oid):
        sql = b"SELECT $1::text AS v"
        packet = self.client.typed(b"P", statement + b"\0" + sql + b"\0" + struct.pack("!HI", 1, oid))
        messages = self.exchange(packet + self.client.typed(b"D", b"S" + statement + b"\0"))
        good = ([kind for kind, _ in messages] == [b"1", b"t", b"T", b"Z"] and
                [body for kind, body in messages if kind == b"t"] == [struct.pack("!HI", 1, oid)] and
                self.descriptions(messages) == [self.fields()] and messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("PARSE/DESCRIBE", oid), good, messages, (oid, self.fields(), self.ready))

    def bind(self, statement, payload, binary_input, binary_output):
        body = b"\0" + statement + b"\0" + struct.pack("!HHH", 1, int(binary_input), 1)
        body += struct.pack("!i", -1 if payload is None else len(payload)) + (payload or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        return self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0")

    def metadata(self, label, statement, payload, binary_input, binary_output):
        messages = self.exchange(self.bind(statement, payload, binary_input, binary_output))
        good = ([kind for kind, _ in messages] == [b"2", b"T", b"Z"] and
                self.descriptions(messages) == [self.fields(binary_output)] and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("BIND/DESCRIBE", label, binary_input, binary_output), good,
                           messages, (self.fields(binary_output), self.ready))

    def execute(self, label, statement, payload, expected, binary_input=True, binary_output=False):
        packet = self.bind(statement, payload, binary_input, binary_output)
        packet += self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        messages = self.exchange(packet)
        wanted = None if expected is None else expected.encode("utf-8")
        good = ([kind for kind, _ in messages] == [b"2", b"T", b"D", b"C", b"Z"] and
                self.descriptions(messages) == [self.fields(binary_output)] and
                self.client.data_row_values(messages) == [[wanted]] and
                [body for kind, body in messages if kind == b"C"] == [b"SELECT 1\0"] and
                messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("EXECUTE", label, binary_input, binary_output), good,
                           messages, (wanted, self.fields(binary_output), self.ready))

    def invalid(self, label, statement, payload, state, binary_output=False):
        messages = self.exchange(self.bind(statement, payload, True, binary_output))
        errors = [self.client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        good = ([kind for kind, _ in messages] == [b"E", b"Z"] and len(errors) == 1 and
                errors[0].get(b"C") == state.encode() and errors[0].get(b"S") == b"ERROR" and
                bool(errors[0].get(b"M")) and messages[-1:] == [(b"Z", self.ready)])
        self.review.record(("INVALID BIND", label, binary_output), good, messages, (state, self.ready))

    def command(self, sql, tag):
        messages = self.client.simple_query(self.sock, sql)
        wanted = [(b"C", tag.encode() + b"\0"), (b"Z", self.ready)]
        self.review.record(sql, messages == wanted, messages, wanted)

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", self.ready)]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)


def main():
    review = Review("BINARY_BUILTIN_ARRAY_PARAMETER_PROTOCOL")
    wire = Wire(review)
    try:
        for oid, element, scalar_format in ARRAYS:
            statement = ("r165_array_" + str(oid)).encode()
            wire.prepare(statement, oid)
            valid = valid_cases(element, scalar_format)
            for name, binary, expected in valid:
                label = (oid, name)
                text = None if expected is None else expected.encode("utf-8")
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        payload = binary if binary_input else text
                        wire.metadata(label, statement, payload, binary_input, binary_output)
                        wire.execute(label, statement, payload, expected, binary_input, binary_output)
            for name, binary, expected in reversed(valid):
                for binary_output in (False, True):
                    wire.execute((oid, name, "rebound"), statement, binary, expected, True, binary_output)
            retry_binary, retry_text = valid[8][1:]
            for name, binary, state in invalid_cases(element, scalar_format):
                for binary_output in (False, True):
                    wire.invalid((oid, name), statement, binary, state, binary_output)
                    wire.execute((oid, name, "after Sync"), statement, retry_binary, retry_text, True, binary_output)
            wire.ready = b"T"
            wire.command("BEGIN", "BEGIN")
            wire.ready = b"E"
            wire.invalid((oid, "transaction"), statement, header((), 23 if element != 23 else 25), "42804")
            wire.ready = b"I"
            wire.command("ROLLBACK", "ROLLBACK")
            wire.execute((oid, "after rollback"), statement, retry_binary, retry_text, True, True)
            wire.close(statement)
    finally:
        if wire.ready != b"I":
            wire.client.simple_query(wire.sock, "ROLLBACK")
        review.close()
    # Five types * (Parse/Close2 + valid27*formats4*metadata/execute2
    # + retained reverse27*formats2 + transaction4) = 1380.
    # Malformed cases: (three integer types*22 + two text types*26)
    # * output formats2 * (Bind error + successful same-statement retry) = 472.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
