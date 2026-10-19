#!/usr/bin/env python3
"""R159 exact encoding bytes, real BYTEA rows and retained wire parameters.

Run the whole fixture with --reference18 on PostgreSQL 18.6 before treating
it as acceptance evidence. Expected codecs follow official REL_18_6 encode.c;
base64's padded last quartet does not cause a linefeed, while every complete
57-byte block does, including at EOF. No general BYTEA admission, typed
carrier, planner, enum-array or JSON behavior is changed by this fixture.
"""

from compat.numeric_type_review import Review
import base64
import struct
import uuid


MODES = ("simple", "extended", "binary")
FORMATS = ("hex", "base64", "escape")
LENGTHS = (0, 1, 2, 3, 54, 55, 56, 57, 58, 59, 60, 111, 112, 113, 114, 115, 116, 117, 170, 171, 172)
ESCAPE_BYTES = (0, 1, 7, 9, 10, 11, 12, 13, 31, 32, 34, 39, 65, 92, 126, 127, 128, 255)
VALID64 = (("", b""), ("TQ==", b"M"), ("TWE=", b"Ma"), ("TWFu", b"Man"),
           ("T Q\n=\t=", b"M"), (" \t\r\n", b""), ("YQ==\n", b"a"), ("TWE=\r\n", b"Ma"),
           # REL_18_6 encode.c:316-379 accepts these noncanonical sequences.
           # They are controls for existing decoder policy, not a new policy.
           ("TQ=A", b"M"), ("TQ==AAAA", b"M\x00"))
INVALID64 = ("TQ", "TWE", "A===", "=AAA", "A=AA", "TQ===", "TQ==!", "TQ=\v=", "TQ=\f=", "TWFu$", "AAAA=", "====")
INVALID_ESCAPE = ("\\", "\\12", "\\128", "\\400", "\\x", "\\08x")
EXPECTED_CONTROLS = 806


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def sql_text(value):
    # Delimiter is absent from the fixed codec vectors, so backslashes,
    # ASCII controls, quotes and linefeeds remain their exact SQL text bytes.
    return "$r159$" + value + "$r159$"


def sql_bytes(value):
    return "NULL::bytea" if value is None else "decode(" + sql_text(value.hex()) + ",'hex')"


def escape(value):
    return "".join("\\%03o" % byte if byte == 0 or byte >= 128 else
                   "\\\\" if byte == 92 else chr(byte) for byte in value)


def pg_base64(value):
    encoded = base64.b64encode(value).decode("ascii")
    complete_lines = len(value) // 57
    return "".join(encoded[index * 76:(index + 1) * 76] + "\n" for index in range(complete_lines)) + encoded[complete_lines * 76:]


def encoded(value, format_name):
    if value is None:
        return None
    return value.hex() if format_name == "hex" else pg_base64(value) if format_name == "base64" else escape(value)


def hex_cases():
    cases = [("", b"", None), ("aAbBcCdDeEfF00", bytes.fromhex("aabbccddeeff00"), None),
             ("61 62\t63\r64\n65", b"abcde", None)]
    cases += [(value, None, "22023") for value in ("0", "616", "zz", "0x61", "\\x61")]
    for gap in " \t\n\r":
        for position in range(5):
            value = "6162"[:position] + gap + "6162"[position:]
            cases.append((value, b"ab" if position % 2 == 0 else None, None if position % 2 == 0 else "22023"))
        cases.append((gap + "61" + gap + "62" + gap, b"ab", None))
        cases.append((gap, b"", None))
    for gap in "\v\f":
        for position in range(5):
            cases.append(("6162"[:position] + gap + "6162"[position:], None, "22023"))
    return cases


def datum(value, oid, binary):
    if value is None:
        return None
    if oid == 17:
        return value if binary else b"\\x" + value.hex().encode("ascii")
    return value.encode("utf-8")


def strict_data_rows(messages):
    """Decode complete DataRow bodies; only -1 denotes a SQL NULL cell."""
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


class EncodingReview(Review):
    def complete(self, expected_controls):
        if self.controls != expected_controls:
            self.failures.append(("CONTROL INVENTORY", self.controls, expected_controls))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                for kind, body in messages if kind == b"E"]

    def parse(self, sql, statement=b"", inputs=()):
        body = statement + b"\0" + sql.encode("utf-8") + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!I", oid) for oid in inputs)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind(self, statement=b"", inputs=(), values=(), binary_input=False, binary_output=False):
        body = b"\0" + statement + b"\0" + struct.pack("!H", len(inputs))
        body += b"".join(struct.pack("!H", int(binary_input)) for _ in inputs)
        body += struct.pack("!H", len(values))
        for oid, value in zip(inputs, values):
            raw = datum(value, oid, binary_input)
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!HH", 1, int(binary_output))
        return (self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0") +
                self.client.typed(b"E", b"\0" + struct.pack("!I", 0)))

    def descriptions(self, messages):
        # Origin OIDs/attnums belong to the source-column owner, not codecs.
        # Check exact name, output OID, width, typmod and format for all rows.
        return [[(field[0], field[3], field[4], field[5], field[6])
                 for field in self.client.row_description_fields([message])]
                for message in messages if message[0] == b"T"]

    def fields(self, oid, binary=False):
        return [(b"v", oid, -1, -1, int(binary))]

    def result(self, label, messages, values, oid, state=None, binary=False,
               parsed=False, bound=False, simple=False, inputs=()):
        errors = self.errors(messages)
        rows, valid_rows = strict_data_rows(messages)
        tags = [body for kind, body in messages if kind == b"C"]
        kinds = [kind for kind, _ in messages]
        parameters = struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", value) for value in inputs)
        complete_descriptions = [self.fields(oid)] if parsed else []
        if simple or bound:
            complete_descriptions.append(self.fields(oid, binary))
        if state:
            if simple:
                allowed = [[b"E", b"Z"]]
            elif parsed:
                # Error at Parse, statement Describe, Bind, portal Describe,
                # or Execute. Describe's t/T response stays in order.
                pipeline = [b"1", b"t", b"T", b"2", b"T"]
                allowed = [pipeline[:offset] + [b"E", b"Z"] for offset in (0, 1, 3, 4, 5)]
            else:
                pipeline = [b"2", b"T"]
                allowed = [pipeline[:offset] + [b"E", b"Z"] for offset in (0, 1, 2)]
            good = (errors == [state] and not rows and not tags and kinds in allowed and
                    self.descriptions(messages) == complete_descriptions[:kinds.count(b"T")] and
                    [body for kind, body in messages if kind == b"t"] == ([parameters] if b"t" in kinds else []))
        else:
            prefix = [b"T"] if simple else [b"1", b"t", b"T"] if parsed else []
            if bound:
                prefix += [b"2", b"T"]
            wanted_kinds = prefix + [b"D"] * len(values) + [b"C", b"Z"]
            good = (not errors and self.descriptions(messages) == complete_descriptions and
                    rows == [[datum(value, oid, binary)] for value in values] and
                    kinds == wanted_kinds and
                    [body for kind, body in messages if kind == b"t"] == ([parameters] if parsed else []) and
                    tags == [("SELECT " + str(len(values))).encode("ascii") + b"\0"] and
                    sum(kind == b"1" for kind, _ in messages) == int(parsed) and
                    sum(kind == b"2" for kind, _ in messages) == int(bound))
        good = (good and valid_rows and messages[-1:] == [(b"Z", b"I")] and
                all(body == b"" for kind, body in messages if kind in (b"1", b"2")) and
                not any(kind in (b"N", b"s") for kind, _ in messages))
        self.review.record(label, good, messages, (values, oid, state, binary))

    def query(self, mode, sql, values, oid=25, state=None):
        if mode == "simple":
            messages = self.client.simple_query(self.sock, sql)
            self.result((mode, sql), messages, values, oid, state, simple=True)
        else:
            binary = mode == "binary"
            messages = self.exchange(self.parse(sql) + self.bind(binary_output=binary))
            self.result((mode, sql), messages, values, oid, state, binary, parsed=True, bound=True)

    def describe(self, sql, oid, statement=b"", inputs=()):
        messages = self.exchange(self.parse(sql, statement, inputs))
        parameters = struct.pack("!H", len(inputs)) + b"".join(struct.pack("!I", value) for value in inputs)
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(oid)] and
                [kind for kind, _ in messages] == [b"1", b"t", b"T", b"Z"] and
                [body for kind, body in messages if kind == b"1"] == [b""] and
                [body for kind, body in messages if kind == b"t"] == [parameters] and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record(("PARSE/DESCRIBE", statement, sql, inputs), good, messages, (oid, "no execution"))

    def parameter(self, statement, inputs, values, expected, oid, binary_input, binary_output, state=None):
        messages = self.exchange(self.bind(statement, inputs, values, binary_input, binary_output))
        self.result(("PARAMETER", statement, values, binary_input, binary_output), messages,
                    [] if state else [expected], oid, state, binary_output, bound=True)

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", b"I")]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)

    def command(self, sql, tag, notices=False):
        messages = self.client.simple_query(self.sock, sql)
        visible = [(kind, body) for kind, body in messages if not (notices and kind == b"N")]
        wanted = [(b"C", tag.encode("ascii") + b"\0"), (b"Z", b"I")]
        self.review.record(sql, visible == wanted, messages, wanted)


def main():
    review = EncodingReview("ENCODING_BOUNDARIES_PROTOCOL")
    wire = Wire(review)
    prefix = "r159_" + uuid.uuid4().hex[:12]
    schema = identifier(prefix)
    table = schema + ".codec_rows"
    all_bytes = bytes(range(256))
    payloads = [all_bytes, b"a" * 55, b"a" * 56, b"a" * 57, b"a" * 58, b"a" * 114, b"", None]

    def scalar(sql, value, oid=25, state=None):
        for mode in MODES:
            wire.query(mode, "SELECT " + sql + " AS v", [] if state else [value], oid, state)

    try:
        for byte in ESCAPE_BYTES:
            raw = bytes([byte])
            scalar("encode(" + sql_bytes(raw) + ",'escape')", escape(raw))
            scalar("decode(" + sql_text(escape(raw)) + ",'escape')", raw, 17)
        scalar("encode(" + sql_bytes(all_bytes) + ",'escape')", escape(all_bytes))
        scalar("decode(" + sql_text(escape(all_bytes)) + ",'escape')", all_bytes, 17)
        scalar("encode(decode(" + sql_text(escape(all_bytes)) + ",'escape'),'hex')", all_bytes.hex())
        for input_text, raw, state in hex_cases():
            scalar("decode(" + sql_text(input_text) + ",'hex')", raw, 17, state)
        for length in LENGTHS:
            raw = b"a" * length
            scalar("encode(" + sql_bytes(raw) + ",'base64')", pg_base64(raw))
            scalar("decode(" + sql_text(pg_base64(raw)) + ",'base64')", raw, 17)
        for input_text, raw in VALID64:
            scalar("decode(" + sql_text(input_text) + ",'base64')", raw, 17)
        for input_text in INVALID64:
            scalar("decode(" + sql_text(input_text) + ",'base64')", None, 17, "22023")

        for format_name in FORMATS:
            argument = sql_text(format_name)
            scalar("encode(" + sql_bytes(b"") + "," + argument + ")", "")
            scalar("decode(" + sql_text("") + "," + argument + ")", b"", 17)
            scalar("encode(NULL::bytea," + argument + ")", None)
            scalar("encode(" + sql_bytes(b"abc") + ",NULL::text)", None)
            scalar("decode(NULL::text," + argument + ")", None, 17)
            scalar("decode('abc',NULL::text)", None, 17)
            scalar("encode(" + sql_bytes(b"abc") + "," + sql_text(format_name.upper()) + ")", encoded(b"abc", format_name))
            scalar("decode(" + sql_text(encoded(b"abc", format_name)) + "," + sql_text(format_name.capitalize()) + ")", b"abc", 17)
        scalar("encode(" + sql_bytes(b"abc") + ",'rot13')", None, state="22023")
        scalar("decode('abc','rot13')", None, 17, "22023")
        for input_text in INVALID_ESCAPE:
            scalar("decode(" + sql_text(input_text) + ",'escape')", None, 17, "22P02")

        wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        wire.command("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY,b BYTEA)", "CREATE TABLE")
        wire.command("INSERT INTO " + table + " VALUES" + ",".join(
            "(" + str(index + 1) + "," + sql_bytes(raw) + ")" for index, raw in enumerate(payloads)), "INSERT 0 8")
        for format_name in FORMATS:
            expression = "encode(b," + sql_text(format_name) + ")"
            sql = "SELECT " + expression + " AS v FROM " + table
            wire.describe(sql, 25)
            for mode in MODES:
                wire.query(mode, sql + " ORDER BY id", [encoded(raw, format_name) for raw in payloads])
                wire.query(mode, "SELECT decode(" + expression + "," + sql_text(format_name) + ") AS v FROM " + table +
                           " ORDER BY id", payloads, 17)
                wire.query(mode, sql + " WHERE false", [])
        for mode in MODES:
            wire.query(mode, "SELECT b AS v FROM " + table + " ORDER BY id", payloads, 17)

        statement = (prefix + "_encode").encode("ascii")
        wire.describe("SELECT encode($1,$2) AS v", 25, statement, (17, 25))
        for raw in payloads:
            for format_name in FORMATS:
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        wire.parameter(statement, (17, 25), (raw, format_name), encoded(raw, format_name), 25,
                                       binary_input, binary_output)
        wire.close(statement)

        statement = (prefix + "_decode").encode("ascii")
        wire.describe("SELECT decode($1,$2) AS v", 17, statement, (25, 25))
        for raw in payloads:
            for format_name in FORMATS:
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        wire.parameter(statement, (25, 25), (encoded(raw, format_name), format_name), raw, 17,
                                       binary_input, binary_output)
        invalid_parameters = (("6 1", "hex", "22023"), ("61\v62", "hex", "22023"),
                              ("61\f62", "hex", "22023"), ("TQ", "base64", "22023"),
                              ("TQ=\v=", "base64", "22023"), ("\\400", "escape", "22P02"))
        for index, (input_text, format_name, state) in enumerate(invalid_parameters):
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.parameter(statement, (25, 25), (input_text, format_name), None, 17,
                                   binary_input, binary_output, state)
            wire.parameter(statement, (25, 25), ("00ff", "hex"), b"\x00\xff", 17,
                           bool(index % 2), bool((index // 2) % 2))
        wire.close(statement)
    finally:
        try:
            wire.command("DROP SCHEMA IF EXISTS " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Escape singles18*2*3=108 + full256 three*3=9 + hex46*3=138
    # + base64 boundaries21*2*3=126 + decoding22*3=66 + NULL/format26*3=78
    # + bad escape6*3=18 = 543 pure controls. Original800 inputs retained;
    # the two accepted noncanonical padding inputs add six controls.
    # Setup/cleanup4 + table(metadata3+encode9+roundtrip9+empty9+raw3)=33
    # + encode parameters24*4+2=98 + decode parameters24*4+2+6*4+6=128.
    # Total 543+4+33+98+128 = 806; all controls run before the stable footer.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
