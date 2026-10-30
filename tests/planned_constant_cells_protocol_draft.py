#!/usr/bin/env python3
"""R156 physical constant cells: a pass/fail fixture, not a fact collector.

The separate external r156-regtype-builtin-discovery-v3.py collector retains
all 34 original observations: both search_path orders, twelve builtin display
outputs, the json/time name-input observations, and three physical identity
comparisons per order. Run that unchanged collector separately. Its builtin
formatter/JSON differences remain OPEN; this fixture does not close all R156.

Only ordinary scalar enum DDL creates the eight private shadow names below.
No user enum array is declared or inspected, and no catalog row is fabricated.
The builtin OID list is frozen from HEAD 6f9cd7d7, independently of query output.
OID 1028 identity is also retained, although this DBMS's missing oid[] catalog
declaration is a separate OPEN finding. Array receivers here are the existing
integer[]/regtype[] types; that is not an oid[] support claim or an error allowlist.

Expectations are physical OIDs, integers, booleans, and SQL NULL bits. Composite
and REGTYPE display spelling cannot substitute for those assertions. PostgreSQL
18.6 must validate the complete fixture before it is acceptance evidence.
"""

from compat.numeric_type_review import Review
import struct
import uuid


# All 108 distinct builtin rows in the accepted source baseline, including
# pseudo-types and builtin array rows. These are not collected from the server.
BUILTIN_OIDS = (
    16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30,
    114, 142, 600, 601, 602, 603, 604, 628, 718, 700, 701, 705,
    650, 774, 790, 829, 869, 1042, 1043, 1082, 1083, 1114, 1184,
    1186, 1266, 1560, 1562, 1700, 2206, 2249, 2278, 2205, 4089,
    4096, 2950, 3614, 3615, 3802, 3904, 3906, 3908, 3910, 3912, 3926,
    1000, 1001, 1002, 1003, 1005, 1007, 1009, 1014, 1015, 1016,
    1017, 1018, 1019, 1020, 1021, 1022, 1027, 629, 719, 1040,
    1041, 651, 775, 791, 1182, 1183, 1115, 1185, 1187, 1270,
    1561, 1563, 1231, 2951, 3643, 3645, 199, 3807, 143, 2211,
    2210, 4090, 4097, 3905, 3907, 3909, 3911, 3913, 3927, 2287,
)
IDENTITY_OIDS = BUILTIN_OIDS + (0, 1028, 4294967294, 4294967295)
SHADOW_NAMES = ("text", "char", "name", "json", "int4", "time", "timestamp", "numeric")
MODES = ("simple", 0, 1)
TYPE_SIZES = {16: 1, 20: 8, 23: 4, 25: -1, 26: 4, 1007: -1,
              2206: 4, 2211: -1, 2249: -1}


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def ref(oid):
    return str(oid) + "::pg_catalog.oid::pg_catalog.regtype"


def binary_array(values, dimensions):
    """Independent PostgreSQL int4[] send envelope, including lower bounds."""
    data = struct.pack("!iiI", len(dimensions), int(None in values), 23)
    data += b"".join(struct.pack("!ii", length, lower) for length, lower in dimensions)
    for value in values:
        raw = None if value is None else struct.pack("!i", value)
        data += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
    return data


def array_cell(text, values, dimensions=None):
    if dimensions is None:
        dimensions = [(len(values), 1)] if values else []
    return text, binary_array(values, dimensions)


def wire_cell(value, oid, format_code):
    if value is None:
        return None
    if isinstance(value, tuple):
        return value[1] if format_code else value[0].encode()
    if not format_code or oid == 25:
        return value.encode()
    if oid == 16:
        return b"\x01" if value == "t" else b"\x00"
    return struct.pack({20: "!q", 23: "!i", 26: "!I"}[oid], int(value))


class Wire:
    def __init__(self, review):
        self.review = review
        self.client = review.client
        self.sock = review.sock

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def fields(self, columns, format_code):
        return [(name.encode(), 0, 0, oid, TYPE_SIZES[oid], -1, format_code)
                for name, oid in columns]

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                for kind, body in messages if kind == b"E"]

    def parse_packet(self, statement, sql, input_oids):
        body = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(input_oids))
        body += b"".join(struct.pack("!I", oid) for oid in input_oids)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind_packet(self, statement, inputs, input_formats, result_format):
        body = b"\0" + statement + b"\0" + struct.pack("!H", len(input_formats))
        body += b"".join(struct.pack("!H", code) for code in input_formats)
        body += struct.pack("!H", len(inputs))
        for raw in inputs:
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!HH", 1, result_format)
        return (self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0") +
                self.client.typed(b"E", b"\0" + struct.pack("!I", 0)))

    def result(self, label, messages, columns, rows, result_format=0,
               input_oids=(), parsed=False, bound=False, simple=False):
        descriptions = [self.client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        expected_descriptions = ([self.fields(columns, 0)] if parsed else [])
        if bound or simple:
            expected_descriptions.append(self.fields(columns, result_format))
        expected_rows = [[wire_cell(value, oid, result_format)
                          for value, (_, oid) in zip(row, columns)] for row in rows]
        actual_rows = self.client.data_row_values(messages)
        parameters = [body for kind, body in messages if kind == b"t"]
        expected_parameters = ([struct.pack("!H", len(input_oids)) +
                                b"".join(struct.pack("!I", oid) for oid in input_oids)]
                               if parsed else [])
        tags = [body for kind, body in messages if kind == b"C"]
        expected_tags = [("SELECT " + str(len(rows))).encode() + b"\0"]
        good = (not self.errors(messages) and descriptions == expected_descriptions and
                actual_rows == expected_rows and parameters == expected_parameters and
                tags == expected_tags and messages[-1:] == [(b"Z", b"I")] and
                sum(kind == b"1" for kind, _ in messages) == int(parsed) and
                sum(kind == b"2" for kind, _ in messages) == int(bound) and
                not any(kind in (b"N", b"s", b"n") for kind, _ in messages))
        self.review.record(label, good,
                           (actual_rows, descriptions, parameters, tags, self.errors(messages),
                            [kind for kind, _ in messages], messages[-1:]),
                           (expected_rows, expected_descriptions, expected_parameters, expected_tags))

    def run(self, label, sql, columns, rows, mode="simple",
            input_oids=(), inputs=(), input_formats=()):
        if mode == "simple":
            messages = self.client.simple_query(self.sock, sql)
            self.result((label, mode), messages, columns, rows, simple=True)
        else:
            messages = self.exchange(self.parse_packet(b"", sql, input_oids) +
                                     self.bind_packet(b"", inputs, input_formats, mode))
            self.result((label, mode, input_oids, input_formats, inputs), messages,
                        columns, rows, mode, input_oids, parsed=True, bound=True)

    def describe(self, label, sql, columns, input_oids=(), statement=b""):
        messages = self.exchange(self.parse_packet(statement, sql, input_oids))
        descriptions = [self.client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        parameters = [body for kind, body in messages if kind == b"t"]
        expected_parameters = [struct.pack("!H", len(input_oids)) +
                               b"".join(struct.pack("!I", oid) for oid in input_oids)]
        good = (not self.errors(messages) and descriptions == [self.fields(columns, 0)] and
                parameters == expected_parameters and messages[-1:] == [(b"Z", b"I")] and
                sum(kind == b"1" for kind, _ in messages) == 1 and
                not any(kind in (b"D", b"C", b"2", b"N") for kind, _ in messages))
        self.review.record((label, "PARSE/DESCRIBE", sql, input_oids), good,
                           (descriptions, parameters, messages),
                           ([self.fields(columns, 0)], expected_parameters, "no execution"))

    def execute(self, label, statement, columns, rows, inputs=(), input_formats=(), result_format=0):
        messages = self.exchange(self.bind_packet(statement, inputs, input_formats, result_format))
        self.result((label, "REEXECUTE", result_format, input_formats, inputs), messages,
                    columns, rows, result_format, bound=True)

    def utility(self, sql, tag):
        messages = self.client.simple_query(self.sock, sql)
        tags = [body for kind, body in messages if kind == b"C"]
        good = (not self.errors(messages) and tags == [tag.encode() + b"\0"] and
                messages[-1:] == [(b"Z", b"I")] and
                not any(kind in (b"T", b"D", b"N") for kind, _ in messages))
        self.review.record(sql, good, messages, tag)

    def error(self, sql, state):
        messages = self.client.simple_query(self.sock, sql)
        good = (self.errors(messages) == [state] and messages[-1:] == [(b"Z", b"I")] and
                not any(kind in (b"D", b"C", b"N") for kind, _ in messages))
        self.review.record(sql, good, messages, (state, "no rows or command completion"))

    def close_statement(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        self.review.record(("CLOSE", statement), messages == [(b"3", b""), (b"Z", b"I")],
                           messages, [(b"3", b""), (b"Z", b"I")])


def main():
    review = Review("PLANNED_CONSTANT_CELLS_PROTOCOL")
    wire = Wire(review)
    schema = "r156_cells_" + uuid.uuid4().hex[:10]
    space = ident(schema)
    sequence = space + ".effects"
    child = "(SELECT pg_catalog.nextval(" + literal(sequence) + "))::pg_catalog.oid::pg_catalog.regtype"
    a, b, c = ref(25), ref(19), ref(23)
    bounded = "'[-2:0]={25,NULL,23}'::pg_catalog.regtype[]"
    multidim = "'[0:1][-2:-1]={{25,NULL},{23,19}}'::pg_catalog.regtype[]"
    bounded_cell = array_cell("[-2:0]={25,NULL,23}", [25, None, 23], [(3, -2)])
    multidim_cell = array_cell("[0:1][-2:-1]={{25,NULL},{23,19}}", [25, None, 23, 19], [(2, 0), (2, -2)])
    record = "ROW(" + a + "," + bounded + ",ROW(" + b + ",NULL::pg_catalog.int4))::pg_catalog.record"
    record_peer = ("ROW('pg_catalog.text'::pg_catalog.regtype,"
                   "'[-2:0]={pg_catalog.text,NULL,pg_catalog.int4}'::pg_catalog.regtype[],"
                   "ROW('pg_catalog.name'::pg_catalog.regtype,NULL::pg_catalog.int4))::pg_catalog.record")
    scalar_cases = [
        ("searched CASE then", "(CASE WHEN true THEN " + a + " ELSE " + b + " END)::pg_catalog.oid", 26, "25"),
        ("searched CASE else", "(CASE WHEN false THEN " + b + " ELSE " + a + " END)::pg_catalog.oid", 26, "25"),
        ("searched CASE NULL condition", "(CASE WHEN NULL THEN " + b + " ELSE " + a + " END)::pg_catalog.oid", 26, "25"),
        ("simple CASE match", "(CASE " + a + " WHEN " + a + " THEN " + b + " ELSE " + c + " END)::pg_catalog.oid", 26, "19"),
        ("simple CASE miss", "(CASE " + a + " WHEN " + b + " THEN " + c + " ELSE " + a + " END)::pg_catalog.oid", 26, "25"),
        ("simple CASE NULL", "(CASE NULL::pg_catalog.regtype WHEN NULL::pg_catalog.regtype THEN " + b + " ELSE " + a + " END)::pg_catalog.oid", 26, "25"),
        ("COALESCE prefix NULL", "COALESCE(NULL::pg_catalog.regtype," + a + "," + b + ")::pg_catalog.oid", 26, "25"),
        ("COALESCE first", "COALESCE(" + a + "," + b + ")::pg_catalog.oid", 26, "25"),
        ("COALESCE all NULL", "COALESCE(NULL::pg_catalog.regtype,NULL::pg_catalog.regtype)::pg_catalog.oid", 26, None),
        ("NULLIF equal", "NULLIF(" + a + "," + a + ")::pg_catalog.oid", 26, None),
        ("NULLIF unequal", "NULLIF(" + a + "," + b + ")::pg_catalog.oid", 26, "25"),
        ("typed selected NULL", "(CASE WHEN true THEN NULL::pg_catalog.regtype ELSE " + a + " END)::pg_catalog.oid", 26, None),
        ("IN match", a + " IN(" + b + "," + a + ")", 16, "t"),
        ("NOT IN miss", a + " NOT IN(" + b + "," + c + ")", 16, "t"),
        ("IN NULL", a + " IN(NULL::pg_catalog.regtype," + b + ")", 16, None),
        ("BETWEEN", a + " BETWEEN " + c + " AND " + ref(26), 16, "t"),
        ("BETWEEN NULL", a + " BETWEEN NULL::pg_catalog.regtype AND " + ref(26), 16, None),
        ("qualified builtin equality", a + "='pg_catalog.text'::pg_catalog.regtype", 16, "t"),
        ("distinct actual shadow OID", a + "=" + literal(space + '."text"') + "::pg_catalog.regtype", 16, "f"),
        ("result physical type", "pg_catalog.pg_typeof(COALESCE(NULL::pg_catalog.regtype," + a + "))::pg_catalog.oid", 26, "2206"),
        ("NULL distinctness", "NULL::pg_catalog.regtype IS DISTINCT FROM NULL::pg_catalog.regtype", 16, "f"),
        ("empty text datum", "CASE WHEN true THEN ''::pg_catalog.text ELSE 'x'::pg_catalog.text END", 25, ""),
        ("NULL text bytes", "COALESCE(NULL::pg_catalog.text,'NULL'::pg_catalog.text)", 25, "NULL"),
        ("quoted text bytes", "CASE WHEN true THEN 'O''Brien'::pg_catalog.text ELSE ''::pg_catalog.text END", 25, "O'Brien"),
        ("carrier collation", "(CASE WHEN true THEN 'a'::pg_catalog.text COLLATE \"C\" ELSE 'z'::pg_catalog.text END)<'b'::pg_catalog.text COLLATE \"C\"", 16, "t"),
    ]
    array_cases = [
        ("bounded conversion", bounded + "::pg_catalog.int4[]", 1007, bounded_cell),
        ("typed integer source", "'[-2:0]={25,NULL,23}'::pg_catalog.int4[]::pg_catalog.regtype[]::pg_catalog.int4[]", 1007, bounded_cell),
        ("array COALESCE", "COALESCE(NULL::pg_catalog.regtype[]," + bounded + ")::pg_catalog.int4[]", 1007, bounded_cell),
        ("array CASE", "(CASE WHEN true THEN " + bounded + " ELSE '{}'::pg_catalog.regtype[] END)::pg_catalog.int4[]", 1007, bounded_cell),
        ("first element", "(" + bounded + ")[-2]::pg_catalog.oid", 26, "25"),
        ("NULL element", "(" + bounded + ")[-1]::pg_catalog.oid", 26, None),
        ("last element", "(" + bounded + ")[0]::pg_catalog.oid", 26, "23"),
        ("outside bounds", "(" + bounded + ")[1]::pg_catalog.oid", 26, None),
        ("slice", "(" + bounded + ")[-1:0]::pg_catalog.int4[]", 1007, array_cell("{NULL,23}", [None, 23])),
        ("multidimensional conversion", multidim + "::pg_catalog.int4[]", 1007, multidim_cell),
        ("multidimensional first", "(" + multidim + ")[0][-2]::pg_catalog.oid", 26, "25"),
        ("multidimensional last", "(" + multidim + ")[1][-1]::pg_catalog.oid", 26, "19"),
        ("physical array equality", bounded + "='[-2:0]={pg_catalog.text,NULL,pg_catalog.int4}'::pg_catalog.regtype[]", 16, "t"),
        ("bounds participate in equality", bounded + "='{25,NULL,23}'::pg_catalog.regtype[]", 16, "f"),
        ("constructor identities", "ARRAY[" + a + ",NULL::pg_catalog.regtype," + c + "]::pg_catalog.int4[]", 1007, array_cell("{25,NULL,23}", [25, None, 23])),
        ("nested constructor identities", "ARRAY[ARRAY[" + a + "," + b + "],ARRAY[" + c + "," + ref(18) + "]]::pg_catalog.int4[]", 1007, array_cell("{{25,19},{23,18}}", [25, 19, 23, 18], [(2, 1), (2, 1)])),
        ("zero and maximum identity", "'{0,NULL,4294967295}'::pg_catalog.regtype[]::pg_catalog.int4[]", 1007, array_cell("{0,NULL,-1}", [0, None, -1])),
        ("maximum scalar fetch", "('[0:1]={0,4294967295}'::pg_catalog.regtype[])[1]::pg_catalog.oid", 26, "4294967295"),
        ("empty array", "'{}'::pg_catalog.regtype[]::pg_catalog.int4[]", 1007, array_cell("{}", [])),
        ("NULL array", "NULL::pg_catalog.regtype[]::pg_catalog.int4[]", 1007, None),
    ]
    record_cases = [
        ("record physical reference", "ROW(" + a + ")::pg_catalog.record=ROW('pg_catalog.text'::pg_catalog.regtype)::pg_catalog.record", 16, "t"),
        ("record reference array nested fields", record + "=" + record_peer, 16, "t"),
        ("record CASE", "(CASE WHEN true THEN " + record + " ELSE " + record_peer + " END)=" + record_peer, 16, "t"),
        ("record COALESCE", "COALESCE(NULL::pg_catalog.record," + record + ")=" + record_peer, 16, "t"),
        ("record mixed IS NULL", "ROW(" + a + ",NULL::pg_catalog.regtype[]) IS NULL", 16, "f"),
        ("record mixed IS NOT NULL", "ROW(" + a + ",NULL::pg_catalog.regtype[]) IS NOT NULL", 16, "f"),
        ("record all NULL", "ROW(NULL::pg_catalog.regtype,NULL::pg_catalog.regtype[]) IS NULL", 16, "t"),
        ("record nonnull fields", "ROW(" + a + "," + bounded + ") IS NOT NULL", 16, "t"),
        ("nested record is a nonnull field", "ROW(ROW(NULL::pg_catalog.regtype)) IS NOT NULL", 16, "t"),
        ("nested record IS NULL", "ROW(ROW(NULL::pg_catalog.regtype)) IS NULL", 16, "f"),
        ("zero-field record IS NULL", "ROW() IS NULL", 16, "t"),
        ("zero-field record IS NOT NULL", "ROW() IS NOT NULL", 16, "t"),
        ("typed NULL record", "NULL::pg_catalog.record IS NULL", 16, "t"),
    ]
    metadata_cases = [
        ("resolved reference", a, 2206),
        ("resolved OID", a + "::pg_catalog.oid", 26),
        ("reference array", bounded, 2211),
        ("integer array", bounded + "::pg_catalog.int4[]", 1007),
        ("structured record", record, 2249),
        ("NULL reference", "NULL::pg_catalog.regtype", 2206),
        ("NULL array", "NULL::pg_catalog.regtype[]", 2211),
        ("NULL record", "NULL::pg_catalog.record", 2249),
    ]
    lazy_cases = [
        ("discarded CASE child", "(CASE WHEN true THEN " + a + " ELSE " + child + " END)::pg_catalog.oid", "25"),
        ("discarded COALESCE child", "COALESCE(" + a + "," + child + ")::pg_catalog.oid", "25"),
        ("discarded simple CASE child", "(CASE " + a + " WHEN " + a + " THEN " + b + " ELSE " + child + " END)::pg_catalog.oid", "19"),
        ("discarded nested child", "(CASE WHEN false THEN (SELECT " + child + ") ELSE " + a + " END)::pg_catalog.oid", "25"),
        ("discarded typed NULL child", "(CASE WHEN true THEN NULL::pg_catalog.regtype ELSE " + child + " END)::pg_catalog.oid", None),
    ]

    try:
        assert len(BUILTIN_OIDS) == len(set(BUILTIN_OIDS)) == 108
        assert len(scalar_cases) == 25 and len(array_cases) == 20 and len(record_cases) == 13
        wire.utility("CREATE SCHEMA " + space, "CREATE SCHEMA")
        for name in SHADOW_NAMES:
            wire.utility("CREATE TYPE " + space + "." + ident(name) + " AS ENUM('z','a')", "CREATE TYPE")
        wire.utility("CREATE SEQUENCE " + sequence + " START WITH 25", "CREATE SEQUENCE")
        for explicit in (False, True):
            scope = "explicit shadow before pg_catalog" if explicit else "implicit pg_catalog first"
            wire.utility("SET search_path TO " + space + (",pg_catalog" if explicit else ""), "SET")
            for oid in IDENTITY_OIDS:
                for mode in MODES:
                    wire.run((scope, "physical identity", oid),
                             "SELECT " + ref(oid) + "::pg_catalog.oid AS v", [("v", 26)], [[str(oid)]], mode)
            for label, expression, oid, value in scalar_cases + array_cases + record_cases:
                for mode in MODES:
                    wire.run((scope, label), "SELECT " + expression + " AS v", [("v", oid)], [[value]], mode)

        # Parse/Describe has no Bind datum and cannot execute any child. Zero
        # rows must still advertise the original complete physical descriptor.
        for label, expression, oid in metadata_cases:
            sql = "SELECT " + expression + " AS v"
            wire.describe(label, sql, [("v", oid)])
            for suffix in (" WHERE false", " LIMIT 0"):
                for mode in MODES:
                    wire.run((label, suffix), sql + suffix, [("v", oid)], [], mode)
        wire.describe("metadata cannot execute discarded child",
                      "SELECT (CASE WHEN true THEN " + a + " ELSE " + child + " END)::pg_catalog.oid AS v", [("v", 26)])
        wire.describe("metadata cannot execute reached volatile child",
                      "SELECT " + child + "::pg_catalog.oid AS v", [("v", 26)])
        wire.error("SELECT pg_catalog.currval(" + literal(sequence) + ") AS v", "55000")

        # These actual binary receivers exist in the current frontend. Binary
        # REGTYPE/array receive codecs are not invented by this fixture.
        numeric_parameters = [(26, value, value) for value in
                              (0, 25, 18, 19, 114, 23, 1083, 1114, 1700, 1009, 1002, 2211,
                               4294967294, 4294967295)]
        numeric_parameters += [(23, value, 4294967295 if value == -1 else value)
                               for value in (-1, 0, 25, 1009)]
        numeric_parameters += [(20, value, value) for value in (0, 25, 4294967295)]
        numeric_parameters += [(26, None, None), (23, None, None), (20, None, None)]
        for input_oid, value, expected in numeric_parameters:
            sql = "SELECT COALESCE($1::pg_catalog.regtype,NULL::pg_catalog.regtype)::pg_catalog.oid AS v"
            for input_format in (0, 1):
                raw = None if value is None else (str(value).encode() if input_format == 0 else
                                                  struct.pack({26: "!I", 23: "!i", 20: "!q"}[input_oid], value))
                for result_format in (0, 1):
                    wire.run(("typed numeric parameter", input_oid, value), sql, [("v", 26)],
                             [[None if expected is None else str(expected)]], result_format,
                             [input_oid], [raw], [input_format])
        text_parameters = [("pg_catalog.text", "25"), ('pg_catalog."char"', "18"),
                           ("pg_catalog.name", "19"), ("pg_catalog.json", "114"),
                           ("pg_catalog.int4", "23"), ("pg_catalog.time", "1083"),
                           ("pg_catalog.timestamp", "1114"), ("pg_catalog.numeric", "1700"),
                           ("0", "0"), ("4294967294", "4294967294"),
                           ("4294967295", "4294967295"), (None, None)]
        for value, expected in text_parameters:
            for input_format in (0, 1):
                for result_format in (0, 1):
                    wire.run(("typed text parameter", value),
                             "SELECT $1::pg_catalog.regtype::pg_catalog.oid AS v", [("v", 26)], [[expected]],
                             result_format, [25], [None if value is None else value.encode()], [input_format])
        for input_oid in (2206,):
            for value in ("0", "25", "4294967295", None):
                for result_format in (0, 1):
                    wire.run(("typed REGTYPE text receiver", value),
                             "SELECT $1::pg_catalog.oid AS v", [("v", 26)], [[value]], result_format,
                             [input_oid], [None if value is None else value.encode()], [0])
        for input_oid in (1007, 2211):
            for text, expected in (("[-2:0]={25,NULL,23}", bounded_cell), ("{}", array_cell("{}", [])), (None, None)):
                for result_format in (0, 1):
                    wire.run(("typed existing array text receiver", input_oid, text),
                             "SELECT COALESCE($1::pg_catalog.regtype[],NULL::pg_catalog.regtype[])::pg_catalog.int4[] AS v",
                             [("v", 1007)], [[expected]], result_format,
                             [input_oid], [None if text is None else text.encode()], [0])

        # One prepared statement survives multiple bindings and path changes.
        # Every execution publishes its actual parameter and result formats.
        statement = (schema + "_conditional").encode()
        wire.describe("retained boolean parameter", "SELECT (CASE WHEN $1 THEN " + a + " ELSE " + b +
                      " END)::pg_catalog.oid AS v", [("v", 26)], [16], statement)
        for path in ("pg_catalog", space + ",pg_catalog", space):
            wire.utility("SET search_path TO " + path, "SET")
            for value, expected in ((True, "25"), (False, "19"), (None, "19")):
                for input_format in (0, 1):
                    raw = None if value is None else ((b"t" if value else b"f") if input_format == 0 else
                                                      (b"\x01" if value else b"\x00"))
                    for result_format in (0, 1):
                        wire.execute(("retained CASE", path, value), statement, [("v", 26)], [[expected]],
                                     [raw], [input_format], result_format)
        wire.close_statement(statement)
        statement = (schema + "_constant").encode()
        wire.describe("retained constant", "SELECT COALESCE(" + a + ",NULL::pg_catalog.regtype)::pg_catalog.oid AS v",
                      [("v", 26)], statement=statement)
        for path in (space + ",pg_catalog", "pg_catalog", space):
            wire.utility("SET search_path TO " + path, "SET")
            for result_format in (0, 1):
                wire.execute(("retained constant", path), statement, [("v", 26)], [["25"]], result_format=result_format)
        wire.close_statement(statement)

        for label, expression, expected in lazy_cases:
            for mode in MODES:
                wire.run(label, "SELECT " + expression + " AS v", [("v", 26)], [[expected]], mode)
        wire.error("SELECT pg_catalog.currval(" + literal(sequence) + ") AS v", "55000")
        wire.run("first actual sequence demand", "SELECT pg_catalog.nextval(" + literal(sequence) + ") AS v",
                 [("v", 20)], [["25"]])
        statement = (schema + "_volatile").encode()
        wire.describe("volatile child Parse is pure", "SELECT (CASE WHEN $1 THEN " + a + " ELSE " + child +
                      " END)::pg_catalog.oid AS v", [("v", 26)], [16], statement)
        wire.run("Parse did not increment sequence", "SELECT pg_catalog.currval(" + literal(sequence) + ") AS v",
                 [("v", 20)], [["25"]])
        for result_format in (0, 1):
            wire.execute("lazy retained child", statement, [("v", 26)], [["25"]], [b"t"], [0], result_format)
        for value in (26, 27):
            wire.execute("fresh volatile child per execution", statement, [("v", 26)], [[str(value)]],
                         [b"\x00"], [1], value % 2)
        wire.run("exact volatile child effects", "SELECT pg_catalog.currval(" + literal(sequence) + ") AS v",
                 [("v", 20)], [["27"]])
        wire.close_statement(statement)
    finally:
        try:
            wire.utility("SET search_path TO public", "SET")
            wire.utility("DROP SCHEMA " + space + " CASCADE", "DROP SCHEMA")
        finally:
            review.close()

    # Fixed inventory, independent of failures and of server-discovered facts:
    # setup/path/matrix + metadata + typed binds + retained plans + lazy effects
    # + final reset/drop. No failed family is skipped to satisfy this counter.
    expected_controls = (10 + 2 * (1 + 112 * 3 + 58 * 3) + 8 * 7 + 3 +
                         24 * 4 + 12 * 4 + 4 * 2 + 2 * 3 * 2 +
                         (1 + 3 * (1 + 3 * 2 * 2) + 1) + (1 + 3 * 3 + 1) +
                         5 * 3 + 1 + 1 + 1 + 1 + 2 + 2 + 1 + 1 + 2)
    review.complete(expected_controls)


if __name__ == "__main__":
    main()
