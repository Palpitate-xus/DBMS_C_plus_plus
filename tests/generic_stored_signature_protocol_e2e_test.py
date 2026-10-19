#!/usr/bin/env python3
"""R162: ordinary stored scalar calls obey their declared input signatures.

Run this complete fixture with --reference18 on PostgreSQL 18.6 before
treating it as acceptance evidence. All candidates below come from actual
SQL/PLpgSQL DDL, with one overload per name per schema. Array controls use
existing builtin integer/string array identities; no synthetic catalog rows
or automatically generated enum arrays are involved.
"""

from compat.numeric_type_review import Review
import struct
import uuid


MODES = ("simple", "extended", "binary")
TYPES = {21: (2, "!h"), 23: (4, "!i"), 20: (8, "!q"),
         700: (4, "!f"), 701: (8, "!d"), 25: (-1, None),
         1043: (-1, None)}
ARRAY_ELEMENTS = {1007: 23, 1016: 20, 1009: 25, 1015: 1043}
EXPECTED_CONTROLS = 1705


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def raw_value(value, oid, binary):
    if value is None:
        return None
    if oid in ARRAY_ELEMENTS:
        element_oid = ARRAY_ELEMENTS[oid]
        if not binary:
            cells = []
            for cell in value:
                if cell is None:
                    cells.append("NULL")
                elif element_oid in (25, 1043):
                    cells.append('"' + cell.replace("\\", "\\\\").replace('"', '\\"') + '"')
                else:
                    cells.append(str(cell))
            return ("{" + ",".join(cells) + "}").encode("utf-8")
        # PostgreSQL's empty arrays have zero dimensions. Nonempty controls
        # are ordinary one-dimensional arrays with the default lower bound.
        body = struct.pack("!iii", int(bool(value)), int(any(cell is None for cell in value)), element_oid)
        if value:
            body += struct.pack("!ii", len(value), 1)
        for cell in value:
            raw = raw_value(cell, element_oid, True)
            body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        return body
    if not binary or TYPES[oid][1] is None:
        return str(value).encode("utf-8")
    return struct.pack(TYPES[oid][1], value)


def matches(raw, value, oid, binary):
    if raw is None or value is None:
        return raw is None and value is None
    try:
        return raw == raw_value(value, oid, binary)
    except (ValueError, UnicodeError, OverflowError, struct.error):
        return False


class Wire:
    """Full-message assertions adapted from the R161 protocol fixture."""

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
        assert len(inputs) == len(values), (inputs, values)
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
            good = errors == [state] and not rows and not tags
        else:
            wanted = [self.fields(oid)] if parsed else []
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

    def query(self, mode, sql, values=(), oid=25, state=None):
        if mode == "simple":
            self.result((mode, sql), self.client.simple_query(self.sock, sql), values, oid, state, simple=True)
        else:
            binary = mode == "binary"
            messages = self.exchange(self.parse(b"", sql) + self.bind(b"", binary_output=binary))
            self.result((mode, sql), messages, values, oid, state, binary, parsed=True, bound=True)

    def describe(self, sql, oid, statement=b"", inputs=(), state=None, resolved_inputs=None):
        messages = self.exchange(self.parse(statement, sql, inputs))
        if state:
            good = (self.errors(messages) == [state] and
                    not any(kind in (b"D", b"C", b"1", b"2", b"N", b"s") for kind, _ in messages))
        else:
            resolved = inputs if resolved_inputs is None else resolved_inputs
            parameter_body = struct.pack("!H", len(resolved)) + b"".join(struct.pack("!I", n) for n in resolved)
            good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(oid)] and
                    [body for kind, body in messages if kind == b"t"] == [parameter_body] and
                    sum(kind == b"1" for kind, _ in messages) == 1 and
                    not any(kind in (b"D", b"C", b"2", b"N", b"s") for kind, _ in messages))
        good = good and messages[-1:] == [(b"Z", b"I")]
        self.review.record(("PARSE/DESCRIBE", sql, inputs), good, messages,
                           (oid, inputs, resolved_inputs, state, "no execution"))

    def bind_describe(self, statement, oid, inputs=(), values=(), binary_input=False, binary_output=False):
        messages = self.exchange(self.bind(statement, inputs, values, binary_input, binary_output, False))
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(oid, binary_output)] and
                sum(kind == b"2" for kind, _ in messages) == 1 and
                not any(kind in (b"D", b"C", b"1", b"t", b"N", b"s") for kind, _ in messages) and
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
    review = Review("GENERIC_STORED_SIGNATURE_PROTOCOL")
    wire = Wire(review)
    token = uuid.uuid4().hex[:12]
    first, second = (identifier("r162_" + suffix + "_" + token) for suffix in ("first", "second"))
    table, empty = first + ".source_rows", first + ".empty_rows"
    tail, empty_tail = " FROM " + table + " AS r", " FROM " + empty + " AS r"
    sequence = literal(first + ".effects")
    mix, dotted, quoted = (first + "." + identifier(name) for name in ("mix", "Odd.Name", 'Say"Hi'))
    integer, text = first + ".int_echo", first + ".text_echo"
    bigint, double, numeric, smallint = (first + ".accept_" + name
                                       for name in ("bigint", "double", "numeric", "smallint"))
    integers, strings, effect = (first + "." + name for name in ("int_array", "text_array", "effect"))
    first_hint, second_hint = first + ".hinted", second + ".hinted"

    def scalar(sql, values, oid=25, state=None):
        wire.describe(sql, oid, state=state)
        for mode in MODES:
            wire.query(mode, sql, values, oid, state)

    def contexts(expression, oid=25):
        fallback = "0" if oid == 23 else "'live'::text"
        return ("SELECT " + expression + " AS v" + tail,
                "SELECT " + expression + " AS v" + empty_tail,
                "SELECT " + expression + " AS v" + tail + " WHERE false",
                "SELECT " + expression + " AS v" + tail + " LIMIT 0",
                "SELECT CASE WHEN false THEN " + expression + " ELSE " + fallback + " END AS v" + tail,
                "SELECT COALESCE(" + fallback + "," + expression + ") AS v" + tail)

    try:
        wire.command("CREATE SCHEMA " + first, "CREATE SCHEMA")
        wire.command("CREATE SCHEMA " + second, "CREATE SCHEMA")
        wire.command("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY,t TEXT,n INTEGER,s SMALLINT,b BIGINT,"
                     "r REAL,d DOUBLE PRECISION,dec NUMERIC,v VARCHAR(16),c CHAR(4),"
                     "ai INTEGER[],al BIGINT[],at TEXT[],av VARCHAR[])", "CREATE TABLE")
        wire.command("CREATE TABLE " + empty + "(t TEXT,n INTEGER)", "CREATE TABLE")
        wire.command("INSERT INTO " + table + " VALUES"
                     "(1,'alpha',7,2,9,1.25,1.5,1.25,'varchar','four','{1,NULL,2}','{1,NULL,2}',"
                     "'{alpha,NULL,beta}','{alpha,NULL,beta}'),"
                     "(2,'',-3,-2,-9,0,-1.5,-1.25,'','abcd','{-3,NULL,5}','{-3,NULL,5}',"
                     "'{\"\",NULL,\"NULL\"}','{\"\",NULL,\"NULL\"}'),"
                     "(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)", "INSERT 0 3")
        wire.command("CREATE SEQUENCE " + first + ".effects", "CREATE SEQUENCE")
        for name in (mix, dotted):
            wire.command("CREATE FUNCTION " + name + "(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                         "AS $$SELECT t || ':' || n::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + quoted + "(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT "
                     "AS $$BEGIN RETURN t || ':' || n::text; END$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + integer + "(p INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT "
                     "AS $$SELECT p$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + text + "(p TEXT) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT "
                     "AS $$BEGIN RETURN p; END$$", "CREATE FUNCTION")
        for name, declaration, tag in ((bigint, "BIGINT", "bigint"), (double, "DOUBLE PRECISION", "double"),
                                       (numeric, "NUMERIC", "numeric"), (smallint, "SMALLINT", "smallint"),
                                       (integers, "BIGINT[]", "int-array"), (strings, "TEXT[]", "text-array")):
            wire.command("CREATE FUNCTION " + name + "(p " + declaration + ") RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                         "AS $$SELECT '" + tag + "'::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + effect + "(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql VOLATILE "
                     "AS $$BEGIN PERFORM nextval(" + sequence + "); RETURN p; END$$", "CREATE FUNCTION")

        for schema, declaration, tag in ((first, "BIGINT", "early-bigint"), (second, "INTEGER", "late-integer")):
            wire.command("CREATE FUNCTION " + schema + ".ranked(p " + declaration + ") RETURNS TEXT LANGUAGE SQL "
                         "IMMUTABLE STRICT AS $$SELECT '" + tag + "'::text$$", "CREATE FUNCTION")
        for schema, tag in ((first, "early-text"), (second, "late-text")):
            wire.command("CREATE FUNCTION " + schema + ".same_signature(p TEXT) RETURNS TEXT LANGUAGE SQL "
                         "IMMUTABLE STRICT AS $$SELECT '" + tag + "'::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + first + ".string_pick(p INTEGER) RETURNS INTEGER LANGUAGE SQL "
                     "IMMUTABLE STRICT AS $$SELECT p$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + second + ".string_pick(p TEXT) RETURNS TEXT LANGUAGE SQL "
                     "IMMUTABLE STRICT AS $$SELECT 'string'::text$$", "CREATE FUNCTION")
        for schema, declaration, tag in ((first, "INTEGER", "integer"), (second, "NUMERIC", "numeric")):
            wire.command("CREATE FUNCTION " + schema + ".ambiguous(p " + declaration + ") RETURNS TEXT LANGUAGE SQL "
                         "IMMUTABLE STRICT AS $$SELECT '" + tag + "'::text$$", "CREATE FUNCTION")
        for name, declaration in ((first_hint, "BIGINT"), (second_hint, "INTEGER")):
            wire.command("CREATE FUNCTION " + name + "(p " + declaration + ") RETURNS " + declaration +
                         " LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p$$", "CREATE FUNCTION")
        wire.command("SET search_path=" + first + "," + second + ",pg_catalog", "SET")
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [1], 20)

        # Qualified, quoted and search-path calls share the same typed column
        # contract. Each retained descriptor is checked without execution.
        exact = [(name + "(r.t,r.n)", 25, ["alpha:7", ":-3", None])
                 for name in (mix, "mix", dotted, identifier("Odd.Name"), quoted, identifier('Say"Hi'))]
        exact += [(integer + "(r.n)", 23, [7, -3, None]), (text + "(r.t)", 25, ["alpha", "", None])]
        # Differing result types expose an earlier namespace's return hint
        # leaking into a later exact input match. Qualified SMALLINT inputs
        # exercise legal implicit conversions without competing candidates.
        exact += [("hinted(r.n)", 23, [7, -3, None]), ("hinted(r.b)", 20, [9, -9, None]),
                  (first_hint + "(r.s)", 20, [2, -2, None]), (second_hint + "(r.s)", 23, [2, -2, None])]
        for index, (expression, oid, values) in enumerate(exact):
            sql, statement = "SELECT " + expression + " AS v" + tail, ("r162_exact_" + str(index)).encode()
            wire.describe(sql, oid, statement)
            for mode in MODES:
                wire.query(mode, sql + " ORDER BY r.id", values, oid)
                wire.query(mode, sql + " WHERE false", [], oid)
                wire.query(mode, sql + " LIMIT 0", [], oid)
            for binary_output in (False, True):
                wire.bind_describe(statement, oid, binary_output=binary_output)
            wire.close(statement)
        for mode in MODES:
            wire.query(mode, "SELECT " + mix + "(r.t,r.n) AS v" + empty_tail, [], 25)

        # A call must bind before row demand, dead CASE arms or COALESCE can
        # hide it. No implicit int/text relabeling is legal in either order.
        for name in (mix, "mix", dotted, quoted):
            for sql in contexts(name + "(r.n,r.t)"):
                scalar(sql, [], state="42883")
        for expression, oid in ((mix + "(NULL::integer,NULL::text)", 25),
                                (mix + "(NULL::text,NULL::text)", 25),
                                (integer + "(NULL::text)", 23),
                                (strings + "(NULL::integer[])", 25),
                                (integers + "(NULL::text[])", 25),
                                (smallint + "(NULL::bigint)", 25)):
            scalar("SELECT " + expression + " AS v", [], oid, "42883")
        for expression, oid, state in (("hinted(NULL::integer)", 23, None),
                                        ("hinted(NULL::bigint)", 20, None),
                                        (first_hint + "(NULL::smallint)", 20, None),
                                        (second_hint + "(NULL::smallint)", 23, None),
                                        (first_hint + "(NULL::integer)", 20, None),
                                        (second_hint + "(NULL::bigint)", 23, "42883"),
                                        ("hinted(NULL::text)", 23, "42883")):
            scalar("SELECT " + expression + " AS v", [] if state else [None], oid, state)

        explicit = ((mix + "(r.n::text,(r.n::text)::integer)", ["7:7", "-3:-3", None], 25, tail + " ORDER BY r.id"),
                    (mix + "('7'::text,'2'::integer)", ["7:2"], 25, ""),
                    (integer + "(('17'::text)::integer)", [17], 23, ""),
                    (smallint + "((7::bigint)::smallint)", ["smallint"], 25, ""),
                    (bigint + "((1.25::numeric)::bigint)", ["bigint"], 25, ""),
                    (integers + "(('{1,2}'::integer[])::bigint[])", ["int-array"], 25, ""),
                    (strings + "(('{1,2}'::integer[])::text[])", ["text-array"], 25, ""))
        for expression, values, oid, suffix in explicit:
            scalar("SELECT " + expression + " AS v" + suffix, values, oid)

        unknowns = ((mix + "('unknown','7')", ["unknown:7"], 25),
                    (mix + "(NULL,'7')", [None], 25), (mix + "('unknown',NULL)", [None], 25),
                    (mix + "(NULL,NULL)", [None], 25), (integer + "('17')", [17], 23),
                    (integer + "(NULL)", [None], 23), (text + "('unknown')", ["unknown"], 25),
                    (text + "(NULL)", [None], 25),
                    (integers + "('{1,NULL,2}')", ["int-array"], 25),
                    (strings + "('{\"a\",NULL,\"NULL\"}')", ["text-array"], 25),
                    (integers + "(NULL)", [None], 25), (strings + "(NULL)", [None], 25))
        for expression, values, oid in unknowns:
            scalar("SELECT " + expression + " AS v", values, oid)
        for expression, oid in ((mix + "('x','bad')", 25), (mix + "(NULL,'bad')", 25),
                                (integer + "('bad')", 23), (effect + "('bad')", 23)):
            scalar("SELECT " + expression + " AS v", [], oid, "22P02")
        for sql in contexts(integer + "('bad')", 23)[1:]:
            scalar(sql, [], 23, "22P02")

        # Legal coercions are admitted for real columns, including builtin
        # arrays. Tags make float/numeric output rendering irrelevant here.
        coercions = ((bigint + "(r.s)", ["bigint", "bigint", None]),
                     (bigint + "(r.n)", ["bigint", "bigint", None]),
                     (double + "(r.s)", ["double", "double", None]),
                     (double + "(r.n)", ["double", "double", None]),
                     (double + "(r.b)", ["double", "double", None]),
                     (double + "(r.r)", ["double", "double", None]),
                     (double + "(r.dec)", ["double", "double", None]),
                     (numeric + "(r.s)", ["numeric", "numeric", None]),
                     (numeric + "(r.n)", ["numeric", "numeric", None]),
                     (numeric + "(r.b)", ["numeric", "numeric", None]),
                     (text + "(r.v)", ["varchar", "", None]),
                     (text + "(r.c)", ["four", "abcd", None]),
                     (integers + "(r.ai)", ["int-array", "int-array", None]),
                     (integers + "(r.al)", ["int-array", "int-array", None]),
                     (strings + "(r.av)", ["text-array", "text-array", None]),
                     (strings + "(r.at)", ["text-array", "text-array", None]),
                     (smallint + "(r.s)", ["smallint", "smallint", None]))
        for expression, values in coercions:
            sql = "SELECT " + expression + " AS v" + tail
            wire.describe(sql, 25)
            for mode in MODES:
                wire.query(mode, sql + " ORDER BY r.id", values, 25)
                wire.query(mode, sql + " WHERE false", [], 25)
                wire.query(mode, sql + " LIMIT 0", [], 25)
        invalid_coercions = ((smallint + "(r.n)", 25), (smallint + "(r.b)", 25),
                             (integer + "(r.b)", 23), (integer + "(r.dec)", 23),
                             (integer + "(r.r)", 23), (integer + "(r.d)", 23),
                             (bigint + "(r.dec)", 25), (bigint + "(r.r)", 25), (bigint + "(r.d)", 25),
                             (numeric + "(r.r)", 25), (numeric + "(r.d)", 25),
                             (bigint + "(r.t)", 25), (text + "(r.n)", 25), (integer + "(r.t)", 23),
                             (integers + "(r.at)", 25), (strings + "(r.ai)", 25),
                             (integers + "(r.av)", 25), (strings + "(r.al)", 25))
        for expression, oid in invalid_coercions:
            sql = "SELECT " + expression + " AS v" + tail
            wire.describe(sql, oid, state="42883")
            for suffix in ("", " WHERE false", " LIMIT 0"):
                for mode in MODES:
                    wire.query(mode, sql + suffix, [], oid, "42883")

        # Candidate collection spans search_path before choosing the exact
        # signature; only identical signatures are hidden by an earlier path.
        path_cases = (("ranked(r.n)", ["late-integer", "late-integer", None], 25, tail + " ORDER BY r.id"),
                      ("ranked(r.b)", ["early-bigint", "early-bigint", None], 25, tail + " ORDER BY r.id"),
                      (first + ".ranked(r.n)", ["early-bigint", "early-bigint", None], 25, tail + " ORDER BY r.id"),
                      (second + ".ranked(r.n)", ["late-integer", "late-integer", None], 25, tail + " ORDER BY r.id"),
                      ("same_signature(r.t)", ["early-text", "early-text", None], 25, tail + " ORDER BY r.id"),
                      ("string_pick('17')", ["string"], 25, ""), ("string_pick(NULL)", [None], 25, ""),
                      (first + ".string_pick('17')", [17], 23, ""),
                      (second + ".string_pick('17')", ["string"], 25, ""))
        for expression, values, oid, suffix in path_cases:
            scalar("SELECT " + expression + " AS v" + suffix, values, oid)
        for argument in ("'17'", "NULL"):
            scalar("SELECT ambiguous(" + argument + ") AS v", [], state="42725")
        for argument, tag in (("17::integer", "integer"), ("17::numeric", "numeric")):
            scalar("SELECT ambiguous(" + argument + ") AS v", [tag])
        wire.command("SET search_path=" + second + "," + first + ",pg_catalog", "SET")
        for expression, values, suffix in (("same_signature(r.t)", ["late-text", "late-text", None], tail + " ORDER BY r.id"),
                                           ("ranked(r.n)", ["late-integer", "late-integer", None], tail + " ORDER BY r.id"),
                                           ("ranked(r.b)", ["early-bigint", "early-bigint", None], tail + " ORDER BY r.id"),
                                           ("string_pick('17')", ["string"], "")):
            scalar("SELECT " + expression + " AS v" + suffix, values)
        wire.command("SET search_path=" + first + "," + second + ",pg_catalog", "SET")
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [2], 20)

        # Parse/Describe and Bind/Describe may discover return descriptors but
        # cannot execute a volatile body, even for a non-STRICT NULL input.
        statement = b"r162_effect_columns"
        wire.describe("SELECT " + effect + "(r.n) AS v" + tail, 23, statement)
        for binary_output in (False, True):
            wire.bind_describe(statement, 23, binary_output=binary_output)
        wire.close(statement)
        statement = b"r162_effect_parameter"
        wire.describe("SELECT " + effect + "($1) AS v", 23, statement, (23,))
        for value in (7, None):
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, 23, (23,), (value,), binary_input, binary_output)
        wire.close(statement)
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [3], 20)
        skipped = (("SELECT " + effect + "(r.n) AS v" + tail + " WHERE false", []),
                   ("SELECT " + effect + "(r.n) AS v" + tail + " LIMIT 0", []),
                   ("SELECT " + effect + "(r.n) AS v" + empty_tail, []),
                   ("SELECT CASE WHEN false THEN " + effect + "(7) ELSE 99 END AS v", [99]),
                   ("SELECT COALESCE(99," + effect + "(7)) AS v", [99]))
        for sql, values in skipped:
            for mode in MODES:
                wire.query(mode, sql, values, 23)
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [4], 20)
        for mode in MODES:
            wire.query(mode, "SELECT " + effect + "(9) AS v", [9], 23)
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [8], 20)

        # Reuse each statement with every text/binary input/output pairing,
        # then reverse the complete value/NULL sequence to expose stale cells.
        pairs = [("bind", 2), ("", -3), ("a,b", 4), ("NULL", 0), (None, 5), ("after", None)]
        pair_outputs = ["bind:2", ":-3", "a,b:4", "NULL:0", None, None]
        plans = [(mix + "($1,$2)", (25, 23), (25, 23), 25, pairs, pair_outputs),
                 (quoted + "($1,$2)", (25, 23), (25, 23), 25, pairs, pair_outputs),
                 (integer + "($1)", (23,), (23,), 23,
                  [(0,), (-1,), (2147483647,), (None,), (7,), (0,)], [0, -1, 2147483647, None, 7, 0]),
                 (text + "($1)", (25,), (25,), 25,
                  [("alpha",), ("",), ("NULL",), (None,), ("a,b",), ("O'Reilly",)],
                  ["alpha", "", "NULL", None, "a,b", "O'Reilly"]),
                 (bigint + "($1)", (23,), (23,), 25,
                  [(0,), (-1,), (7,), (None,), (2,), (0,)], ["bigint", "bigint", "bigint", None, "bigint", "bigint"]),
                 (double + "($1)", (700,), (700,), 25,
                  [(0.0,), (1.25,), (-1.5,), (None,), (3.0,), (0.0,)], ["double", "double", "double", None, "double", "double"]),
                 (integers + "($1)", (1007,), (1007,), 25,
                  [([1, None, -2],), ([],), (None,), ([0],), ([7, 7],), ([1],)],
                  ["int-array", "int-array", None, "int-array", "int-array", "int-array"]),
                 (strings + "($1)", (1015,), (1015,), 25,
                  [(["a", None, "NULL"],), ([],), (None,), (["a,b"],), ([""],), (['O"Reilly'],)],
                  ["text-array", "text-array", None, "text-array", "text-array", "text-array"]),
                 (mix + "($1,$2)", (0, 0), (25, 23), 25, pairs, pair_outputs)]
        small_values = [32767, -32768, 0, None, 7, -9]
        int_values = [2147483647, -2147483648, 0, None, 7, -9]
        big_values = [9223372036854775807, -9223372036854775808, 0, None, 7, -9]
        for expression, input_oid, output_oid, values in (("hinted($1)", 23, 23, int_values),
                                                         ("hinted($1)", 20, 20, big_values),
                                                         (first_hint + "($1)", 21, 20, small_values),
                                                         (second_hint + "($1)", 21, 23, small_values),
                                                         (first_hint + "($1)", 23, 20, int_values)):
            plans.append((expression, (input_oid,), (input_oid,), output_oid, [(value,) for value in values], values))
        for index, (expression, declared, inputs, oid, values, outputs) in enumerate(plans):
            statement = ("r162_retained_" + str(index)).encode()
            wire.describe("SELECT " + expression + " AS v", oid, statement, declared, resolved_inputs=inputs)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, oid, inputs, values[0], binary_input, binary_output)
            for repeat in (0, 1):
                cells = list(zip(values, outputs))
                for value, output in (cells if repeat == 0 else list(reversed(cells))):
                    for binary_input in (False, True):
                        for binary_output in (False, True):
                            messages = wire.exchange(wire.bind(statement, inputs, value, binary_input, binary_output))
                            wire.result(("REEXECUTE", statement, value, binary_input, binary_output, repeat),
                                        messages, [output], oid, binary=binary_output, bound=True)
            wire.close(statement)

        # Error recovery preserves a separately retained, correctly typed
        # statement. Every rejected typed parameter signature remains visible.
        statement = b"r162_recovery"
        wire.describe("SELECT " + mix + "($1,$2) AS v", 25, statement, (25, 23))
        wrong_parameters = ((mix + "($1,$2)", (23, 25), 25),
                            (quoted + "($1,$2)", (23, 25), 25),
                            (integer + "($1)", (25,), 23), (smallint + "($1)", (20,), 25),
                            (integers + "($1)", (1009,), 25), (strings + "($1)", (1007,), 25),
                            (effect + "($1)", (25,), 23))
        for expression, inputs, oid in wrong_parameters:
            wire.describe("SELECT " + expression + " AS v", oid, inputs=inputs, state="42883")
            for binary_input in (False, True):
                for binary_output in (False, True):
                    messages = wire.exchange(wire.bind(statement, (25, 23), ("recover", 9), binary_input, binary_output))
                    wire.result(("RECOVERY", expression, binary_input, binary_output), messages,
                                ["recover:9"], 25, binary=binary_output, bound=True)
        wire.close(statement)
        for mode in MODES:
            wire.query(mode, "SELECT " + integer + "(9) AS v", [9], 23)
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", [9], 20)
    finally:
        try:
            wire.command("SET search_path=public", "SET")
            wire.command("DROP SCHEMA " + first + " CASCADE", "DROP SCHEMA", notices=True)
            wire.command("DROP SCHEMA " + second + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Original inventory, with all 1353 controls retained:
    # Setup27 + baseline1 + exact8*13=104 + empty3 + reversed4*6*4=96
    # + typedNULL6*4=24 + explicit7*4=28 + UNKNOWN12*4=48
    # + badUNKNOWN(4+5)*4=36 + coercions17*10=170 + rejected18*10=180
    # + path(9+2+2+4)*4+2=70 + purity(1+14+1+15+1+3+1)=36
    # + retained9*54=486 + recovery(1+7*5+1+3+1)=41 + cleanup3 = 1353.
    # Differing-result additions: DDL2 + column/metadata4*13=52
    # + strictNULL7*4=28 + retained5*54=270 = 352; 1353+352 = 1705.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
