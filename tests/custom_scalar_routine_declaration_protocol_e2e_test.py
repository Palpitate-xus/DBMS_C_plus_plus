#!/usr/bin/env python3
"""R164: scalar routine declarations retain decoded custom-type owners.

Run the complete fixture with --reference18 on PostgreSQL 18.6 before
treating it as acceptance evidence. The core is 144 declaration cases:
enum/domain/composite x SQL/PLpgSQL x argument/return/both x eight SQL
spellings. Each case runs as simple CREATE and extended/binary CREATE OR
REPLACE, for 432 core DDL commands. All types are ordinary scalar SQL DDL;
there are no user-array producers/declarations or synthetic catalog rows.
Composite checks deliberately stop at declarations and replacement: their
value execution and pg_type registration are independent existing owners.
"""

from compat.numeric_type_review import Review
import re
import struct
import uuid


MODES = ("simple", "extended", "binary")
KINDS = ("e", "d", "c")
LANGUAGES = ("SQL", "plpgsql")
POSITIONS = ("argument", "return", "both")
EXPECTED_CONTROLS = 956
TYPES = {16: (1, "!?"), 20: (8, "!q"), 25: (-1, None)}
RUN_TOKEN = uuid.uuid4().hex[:12]


def scoped_sql(sql):
    # Mechanical identity projection of this fixture's own generated SQL,
    # including regtype strings and function bodies. No user SQL is accepted.
    # Every public object and private schema is unique to this invocation;
    # cleanup cannot remove a pre-existing e_kind or another run's objects.
    sql = sql.replace("r164_", "r164_" + RUN_TOKEN + "_")
    sql = sql.replace("R164.Scope", "R164." + RUN_TOKEN + ".Scope")
    sql = sql.replace("r164.scope", "r164." + RUN_TOKEN + ".scope")
    for kind in KINDS:
        sql = re.sub(r"\b" + kind + r"_kind\b", "r164_" + RUN_TOKEN + "_" + kind + "_kind", sql)
    return sql


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def qualified(schema, name):
    return identifier(schema) + "." + identifier(name)


def raw_value(value, oid, binary):
    if value is None:
        return None
    if binary and TYPES[oid][1]:
        return struct.pack(TYPES[oid][1], value)
    if oid == 16:
        return b"t" if value else b"f"
    return str(value).encode("utf-8")


class DeclarationReview(Review):
    def complete(self, expected_controls):
        # Inventory failures also reach the stable footer, after all controls.
        if self.controls != expected_controls:
            self.failures.append(("CONTROL INVENTORY", self.controls, expected_controls))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


class Wire:
    """Assert complete command/row messages without stopping on SQL errors."""

    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock

    def exchange(self, packet):
        self.sock.sendall(packet + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def errors(self, messages):
        return [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                for kind, body in messages if kind == b"E"]

    def parse(self, sql):
        body = b"\0" + sql.encode("utf-8") + b"\0" + struct.pack("!H", 0)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S\0")

    def bind(self, binary=False):
        body = b"\0\0" + struct.pack("!HHHH", 0, 0, 1, int(binary))
        return (self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0") +
                self.client.typed(b"E", b"\0" + struct.pack("!I", 0)))

    def messages(self, mode, sql):
        sql = scoped_sql(sql)
        if mode == "simple":
            return self.client.simple_query(self.sock, sql)
        return self.exchange(self.parse(sql) + self.bind(mode == "binary"))

    def command(self, mode, sql, tag=None, state=None, notices=False, parameter_status=None):
        messages = self.messages(mode, sql)
        statuses = [body for kind, body in messages if kind == b"S"]
        valid_status = not statuses
        if parameter_status is not None:
            name, value = parameter_status
            valid_status = statuses in ([], [name + b"\0" + value + b"\0"])
        visible = [(kind, body) for kind, body in messages
                   if not (notices and kind == b"N") and
                   not (parameter_status is not None and kind == b"S")]
        if state:
            good = (self.errors(messages) == [state] and
                    not any(kind in (b"T", b"D", b"C", b"N", b"s") for kind, _ in messages) and
                    messages[-1:] == [(b"Z", b"I")])
        else:
            wanted = [(b"C", tag.encode("ascii") + b"\0"), (b"Z", b"I")]
            if mode != "simple":
                wanted = [(b"1", b""), (b"t", b"\0\0"), (b"n", b""),
                          (b"2", b""), (b"n", b"")] + wanted
            # A reported GUC follows CommandComplete and precedes Ready;
            # only this explicitly expected status payload may be present.
            good = visible == wanted and (not statuses or messages[-2][0] == b"S")
        self.review.record((mode, sql), good and valid_status, messages, (tag, state, "NoData"))

    def fields(self, oid, binary=False):
        return [(b"v", 0, 0, oid, TYPES[oid][0], -1, int(binary))]

    def query(self, mode, sql, value, oid):
        messages = self.messages(mode, sql)
        binary = mode == "binary"
        descriptions = [self.client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        rows = self.client.data_row_values(messages)
        wanted_descriptions = [self.fields(oid)]
        wanted_kinds = [b"T", b"D", b"C", b"Z"]
        if mode != "simple":
            wanted_descriptions.append(self.fields(oid, binary))
            wanted_kinds = [b"1", b"t", b"T", b"2", b"T", b"D", b"C", b"Z"]
        good = (not self.errors(messages) and descriptions == wanted_descriptions and
                rows == [[raw_value(value, oid, binary)]] and
                [kind for kind, _ in messages] == wanted_kinds and
                [body for kind, body in messages if kind == b"C"] == [b"SELECT 1\0"] and
                [body for kind, body in messages if kind == b"t"] == ([] if mode == "simple" else [b"\0\0"]) and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record((mode, sql), good, messages, (value, oid, binary))

    def describe(self, sql, oid):
        messages = self.exchange(self.parse(scoped_sql(sql)))
        descriptions = [self.client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        good = (not self.errors(messages) and descriptions == [self.fields(oid)] and
                [kind for kind, _ in messages] == [b"1", b"t", b"T", b"Z"] and
                [body for kind, body in messages if kind == b"t"] == [b"\0\0"] and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record(("PARSE/DESCRIBE", sql), good, messages, (oid, "no execution"))

    def scalar(self, sql, value, oid):
        self.describe(sql, oid)
        for mode in MODES:
            self.query(mode, sql, value, oid)


def declaration(name, declaration_type, language, position, replace=False, body_type=None):
    argument = "" if position == "return" else "p " + declaration_type
    result = "TEXT" if position == "argument" else declaration_type
    if position == "argument":
        expression = "'ok'::text"
    elif position == "return":
        expression = "NULL::" + (body_type or declaration_type)
    else:
        expression = "p"
    body = "SELECT " + expression if language == "SQL" else "BEGIN RETURN " + expression + "; END"
    return ("CREATE " + ("OR REPLACE " if replace else "") + "FUNCTION public." + name +
            "(" + argument + ") RETURNS " + result + " LANGUAGE " + language +
            " IMMUTABLE" + (" STRICT" if position != "return" else "") + " AS $$" + body + "$$")


def spellings(kind):
    name = kind + "_kind"
    ordinary = "public,r164_a,r164_b,pg_catalog"
    forward = "r164_a,r164_b,public,pg_catalog"
    reversed_path = "r164_b,r164_a,public,pg_catalog"
    return (("public bare", name, ordinary),
            ("public qualified", "public." + name, reversed_path),
            ("a qualified", "r164_a." + name, reversed_path),
            ("a bare via path", name, forward),
            ("b qualified", "r164_b." + name, forward),
            ("b bare reversed path", name, reversed_path),
            ("quoted dotted namespace and mixed type", qualified("R164.Scope", "Mixed." + kind.upper()), ordinary),
            ("escaped quote type", qualified("R164.Scope", 'Esc"' + kind.upper()), ordinary))


def main():
    print("R164_RUN_TOKEN", RUN_TOKEN, flush=True)
    review = DeclarationReview("CUSTOM_SCALAR_ROUTINE_DECLARATION_PROTOCOL")
    wire = Wire(review)
    routines = []
    sequence = literal("public.r164_effects")

    def remember(name):
        routines.append(name)
        return name

    def set_path(path):
        wire.command("simple", "SET search_path=" + path, "SET",
                     parameter_status=(b"search_path", scoped_sql(path).replace(",", ", ").encode()))

    def all_modes(name, declaration_type, language, position, state=None):
        for index, mode in enumerate(MODES):
            wire.command(mode, declaration(name, declaration_type, language, position, index != 0),
                         "CREATE FUNCTION" if state is None else None, state)

    def owner_probe(name, canonical_type):
        # Compare actual regtype identities, not unstable user OIDs or the
        # search_path-sensitive display spelling of a regtype result.
        wire.scalar("SELECT pg_typeof(public." + name + "())::oid=" + literal(canonical_type) +
                    "::regtype::oid AS v", True, 16)

    try:
        for schema in ("r164_a", "r164_b", "R164.Scope"):
            wire.command("simple", "CREATE SCHEMA " + identifier(schema), "CREATE SCHEMA")
        for schema in ("public", "r164_a", "r164_b", "R164.Scope"):
            names = [(kind, kind + "_kind") for kind in KINDS] if schema != "R164.Scope" else [
                (kind, prefix + kind.upper()) for prefix in ("Mixed.", 'Esc"') for kind in KINDS]
            for kind, name in names:
                target = qualified(schema, name)
                if kind == "e":
                    sql, tag = "CREATE TYPE " + target + " AS ENUM('red','blue')", "CREATE TYPE"
                elif kind == "d":
                    sql, tag = "CREATE DOMAIN " + target + " AS INTEGER", "CREATE DOMAIN"
                else:
                    sql, tag = "CREATE TYPE " + target + " AS(v INTEGER)", "CREATE TYPE"
                wire.command("simple", sql, tag)
        wire.command("simple", "CREATE SEQUENCE public.r164_effects", "CREATE SEQUENCE")
        set_path("public,pg_catalog")
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", 1, 20)

        # 3 kinds x 8 spellings x 2 languages x 3 positions = 144 cases.
        # Every case preserves the same identity through all three modes.
        case_index = 0
        for kind in KINDS:
            for spelling, declaration_type, path in spellings(kind):
                set_path(path)
                for language in LANGUAGES:
                    for position in POSITIONS:
                        name = remember("r164_" + str(case_index))
                        all_modes(name, declaration_type, language, position)
                        case_index += 1

        # The stored identity must outlive the path that resolved its bare
        # declaration. A new bare return owner is not a valid replacement;
        # rejection retains the original and a same-owner replacement works.
        # Input-owner changes could create PG overloads and are not asserted.
        for kind in KINDS:
            for language in LANGUAGES:
                suffix = kind + "_" + language.lower()
                both = remember("r164_equivalent_" + suffix)
                result = remember("r164_return_owner_" + suffix)
                bare, canonical = kind + "_kind", "r164_a." + kind + "_kind"
                set_path("r164_a,r164_b,public,pg_catalog")
                wire.command("simple", declaration(both, bare, language, "both"), "CREATE FUNCTION")
                wire.command("extended", declaration(both, canonical, language, "both", True), "CREATE FUNCTION")
                wire.command("simple", declaration(result, bare, language, "return", body_type=canonical), "CREATE FUNCTION")
                set_path("r164_b,r164_a,public,pg_catalog")
                wire.command("binary", declaration(both, qualified("r164_a", bare), language, "both", True),
                             "CREATE FUNCTION")
                if kind != "c":
                    owner_probe(result, canonical)
                wire.command("extended", declaration(result, bare, language, "return", True), state="42P13")
                if kind != "c":
                    owner_probe(result, canonical)
                wire.command("binary", declaration(result, canonical, language, "return", True), "CREATE FUNCTION")
                if kind != "c":
                    owner_probe(result, canonical)

        # Names and schemas are decoded independently. Case-sensitive quoted
        # lookalikes must not alias their real custom scalar declaration.
        set_path("public,r164_a,r164_b,pg_catalog")
        bad_types = (("r164_missing_type", "42704"),
                     ("public.r164_missing_type", "42704"),
                     ("r164_missing_schema.r164_missing_type", "3F000"),
                     (qualified("R164.Scope", "mixed.e"), "42704"),
                     (qualified("r164.scope", "Mixed.E"), "3F000"),
                     (qualified("R164.Scope", 'Esc"e'), "42704"))
        bad_index = 0
        for declaration_type, state in bad_types:
            for language in LANGUAGES:
                for position in POSITIONS:
                    name = remember("r164_bad_" + str(bad_index))
                    all_modes(name, declaration_type, language, position, state)
                    bad_index += 1

        # Real volatile bodies are stored, not evaluated, by declaration or
        # replacement. Even composite arguments are only declared here.
        set_path("r164_a,r164_b,public,pg_catalog")
        for kind in KINDS:
            for language in LANGUAGES:
                name = remember("r164_effect_" + kind + "_" + language.lower())
                expression = "nextval(" + sequence + ")"
                body = "SELECT " + expression if language == "SQL" else "BEGIN RETURN " + expression + "; END"
                for index, mode in enumerate(MODES):
                    sql = ("CREATE " + ("OR REPLACE " if index else "") + "FUNCTION public." + name +
                           "(p " + kind + "_kind) RETURNS BIGINT LANGUAGE " + language +
                           " VOLATILE STRICT AS $$" + body + "$$")
                    wire.command(mode, sql, "CREATE FUNCTION")
        wire.query("simple", "SELECT nextval(" + sequence + ") AS v", 2, 20)

        # Existing builtin array declarations remain valid. User arrays and
        # automatic user-enum array production are intentionally not tested.
        builtin_index = 0
        for declaration_type in ("INTEGER[]", "TEXT[]"):
            for language in LANGUAGES:
                for position in POSITIONS:
                    name = remember("r164_builtin_" + str(builtin_index))
                    all_modes(name, declaration_type, language, position)
                    builtin_index += 1
    finally:
        try:
            set_path("public,pg_catalog")
            # Bare names are unambiguous: this fixture creates no overloads.
            # IF EXISTS also cleans up unexpected acceptance of bad cases.
            for name in routines:
                wire.command("simple", "DROP FUNCTION IF EXISTS public." + name, "DROP FUNCTION", notices=True)
            wire.command("simple", "DROP SEQUENCE public.r164_effects", "DROP SEQUENCE")
            for kind in KINDS:
                wire.command("simple", "DROP " + ("DOMAIN" if kind == "d" else "TYPE") +
                             " public." + kind + "_kind", "DROP DOMAIN" if kind == "d" else "DROP TYPE")
            for schema in ("r164_a", "r164_b", "R164.Scope"):
                wire.command("simple", "DROP SCHEMA " + identifier(schema) + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()

    # Setup: schemas3 + scalar types15 + sequence1 + path1 + baseline1 = 21.
    # Core: path24 + declarations144*3 = 456.
    # Owner/replacement: 6*8 commands + enum/domain probes4*3*4 = 96.
    # Rejections: path1 + invalid signatures6*2*3*3 = 109.
    # Purity/builtin arrays: path1 + volatile DDL6*3 + counter1 + arrays12*3 = 56.
    # Cleanup: path1 + functions(144+12+36+6+12) + sequence1 + types3 + schemas3 = 218.
    # 21 + 456 + 96 + 109 + 56 + 218 = 956 controls; all failures reach the footer.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
