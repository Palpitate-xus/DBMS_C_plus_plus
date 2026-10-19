#!/usr/bin/env python3
"""R168: actual composite namespace pairs survive real DDL lifecycles.

The full 332-control fixture is shared with PostgreSQL 18.6. All objects
come from ordinary SQL DDL and are unique to this invocation. No composite
pg_type/value-codec capability or user-array producer is assumed on product.
Legacy/corrupt-file policy belongs exclusively to the native fixture.
"""
from compat.numeric_type_review import Review
import struct
import uuid

MODES = ("simple", "extended", "binary")
TOKEN = uuid.uuid4().hex[:12]
EXPECTED_CONTROLS = 332


def q(value):
    return '"' + value.replace('"', '""') + '"'


def name(schema, local):
    return q(schema) + "." + q(local)


class OwnerReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("CONTROL INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


class Wire:
    def __init__(self, review):
        self.review, self.client, self.sock = review, review.client, review.sock

    def messages(self, mode, sql):
        if mode == "simple":
            return self.client.simple_query(self.sock, sql)
        parse = self.client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        describe = self.client.typed(b"D", b"S\0")
        bind = self.client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, int(mode == "binary")))
        portal = self.client.typed(b"D", b"P\0")
        execute = self.client.typed(b"E", b"\0" + struct.pack("!I", 0))
        self.sock.sendall(parse + describe + bind + portal + execute + self.client.typed(b"S"))
        return self.client.read_until_ready(self.sock)

    def command(self, mode, sql, tag=None, state=None, ready=b"I", notices=False, status=None):
        messages = self.messages(mode, sql)
        diagnostics = [self.client.diagnostic_fields(body) for kind, body in messages if kind == b"N"]
        notice_ok = (not diagnostics or (notices and all(
            fields.get(b"S") == b"NOTICE" and fields.get(b"C") == b"00000" for fields in diagnostics)))
        visible = [(kind, body) for kind, body in messages if not (notices and kind == b"N")]
        errors = [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                  for kind, body in messages if kind == b"E"]
        if state:
            good = (errors == [state] and not any(kind in (b"T", b"D", b"C", b"N", b"s", b"S")
                                                 for kind, _ in messages) and
                    messages[-1:] == [(b"Z", ready)])
        else:
            wanted = [(b"C", tag.encode() + b"\0"), (b"Z", ready)]
            if mode != "simple":
                wanted = [(b"1", b""), (b"t", b"\0\0"), (b"n", b""),
                          (b"2", b""), (b"n", b"")] + wanted
            allowed = [wanted]
            if status is not None:
                # This exact optional search_path report is legal only at
                # our one SET. Never discard arbitrary ParameterStatus.
                allowed.append(wanted[:-1] + [(b"S", status)] + wanted[-1:])
            good = not errors and notice_ok and visible in allowed
        self.review.record((mode, sql), good, messages, (tag, state, ready))


def main():
    review = OwnerReview("COMPOSITE_NAMESPACE_OWNER_PROTOCOL")
    wire = Wire(review)
    print("R168_RUN_TOKEN", TOKEN, flush=True)
    wire.command("simple", "SET search_path=public,pg_catalog", "SET",
                 status=b"search_path\0public, pg_catalog\0")
    # Each real history has its own schema. Native tests additionally use
    # separate databases with exactly the same physical key in both histories.
    profiles = (("scope", "Pair"), ("Scope.Dot", "Mixed.Pair"), ('Scope"Quote', 'Esc"Pair'))
    index = 0
    for mode in MODES:
        for schema_suffix, local in profiles:
            for outside in (False, True):
                schema = "r168_" + TOKEN + "_" + str(index) + "_" + schema_suffix
                function = "r168_" + TOKEN + "_wrong_" + str(index)
                index += 1
                physical = schema + "." + local
                actual = name("public", physical) if outside else name(schema, local)
                wrong = name(schema, local) if outside else name("public", physical)
                renamed = "Renamed." + (physical if outside else local)
                renamed_sql = name("public" if outside else schema, renamed)
                wire.command(mode, "CREATE SCHEMA " + q(schema), "CREATE SCHEMA")
                wire.command(mode, "CREATE TYPE " + actual + " AS(v INTEGER)", "CREATE TYPE")
                wire.command(mode, "DROP TYPE " + wrong, state="42704")
                wire.command(mode, "CREATE FUNCTION " + name("public", function) + "(p " + wrong +
                             ") RETURNS TEXT LANGUAGE SQL AS $$SELECT 'no'::text$$", state="42704")
                # Clean up this fixture's name even on unexpected acceptance.
                wire.command(mode, "DROP FUNCTION IF EXISTS " + name("public", function), "DROP FUNCTION", notices=True)
                if outside:
                    wire.command(mode, "DROP SCHEMA " + q(schema) + " CASCADE", "DROP SCHEMA", notices=True)
                wire.command(mode, "ALTER TYPE " + actual + " ADD ATTRIBUTE kept TEXT", "ALTER TYPE")
                wire.command(mode, "ALTER TYPE " + actual + " RENAME ATTRIBUTE kept TO kept_renamed", "ALTER TYPE")
                wire.command(mode, "ALTER TYPE " + actual + " ALTER ATTRIBUTE kept_renamed TYPE INTEGER", "ALTER TYPE")
                wire.command(mode, "ALTER TYPE " + actual + " DROP ATTRIBUTE kept_renamed", "ALTER TYPE")
                wire.command(mode, "ALTER TYPE " + actual + " ADD ATTRIBUTE kept TEXT", "ALTER TYPE")
                wire.command(mode, "ALTER TYPE " + actual + " RENAME TO " + q(renamed), "ALTER TYPE")
                if outside:
                    wire.command(mode, "DROP TYPE " + renamed_sql, "DROP TYPE")
                    wire.command(mode, "CREATE TYPE " + renamed_sql + " AS(v INTEGER)", "CREATE TYPE")
                    wire.command(mode, "DROP TYPE " + renamed_sql, "DROP TYPE")
                else:
                    wire.command(mode, "DROP SCHEMA " + q(schema) + " CASCADE", "DROP SCHEMA", notices=True)
                    wire.command(mode, "CREATE SCHEMA " + q(schema), "CREATE SCHEMA")
                    wire.command(mode, "CREATE TYPE " + renamed_sql + " AS(v INTEGER)", "CREATE TYPE")
                    wire.command(mode, "DROP SCHEMA " + q(schema) + " CASCADE", "DROP SCHEMA", notices=True)

    # Actual Main ALTER uses the normal DDL snapshot owner. The later ADD
    # and rename commands prove rollback restored both definition and name,
    # without relying on a missing composite pg_type/value-output path.
    for index, mode in enumerate(MODES):
        suffix, local = profiles[index]
        schema = "r168_" + TOKEN + "_tx_" + suffix
        original = name(schema, local)
        moved = "Moved." + local
        wire.command(mode, "CREATE SCHEMA " + q(schema), "CREATE SCHEMA")
        wire.command(mode, "CREATE TYPE " + original + " AS(v INTEGER)", "CREATE TYPE")
        wire.command(mode, "BEGIN", "BEGIN", ready=b"T")
        wire.command(mode, "ALTER TYPE " + original + " ADD ATTRIBUTE transient TEXT", "ALTER TYPE", ready=b"T")
        wire.command(mode, "ALTER TYPE " + original + " RENAME TO " + q(moved), "ALTER TYPE", ready=b"T")
        wire.command(mode, "ROLLBACK", "ROLLBACK")
        wire.command(mode, "ALTER TYPE " + original + " ADD ATTRIBUTE transient TEXT", "ALTER TYPE")
        wire.command(mode, "BEGIN", "BEGIN", ready=b"T")
        wire.command(mode, "SAVEPOINT owner_before", "SAVEPOINT", ready=b"T")
        wire.command(mode, "ALTER TYPE " + original + " RENAME TO " + q(moved), "ALTER TYPE", ready=b"T")
        wire.command(mode, "ROLLBACK TO SAVEPOINT owner_before", "ROLLBACK", ready=b"T")
        wire.command(mode, "ALTER TYPE " + original + " ADD ATTRIBUTE after_sp INTEGER", "ALTER TYPE", ready=b"T")
        wire.command(mode, "RELEASE SAVEPOINT owner_before", "RELEASE", ready=b"T")
        wire.command(mode, "COMMIT", "COMMIT")
        wire.command(mode, "BEGIN", "BEGIN", ready=b"T")
        wire.command(mode, "DROP SCHEMA " + q(schema) + " CASCADE", "DROP SCHEMA", ready=b"T", notices=True)
        wire.command(mode, "ROLLBACK", "ROLLBACK")
        wire.command(mode, "ALTER TYPE " + original + " ADD ATTRIBUTE after_drop INTEGER", "ALTER TYPE")
        wire.command(mode, "DROP SCHEMA " + q(schema) + " CASCADE", "DROP SCHEMA", notices=True)

    # Preserve the actual unqualified enum receiver displaced by no composite
    # owner path. Ordinary scalar enum DDL does not request an enum-array API.
    enum = "r168_" + TOKEN + "_enum"
    wire.command("simple", "CREATE TYPE " + name("public", enum) + " AS ENUM('a','b')", "CREATE TYPE")
    wire.command("extended", "ALTER TYPE " + enum + " ADD VALUE 'c'", "ALTER TYPE")
    wire.command("binary", "ALTER TYPE " + enum + " RENAME VALUE 'a' TO 'aa'", "ALTER TYPE")
    wire.command("simple", "DROP TYPE " + name("public", enum), "DROP TYPE")
    review.close()
    # SET1 + histories18*15=270 + transactions3*19=57 + enum4 =332.
    review.complete()


if __name__ == "__main__":
    main()
