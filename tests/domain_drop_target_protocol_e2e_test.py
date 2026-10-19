#!/usr/bin/env python3
"""R167 PostgreSQL18-common DROP DOMAIN target grammar and actual deletion.

Draft for installation under tests/. All owned schemas and public targets
have a fresh per-run UUID. Domains named cascade/restrict/if are isolated
inside those UUID schemas. No valid multi-target DROP is executed: PG
supports it, while the product's explicit 0A000 policy is native-only.
Every malformed named Parse is retried with the exact same statement name,
then the valid DROP is actually bound/executed and the real target's absence
is checked. No compilation or runtime evidence is implied by this draft.
"""

from compat.numeric_type_review import Review
import struct
import re
import uuid


MODES = ("simple", "extended", "binary")
EXPECTED_CONTROLS = 663


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def qualified(schema, name):
    return identifier(schema) + "." + identifier(name)


def malformed(target, other, schema, name):
    # PG18's observed keyword-as-name policy is not guessed here.
    return ("DROP DOMAIN", "DROP DOMAIN IF EXISTS", "DROP DOMAIN ," + target,
            "DROP DOMAIN " + target + ",", "DROP DOMAIN " + target + ",," + other,
            "DROP DOMAIN " + target + " " + other, "DROP DOMAIN " + schema + ".",
            "DROP DOMAIN ." + name, "DROP DOMAIN " + schema + ".." + name,
            "DROP DOMAIN " + target + ".extra", "DROP DOMAIN " + schema + ".," + other,
            "DROP DOMAIN " + target + " CASCADE RESTRICT", "DROP DOMAIN " + target + " RESTRICT CASCADE",
            "DROP DOMAIN " + target + " CASCADE CASCADE", "DROP DOMAIN " + target + " RESTRICT RESTRICT",
            "DROP DOMAIN " + target + " CASCADE garbage", "DROP DOMAIN " + target + " RESTRICT garbage",
            "DROP DOMAIN " + target + " CASCADE," + other, "DROP DOMAIN " + target + "()",
            "DROP DOMAIN " + target + "[]", "DROP DOMAIN " + target + "=1", "DROP DOMAIN 'not_an_identifier'",
            "DROP DOMAIN 7", 'DROP DOMAIN ""', "DROP DOMAIN " + target + ",,",
            "DROP DOMAIN " + target + " RESTRICT " + other, "DROP DOMAIN IF EXISTS " + target + ",",
            "DROP DOMAIN IF EXISTS ," + target, "DROP DOMAIN IF EXISTS " + schema + ".",
            "DROP DOMAIN " + target + " IF EXISTS", "DROP DOMAIN " + target + "; garbage",
            "DROP DOMAIN " + target + " CASCADE; garbage", "DROP DOMAIN exists")


class DropReview(Review):
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

    def parse(self, sql, statement=b""):
        body = statement + b"\0" + sql.encode("utf-8") + b"\0" + struct.pack("!H", 0)
        return self.client.typed(b"P", body) + self.client.typed(b"D", b"S" + statement + b"\0")

    def bind(self, statement=b"", binary=False):
        body = b"\0" + statement + b"\0" + struct.pack("!HHHH", 0, 0, 1, int(binary))
        return (self.client.typed(b"B", body) + self.client.typed(b"D", b"P\0") +
                self.client.typed(b"E", b"\0" + struct.pack("!I", 0)))

    def messages(self, mode, sql):
        if mode == "simple":
            return self.client.simple_query(self.sock, sql)
        return self.exchange(self.parse(sql) + self.bind(binary=mode == "binary"))

    def command(self, mode, sql, tag=None, state=None, notices=False, parameter_status=None):
        messages = self.messages(mode, sql)
        statuses = [body for kind, body in messages if kind == b"S"]
        valid_status = not statuses
        if parameter_status is not None:
            valid_status = statuses in ([], [b"search_path\0" + parameter_status + b"\0"])
        visible = [(kind, body) for kind, body in messages
                   if not (notices and kind == b"N") and not (parameter_status is not None and kind == b"S")]
        if state:
            good = (self.errors(messages) == [state] and
                    not any(kind in (b"T", b"D", b"C", b"N", b"s") for kind, _ in messages) and
                    messages[-1:] == [(b"Z", b"I")])
        else:
            wanted = [(b"C", tag.encode("ascii") + b"\0"), (b"Z", b"I")]
            if mode != "simple":
                wanted = [(b"1", b""), (b"t", b"\0\0"), (b"n", b""),
                          (b"2", b""), (b"n", b"")] + wanted
            good = visible == wanted and (not statuses or messages[-2][0] == b"S")
        self.review.record((mode, sql), good and valid_status, messages, (tag, state))

    def parse_command(self, sql, statement, state=None):
        messages = self.exchange(self.parse(sql, statement))
        if state:
            good = (self.errors(messages) == [state] and
                    [kind for kind, _ in messages] == [b"E", b"Z"] and
                    messages[-1:] == [(b"Z", b"I")])
        else:
            good = messages == [(b"1", b""), (b"t", b"\0\0"), (b"n", b""), (b"Z", b"I")]
        self.review.record(("PARSE/DESCRIBE", statement, sql), good, messages, (state, "NoData; no execution"))

    def execute_command(self, statement, sql=None):
        messages = self.exchange((self.parse(sql, statement) if sql is not None else b"") + self.bind(statement))
        wanted = [(b"2", b""), (b"n", b""), (b"C", b"DROP DOMAIN\0"), (b"Z", b"I")]
        if sql is not None:
            wanted = [(b"1", b""), (b"t", b"\0\0"), (b"n", b"")] + wanted
        self.review.record(("BIND/EXECUTE", statement), messages == wanted, messages, wanted)

    def close(self, statement):
        messages = self.exchange(self.client.typed(b"C", b"S" + statement + b"\0"))
        wanted = [(b"3", b""), (b"Z", b"I")]
        self.review.record(("CLOSE", statement), messages == wanted, messages, wanted)

    def integer(self, mode, sql, value):
        messages = self.messages(mode, sql)
        binary = mode == "binary"
        fields = [(b"v", 0, 0, 23, 4, -1, 0)]
        descriptions = [self.client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        wanted_descriptions = [fields]
        kinds = [b"T", b"D", b"C", b"Z"]
        if mode != "simple":
            wanted_descriptions.append([(b"v", 0, 0, 23, 4, -1, int(binary))])
            kinds = [b"1", b"t", b"T", b"2", b"T", b"D", b"C", b"Z"]
        raw = struct.pack("!i", value) if binary else str(value).encode("ascii")
        good = (not self.errors(messages) and descriptions == wanted_descriptions and
                self.client.data_row_values(messages) == [[raw]] and
                [kind for kind, _ in messages] == kinds and
                [body for kind, body in messages if kind == b"C"] == [b"SELECT 1\0"] and
                [body for kind, body in messages if kind == b"t"] == ([] if mode == "simple" else [b"\0\0"]) and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record((mode, sql), good, messages, (value, 23, binary))

    def exists(self, target, mode="simple"):
        self.integer(mode, "SELECT (1::" + target + ")::integer AS v", 1)


def main():
    review = DropReview("DOMAIN_DROP_TARGET_PROTOCOL")
    wire = Wire(review)
    prefix = "r167_" + uuid.uuid4().hex[:12]
    first, second, dotted, escaped = (prefix + suffix for suffix in ("_first", "_second", ".Scope", '"Quote'))
    schemas = (first, second, dotted, escaped)
    shared = prefix + "_shared"
    guards = (qualified(first, shared), qualified(second, shared), qualified("public", shared))
    quoted_guard = qualified(dotted, prefix + ".Mixed.Guard")
    wrong_case = qualified(dotted, prefix + ".mixed.guard")
    public_targets = [qualified("public", prefix + suffix) for suffix in ("_bare", "_qualified", "_spaced", ".Dot")]

    def path(*owners):
        normalized = ", ".join(owner if re.fullmatch(r"[a-z_][a-z0-9_$]*", owner) else identifier(owner)
                               for owner in owners + ("pg_catalog",)).encode()
        wire.command("simple", "SET search_path=" + ",".join(identifier(owner) for owner in owners) + ",pg_catalog", "SET",
                     parameter_status=normalized)

    def create(target, mode="simple"):
        wire.command(mode, "CREATE DOMAIN " + target + " AS INTEGER DEFAULT 7", "CREATE DOMAIN")

    def guards_survive():
        wire.integer("simple", "SELECT " + "+".join("(1::" + target + ")::integer" for target in guards) + " AS v", 3)

    # Each full identity contains the per-run UUID, even where the terminal
    # domain identifier is an intentionally literal keyword or quoted number.
    targets = ((public_targets[0], prefix + "_bare", ("public", first)),
               (public_targets[1], "PUBLIC." + (prefix + "_qualified").upper(), (second, "public")),
               (public_targets[2], "public . " + prefix + "_spaced", (first, "public")),
               (qualified(first, prefix + "_private"), qualified(first, prefix + "_private"), (second, "public")),
               (qualified(dotted, prefix + ".Mixed.Type"), qualified(dotted, prefix + ".Mixed.Type"), ("public",)),
               (qualified(escaped, prefix + '"Domain'), qualified(escaped, prefix + '"Domain'), ("public",)),
               (public_targets[3], public_targets[3], ("public",)),
               (qualified(first, "cascade"), qualified(first, "cascade"), ("public",)),
               (qualified(second, "restrict"), qualified(second, "restrict"), ("public",)),
               (qualified(dotted, "123"), qualified(dotted, "123"), ("public",)),
               (qualified(first, "cascade"), "cascade", (first, "public")),
               (qualified(second, "restrict"), "restrict", (second, "public")),
               (qualified(first, "if"), "if", (first, "public")))
    try:
        for schema in schemas:
            wire.command("simple", "CREATE SCHEMA " + identifier(schema), "CREATE SCHEMA")
        path("public")
        for target in guards:
            create(target)
        create(quoted_guard)
        guards_survive()

        # 13 targets x 3 modes x 6 assertions, plus 13 independent paths.
        for index, (target, spelling, owners) in enumerate(targets):
            path(*owners)
            for mode_index, mode in enumerate(MODES):
                create(target, mode)
                wire.exists(target, mode)
                options = ("", " RESTRICT", " CASCADE")[(index + mode_index) % 3]
                wire.command(mode, "DROP DOMAIN " + ("IF EXISTS " if index % 2 else "") + spelling + options,
                             "DROP DOMAIN")
                wire.command(mode, "DROP DOMAIN " + target, state="42704")
                wire.command(mode, "SELECT NULL::" + target + " AS v", state="42704")
                wire.command(mode, "DROP DOMAIN IF EXISTS " + target + options, "DROP DOMAIN", notices=True)

        for index, sql in enumerate(malformed(guards[0], guards[1], identifier(first), identifier(shared))):
            statement = (prefix + "_retry_" + str(index)).encode("ascii")
            retry = qualified(first, prefix + "_retry_domain_" + str(index))
            semantic_state = {9: "0A000", 19: "42809"}.get(index)
            wire.command("simple", sql, state=semantic_state or "42601")
            wire.parse_command(sql, statement, None if semantic_state else "42601")
            wire.command("binary", sql, state=semantic_state or "42601")
            guards_survive()
            create(retry)
            # Same name, not a new name after failed Parse. The succeeding
            # Parse/Describe cannot drop the domain before actual Execute.
            if semantic_state:
                # A valid Parse owns its name even though Execute fails;
                # close that owner before reusing the exact same name.
                wire.close(statement)
            else:
                wire.parse_command("DROP DOMAIN " + retry + " RESTRICT", statement)
            wire.exists(retry)
            wire.execute_command(statement, "DROP DOMAIN " + retry + " RESTRICT" if semantic_state else None)
            wire.command("simple", "DROP DOMAIN " + retry, state="42704")
            wire.close(statement)

        # Both path order and explicit public qualification choose actual
        # owners; every drop leaves the other two same-name domains usable.
        for mode in MODES:
            for owner_index in (0, 1, 2):
                owners = (first, second, "public") if owner_index == 0 else (second, first, "public")
                path(*owners)
                spelling = identifier(shared) if owner_index != 2 else guards[2]
                wire.command(mode, "DROP DOMAIN " + spelling, "DROP DOMAIN")
                wire.command(mode, "DROP DOMAIN " + guards[owner_index], state="42704")
                for other_index, other in enumerate(guards):
                    if other_index != owner_index:
                        wire.exists(other, mode)
                create(guards[owner_index], mode)

        for mode in MODES:
            wire.command(mode, "DROP DOMAIN " + wrong_case, state="42704")
            wire.exists(quoted_guard, mode)
            wire.command(mode, "DROP DOMAIN IF EXISTS " + wrong_case + " RESTRICT", "DROP DOMAIN", notices=True)
            wire.exists(quoted_guard, mode)
    finally:
        try:
            path("public")
            for target in public_targets + [guards[2]]:
                wire.command("simple", "DROP DOMAIN IF EXISTS " + target, "DROP DOMAIN", notices=True)
            for schema in schemas:
                wire.command("simple", "DROP SCHEMA IF EXISTS " + identifier(schema) + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Setup: schemas4 + path1 + guards3 + quoted guard1 + guard query1 = 10.
    # Targets: paths13 + 13*3*6 = 247. Malformed/retry: 33*10 = 330.
    # Namespace owner: 3 modes*3 owners*6 = 54. Wrong quoted case: 3*4 = 12.
    # Cleanup: path1 + public domains5 + schemas4 = 10. Total 663.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
