#!/usr/bin/env python3
"""Exercise configuration functions through real SQL and the wire protocol."""

from contextlib import closing
from pathlib import Path
import select
import socket
import struct
import subprocess
import tempfile

from compat import pg_diff_runner as runner


TEXT = 25
BOOLEAN = 16
SETTINGS = (
    ("lock_timeout", "0", "2s", "3s"),
    ("statement_timeout", "0", "5s", "6s"),
    ("deadlock_timeout", "1s", "7s", "8s"),
    ("work_mem", "4MB", "128kB", "256kB"),
)


class Wire:
    def __init__(self, client, sock, extended=False):
        self.client, self.sock, self.extended = client, sock, extended

    def packet(self, kind, body=b""):
        return self.client.typed(kind, body)

    def exchange(self, packet):
        self.sock.sendall(packet)
        return self.client.read_until_ready(self.sock)

    def parse(self, sql, name=b"", oids=()):
        body = name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids))
        body += b"".join(struct.pack("!I", oid) for oid in oids)
        return self.packet(b"P", body)

    def bind(self, name=b"", arguments=(), result_format=None):
        body = b"\0" + name + b"\0" + struct.pack("!HH", 0, len(arguments))
        for argument in arguments:
            body += struct.pack("!i", -1 if argument is None else len(argument))
            if argument is not None:
                body += argument
        formats = struct.pack("!H", 0) if result_format is None else struct.pack("!HH", 1, result_format)
        return self.packet(b"B", body + formats)

    def describe_portal(self):
        return self.packet(b"D", b"P\0")

    def execute(self):
        return self.packet(b"E", b"\0" + struct.pack("!I", 0))

    def send_statements(self, *statements):
        if not self.extended:
            return self.exchange(self.packet(b"Q", "; ".join(statements).encode() + b"\0"))
        messages = b"".join(self.parse(sql) + self.bind() + self.describe_portal() +
                            self.execute() for sql in statements)
        return self.exchange(messages + self.packet(b"S"))

    def check(self, label, messages, *, rows=(), tag="SELECT 1", state=None,
              ready=b"I", fields=None):
        actual_rows, error, message, headers, actual_tag, oids = runner.decode_wire_result(
            messages, include_types=True)
        errors = [payload for kind, payload in messages if kind == b"E"]
        observed = (actual_rows, error, actual_tag, messages[-1], len(errors))
        expected = ([list(row) for row in rows], state, tag, (b"Z", ready), int(state is not None))
        assert observed == expected, (label, observed, expected, message)
        assert not any(kind == b"N" for kind, _ in messages), (label, messages)
        if fields is not None:
            assert list(zip(headers, oids)) == fields, (label, headers, oids)

    def query(self, sql, **expected):
        self.check(sql, self.send_statements(sql), **expected)

    def command(self, sql, tag, ready=b"I"):
        self.query(sql, tag=tag, ready=ready)

    def value(self, expression, expected, ready=b"I"):
        self.query(f"SELECT {expression} AS v", rows=[(expected,)], ready=ready,
                   fields=[("v", TEXT)])

    def setting(self, name, value, ready=b"I"):
        self.value(f"pg_catalog.current_setting('{name}')", value, ready)

    def assign(self, name, value, local=False, ready=b"I"):
        self.value(f"pg_catalog.set_config('{name}', '{value}', {str(local).lower()})",
                   value, ready)

    def show(self, name, value, ready=b"I"):
        self.query(f"SHOW {name}", tag="SHOW", rows=[(value,)], ready=ready,
                   fields=[(name, TEXT)])

    def prepare(self, name, sql, oids, inferred):
        messages = self.exchange(self.parse(sql, name, oids) +
                                 self.packet(b"D", b"S" + name + b"\0") + self.packet(b"S"))
        wanted = struct.pack("!H", len(inferred))
        wanted += b"".join(struct.pack("!I", oid) for oid in inferred)
        assert [kind for kind, payload in messages] == [b"1", b"t", b"T", b"Z"], (sql, messages)
        assert messages[1] == (b"t", wanted) and messages[-1] == (b"Z", b"I"), (sql, messages)
        assert self.client.row_description_fields(messages) == [(b"v", 0, 0, TEXT, -1, -1, 0)], (
            sql, messages)

    def run_prepared(self, name, arguments, expected, *, state=None):
        messages = self.exchange(self.bind(name, arguments) + self.describe_portal() +
                                 self.execute() + self.packet(b"S"))
        if state is None:
            assert [kind for kind, _ in messages] == [b"2", b"T", b"D", b"C", b"Z"], (name, messages)
            self.check(name, messages, rows=[(expected,)], fields=[("v", TEXT)])
        else:
            self.check(name, messages, tag=None, state=state)


def function_arguments(wire):
    wire.query("SELECT current_setting('lock_timeout')", rows=[("0",)],
               fields=[("current_setting", TEXT)])
    wire.value("current_setting('LOCK_TIMEOUT', false)", "0")
    wire.value("current_setting('no_such_setting', true)", None)
    wire.value("current_setting('lock_timeout_ms', true)", None)
    wire.value("current_setting(NULL)", None)
    wire.value("current_setting('lock_timeout', NULL)", None)
    wire.value("current_setting('no_such_setting', NULL)", None)
    for expression, state in (
            ("current_setting('no_such_setting')", "42704"),
            ("current_setting('no_such_setting', false)", "42704"),
            ("public.current_setting('lock_timeout')", "42883"),
            ("pg_catalog.\"CURRENT_SETTING\"('lock_timeout')", "42883"),
            ("current_setting(42)", "42883"),
            ("set_config('lock_timeout', 1, false)", "42883"),
            ("set_config('lock_timeout', 'bad unit', false)", "22023"),
            ("set_config('deadlock_timeout', '0', false)", "22023"),
            ("set_config('work_mem', '63kB', false)", "22023"),
            ("set_config('no_such_setting', '1', false)", "42704"),
            ("set_config('lock_timeout_ms', '1', false)", "42704"),
            ("set_config(NULL, '1', false)", "22004")):
        wire.query(f"SELECT {expression} AS v", tag=None, state=state)

    # A NULL local flag acts like false.
    wire.value("set_config('lock_timeout', '1000ms', NULL)", "1s")
    wire.show("lock_timeout", "1s")
    wire.value("set_config('lock_timeout', NULL, false)", "0")
    wire.show("lock_timeout", "0")


def transaction_controls(wire, peer):
    for name, default, session, local in SETTINGS:
        wire.setting(name, default)
        wire.assign(name, local, local=True)
        wire.show(name, default)
        wire.command("BEGIN", "BEGIN", b"T")
        wire.assign(name, session, ready=b"T")
        wire.command("SAVEPOINT audit", "SAVEPOINT", b"T")
        wire.assign(name, local, local=True, ready=b"T")
        wire.show(name, local, b"T")
        peer.setting(name, default)
        wire.command("ROLLBACK TO audit", "ROLLBACK", b"T")
        wire.setting(name, session, b"T")
        wire.assign(name, local, local=True, ready=b"T")
        wire.command("COMMIT", "COMMIT")
        wire.setting(name, session)
        wire.command("BEGIN", "BEGIN", b"T")
        wire.value(f"set_config('{name}', NULL, false)", default, b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.setting(name, session)
        wire.command(f"RESET {name}", "RESET")
        wire.setting(name, default)

    setter = "SELECT pg_catalog.set_config('lock_timeout', '2s', true) AS v"
    getter = "SELECT pg_catalog.current_setting('lock_timeout') AS v"
    wire.check("temporary change in a query batch", wire.send_statements(setter, getter),
               rows=[("2s",), ("2s",)])
    wire.setting("lock_timeout", "0")

    # Cast failure occurs on the returned text, after the setter has run.
    fails = "SELECT CAST(set_config('lock_timeout', '7s', false) AS INTEGER) AS v"
    wire.query(fails, tag=None, state="22P02")
    wire.setting("lock_timeout", "0")
    persistent_change = "SELECT pg_catalog.set_config('lock_timeout', '7s', false) AS v"
    wire.check("failed query batch", wire.send_statements(persistent_change, "SELECT 1/0"),
               rows=[("7s",)], state="22012")
    wire.setting("lock_timeout", "0")
    wire.command("BEGIN", "BEGIN", b"T")
    wire.assign("lock_timeout", "3s", ready=b"T")
    wire.command("SAVEPOINT audit", "SAVEPOINT", b"T")
    wire.query(fails, tag=None, state="22P02", ready=b"E")
    wire.query(getter, tag=None, state="25P02", ready=b"E")
    wire.command("ROLLBACK TO audit", "ROLLBACK", b"T")
    wire.setting("lock_timeout", "3s", b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    wire.setting("lock_timeout", "0")

    # No row can execute the volatile function in this statement.
    wire.query(setter + " WHERE FALSE", tag="SELECT 0", fields=[("v", TEXT)])
    wire.setting("lock_timeout", "0")


def prepared_controls(wire, peer):
    lookup = "SELECT pg_catalog.current_setting($1, $2) AS v"
    setter = "SELECT pg_catalog.set_config($1, $2, $3) AS v"
    wire.prepare(b"settings_get", lookup, (TEXT, BOOLEAN), (TEXT, BOOLEAN))
    wire.prepare(b"settings_set", setter, (), (TEXT, TEXT, BOOLEAN))
    messages = wire.exchange(wire.bind(b"settings_set", (b"lock_timeout", b"2s", b"false")) +
                             wire.describe_portal() + wire.packet(b"S"))
    assert [kind for kind, _ in messages] == [b"2", b"T", b"Z"], messages
    assert wire.client.row_description_fields(messages) == [(b"v", 0, 0, TEXT, -1, -1, 0)], messages
    assert messages[-1] == (b"Z", b"I"), messages
    wire.run_prepared(b"settings_get", (b"lock_timeout", b"false"), "0")
    wire.run_prepared(b"settings_set", (b"lock_timeout", b"2s", b"false"), "2s")
    peer.setting("lock_timeout", "0")
    wire.run_prepared(b"settings_get", (b"lock_timeout", b"false"), "2s")
    wire.run_prepared(b"settings_set", (b"lock_timeout", None, b"false"), "0")
    wire.run_prepared(b"settings_get", (b"lock_timeout", None), None)
    wire.run_prepared(b"settings_get", (b"no_such_setting", b"true"), None)
    wire.run_prepared(b"settings_get", (b"no_such_setting", b"false"), None, state="42704")

    # Resolve the requested setting only when the expression executes.
    wire.prepare(b"settings_unknown", "SELECT pg_catalog.current_setting('no_such_setting') AS v", (), ())
    wire.run_prepared(b"settings_unknown", (), None, state="42704")

    wire.prepare(b"settings_binary", "SELECT pg_catalog.current_setting('lock_timeout') AS v", (), ())
    messages = wire.exchange(wire.bind(b"settings_binary", result_format=1) +
                             wire.describe_portal() + wire.execute() + wire.packet(b"S"))
    assert [kind for kind, _ in messages] == [b"2", b"T", b"D", b"C", b"Z"], messages
    assert wire.client.row_description_fields(messages) == [(b"v", 0, 0, TEXT, -1, -1, 1)], messages
    wire.check("binary getter result", messages, rows=[("0",)], fields=[("v", TEXT)])

    # The bound NULL is available as a constant for this execution.
    wire.prepare(b"settings_null_name", "SELECT current_setting($1, "
                 "set_config('lock_timeout', '840ms', false) IS NOT NULL) AS v", (TEXT,), (TEXT,))
    wire.prepare(b"settings_null_flag", "SELECT current_setting("
                 "set_config('lock_timeout', '850ms', false), $1) AS v", (BOOLEAN,), (BOOLEAN,))
    wire.run_prepared(b"settings_null_name", (None,), None)
    wire.show("lock_timeout", "0")
    wire.run_prepared(b"settings_null_flag", (None,), None)
    wire.show("lock_timeout", "0")


def qualified_snapshot_controls(wire):
    begin = "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"
    isolation = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    wire.command(begin, "BEGIN", b"T")
    wire.assign("lock_timeout", "37ms", ready=b"T")
    wire.setting("lock_timeout", "37ms", b"T")
    wire.query(isolation, tag=None, state="25001", ready=b"E")
    wire.command("ROLLBACK", "ROLLBACK")
    wire.show("lock_timeout", "0")

    for sql, state in (
            ("SELECT pg_catalog.set_config('lock_timeout', 'invalid unit', false) AS v", "22023"),
            ("SELECT pg_catalog.set_config('lock_timeout', '73ms', false) AS ;", "42601")):
        wire.command("BEGIN", "BEGIN", b"T")
        wire.assign("lock_timeout", "37ms", ready=b"T")
        wire.query(sql, tag=None, state=state, ready=b"E")
        wire.query("SHOW lock_timeout", tag=None, state="25P02", ready=b"E")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.show("lock_timeout", "0")

    if not wire.extended:
        return
    # Parse and Bind each establish a query snapshot without running a setter.
    sql = "SELECT pg_catalog.set_config('lock_timeout', '37ms', false) AS v"
    wire.command(begin, "BEGIN", b"T")
    reply = wire.exchange(wire.parse(sql, b"snapshot_set_parse") + wire.packet(b"S"))
    assert reply == [(b"1", b""), (b"Z", b"T")], reply
    wire.show("lock_timeout", "0", b"T")
    wire.query(isolation, tag=None, state="25001", ready=b"E")
    wire.command("ROLLBACK", "ROLLBACK")

    wire.prepare(b"snapshot_set_bind", sql, (), ())
    wire.command(begin, "BEGIN", b"T")
    reply = wire.exchange(wire.bind(b"snapshot_set_bind") + wire.packet(b"S"))
    assert reply == [(b"2", b""), (b"Z", b"T")], reply
    wire.show("lock_timeout", "0", b"T")
    wire.query(isolation, tag=None, state="25001", ready=b"E")
    wire.command("ROLLBACK", "ROLLBACK")


def setting_dispatch_controls(wire, sequence):
    wire.query("SELECT \"pg_catalog\".\"current_setting\"('lock_timeout')",
               rows=[("0",)], fields=[("current_setting", TEXT)])
    wire.query("SELECT pg_catalog.set_config('lock_timeout', \",\", false)",
               tag=None, state="42703")

    # A nested value must execute once, and a zero-row query must skip it.
    wire.command(f"CREATE SEQUENCE {sequence}", "CREATE SEQUENCE")
    setter = ("SELECT pg_catalog.set_config('lock_timeout', "
              f"CAST(nextval('{sequence}') AS TEXT) || 'ms', false) AS v")
    wire.query(setter + " LIMIT 0", tag="SELECT 0", fields=[("v", TEXT)])
    wire.show("lock_timeout", "0")
    wire.query(setter, rows=[("1ms",)], fields=[("v", TEXT)])
    wire.show("lock_timeout", "1ms")
    wire.query(f"SELECT currval('{sequence}') AS v", rows=[("1",)], fields=[("v", 20)])
    wire.command("RESET lock_timeout", "RESET")

    # The exact INTEGER overload wins even with pg_catalog implicitly first.
    wire.command("SET search_path = setting_function_shadow, public", "SET")
    wire.value("current_setting('lock_timeout')", "0")
    wire.value("current_setting(42)", "shadow-int")
    wire.command("SET search_path = setting_function_shadow, pg_catalog, public", "SET")
    wire.value("current_setting('lock_timeout')", "shadow-text")
    wire.value("set_config('lock_timeout', '37ms', false)", "shadow-set")
    wire.show("lock_timeout", "0")
    wire.assign("lock_timeout", "37ms")
    wire.setting("lock_timeout", "37ms")
    wire.command("RESET lock_timeout", "RESET")
    wire.command("SET search_path = public", "SET")


def setting_projection_syntax_controls(wire, original_timeout):
    setter = "SELECT pg_catalog.set_config("
    arguments = "'lock_timeout', '73ms', false"
    for tail in (arguments + ") 3", arguments + ") AS 3", arguments + ") AS 'q'",
                 arguments + ") AS", arguments + ") AS ;", arguments + ") AS /* missing alias */ ;",
                 arguments + ",)", arguments + ", /* unused slot */ )",
                 ", " + arguments + ")", "'lock_timeout', , '73ms', false)"):
        sql = setter + tail
        reply = wire.send_statements(sql)
        assert [kind for kind, _ in reply] == [b"E", b"Z"], (sql, reply)
        wire.check(sql, reply, tag=None, state="42601")
        wire.setting("lock_timeout", original_timeout)

    getter = "SELECT pg_catalog.current_setting('lock_timeout')"
    for alias in (' AS "as";', ' "as";', ' AS AS;'):
        wire.query(getter + alias, rows=[(original_timeout,)], fields=[("as", TEXT)])

    if not wire.extended:
        # A malformed later statement prevents the first setter from running.
        for tail in (",)", ") AS ;"):
            reply = wire.send_statements(setter + arguments + ")", setter + arguments + tail)
            assert [kind for kind, _ in reply] == [b"E", b"Z"], reply
            wire.check("malformed setting batch", reply, tag=None, state="42601")
            wire.setting("lock_timeout", original_timeout)


def strict_arguments(wire):
    wire.value("current_setting(NULL, set_config('lock_timeout', '840ms', false) IS NOT NULL)", None)
    wire.show("lock_timeout", "0")
    wire.value("current_setting(set_config('lock_timeout', '850ms', false), NULL)", None)
    wire.show("lock_timeout", "0")

    # A value read from a row is only known at execution, so the other
    # operand still runs before the strict function returns NULL.
    wire.query("SELECT current_setting(setting_name, "
               "set_config('lock_timeout', '840ms', false) IS NOT NULL) AS v "
               "FROM setting_function_null_input", rows=[(None,)], fields=[("v", TEXT)])
    wire.show("lock_timeout", "840ms")
    wire.query("SELECT current_setting(set_config('lock_timeout', '850ms', false), missing_ok) AS v "
               "FROM setting_function_null_input", rows=[(None,)], fields=[("v", TEXT)])
    wire.show("lock_timeout", "850ms")
    wire.command("RESET lock_timeout", "RESET")


def startup_controls(wire):
    for name, expected in (("lock_timeout", "3s"), ("statement_timeout", "4s"),
                           ("deadlock_timeout", "5s"), ("work_mem", "8MB")):
        wire.setting(name, expected)
        wire.assign(name, "64kB" if name == "work_mem" else "2s")
        wire.value(f"set_config('{name}', NULL, false)", expected)
        wire.show(name, expected)


def role_controls(wire):
    wire.command("SET ROLE setting_function_reader", "SET")
    wire.setting("deadlock_timeout", "1s")
    for operand in ("'9s'", "NULL"):
        wire.query(f"SELECT pg_catalog.set_config('deadlock_timeout', {operand}, false) AS v",
                   tag=None, state="42501")
    wire.assign("lock_timeout", "3s")
    wire.command("RESET lock_timeout", "RESET")
    wire.command("RESET ROLE", "RESET")


def table_controls(wire, peer):
    wire.command("BEGIN", "BEGIN", b"T")
    wire.query("SELECT pg_catalog.set_config(setting_name, setting_value, true) AS v "
               "FROM setting_function_input WHERE id=2", ready=b"T", rows=[("3s",)],
               fields=[("v", TEXT)])
    wire.query("SELECT pg_catalog.current_setting(setting_name) AS v "
               "FROM setting_function_input WHERE id=2", ready=b"T", rows=[("3s",)],
               fields=[("v", TEXT)])
    wire.command("ROLLBACK", "ROLLBACK")
    wire.setting("lock_timeout", "0")

    for name, state in (("lock_timeout", "55P03"), ("statement_timeout", "57014")):
        peer.command("BEGIN", "BEGIN", b"T")
        peer.command("LOCK TABLE setting_function_input IN ACCESS EXCLUSIVE MODE", "LOCK TABLE", b"T")
        wire.command("BEGIN", "BEGIN", b"T")
        wire.assign(name, "120ms", local=True, ready=b"T")
        wire.query("LOCK TABLE setting_function_input IN ACCESS SHARE MODE", tag=None,
                   state=state, ready=b"E")
        wire.command("ROLLBACK", "ROLLBACK")
        peer.command("COMMIT", "COMMIT")
        wire.setting(name, "0")


def exercise(client, primary, peer, startup):
    setup = Wire(client, primary)
    observer = Wire(client, peer)
    setup.command("CREATE ROLE setting_function_reader", "CREATE ROLE")
    setup.command("CREATE TABLE setting_function_input (id INTEGER PRIMARY KEY, "
                  "setting_name TEXT, setting_value TEXT)", "CREATE TABLE")
    setup.command("INSERT INTO setting_function_input VALUES "
                  "(1, 'lock_timeout', '2s'), (2, 'lock_timeout', '3s')", "INSERT 0 2")
    setup.command("CREATE TABLE setting_function_null_input (setting_name TEXT, missing_ok BOOLEAN)",
                  "CREATE TABLE")
    setup.command("INSERT INTO setting_function_null_input VALUES (NULL, NULL)", "INSERT 0 1")
    setup.command("CREATE SCHEMA setting_function_shadow", "CREATE SCHEMA")
    for argument, value in (("p TEXT", "shadow-text"), ("p INTEGER", "shadow-int")):
        setup.command("CREATE FUNCTION setting_function_shadow.current_setting(" + argument +
                      ") RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS "
                      f"$$ SELECT '{value}'::TEXT $$", "CREATE FUNCTION")
    setup.command("CREATE FUNCTION setting_function_shadow.set_config(p TEXT, v TEXT, l BOOLEAN) "
                  "RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$ SELECT 'shadow-set'::TEXT $$",
                  "CREATE FUNCTION")
    for extended in (False, True):
        wire = Wire(client, primary, extended)
        function_arguments(wire)
        strict_arguments(wire)
        transaction_controls(wire, observer)
        qualified_snapshot_controls(wire)
        setting_projection_syntax_controls(wire, "0")
        setting_dispatch_controls(wire, "setting_function_seq_ext" if extended else "setting_function_seq_simple")
        role_controls(wire)
        table_controls(wire, observer)
    prepared_controls(Wire(client, primary, True), observer)
    startup_controls(Wire(client, startup, True))


def legacy_trigger_timeout_isolation(wire, blocker):
    wire.command("CREATE TABLE setting_function_trigger_source (id INTEGER)", "CREATE TABLE")
    wire.command("CREATE TABLE setting_function_trigger_hold (id INTEGER)", "CREATE TABLE")
    wire.command("CREATE TRIGGER change_lock_timeout AFTER INSERT ON setting_function_trigger_source "
                 "FOR EACH ROW SELECT set_config('lock_timeout', '100ms', false)", "CREATE TRIGGER")
    wire.command("INSERT INTO setting_function_trigger_source VALUES (1)", "INSERT 0 1")
    wire.show("lock_timeout", "0")

    blocker.command("BEGIN", "BEGIN", b"T")
    blocker.command("LOCK TABLE setting_function_trigger_hold IN ACCESS EXCLUSIVE MODE",
                    "LOCK TABLE", b"T")
    wire.command("BEGIN", "BEGIN", b"T")
    request = "LOCK TABLE setting_function_trigger_hold IN ACCESS SHARE MODE"
    wire.sock.sendall(wire.packet(b"Q", request.encode() + b"\0"))
    # Keep the exclusive lock past the timeout assigned inside the trigger.
    if select.select([wire.sock], [], [], 0.5)[0]:
        early = wire.client.read_until_ready(wire.sock)
        raise AssertionError(("outer lock request completed before the blocker released it", early))
    blocker.command("COMMIT", "COMMIT")
    wire.check(request, wire.client.read_until_ready(wire.sock), tag="LOCK TABLE", ready=b"T")
    wire.command("COMMIT", "COMMIT")
    wire.query("SELECT id FROM setting_function_trigger_source", rows=[("1",)],
               fields=[("id", 23)])


def database_owner_controls(wire, blocker):
    blocker.command("CREATE TABLE setting_function_owner (id INTEGER PRIMARY KEY)", "CREATE TABLE")
    blocker.command("INSERT INTO setting_function_owner VALUES (1)", "INSERT 0 1")
    wire.command("SET lock_timeout = '70ms'", "SET")
    extended = Wire(wire.client, wire.sock, extended=True)
    setter = b"owner_setting_setter"
    extended.prepare(setter, "SELECT pg_catalog.set_config($1, $2, $3) AS v",
                     (), (TEXT, TEXT, BOOLEAN))
    blocker.command("BEGIN", "BEGIN", b"T")
    blocker.command("ALTER TABLE setting_function_owner ADD COLUMN payload INTEGER",
                    "ALTER TABLE", b"T")
    try:
        # The catalog owner blocks relation access, while these builtin calls
        # read or update only the caller's session.
        wire.setting("lock_timeout", "70ms")
        wire.assign("work_mem", "128kB", local=True)
        wire.show("work_mem", "4MB")
        setting_projection_syntax_controls(wire, "70ms")
        setting_projection_syntax_controls(extended, "70ms")
        read_table = "SELECT id FROM setting_function_owner"
        wire.query(read_table, tag=None, state="55P03")

        getter = b"owner_setting_getter"
        extended.prepare(getter, "SELECT pg_catalog.current_setting($1, $2) AS v",
                         (), (TEXT, BOOLEAN))
        extended.run_prepared(getter, (b"lock_timeout", b"false"), "70ms")
        extended.run_prepared(setter, (b"work_mem", b"128kB", b"true"), "128kB")
        wire.show("work_mem", "4MB")

        # A real table access must acquire ownership in the same request
        # group. Its timeout rolls back the successful session assignment.
        reply = extended.exchange(extended.bind(setter, (b"work_mem", b"256kB", b"false")) +
                                  extended.describe_portal() + extended.execute() +
                                  extended.parse(read_table) + extended.packet(b"S"))
        extended.check("failed ownership promotion", reply, rows=[("256kB",)], state="55P03")
        wire.show("work_mem", "4MB")
    finally:
        blocker.command("ROLLBACK", "ROLLBACK")
    wire.query("SELECT id FROM setting_function_owner", rows=[("1",)])
    wire.command("RESET lock_timeout", "RESET")


STARTUP_OPTIONS = {"options": "-c lock_timeout=3s -c statement_timeout=4s "
                              "-c deadlock_timeout=5s -c work_mem=8MB"}


def cli_controls(client):
    binary = Path(runner.DBMS_MAIN).resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="dbms-setting-functions-cli-") as work:
        info = Path(work, "info")
        info.mkdir()
        (info / "tlist.lst").touch()
        client.write_auth_catalog(work, "alice", "secret")
        commands = (
            "alice\nsecret\n"
            "SELECT pg_catalog.set_config('lock_timeout', '37ms', true) AS local_value\n"
            "SELECT pg_catalog.current_setting('lock_timeout') AS after_statement\n"
            "BEGIN\n"
            "SELECT pg_catalog.set_config('lock_timeout', '41ms', false) AS transaction_value\n"
            "ROLLBACK\n"
            "SELECT pg_catalog.current_setting('lock_timeout') AS after_rollback\n"
            "SELECT pg_catalog.current_setting('lock_timeout') AS \"Quoted Alias\"\n"
            "SELECT pg_catalog.current_setting('no_such_setting', true) AS \"NULL Column\"\n"
            "SELECT pg_catalog.current_setting('lock_timeout')\n"
            "exit\n"
        )
        result = subprocess.run([str(binary), "--data-dir", work], input=commands,
                                text=True, capture_output=True, cwd=work, timeout=15)
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
        for label, value in (("local_value", "37ms"), ("after_statement", "0"),
                             ("transaction_value", "41ms"), ("after_rollback", "0"),
                             ("Quoted Alias", "0"), ("NULL Column", "NULL"),
                             ("current_setting", "0")):
            assert f"{label} \n{value} \n" in result.stdout, (label, result.stdout)


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        endpoint = ("127.0.0.1", server["port"])
        with closing(socket.create_connection(endpoint, runner.wire_timeout())) as peer, \
             closing(socket.create_connection(endpoint, runner.wire_timeout())) as startup:
            client.startup(peer, "alice", "info")
            client.startup(startup, "alice", "info", startup_parameters=STARTUP_OPTIONS)
            exercise(client, server["sock"], peer, startup)
            legacy_trigger_timeout_isolation(Wire(client, server["sock"]), Wire(client, peer))
            database_owner_controls(Wire(client, server["sock"]), Wire(client, peer))
    finally:
        runner.stop_ours(server)
    cli_controls(client)
    print("[SESSION SETTING FUNCTIONS PROTOCOL E2E] function, transaction and protocol checks passed")


if __name__ == "__main__":
    main()
