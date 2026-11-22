#!/usr/bin/env python3
"""Session timeout changes follow transaction boundaries and affect real waits."""

from contextlib import ExitStack
import select
import socket
import struct
import time

from compat import pg_diff_runner as runner


class Wire:
    def __init__(self, client, sock, extended=False):
        self.client = client
        self.sock = sock
        self.extended = extended

    def send(self, *statements):
        typed = self.client.typed
        if not self.extended:
            self.sock.sendall(typed(b"Q", "; ".join(statements).encode() + b"\0"))
            return
        packet = bytearray()
        for sql in statements:
            packet += typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
            packet += typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
            packet += typed(b"D", b"P\0")
            packet += typed(b"E", b"\0" + struct.pack("!I", 0))
        self.sock.sendall(packet + typed(b"S"))

    def receive(self, sql, tag, ready, state, rows, warnings, columns, oids,
                message_has=""):
        messages = self.client.read_until_ready(self.sock)
        actual_rows, error, message, actual_columns, actual_tag, actual_oids = (
            runner.decode_wire_result(messages, include_types=True))
        notices = [self.client.diagnostic_fields(body).get(b"C", b"").decode()
                   for kind, body in messages if kind == b"N"]
        errors = [kind for kind, _ in messages if kind == b"E"]
        observed = (actual_tag, messages[-1], error, actual_rows, notices)
        expected = (tag, (b"Z", ready), state, rows, list(warnings))
        label = ("extended" if self.extended else "simple", sql)
        assert observed == expected, (label, observed, expected, message)
        assert len(errors) == (state is not None), (label, messages)
        assert message_has in message.lower(), (label, message_has, message)
        if columns is not None:
            assert (actual_columns, actual_oids) == (columns, oids), (label, messages)
        return messages

    def query(self, sql, tag="SET", ready=b"I", state=None, rows=None,
              warnings=(), columns=None, oids=None, message_has=""):
        self.send(sql)
        return self.receive(sql, tag, ready, state, rows or [], warnings, columns,
                            oids, message_has)

    def batch(self, statements, tag, state=None, rows=None, warnings=()):
        self.send(*statements)
        return self.receive(statements, tag, b"I", state, rows or [], warnings,
                            None, None)

    def show(self, name, value, ready=b"I", quoted=False):
        identifier = f'"{name.upper()}"' if quoted else name
        self.query(f"SHOW {identifier}", "SHOW", ready, rows=[[value]],
                   columns=[name], oids=[25])


def lock(wire, mode, state=None, nowait=False):
    sql = f"LOCK TABLE timeout_settings_wait IN {mode} MODE"
    if nowait:
        sql += " NOWAIT"
    wire.query(sql, "LOCK TABLE" if state is None else None,
               ready=b"T" if state is None else b"E", state=state)


def wait_until_release(waiter, blocker):
    # The conflicting lock is already held. Leave it held past either short
    # timeout to distinguish a leaked timer from ordinary waiting. The socket
    # read deadline bounds a hang after the blocker releases.
    sql = "LOCK TABLE timeout_settings_wait IN ACCESS SHARE MODE"
    waiter.send(sql)
    deadline = time.monotonic() + 0.65
    while select.select([waiter.sock], [], [], max(0, deadline - time.monotonic()))[0]:
        # An Extended Execute may send its preliminary replies while waiting.
        message = waiter.client.read_message(waiter.sock)
        assert waiter.extended and message[0] in (b"1", b"2", b"n"), (sql, message)
    blocker.query("COMMIT", "COMMIT")
    waiter.receive(sql, "LOCK TABLE", b"T", None, [], (), None, None)


def wait_controls(waiter, blocker, observer):
    waiter.query("SET lock_timeout=0")
    waiter.query("BEGIN", "BEGIN", b"T")
    waiter.query("SET lock_timeout=200", ready=b"T")
    waiter.query("ROLLBACK", "ROLLBACK")
    blocker.query("BEGIN", "BEGIN", b"T")
    lock(blocker, "ACCESS EXCLUSIVE")
    waiter.query("BEGIN", "BEGIN", b"T")
    wait_until_release(waiter, blocker)
    waiter.query("ROLLBACK", "ROLLBACK")

    # SET LOCAL changes the actual lock wait. Restore the savepoint's original
    # lock_timeout and verify the table remains protected against a third owner.
    waiter.query("BEGIN", "BEGIN", b"T")
    lock(waiter, "ACCESS SHARE")
    waiter.query("SAVEPOINT short_wait", "SAVEPOINT", b"T")
    waiter.query("SET LOCAL lock_timeout='100ms'", ready=b"T")
    blocker.query("BEGIN", "BEGIN", b"T")
    lock(blocker, "ACCESS SHARE")
    lock(waiter, "ACCESS EXCLUSIVE", "55P03")
    waiter.query("ROLLBACK TO short_wait", "ROLLBACK", b"T")
    waiter.show("lock_timeout", "0", b"T")
    blocker.query("COMMIT", "COMMIT")
    observer.query("BEGIN", "BEGIN", b"T")
    lock(observer, "ACCESS EXCLUSIVE", "55P03", nowait=True)
    observer.query("ROLLBACK", "ROLLBACK")
    waiter.query("COMMIT", "COMMIT")
    observer.query("BEGIN", "BEGIN", b"T")
    lock(observer, "ACCESS EXCLUSIVE", nowait=True)
    observer.query("COMMIT", "COMMIT")

    # The statement timer, with lock_timeout disabled, is a separate consumer.
    blocker.query("BEGIN", "BEGIN", b"T")
    lock(blocker, "ACCESS EXCLUSIVE")
    waiter.query("BEGIN", "BEGIN", b"T")
    waiter.query("SET LOCAL statement_timeout='300ms'", ready=b"T")
    sql = "LOCK TABLE timeout_settings_wait IN ACCESS SHARE MODE"
    waiter.query(sql, None, b"E", "57014", message_has="statement timeout")
    waiter.query("ROLLBACK", "ROLLBACK")
    waiter.show("statement_timeout", "0")
    waiter.query("BEGIN", "BEGIN", b"T")
    wait_until_release(waiter, blocker)
    waiter.query("ROLLBACK", "ROLLBACK")


def transaction_controls(wire, name, default):
    wire.show(name, default)
    wire.query(f"SET SESSION {name} TO '2s'")
    wire.show(name, "2s", quoted=True)
    wire.query(f"SET LOCAL {name}='3s'", warnings=("25P01",))
    wire.show(name, "2s")

    wire.query("BEGIN", "BEGIN", b"T")
    wire.query(f"SET {name}='3s'", ready=b"T")
    wire.query(f"SET LOCAL {name}='4s'", ready=b"T")
    wire.show(name, "4s", b"T")
    wire.query("COMMIT AND CHAIN", "COMMIT", b"T")
    wire.show(name, "3s", b"T")
    wire.query(f"SET LOCAL {name}='4s'", ready=b"T")
    wire.query(f"SET SESSION {name} FROM CURRENT", ready=b"T")
    wire.query("COMMIT", "COMMIT")
    wire.show(name, "4s")

    wire.query("BEGIN", "BEGIN", b"T")
    wire.query(f"SET LOCAL {name}='6s'", ready=b"T")
    wire.query("SAVEPOINT timeout_change", "SAVEPOINT", b"T")
    wire.query(f"SET {name}='7s'", ready=b"T")
    wire.query("SAVEPOINT timeout_change", "SAVEPOINT", b"T")
    wire.query(f"SET LOCAL {name}='8s'", ready=b"T")
    wire.query("SELECT 1/0", None, b"E", "22012")
    wire.query(f"SHOW {name}", None, b"E", "25P02")
    wire.query("ROLLBACK TO timeout_change", "ROLLBACK", b"T")
    wire.show(name, "7s", b"T")
    wire.query("RELEASE timeout_change", "RELEASE", b"T")
    wire.query("ROLLBACK TO timeout_change", "ROLLBACK", b"T")
    wire.show(name, "6s", b"T")
    wire.query("ROLLBACK AND CHAIN", "ROLLBACK", b"T")
    wire.show(name, "4s", b"T")
    wire.query(f"SET {name}='2s'", ready=b"T")
    wire.query("SELECT 1/0", None, b"E", "22012")
    wire.query("COMMIT", "ROLLBACK")
    wire.show(name, "4s")

    # A failed implicit block restores changes from an earlier statement in
    # the same Simple Query message or preceding Execute in the Sync group.
    wire.batch((f"SET {name}='6s'", "SELECT 1/0"), "SET", "22012")
    wire.show(name, "4s")
    wire.batch((f"SET {name}='5s'", f"SHOW {name}"), "SHOW", rows=[["5s"]])
    wire.show(name, "5s")
    wire.query("BEGIN", "BEGIN", b"T")
    wire.query(f"SET {name}='6s'", ready=b"T")
    wire.query(f"SET LOCAL {name}='7s'", ready=b"T")
    wire.query("PREPARE TRANSACTION 'prepare_timeout_change'", "PREPARE TRANSACTION")
    wire.show(name, "6s")
    wire.query("ROLLBACK PREPARED 'prepare_timeout_change'", "ROLLBACK PREPARED")
    wire.show(name, "6s")

    wire.query("BEGIN", "BEGIN", b"T")
    wire.query(f'RESET "{name.upper()}"', "RESET", b"T")
    wire.show(name, default, b"T")
    wire.query("ROLLBACK", "ROLLBACK")
    wire.show(name, "6s")
    for value in ("'-1'", "'bogus'", "'3MB'", "2147483648", "1,2"):
        wire.query(f"SET {name}={value}", None, state="22023")
    if name == "deadlock_timeout":
        wire.query("SET deadlock_timeout=0", None, state="22023")
    wire.show(name, "6s")
    wire.query(f"SET {name}='0.125 s'")
    wire.show(name, "125ms")
    wire.query(f"SET {name} TO DEFAULT")
    wire.show(name, default)


def startup_controls(wire, peer):
    for name, initial, peer_value in (("lock_timeout", "2s", "0"),
                                      ("statement_timeout", "3s", "0"),
                                      ("deadlock_timeout", "4s", "1s")):
        wire.show(name, initial)
        wire.query(f"SET {name}='8s'")
        peer.show(name, peer_value)
        wire.query(f"RESET {name}", "RESET")
        wire.show(name, initial)
        wire.query(f"SET {name}='8s'")
        wire.query("BEGIN", "BEGIN", b"T")
        wire.query("RESET ALL", "RESET", b"T")
        wire.show(name, initial, b"T")
        wire.query("ROLLBACK", "ROLLBACK")
        wire.show(name, "8s")
        wire.query("DISCARD ALL", "DISCARD ALL")
        wire.show(name, initial)
    wire.query("SELECT setting, unit FROM pg_catalog.pg_settings WHERE name='lock_timeout'",
               "SELECT 1", rows=[["2000", "ms"]], columns=["setting", "unit"],
               oids=[25, 25])


def reload_controls(primary, startup, blocker, observer):
    settings = (("lock_timeout", 200, "200ms", "2s"),
                ("statement_timeout", 6000, "6s", "3s"),
                ("deadlock_timeout", 7000, "7s", "4s"))
    blocker.query("BEGIN", "BEGIN", b"T")
    blocker.query("SAVEPOINT before_reload", "SAVEPOINT", b"T")
    for name, new_ms, _, _ in settings:
        primary.query(f"SET {name}=8000")
        startup.query(f"SET {name}=9000")
        blocker.query(f"SET LOCAL {name}=10000", ready=b"T")
        observer.query(f"ALTER SYSTEM SET {name}={new_ms}", "ALTER SYSTEM")
    observer.show("lock_timeout", "0")
    observer.query("SELECT pg_reload_conf()", "SELECT 1", rows=[["t"]])

    # PostgreSQL processes reload asynchronously. Poll an existing idle
    # connection with a bounded deadline before asserting all three defaults.
    deadline = time.monotonic() + 5
    while True:
        messages = observer.client.simple_query(observer.sock, "SHOW lock_timeout")
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        assert state is None and tag == "SHOW" and messages[-1] == (b"Z", b"I"), (
            state, message, messages)
        if rows == [["200ms"]]:
            break
        assert rows == [["0"]] and time.monotonic() < deadline, rows
        time.sleep(0.025)

    for name, _, updated, startup_default in settings:
        observer.show(name, updated)
        primary.show(name, "8s")
        startup.show(name, "9s")
        blocker.show(name, "10s", b"T")
        primary.query(f"RESET {name}", "RESET")
        startup.query(f"RESET {name}", "RESET")
        primary.show(name, updated)
        startup.show(name, startup_default)

    blocker.query("ROLLBACK TO before_reload", "ROLLBACK", b"T")
    for name, _, updated, _ in settings:
        blocker.show(name, updated, b"T")
        blocker.query(f"SET {name}=8000", ready=b"T")
    blocker.query("ROLLBACK", "ROLLBACK")
    for name, _, updated, _ in settings:
        blocker.show(name, updated)

    # The new default must also reach the lock manager of an existing socket.
    blocker.query("BEGIN", "BEGIN", b"T")
    lock(blocker, "ACCESS EXCLUSIVE")
    observer.query("BEGIN", "BEGIN", b"T")
    lock(observer, "ACCESS SHARE", "55P03")
    observer.query("ROLLBACK", "ROLLBACK")
    blocker.query("COMMIT", "COMMIT")


def extended_utility_context(wire):
    set_local = "SET LOCAL lock_timeout='2s'"
    show = "SHOW lock_timeout"
    wire.batch((set_local, show), "SHOW", rows=[["2s"]], warnings=("25P01",))
    wire.show("lock_timeout", "0")
    wire.batch(("SELECT 1", set_local, show), "SHOW", rows=[["1"], ["2s"]])
    wire.show("lock_timeout", "0")

    # Parse and Bind can start engine work before any SQL statement executes.
    # An unused portal alone does not make the following SET LOCAL silent.
    typed = wire.client.typed
    wire.sock.sendall(typed(b"P", b"deferred\0SELECT 1\0\0\0") +
                      typed(b"B", b"deferred\0deferred\0" + struct.pack("!HHH", 0, 0, 0)))
    wire.batch((set_local, show), "SHOW", rows=[["2s"]], warnings=("25P01",))
    wire.show("lock_timeout", "0")


def role_controls(wire):
    wire.query("SET deadlock_timeout='7s'")
    wire.query("SET ROLE timeout_settings_reader")
    wire.show("deadlock_timeout", "7s")
    wire.query("SET deadlock_timeout='8s'", None, state="42501")
    wire.query("RESET deadlock_timeout", None, state="42501")
    wire.query("SET lock_timeout='3s'")
    wire.query("SET statement_timeout='4s'")
    wire.query("RESET ALL", "RESET")
    wire.show("deadlock_timeout", "1s")
    wire.show("lock_timeout", "0")
    wire.show("statement_timeout", "0")
    wire.query("RESET ROLE", "RESET")
    wire.query("RESET deadlock_timeout", "RESET")
    wire.show("deadlock_timeout", "1s")


def deadlock_wait_control(first, second):
    # Both waiters' default 1 s deadlock check should run before the 2.5 s
    # lock timeout. A leaked 5 s value on both would instead cause 55P03.
    for wire in (first, second):
        wire.query("BEGIN", "BEGIN", b"T")
        wire.query("SET deadlock_timeout='5s'", ready=b"T")
        wire.query("ROLLBACK", "ROLLBACK")
        wire.query("SET lock_timeout='2500ms'")
        wire.query("BEGIN", "BEGIN", b"T")
        lock(wire, "ACCESS SHARE")
    sql = "LOCK TABLE timeout_settings_wait IN ACCESS EXCLUSIVE MODE"
    first.send(sql)
    second.send(sql)
    ready = select.select([first.sock, second.sock], [], [], runner.wire_timeout())[0]
    assert ready, "neither table lock upgrade completed"
    earlier, later = (first, second) if ready[0] is first.sock else (second, first)
    messages = earlier.client.read_until_ready(earlier.sock)
    _, state, message, _, tag = runner.decode_wire_result(messages)
    if state is None:
        assert tag == "LOCK TABLE" and messages[-1] == (b"Z", b"T"), messages
        later.receive(sql, None, b"E", "40P01", [], (), None, None)
        earlier.query("ROLLBACK", "ROLLBACK")
    else:
        assert state == "40P01" and tag is None and messages[-1] == (b"Z", b"E"), (
            state, message, messages)
        earlier.query("ROLLBACK", "ROLLBACK")
        later.receive(sql, "LOCK TABLE", b"T", None, [], (), None, None)
    later.query("ROLLBACK", "ROLLBACK")
    for wire in (first, second):
        wire.query("RESET lock_timeout", "RESET")


def session_controls(primary, blocker, observer):
    primary.query("CREATE TABLE timeout_settings_wait(id INTEGER)", "CREATE TABLE")
    primary.query("CREATE ROLE timeout_settings_reader", "CREATE ROLE")
    for extended in (False, True):
        wire = Wire(primary.client, primary.sock, extended)
        wait_controls(wire, blocker, observer)
        for name, default in (("lock_timeout", "0"), ("statement_timeout", "0"),
                              ("deadlock_timeout", "1s")):
            transaction_controls(wire, name, default)
        role_controls(wire)
        if extended:
            extended_utility_context(wire)
    deadlock_wait_control(primary, blocker)


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        with ExitStack() as stack:
            def connect(options=None):
                sock = stack.enter_context(socket.create_connection(
                    ("127.0.0.1", server["port"]), timeout=runner.wire_timeout()))
                client.startup(sock, "alice", "info", startup_parameters=options)
                return sock

            primary = Wire(client, server["sock"])
            blocker, observer = Wire(client, connect()), Wire(client, connect())
            session_controls(primary, blocker, observer)
            startup = Wire(client, connect({"options": "-c lock_timeout=2s "
                                            "-c statement_timeout=3s -c deadlock_timeout=4s"}),
                           extended=True)
            startup_controls(startup, primary)
            reload_controls(primary, startup, blocker, observer)
    finally:
        runner.stop_ours(server)
    print("[TIMEOUT SETTINGS PROTOCOL E2E] transaction, startup, reload and wait controls passed")


if __name__ == "__main__":
    main()
