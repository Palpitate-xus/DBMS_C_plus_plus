#!/usr/bin/env python3
"""Session work_mem utility commands, transaction lifetime and startup defaults.

The accepted surface is SET/SHOW/RESET and the existing pg_settings subset.
Configuration functions and stored-function SET clauses have separate owners.
Every observation is recorded before the final assertion, including a frozen
server rejecting a SET or a connection's startup options.
"""
import re
import socket
import struct

from compat.numeric_type_review import Review


def steps():
    cases = []

    def command(sql, tag="SET", ready=b"I", state=None, warning=None):
        cases.append((sql, [], [], [], tag, ready, state, warning))

    def show(value, ready=b"I"):
        cases.append(("SHOW work_mem", [[value]], ["work_mem"], [25], "SHOW", ready, None, None))

    def settings(value, ready=b"I"):
        cases.append(("SELECT setting,unit FROM pg_catalog.pg_settings WHERE name='work_mem'",
                      [[str(value), "kB"]], ["setting", "unit"], [25, 25], "SELECT 1", ready, None, None))

    show("4MB")
    settings(4096)
    command("SET work_mem='64kB'")
    show("64kB")
    settings(64)
    command("SET SESSION work_mem TO 128")
    show("128kB")
    command('SET "WORK_MEM" = \'256kB\'')
    show("256kB")
    command("SET work_mem TO '1MB'")
    show("1MB")
    command("SET work_mem FROM CURRENT")
    show("1MB")
    command("RESET work_mem", "RESET")
    show("4MB")
    command("SET SESSION work_mem FROM CURRENT")
    show("4MB")
    command("SET work_mem='2048kB'")
    show("2MB")
    command("SET work_mem TO DEFAULT")
    show("4MB")
    command("SET work_mem='3MB'")
    command("RESET ALL", "RESET")
    show("4MB")
    command("SET work_mem='5MB'")
    command("DISCARD ALL", "DISCARD ALL")
    show("4MB")
    command("SET LOCAL work_mem='64kB'", warning="25P01")
    show("4MB")
    for value in ("63kB", "-1", "0", "2KB", "bogus", "", "2147483648kB"):
        command("SET work_mem='" + value + "'", tag=None, state="22023")
        show("4MB")
    for value, display in (("65536 B", "64kB"), ("0.5MB", "512kB"), ("1.5MB", "1536kB")):
        command("SET work_mem='" + value + "'")
        show(display)
    command("RESET work_mem", "RESET")
    show("4MB")

    command("SET work_mem='128kB'")
    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='256kB'", ready=b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    show("64kB", b"T")
    command("COMMIT", "COMMIT")
    show("256kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='1MB'", ready=b"T")
    command("SET LOCAL work_mem='2MB'", ready=b"T")
    show("2MB", b"T")
    command("ROLLBACK", "ROLLBACK")
    show("256kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='512kB'", ready=b"T")
    show("512kB", b"T")
    command("COMMIT", "COMMIT")
    show("256kB")

    # FROM CURRENT is a real assignment: the session form promotes the
    # currently visible local value; the local form does not persist it.
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    command("SET SESSION work_mem FROM CURRENT", ready=b"T")
    show("64kB", b"T")
    command("COMMIT", "COMMIT")
    show("64kB")
    command("SET work_mem='128kB'")
    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='1MB'", ready=b"T")
    command("SET LOCAL work_mem FROM CURRENT", ready=b"T")
    show("1MB", b"T")
    command("ROLLBACK", "ROLLBACK")
    show("128kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='256kB'", ready=b"T")
    command("SET LOCAL work_mem TO DEFAULT", ready=b"T")
    show("4MB", b"T")
    command("COMMIT", "COMMIT")
    show("128kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='256kB'", ready=b"T")
    command("RESET work_mem", "RESET", b"T")
    show("4MB", b"T")
    command("ROLLBACK", "ROLLBACK")
    show("128kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='256kB'", ready=b"T")
    command("RESET ALL", "RESET", b"T")
    show("4MB", b"T")
    command("COMMIT", "COMMIT")
    show("4MB")

    command("SET work_mem='128kB'")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='256kB'", ready=b"T")
    command("SAVEPOINT outer_mem", "SAVEPOINT", b"T")
    command("SET work_mem='1MB'", ready=b"T")
    command("SAVEPOINT inner_mem", "SAVEPOINT", b"T")
    command("SET LOCAL work_mem='2MB'", ready=b"T")
    show("2MB", b"T")
    command("ROLLBACK TO outer_mem", "ROLLBACK", b"T")
    show("256kB", b"T")
    command("SET work_mem='512kB'", ready=b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    show("64kB", b"T")
    command("RELEASE outer_mem", "RELEASE", b"T")
    command("COMMIT", "COMMIT")
    show("512kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='1MB'", ready=b"T")
    command("SAVEPOINT same_mem", "SAVEPOINT", b"T")
    command("SET LOCAL work_mem='2MB'", ready=b"T")
    command("SAVEPOINT same_mem", "SAVEPOINT", b"T")
    command("SET SESSION work_mem='4MB'", ready=b"T")
    show("4MB", b"T")
    command("ROLLBACK TO same_mem", "ROLLBACK", b"T")
    show("2MB", b"T")
    command("RELEASE same_mem", "RELEASE", b"T")
    command("ROLLBACK TO same_mem", "ROLLBACK", b"T")
    show("1MB", b"T")
    command("ROLLBACK", "ROLLBACK")
    show("512kB")

    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='1MB'", ready=b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    command("SELECT 1/0", tag=None, ready=b"E", state="22012")
    command("SHOW work_mem", tag=None, ready=b"E", state="25P02")
    command("ROLLBACK", "ROLLBACK")
    show("512kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='256kB'", ready=b"T")
    command("SAVEPOINT recover_mem", "SAVEPOINT", b"T")
    command("SET work_mem='2MB'", ready=b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    command("SELECT 1/0", tag=None, ready=b"E", state="22012")
    command("SHOW work_mem", tag=None, ready=b"E", state="25P02")
    command("ROLLBACK TO recover_mem", "ROLLBACK", b"T")
    show("256kB", b"T")
    command("COMMIT", "COMMIT")
    show("512kB")
    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='1MB'", ready=b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    command("COMMIT AND CHAIN", "COMMIT", b"T")
    show("1MB", b"T")
    command("SET LOCAL work_mem='2MB'", ready=b"T")
    command("ROLLBACK AND CHAIN", "ROLLBACK", b"T")
    show("1MB", b"T")
    command("ROLLBACK", "ROLLBACK")
    show("1MB")
    command("BEGIN", "BEGIN", b"T")
    command("SET work_mem='128kB'", ready=b"T")
    command("SELECT 1/0", tag=None, ready=b"E", state="22012")
    command("COMMIT", "ROLLBACK")
    show("1MB")
    command("BEGIN", "BEGIN", b"T")
    command("SET LOCAL work_mem='64kB'", ready=b"T")
    command("DISCARD ALL", tag=None, ready=b"E", state="25001")
    command("ROLLBACK", "ROLLBACK")
    show("1MB")
    command("RESET work_mem", "RESET")
    show("4MB")
    settings(4096)
    return cases


class Wire:
    def __init__(self, review, sock, name):
        self.review, self.sock, self.name = review, sock, name

    def query(self, sql, rows=(), columns=(), oids=(), tag="SET", ready=b"I",
              state=None, warning=None, mode="simple", result_format=0):
        expected = (list(rows), list(columns), list(oids), tag, ready.decode(), state, warning)
        label = (self.name, mode, result_format, sql)
        try:
            if self.sock is None:
                raise RuntimeError("startup rejected; this connection is unavailable")
            client = self.review.client
            if mode == "simple":
                messages = client.simple_query(self.sock, sql)
            else:
                # Empty parameter lists, ordinary valid result formats and an
                # unbounded Execute. No retained/error-format portal cases.
                parse = b"\0" + sql.encode() + b"\0\0\0"
                bind = b"\0\0" + struct.pack("!HHHh", 0, 0, 1, result_format)
                self.sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                                  client.typed(b"D", b"P\0") +
                                  client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                messages = client.read_until_ready(self.sock)
            errors = [client.diagnostic_fields(body).get(b"C", b"").decode()
                      for kind, body in messages if kind == b"E"]
            notices = [(client.diagnostic_fields(body).get(b"S", b""),
                        client.diagnostic_fields(body).get(b"C", b""))
                       for kind, body in messages if kind == b"N"]
            fields = [client.row_description_fields([message]) for message in messages if message[0] == b"T"]
            actual_rows = client.data_row_values(messages)
            tags = [body for kind, body in messages if kind == b"C"]
            statuses = client.parameter_status_values(messages)
            kinds = [kind for kind, _ in messages if kind != b"S"]
            expected_rows = [[None if value is None else value.encode() for value in row] for row in rows]
            prefix = [] if mode == "simple" else [b"1", b"2"]
            if state:
                expected_kinds = [b"E", b"Z"]
            elif columns:
                expected_kinds = prefix + [b"T"] + [b"D"] * len(rows) + [b"C", b"Z"]
            else:
                expected_kinds = prefix + ([] if mode == "simple" else [b"n"])
                expected_kinds += ([b"N"] if warning else []) + [b"C", b"Z"]
            valid = (messages[-1:] == [(b"Z", ready)] and kinds == expected_kinds and
                     errors == ([state] if state else []) and actual_rows == expected_rows and
                     tags == ([] if tag is None else [tag.encode() + b"\0"]) and
                     notices == ([(b"WARNING", warning.encode())] if warning else []) and
                     all(name != b"work_mem" for name, _ in statuses))
            if columns:
                valid = valid and len(fields) == 1 and [field[0] for field in fields[0]] == [name.encode() for name in columns] and \
                    [field[3] for field in fields[0]] == list(oids) and \
                    all(field[4:] == (-1, -1, result_format) for field in fields[0])
                if columns == ["work_mem"]:
                    valid = valid and fields == [[(b"work_mem", 0, 0, 25, -1, -1, result_format)]]
            else:
                valid = valid and not fields
            actual = {"rows": actual_rows, "fields": fields, "errors": errors,
                      "warnings": notices, "tags": tags, "ready": messages[-1:],
                      "frames": kinds, "parameter_status": statuses}
            self.review.record(label, valid, actual, expected)
        except Exception as error:
            # A failure is an observation, not permission to skip the rest of
            # the frozen baseline's state transitions and later controls.
            self.review.record(label, False, (type(error).__name__, str(error)), expected)

    def command(self, sql, tag="SET", **kwargs):
        self.query(sql, tag=tag, **kwargs)

    def show(self, value, **kwargs):
        self.query("SHOW work_mem", [[value]], ["work_mem"], [25], "SHOW", **kwargs)

    def settings(self, value, **kwargs):
        self.query("SELECT setting,unit FROM pg_catalog.pg_settings WHERE name='work_mem'",
                   [[str(value), "kB"]], ["setting", "unit"], [25, 25], "SELECT 1", **kwargs)


def cached_explain_controls(review, wire, peer):
    sql = "EXPLAIN (SETTINGS, COSTS OFF) SELECT id FROM work_mem_explain_rows ORDER BY id"

    def explain(connection, expected, label, ready=b"I"):
        try:
            if connection.sock is None:
                raise RuntimeError("startup rejected; this connection is unavailable")
            messages = review.client.simple_query(connection.sock, sql)
            result = review.runner.decode_wire_result(messages, include_types=True)
            text = "\n".join(row[0] for row in result[0])
            units = {"kB": 1, "MB": 1024, "GB": 1048576, "TB": 1073741824}
            values = [int(value) * units[unit] for value, unit in
                      re.findall(r"work_mem\s*=\s*'?([0-9]+)(kB|MB|GB|TB)'?", text)]
            fields = [review.client.row_description_fields([message]) for message in messages if message[0] == b"T"]
            frames = [kind for kind, _ in messages]
            valid = (result[1] is None and result[4] == "EXPLAIN" and result[5] == [25] and
                     messages[-1:] == [(b"Z", ready)] and values == [expected] and
                     frames == [b"T"] + [b"D"] * len(result[0]) + [b"C", b"Z"] and
                     fields == [[(b"QUERY PLAN", 0, 0, 25, -1, -1, 0)]])
            review.record(label, valid, (result, values, fields, frames, messages[-1:]), (expected, ready))
        except Exception as error:
            review.record(label, False, (type(error).__name__, str(error)), (expected, ready))

    # Use the same SQL/cache identity throughout, including across two live
    # sessions. Compare the one actual work_mem setting, not different PG and
    # project plan layouts or unit display choices.
    wire.command("CREATE TABLE work_mem_explain_rows(id INTEGER)", "CREATE TABLE")
    wire.command("SET work_mem='64kB'")
    explain(wire, 64, "first cached ordinary plan64")
    wire.command("SET work_mem='128kB'")
    explain(wire, 128, "same query after SET128")
    wire.command("BEGIN", "BEGIN", ready=b"T")
    wire.command("SET LOCAL work_mem='256kB'", ready=b"T")
    explain(wire, 256, "same query under LOCAL256", b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    explain(wire, 128, "same query after LOCAL rollback")
    peer.command("SET work_mem='1MB'")
    explain(peer, 1024, "same query in independent session1MB")
    explain(wire, 128, "first session remains128")


def grammar_controls(wire):
    for mode in ("simple", "extended"):
        for sql in ("SET work_mem='64'\n'kB'", "SET work_mem='64' -- continuation\n'kB'"):
            wire.command(sql, mode=mode)
            wire.show("64kB", mode=mode)
        for sql, state in (("SET work_mem='64' /*\n*/ 'kB'", "42601"),
                           ("SET work_mem=64kB", "42601"),
                           ("SET work_mem=64,128", "22023"),
                           ("SET work_mem=DEFAULT extra", "42601"),
                           ("SHOW work_mem extra", "42601"),
                           ("RESET work_mem TO DEFAULT", "42601"),
                           ("RESET work_mem extra", "42601")):
            # Invalid datums remain Simple execution observations. The raw
            # syntax checks use a normal Parse and valid Bind format only.
            wire.command(sql, tag=None, state=state,
                         mode="simple" if state == "22023" else mode)
            wire.show("64kB", mode=mode)
        wire.command('SET "WORK_MEM" FROM CURRENT', mode=mode)
        wire.query('SHOW "WORK_MEM"', [["64kB"]], ["work_mem"], [25], "SHOW", mode=mode)
        wire.command('RESET "WORK_MEM"', "RESET", mode=mode)
        wire.show("4MB", mode=mode)


def local_warning_controls(review, wire):
    def warning_error(sql, mode):
        try:
            client = review.client
            if mode == "simple":
                messages = client.simple_query(wire.sock, sql)
            else:
                parse = b"\0" + sql.encode() + b"\0\0\0"
                bind = b"\0\0" + struct.pack("!HHHh", 0, 0, 1, 0)
                wire.sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                                  client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" +
                                  struct.pack("!I", 0)) + client.typed(b"S"))
                messages = client.read_until_ready(wire.sock)
            diagnostics = [(kind, client.diagnostic_fields(body).get(b"S", b""),
                            client.diagnostic_fields(body).get(b"C", b""))
                           for kind, body in messages if kind in (b"N", b"E")]
            frames = [kind for kind, _ in messages]
            expected_frames = ([] if mode == "simple" else [b"1", b"2", b"n"]) + [b"N", b"E", b"Z"]
            expected_diagnostics = [(b"N", b"WARNING", b"25P01"), (b"E", b"ERROR", b"22023")]
            good = (frames == expected_frames and diagnostics == expected_diagnostics and
                    messages[-1:] == [(b"Z", b"I")] and not client.data_row_values(messages) and
                    not any(kind in (b"C", b"T", b"S") for kind, _ in messages))
            review.record((mode, sql), good, (frames, diagnostics, messages[-1:]),
                          (expected_frames, expected_diagnostics, b"I"))
        except Exception as error:
            review.record((mode, sql), False, (type(error).__name__, str(error)),
                          "WARNING25P01 then ERROR22023")

    # PostgreSQL warns before flattening/checking a syntactically valid LOCAL
    # value outside a block. Raw syntax errors precede execution and emit no
    # warning. Keep this distinct from Wire's unchanged E,Z error contract.
    wire.command("SET work_mem='128kB'")
    wire.show("128kB")
    for mode in ("simple", "extended"):
        for sql in ("SET LOCAL work_mem='63kB'", "SET LOCAL work_mem=64,128"):
            warning_error(sql, mode)
            wire.show("128kB", mode=mode)
        wire.command("SET LOCAL work_mem TO DEFAULT", mode=mode, warning="25P01")
        wire.show("128kB", mode=mode)
        wire.command("SET LOCAL work_mem FROM CURRENT", mode=mode, warning="25P01")
        wire.show("128kB", mode=mode)
        wire.command("SET LOCAL work_mem=64kB", mode=mode, tag=None, state="42601")
        wire.show("128kB", mode=mode)


def main():
    review = Review("WORK_MEM_SESSION_PROTOCOL")
    children = []
    if review.server:
        endpoint = ("127.0.0.1", review.server["port"], "alice", "info", "secret")
    else:
        endpoint = review.runner._reference_connection_settings()
    host, port, user, database, password = endpoint

    def connection(name, parameters=None, state=None):
        sock = None
        try:
            sock = socket.create_connection((host, port), timeout=review.runner.wire_timeout())
            review.client.send_startup_packet(sock, user, database, "work-mem-session-test", 196608,
                                              {}, parameters or {}, False)
            review.client.complete_startup_authentication(sock, user, password)
            messages = []
            while not messages or messages[-1][0] not in (b"E", b"Z"):
                messages.append(review.client.read_message(sock))
            errors = [review.client.diagnostic_fields(body).get(b"C", b"").decode()
                      for kind, body in messages if kind == b"E"]
            statuses = review.client.parameter_status_values(messages)
            if state:
                valid = errors == [state] and messages[-1][0] == b"E" and \
                    not any(kind in (b"Z", b"K") for kind, _ in messages) and \
                    review.client.wait_for_disconnect(sock)
            else:
                valid = not errors and messages[0] == (b"R", struct.pack("!I", 0)) and \
                    messages[-1] == (b"Z", b"I") and \
                    sum(kind == b"K" and len(body) == 8 for kind, body in messages) == 1 and \
                    all(kind in (b"R", b"S", b"K", b"Z") for kind, _ in messages) and \
                    all(setting != b"work_mem" for setting, _ in statuses)
            review.record(("STARTUP", name, parameters), valid,
                          (errors, [kind for kind, _ in messages], statuses), state)
            if state or errors:
                sock.close()
                return Wire(review, None, name)
            children.append(sock)
            return Wire(review, sock, name)
        except Exception as error:
            if sock is not None:
                sock.close()
            review.record(("STARTUP", name, parameters), False, (type(error).__name__, str(error)), state)
            return Wire(review, None, name)

    try:
        main_wire = Wire(review, review.sock, "main")
        for mode in ("simple", "extended"):
            for case in steps():
                # Invalid values and failed-block observations use Simple;
                # successful mutations still exercise both execution paths.
                main_wire.query(*case, mode="simple" if case[6] else mode)
            main_wire.command("SET work_mem='128kB'", mode=mode)
            main_wire.show("128kB", mode="extended", result_format=1)
            main_wire.settings(128, mode="extended", result_format=1)
            main_wire.command("RESET work_mem", "RESET", mode=mode)

        second = connection("independent")
        second.show("4MB")
        main_wire.command("SET work_mem='64kB'")
        second.show("4MB", mode="extended")
        second.command("SET work_mem='2MB'", mode="extended")
        main_wire.show("64kB")
        second.settings(2048)
        main_wire.command("RESET ALL", "RESET")
        main_wire.show("4MB")
        second.show("2MB", mode="extended", result_format=1)
        second.command("DISCARD ALL", "DISCARD ALL")
        second.show("4MB")

        startup = connection("startup-options", {"options": "-c work_mem=256kB"})
        startup.show("256kB")
        startup.settings(256, mode="extended")
        main_wire.show("4MB")
        startup.command("SET work_mem='64kB'", mode="extended")
        startup.command("RESET work_mem", "RESET")
        startup.show("256kB", mode="extended", result_format=1)
        startup.command("SET work_mem='512kB'")
        startup.command("SET work_mem TO DEFAULT", mode="extended")
        startup.show("256kB")
        startup.command("SET work_mem='1MB'")
        startup.command("RESET ALL", "RESET", mode="extended")
        startup.show("256kB")
        startup.command("SET work_mem='2MB'")
        startup.command("DISCARD ALL", "DISCARD ALL")
        startup.show("256kB", mode="extended")
        startup.command("BEGIN", "BEGIN", ready=b"T")
        startup.command("SET LOCAL work_mem TO DEFAULT", ready=b"T")
        startup.show("256kB", ready=b"T")
        startup.command("ROLLBACK", "ROLLBACK")
        startup.show("256kB")

        direct = connection("startup-direct", {"work_mem": "128kB"})
        direct.show("128kB")
        direct.command("SET work_mem='1MB'")
        direct.command("RESET work_mem", "RESET")
        direct.show("128kB", mode="extended")
        connection("startup-too-small", {"options": "-c work_mem=63kB"}, "22023")
        connection("startup-bad-unit", {"options": "-c work_mem=2KB"}, "22023")
        main_wire.show("4MB")
        cached_explain_controls(review, main_wire, second)
        grammar_controls(main_wire)
        local_warning_controls(review, main_wire)
        review.complete(470)
    finally:
        for sock in children:
            try:
                review.client.simple_query(sock, "ROLLBACK")
            except Exception:
                pass
            sock.close()
        review.close()


if __name__ == "__main__":
    main()
