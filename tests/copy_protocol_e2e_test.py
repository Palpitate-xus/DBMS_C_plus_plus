#!/usr/bin/env python3
"""PostgreSQL COPY text-mode wire flow, recovery, and atomicity."""

import importlib.util
import socket
import struct
import time
from pathlib import Path


def state(messages):
    for kind, body in messages:
        if kind != b"E":
            continue
        for field in body.rstrip(b"\0").split(b"\0"):
            if field.startswith(b"C"):
                return field[1:].decode()
    return None


def command_tags(messages):
    return [body[:-1] for kind, body in messages if kind == b"C"]


def expect_copy_response(body, columns):
    assert body == b"\0" + struct.pack("!H", columns) + b"\0\0" * columns, body


def parse(name, sql):
    return name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)


def bind(portal, statement):
    return (portal.encode() + b"\0" + statement.encode() + b"\0" +
            struct.pack("!HHH", 0, 0, 0))


def execute(portal=""):
    return portal.encode() + b"\0" + struct.pack("!I", 0)


def simple(client, sock, sql):
    messages = client.simple_query(sock, sql)
    return messages, state(messages)


def begin_copy(client, sock, sql, response=b"G", columns=3):
    sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
    kind, body = client.read_message(sock)
    assert kind == response, (kind, body)
    expect_copy_response(body, columns)


def finish_simple_copy(client, sock):
    messages = client.read_until_ready(sock)
    assert messages[-1] == (b"Z", b"I"), messages
    return messages


def copy_out(client, sock, sql, columns):
    begin_copy(client, sock, sql, b"H", columns)
    messages = []
    while True:
        message = client.read_message(sock)
        messages.append(message)
        if message[0] == b"Z":
            return messages


def check_pg_type_copy_selection(client, runner, sock):
    sync = client.typed(b"S")

    def command(sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    def send_extended(sql, flush=False):
        request = (client.typed(b"P", parse("", sql)) +
                   client.typed(b"B", bind("", "")) +
                   client.typed(b"E", execute()))
        if flush:
            request += client.typed(b"H")
        sock.sendall(request)
        assert client.read_message(sock) == (b"1", b""), sql
        assert client.read_message(sock) == (b"2", b""), sql

    def reject(sql, expected, extended, ready=b"I"):
        if extended:
            send_extended(sql, flush=True)
        else:
            sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
        first = client.read_message(sock)
        assert first[0] == b"E", (sql, first)
        if extended:
            sock.sendall(sync)
        messages = [first, *client.read_until_ready(sock)]
        assert [kind for kind, _ in messages] == [b"E", b"Z"], (sql, messages)
        assert state(messages) == expected and messages[-1] == (b"Z", ready), (sql, messages)

    def out_rows(sql, columns, extended):
        if extended:
            send_extended(sql, flush=True)
            kind, body = client.read_message(sock)
            assert kind == b"H", (sql, kind, body)
            expect_copy_response(body, columns)
            sock.sendall(sync)
            messages = client.read_until_ready(sock)
        else:
            messages = copy_out(client, sock, sql, columns)
        rows = [body for kind, body in messages if kind == b"d"]
        assert [kind for kind, _ in messages] == [b"d"] * len(rows) + [b"c", b"C", b"Z"], (sql, messages)
        assert command_tags(messages) == [f"COPY {len(rows)}".encode()], (sql, messages)
        assert messages[-1] == (b"Z", b"I"), (sql, messages)
        return rows

    def insert_row(sql, row, extended):
        if extended:
            send_extended(sql)
            kind, body = client.read_message(sock)
            assert kind == b"G", (sql, kind, body)
            expect_copy_response(body, 1)
        else:
            begin_copy(client, sock, sql, columns=1)
        completion = client.typed(b"d", row) + client.typed(b"c")
        sock.sendall(completion + (sync if extended else b""))
        assert finish_simple_copy(client, sock) == [(b"C", b"COPY 1\0"), (b"Z", b"I")], sql

    def table_rows(sql, expected):
        messages = client.simple_query(sock, sql)
        rows, sqlstate, _, _, tag = runner.decode_wire_result(messages)
        assert (rows, sqlstate, tag, messages[-1]) == (
            [[value] for value in expected], None, f"SELECT {len(expected)}", (b"Z", b"I")), (sql, messages)

    for extended in (False, True):
        command("RESET search_path", b"RESET")
        command("CREATE TABLE public.pg_type (id INT)", b"CREATE TABLE")
        command("INSERT INTO public.pg_type VALUES (11)", b"INSERT 0 1")
        reject("COPY pg_type (id) TO STDOUT", "42703", extended)
        reject("COPY pg_type (id) FROM STDIN", "42703", extended)
        builtin = out_rows("COPY pg_type (oid, typname) TO STDOUT", 2, extended)
        assert builtin.count(b"23\tint4\n") == builtin.count(b"25\ttext\n") == 1, builtin
        assert all(row.endswith(b"\n") and row.count(b"\t") == 1 for row in builtin), builtin
        reject("COPY pg_catalog.pg_type (oid, typname) FROM STDIN", "0A000", extended)

        command("CREATE TEMP TABLE pg_type (id INT)", b"CREATE TABLE")
        command("INSERT INTO pg_temp.pg_type VALUES (31)", b"INSERT 0 1")
        command("SET search_path = public, pg_catalog, pg_temp", b"SET")
        insert_row("COPY pg_type (id) FROM STDIN", b"17\n", extended)
        assert sorted(out_rows("COPY pg_type (id) TO STDOUT", 1, extended)) == [b"11\n", b"17\n"]
        for relation in ("pg_type", "public.pg_type"):
            command("BEGIN READ ONLY", b"BEGIN", b"T")
            reject(f"COPY {relation} (id) FROM STDIN", "25006", extended, b"E")
            command("ROLLBACK", b"ROLLBACK")

        command("SET search_path = pg_catalog, pg_temp, public", b"SET")
        reject("COPY pg_type (id) TO STDOUT", "42703", extended)
        command("SET search_path = pg_temp, pg_catalog, public", b"SET")
        insert_row("COPY pg_type (id) FROM STDIN", b"47\n", extended)
        assert sorted(out_rows("COPY pg_type (id) TO STDOUT", 1, extended)) == [b"31\n", b"47\n"]
        table_rows("SELECT id FROM public.pg_type ORDER BY id", ["11", "17"])
        table_rows("SELECT id FROM pg_temp.pg_type ORDER BY id", ["31", "47"])
        command("DROP TABLE pg_temp.pg_type", b"DROP TABLE")
        command("DROP TABLE public.pg_type", b"DROP TABLE")
        command("RESET search_path", b"RESET")
    print("COPY_PG_TYPE_RELATION_MODES_COMPLETE 2")


def check_copy_schema_lock(client, owner, reader):
    def command(sock, sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    command(owner, "CREATE TABLE copy_schema_lock (id INT, marker INT)", b"CREATE TABLE")
    command(owner, "INSERT INTO copy_schema_lock VALUES (1, 29)", b"INSERT 0 1")
    command(reader, "SET lock_timeout = 50", b"SET")
    command(owner, "BEGIN", b"BEGIN", b"T")
    sql = "COPY copy_schema_lock (marker) TO STDOUT"
    try:
        command(owner, "ALTER TABLE copy_schema_lock DROP COLUMN marker", b"ALTER TABLE", b"T")
        messages, sqlstate = simple(client, reader, sql)
        assert [kind for kind, _ in messages] == [b"E", b"Z"], (sql, messages)
        assert (sqlstate, messages[-1]) == ("55P03", (b"Z", b"I")), (sql, messages)
    finally:
        command(owner, "ROLLBACK", b"ROLLBACK")

    exported = copy_out(client, reader, sql, 1)
    assert exported == [(b"d", b"29\n"), (b"c", b""), (b"C", b"COPY 1\0"), (b"Z", b"I")], exported
    command(owner, "DROP TABLE copy_schema_lock", b"DROP TABLE")
    print("COPY_SCHEMA_LOCK_CHECK_COMPLETE 1")


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "copy_runner", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    slow_reader = None
    try:
        check_pg_type_copy_selection(client, runner, sock)
        with socket.create_connection(("127.0.0.1", server["port"]), 15) as peer:
            client.startup(peer, "alice", "info")
            check_copy_schema_lock(client, sock, peer)
        _, sqlstate = simple(
            client, sock,
            "CREATE TABLE copy_wire (id INT PRIMARY KEY, payload TEXT, optional TEXT DEFAULT 'fallback');")
        assert sqlstate is None, sqlstate

        # CopyData boundaries are not record boundaries.  NULL, empty text,
        # escaped newlines, a custom delimiter, and a final unterminated row
        # retain distinct values.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"1\tal") +
                     client.typed(b"d", b"pha\t\\N\n2\tline\\nvalue\t\n"))
        sock.sendall(client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert state(copied) is None, copied
        assert command_tags(copied) == [b"COPY 2"], copied

        exported = copy_out(
            client, sock,
            "COPY copy_wire (id, payload, optional) TO STDOUT", 3)
        assert [body for kind, body in exported if kind == b"d"] == [
            b"1\talpha\t\\N\n", b"2\tline\\nvalue\t\n"
        ], exported
        assert [kind for kind, _ in exported].count(b"c") == 1, exported
        assert command_tags(exported) == [b"COPY 2"], exported

        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload) FROM STDIN "
            "WITH (FORMAT text, DELIMITER '|', NULL 'NULL', ENCODING 'UTF8')",
            columns=2)
        sock.sendall(client.typed(b"d", b"3|pipe\\|value\n4|tail") +
                     client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert command_tags(copied) == [b"COPY 2"], copied
        exported = copy_out(
            client, sock,
            "COPY copy_wire (id, payload) TO STDOUT "
            "WITH (DELIMITER '|', NULL 'NULL')", 2)
        assert b"".join(body for kind, body in exported if kind == b"d") == (
            b"1|alpha\n2|line\\nvalue\n3|pipe\\|value\n4|tail\n"
        ), exported

        # A server-side parse/insert failure is reported immediately, but the
        # backend drains to CopyFail/CopyDone and rolls every prior row back.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"10\tok\tv\n11\ttoo-few\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"f", b"client stopped after server error\0"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"10\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # CopyFail is a statement cancellation and also rolls back earlier
        # chunks from the same COPY.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"12\twill rollback\tv\n") +
                     client.typed(b"f", b"application aborted\0"))
        failed = finish_simple_copy(client, sock)
        assert state(failed) == "57014", failed

        # An error inside an explicit transaction leaves ReadyForQuery in E;
        # ROLLBACK restores usability and no COPY row survives.
        assert simple(client, sock, "BEGIN")[0][-1] == (b"Z", b"T")
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"20\tbefore error\tv\n21\tbad\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"c"))
        assert client.read_until_ready(sock) == [(b"Z", b"E")]
        messages, sqlstate = simple(client, sock, "SELECT 1")
        assert sqlstate == "25P02" and messages[-1] == (b"Z", b"E"), messages
        assert simple(client, sock, "ROLLBACK")[0][-1] == (b"Z", b"I")

        # Unsupported semantics fail before CopyInResponse and before writes.
        for sql in (
            "COPY copy_wire FROM STDIN WITH (FORMAT binary)",
            "COPY copy_wire FROM PROGRAM 'producer'",
            "COPY copy_wire FROM STDIN WITH (FREEZE true)",
            "COPY copy_wire FROM STDIN WITH (ON_ERROR ignore)",
            "COPY copy_wire FROM STDIN WITH (HEADER true)",
            "COPY copy_wire FROM '/tmp/copy.csv' WITH (FORMAT csv)",
        ):
            messages, sqlstate = simple(client, sock, sql)
            assert sqlstate == "0A000", (sql, messages)
            assert all(kind != b"G" for kind, _ in messages), messages
        messages, sqlstate = simple(
            client, sock,
            "INSERT INTO copy_wire VALUES (99, 'must rollback', 'x'); "
            "COPY copy_wire FROM STDIN")
        assert sqlstate == "0A000", messages
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"99\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # Malformed CopyDone receives 08P01 and a normal simple-query recovery
        # boundary.  No row from the damaged stream is committed.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"30\tdamaged\tv\n") +
                     client.typed(b"c", b"not-empty"))
        damaged = finish_simple_copy(client, sock)
        assert state(damaged) == "08P01", damaged

        # Extended Query COPY waits for Sync.  A Sync consumed as the error
        # recovery boundary still produces exactly one ReadyForQuery.
        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext", "COPY copy_wire (id, payload) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_portal", "copy_ext")) +
            client.typed(b"E", execute("copy_ext_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G"
        expect_copy_response(body, 2)
        sock.sendall(client.typed(b"d", b"40\textended\n") +
                     client.typed(b"c") + client.typed(b"S"))
        extended = client.read_until_ready(sock)
        assert state(extended) is None, extended
        assert command_tags(extended) == [b"COPY 1"], extended

        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext_bad", "COPY copy_wire (id, payload) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_bad_portal", "copy_ext_bad")) +
            client.typed(b"E", execute("copy_ext_bad_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G"
        expect_copy_response(body, 2)
        sock.sendall(client.typed(b"d", b"41\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]

        # CancelRequest interrupts an active COPY even while the backend is
        # waiting for the next frontend message, and leaves the implicit
        # transaction empty without requiring another CopyData/CopyFail.
        sock.close()
        sock = socket.create_connection(("127.0.0.1", server["port"]), 15)
        sock.settimeout(15)
        backend_pid, secret = client.startup(sock, "alice", "info")
        server["sock"] = sock
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        cancel = socket.create_connection(("127.0.0.1", server["port"]), 15)
        cancel.sendall(struct.pack("!IIII", 16, 80877102, backend_pid, secret))
        cancel.close()
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"50\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # The same active COPY interruption contract also owns Execute's
        # extended-protocol path; preserve all existing rows across its abort.
        kept_ids = [body for kind, body in exported if kind == b"d"]
        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext_cancel", "COPY copy_wire (id, payload, optional) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_cancel_portal", "copy_ext_cancel")) +
            client.typed(b"E", execute("copy_ext_cancel_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G", (kind, body)
        expect_copy_response(body, 3)
        with socket.create_connection(("127.0.0.1", server["port"]), 15) as cancel:
            cancel.sendall(struct.pack("!IIII", 16, 80877102, backend_pid, secret))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert [body for kind, body in exported if kind == b"d"] == kept_ids, exported
        print("COPY_CANCEL_MODES_COMPLETE 2")

        # A COPY TO client that stops reading must not pin the backend in a
        # blocking socket write beyond statement_timeout.  Constrain the
        # receive buffer and make the output larger than the kernel buffers.
        payload = "x" * 16384
        large_rows = b"".join(
            f"{1000 + row}\t{payload}\n".encode()
            for row in range(256))
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload) FROM STDIN", columns=2)
        sock.sendall(client.typed(b"d", large_rows))
        sock.sendall(client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert command_tags(copied) == [b"COPY 256"], copied

        slow_reader = socket.create_connection(
            ("127.0.0.1", server["port"]), 15)
        slow_reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        slow_reader.settimeout(5)
        slow_pid, _ = client.startup(slow_reader, "alice", "info")
        timeout_messages, timeout_state = simple(
            client, slow_reader, "SET statement_timeout = 5000")
        assert timeout_state is None, timeout_messages
        settings = client.simple_query(
            slow_reader,
            "SELECT setting FROM pg_catalog.pg_settings "
            "WHERE name = 'statement_timeout'")
        assert runner.decode_wire_result(settings, include_types=True)[0] == [
            ["5000"]
        ], settings
        begin_copy(
            client, slow_reader,
            "COPY copy_wire (id, payload) TO STDOUT", response=b"H",
            columns=2)
        initial_activity = client.simple_query(
            sock,
            "SELECT state, query FROM pg_catalog.pg_stat_activity "
            f"WHERE pid = {slow_pid}")
        initial_rows = runner.decode_wire_result(
            initial_activity, include_types=True)[0]
        assert initial_rows and initial_rows[0][0] == "active", initial_rows
        time.sleep(5.3)
        timeout_finished = False
        final_activity_rows = None
        activity_deadline = time.monotonic() + 3
        while time.monotonic() < activity_deadline:
            activity_messages = client.simple_query(
                sock,
                "SELECT state FROM pg_catalog.pg_stat_activity "
                f"WHERE pid = {slow_pid}")
            activity_rows = runner.decode_wire_result(
                activity_messages, include_types=True)[0]
            if not activity_rows or activity_rows[0][0] != "active":
                timeout_finished = True
                final_activity_rows = activity_rows
                break
            time.sleep(0.05)
        assert timeout_finished, (
            "COPY TO backend remained active after statement_timeout while "
            "the client receive window was not drained")
        assert not final_activity_rows, (
            "COPY TO backend stayed connected/idle instead of closing after "
            "the blocked output frame timed out", final_activity_rows)
        slow_reader.close()
        slow_reader = None

        print("[COPY PROTOCOL E2E] passed")
    finally:
        if slow_reader is not None:
            slow_reader.close()
        server["sock"] = sock
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
