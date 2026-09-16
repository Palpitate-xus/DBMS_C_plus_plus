#!/usr/bin/env python3
"""PostgreSQL COPY text-mode wire flow, recovery, and atomicity."""

import importlib.util
import socket
import struct
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


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "copy_runner", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    try:
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

        # CancelRequest targets an active COPY, wakes on the next CopyData,
        # drains CopyFail, and leaves the implicit transaction empty.
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
        sock.sendall(client.typed(b"d", b"50\tcancelled\tv\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        sock.sendall(client.typed(b"f", b"cancel acknowledged\0"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"50\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        print("[COPY PROTOCOL E2E] passed")
    finally:
        server["sock"] = sock
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
