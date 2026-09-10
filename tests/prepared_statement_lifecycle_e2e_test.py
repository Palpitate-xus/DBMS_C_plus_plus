#!/usr/bin/env python3
"""SQL and wire prepared statements share parameters and portal lifetime."""

import importlib.util
import struct
from pathlib import Path


def parse_message(name, sql, type_oids=()):
    body = name.encode() + b"\0" + sql.encode() + b"\0"
    body += struct.pack("!H", len(type_oids))
    for oid in type_oids:
        body += struct.pack("!I", oid)
    return body


def bind_message(portal, statement, values=()):
    body = portal.encode() + b"\0" + statement.encode() + b"\0"
    body += struct.pack("!H", 0)  # all parameters use text format
    body += struct.pack("!H", len(values))
    for value in values:
        if value is None:
            body += struct.pack("!i", -1)
        else:
            raw = value.encode()
            body += struct.pack("!i", len(raw)) + raw
    return body + struct.pack("!H", 0)  # all results use text format


def execute_message(portal):
    return portal.encode() + b"\0" + struct.pack("!I", 0)


def error_state(messages):
    for kind, body in messages:
        if kind != b"E":
            continue
        for field in body.rstrip(b"\0").split(b"\0"):
            if field.startswith(b"C"):
                return field[1:].decode()
    return None


def simple(client, sock, sql):
    messages = client.simple_query(sock, sql)
    return messages, error_state(messages), client.data_row_values(messages)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "prepared_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    try:
        # SQL PREPARE validates replacement, declared/inferred arity and
        # undefined statement/type errors with PostgreSQL SQLSTATEs.
        _, state, _ = simple(
            client, sock, "PREPARE sql_stmt(integer) AS SELECT $1;")
        assert state is None, state
        _, state, _ = simple(
            client, sock, "PREPARE sql_stmt(integer) AS SELECT $1;")
        assert state == "42P05", state
        _, state, _ = simple(client, sock, "EXECUTE sql_stmt;")
        assert state == "42601", state
        _, state, rows = simple(client, sock, "EXECUTE sql_stmt(41);")
        assert state is None and rows == [[b"41"]], (state, rows)
        _, state, _ = simple(
            client, sock, "PREPARE bad_type(type_does_not_exist) AS SELECT $1;")
        assert state == "42704", state

        _, state, _ = simple(
            client, sock, "PREPARE inferred AS SELECT $2;")
        assert state is None, state
        _, state, rows = simple(client, sock, "EXECUTE inferred(1, 9);")
        assert state is None and rows == [[b"9"]], (state, rows)
        _, state, _ = simple(
            client, sock, "PREPARE quoted_param AS SELECT '$1';")
        assert state is None, state
        _, state, rows = simple(client, sock, "EXECUTE quoted_param;")
        assert state is None and rows == [[b"$1"]], (state, rows)
        _, state, _ = simple(client, sock, "DEALLOCATE sql_stmt;")
        assert state is None, state
        _, state, _ = simple(client, sock, "EXECUTE sql_stmt(1);")
        assert state == "26000", state
        _, state, _ = simple(client, sock, "DEALLOCATE sql_stmt;")
        assert state == "26000", state

        # A zero-length Parse OID list is allowed: parameter slots are derived
        # from real $n tokens, while quoted/commented text is ignored.
        sock.sendall(
            client.typed(b"P", parse_message("wire_infer", "SELECT $1")) +
            client.typed(b"D", b"Swire_infer\0") +
            client.typed(b"B", bind_message("wire_infer_portal", "wire_infer", ("wire",))) +
            client.typed(b"E", execute_message("wire_infer_portal")) +
            client.typed(b"S"))
        inferred = client.read_until_ready(sock)
        parameter_descriptions = [body for kind, body in inferred if kind == b"t"]
        assert parameter_descriptions == [struct.pack("!HI", 1, 0)], inferred
        assert client.data_row_values(inferred) == [[b"wire"]], inferred

        # SQL PREPARE and wire Parse use one statement namespace in both
        # directions, including declared type OIDs and duplicate detection.
        _, state, _ = simple(
            client, sock, "PREPARE sql_to_wire(text) AS SELECT $1;")
        assert state is None, state
        sock.sendall(
            client.typed(b"B", bind_message(
                "sql_to_wire_portal", "sql_to_wire", ("shared",))) +
            client.typed(b"E", execute_message("sql_to_wire_portal")) +
            client.typed(b"S"))
        shared = client.read_until_ready(sock)
        assert client.data_row_values(shared) == [[b"shared"]], shared

        sock.sendall(client.typed(
            b"P", parse_message("wire_to_sql", "SELECT $1", (23,))) +
            client.typed(b"S"))
        assert error_state(client.read_until_ready(sock)) is None
        _, state, rows = simple(client, sock, "EXECUTE wire_to_sql(7);")
        assert state is None and rows == [[b"7"]], (state, rows)
        _, state, _ = simple(
            client, sock, "PREPARE wire_to_sql AS SELECT 8;")
        assert state == "42P05", state

        sock.sendall(client.typed(
            b"P", parse_message("bad_oid", "SELECT $1", (999999,))))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C42704\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]

        # Parse validation recognizes the complete built-in OID registry, not
        # merely the subset bootstrapped into each on-disk catalog.
        adjacent_types = (
            ("xml", 142), ("inet", 869), ("cidr", 650),
            ("macaddr", 829), ("macaddr8", 774),
            ("bit", 1560), ("varbit", 1562),
        )
        for type_name, oid in adjacent_types:
            statement = "known_" + type_name
            sock.sendall(
                client.typed(b"P", parse_message(
                    statement, "SELECT $1::" + type_name, (oid,))) +
                client.typed(b"C", b"S" + statement.encode() + b"\0") +
                client.typed(b"S"))
            known_type = client.read_until_ready(sock)
            assert error_state(known_type) is None, (type_name, known_type)

        # Closing/deallocating a prepared statement does not destroy a portal
        # that already owns a bound query. Transaction end does destroy it.
        assert simple(client, sock, "BEGIN;")[0][-1] == (b"Z", b"T")
        sock.sendall(
            client.typed(b"P", parse_message("close_source", "SELECT 73")) +
            client.typed(b"B", bind_message("close_portal", "close_source")) +
            client.typed(b"C", b"Sclose_source\0") +
            client.typed(b"E", execute_message("close_portal")) +
            client.typed(b"S"))
        closed_source = client.read_until_ready(sock)
        assert client.data_row_values(closed_source) == [[b"73"]], closed_source

        sock.sendall(
            client.typed(b"P", parse_message("dealloc_source", "SELECT 74")) +
            client.typed(b"B", bind_message("dealloc_portal", "dealloc_source")) +
            client.typed(b"S"))
        assert error_state(client.read_until_ready(sock)) is None
        _, state, _ = simple(client, sock, "DEALLOCATE dealloc_source;")
        assert state is None, state
        sock.sendall(
            client.typed(b"E", execute_message("dealloc_portal")) +
            client.typed(b"S"))
        deallocated_source = client.read_until_ready(sock)
        assert client.data_row_values(deallocated_source) == [[b"74"]], deallocated_source

        sock.sendall(
            client.typed(b"P", parse_message("txn_source", "SELECT 75")) +
            client.typed(b"B", bind_message("txn_portal", "txn_source")) +
            client.typed(b"S"))
        assert error_state(client.read_until_ready(sock)) is None
        assert simple(client, sock, "COMMIT;")[0][-1] == (b"Z", b"I")
        sock.sendall(client.typed(b"E", execute_message("txn_portal")))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C34000\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]

        print("[PREPARED STATEMENT LIFECYCLE E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
