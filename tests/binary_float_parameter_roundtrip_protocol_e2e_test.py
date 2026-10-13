#!/usr/bin/env python3
"""Binary float4/float8 Bind preserves values through SELECT and storage."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("float_binary_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    controls = 0
    rejected_controls = 0
    descriptor_controls = 0

    def simple(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def roundtrip(sql, oid, raw, tag="SELECT 1", parameter=True):
        nonlocal controls
        controls += 1
        name = ("float_value_" + str(controls)).encode()
        parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", int(parameter))
        if parameter:
            parse += struct.pack("!I", oid)
            value = struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
            bind = name + b"\0" + name + b"\0" + struct.pack("!HHH", 1, 1, 1) + value
        else:
            bind = name + b"\0" + name + b"\0" + struct.pack("!HH", 0, 0)
        bind += struct.pack("!HH", 1, 1)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                     client.typed(b"D", b"P" + name + b"\0") +
                     client.typed(b"E", name + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        expected_size = 4 if oid == 700 else 8
        valid = (not errors and rows == [[raw]] and len(fields) == 1 and
                 fields[0][3:] == (oid, expected_size, -1, 1) and tags == [tag] and
                 messages[-1] == (b"Z", b"T" if reference else b"I"))
        print("BINARY_FLOAT_ROUNDTRIP", controls, sql, "oid", oid,
              "input", None if raw is None else raw.hex(), "output", rows,
              "errors", errors, "match", valid, flush=True)
        if not valid:
            failures.append((sql, oid, raw, rows, fields, tags, errors))

    def rejected(sql, oid, value, expected_state):
        nonlocal rejected_controls
        rejected_controls += 1
        if reference:
            simple("SAVEPOINT bad_float")
        name = ("bad_float_" + str(rejected_controls)).encode()
        raw = value.encode()
        parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)
        bind = name + b"\0" + name + b"\0" + struct.pack("!HH", 0, 1)
        bind += struct.pack("!i", len(raw)) + raw + struct.pack("!HH", 1, 1)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                     client.typed(b"E", name + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        states = [client.diagnostic_fields(body).get(b"C", b"").decode()
                  for kind, body in messages if kind == b"E"]
        valid = states == [expected_state] and not any(kind in (b"D", b"C") for kind, _ in messages)
        print("FLOAT_RANGE_OR_SYNTAX", sql, oid, value, states, "match", valid, flush=True)
        if not valid:
            failures.append((sql, oid, value, states))
        if reference:
            simple("ROLLBACK TO SAVEPOINT bad_float")
            simple("RELEASE SAVEPOINT bad_float")

    def descriptor(sql, expected_types, expected_rows, tag="SELECT 0"):
        nonlocal descriptor_controls
        descriptor_controls += 1
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        metadata = [(field[3], field[4]) for field in fields]
        valid = result[1] is None and result[0] == expected_rows and result[4] == tag and metadata == expected_types
        print("FLOAT_PHYSICAL_DESCRIPTOR", sql, result, metadata, "match", valid, flush=True)
        if not valid:
            failures.append((sql, result, metadata))

    # Positive canonical NaN tests its value, not arbitrary NaN payload bits.
    # Every finite IEEE bit pattern below (including signed zero/subnormal)
    # must return exactly the bytes that Bind received.
    patterns = {
        700: ["3fa00000", "3f9e0652", "2b8cbccc", "00000001", "007fffff", "00800000", "7f7fffff",
              "80000000", "bf9e0652", "ff7fffff", "7f800000", "ff800000", "7fc00000", "00000000"],
        701: ["3ff4000000000000", "3fbf9add3746f659", "3d719799812dea11", "0000000000000001",
              "000fffffffffffff", "0010000000000000", "7fefffffffffffff", "8000000000000000",
              "bfbf9add3746f659", "ffefffffffffffff", "7ff0000000000000", "fff0000000000000",
              "7ff8000000000000", "0000000000000000"],
    }
    try:
        if reference:
            simple("BEGIN")
            schema = "float_binary_" + uuid.uuid4().hex[:12]
            simple("CREATE SCHEMA " + schema)
            simple("SET search_path TO " + schema)
        simple("CREATE TABLE float_values(id INTEGER PRIMARY KEY,f REAL,d DOUBLE PRECISION)")
        for oid, values in patterns.items():
            for spelling in values + [None]:
                roundtrip("SELECT $1 AS value", oid, None if spelling is None else bytes.fromhex(spelling))
            column = "f" if oid == 700 else "d"
            for index in (1, 2, 3, 7, 10, 12):
                raw = bytes.fromhex(values[index])
                row = oid * 100 + index
                roundtrip("INSERT INTO float_values(id," + column + ") VALUES(" + str(row) + ",$1) RETURNING " + column,
                          oid, raw, "INSERT 0 1")
                roundtrip("UPDATE float_values SET " + column + "=$1 WHERE id=" + str(row) + " RETURNING " + column,
                          oid, raw, "UPDATE 1")
                roundtrip("SELECT " + column + " FROM float_values WHERE id=" + str(row), oid, raw, parameter=False)
        assert controls == 66, controls
        for oid, column in ((700, "f"), (701, "d")):
            for value, state in (("1e1000", "22003"), ("-1e1000", "22003"),
                                 ("1e-1000", "22003"), ("1.5tail", "22P02")):
                rejected("SELECT $1 AS value", oid, value, state)
                rejected("INSERT INTO float_values(id," + column + ") VALUES(999999,$1)",
                         oid, value, state)
        assert rejected_controls == 16, rejected_controls
        descriptor("SELECT f,d FROM float_values WHERE false", [(700, 4), (701, 8)], [])
        descriptor("INSERT INTO float_values(id) VALUES(888888) RETURNING f,d",
                   [(700, 4), (701, 8)], [[None, None]], "INSERT 0 1")
        descriptor("SELECT f AS d,d AS f FROM float_values WHERE id=888888",
                   [(700, 4), (701, 8)], [[None, None]], "SELECT 1")
        descriptor("SELECT CAST(1 AS DOUBLE PRECISION) AS f,CAST(2 AS REAL) AS d FROM float_values WHERE id=888888",
                   [(701, 8), (700, 4)], [["1", "2"]], "SELECT 1")
        assert descriptor_controls == 4, descriptor_controls
        assert not failures, ("binary float roundtrip failures", failures)
        print("[BINARY FLOAT PARAMETER ROUNDTRIP] all 66 roundtrip, 16 range/syntax and 4 descriptor controls passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
