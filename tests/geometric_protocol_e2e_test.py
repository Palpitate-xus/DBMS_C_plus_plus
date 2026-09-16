#!/usr/bin/env python3
"""Geometric OIDs, strict text grammar, and PostgreSQL binary wire I/O."""

import importlib.util
import math
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "geometry_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        messages = client.simple_query(server["sock"], sql)
        return messages, runner.decode_wire_result(messages, include_types=True)

    def bad_parameter(name, sql, oid, raw, binary, sqlstate):
        statement = name.encode()
        parse = (statement + b"\0" + sql.encode() + b"\0" +
                 struct.pack("!H", 1) + struct.pack("!I", oid))
        server["sock"].sendall(client.typed(b"P", parse))
        kind, body = client.read_message(server["sock"])
        assert kind == b"1" and body == b"", (kind, body)
        bind = (b"\0" + statement + b"\0" + struct.pack("!H", 1) +
                struct.pack("!H", 1 if binary else 0) +
                struct.pack("!H", 1) + struct.pack("!i", len(raw)) + raw +
                struct.pack("!H", 0))
        server["sock"].sendall(client.typed(b"B", bind))
        kind, body = client.read_message(server["sock"])
        assert kind == b"E", (kind, body)
        assert client.diagnostic_fields(body).get(b"C") == sqlstate, body
        server["sock"].sendall(client.typed(b"S"))
        messages = client.read_until_ready(server["sock"])
        assert messages[-1][0] == b"Z", messages

    try:
        _, decoded = execute(
            "CREATE TABLE geo_wire (id INT PRIMARY KEY, p POINT, ln LINE, "
            "ls LSEG, b BOX, pa PATH, pg POLYGON, c CIRCLE);")
        assert decoded[1] is None, decoded
        _, decoded = execute(
            "INSERT INTO geo_wire VALUES (1, '(1,2)', '{1,2,3}', "
            "'[(0,0),(1,2)]', '(0,0),(2,3)', '[(0,0),(1,2)]', "
            "'((0,0),(1,0),(0,1))', '<(1,2),3>');")
        assert decoded[1] is None, decoded

        messages, decoded = execute(
            "SELECT p, ln, ls, b, pa, pg, c FROM geo_wire;")
        rows, state, message, headers, tag, type_oids = decoded
        assert state is None, message
        assert rows == [["(1,2)", "{1,2,3}", "[(0,0),(1,2)]",
                         "(2,3),(0,0)", "[(0,0),(1,2)]",
                         "((0,0),(1,0),(0,1))", "<(1,2),3>"]], rows
        assert headers == ["p", "ln", "ls", "b", "pa", "pg", "c"]
        assert tag == "SELECT 1"
        assert type_oids == [600, 628, 601, 603, 602, 604, 718], type_oids
        fields = client.row_description_fields(messages)
        assert [field[4] for field in fields] == [16, 24, 32, 32, -1, -1, 24]

        point = struct.pack("!dd", 4.5, -2.25)
        line = struct.pack("!ddd", 1.0, -1.0, 0.0)
        lseg = struct.pack("!dddd", 0.0, 0.0, 1.0, 2.0)
        box = struct.pack("!dddd", 2.0, 3.0, 0.0, 0.0)
        path = b"\0" + struct.pack("!i", 2) + lseg
        polygon = struct.pack("!i", 3) + struct.pack(
            "!dddddd", 0.0, 0.0, 1.0, 0.0, 0.0, 1.0)
        circle = struct.pack("!ddd", 1.0, 2.0, 3.0)
        for name, cast, oid, raw in [
                ("point", "point", 600, point),
                ("line", "line", 628, line),
                ("lseg", "lseg", 601, lseg),
                ("box", "box", 603, box),
                ("path", "path", 602, path),
                ("polygon", "polygon", 604, polygon),
                ("circle", "circle", 718, circle)]:
            client.extended_query_binary_parameter(
                server["sock"], name, f"SELECT $1::{cast}", oid, raw, raw)

        bad_parameter("bad_point_text", "SELECT $1::point", 600,
                      b"(1,2]", False, b"22P02")
        bad_parameter("bad_line_binary", "SELECT $1::line", 628,
                      struct.pack("!ddd", 0.0, 0.0, 1.0), True, b"22P03")
        bad_parameter("bad_path_binary", "SELECT $1::path", 602,
                      b"\2" + struct.pack("!i", 1) +
                      struct.pack("!dd", 0.0, 0.0), True, b"22P03")
        bad_parameter("bad_circle_binary", "SELECT $1::circle", 718,
                      struct.pack("!ddd", 0.0, 0.0, -1.0), True, b"22P03")
        bad_parameter("bad_point_nan", "SELECT $1::point", 600,
                      struct.pack("!dd", math.nan, 0.0), True, b"22P03")
        print("[GEOMETRIC PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
