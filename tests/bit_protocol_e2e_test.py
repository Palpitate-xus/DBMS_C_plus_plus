#!/usr/bin/env python3
"""BIT/VARBIT literals, typmods, operators, and binary wire I/O."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "bit_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        messages = client.simple_query(server["sock"], sql)
        return messages, runner.decode_wire_result(messages, include_types=True)

    def expect_error(sql, expected):
        _, decoded = execute(sql)
        rows, state, message, _, _, _ = decoded
        assert rows == [] and state == expected, (sql, state, message, rows)

    try:
        _, decoded = execute(
            "CREATE TABLE bit_wire (id INT PRIMARY KEY, fixed BIT(5), varying VARBIT);")
        assert decoded[1] is None, decoded
        _, decoded = execute(
            "INSERT INTO bit_wire VALUES "
            "(1, B'10101', B''), (2, X'0f'::bit(5), B'1011111011');")
        assert decoded[1] is None, decoded

        messages, decoded = execute(
            "SELECT fixed, varying FROM bit_wire ORDER BY id;")
        rows, state, message, headers, tag, type_oids = decoded
        assert state is None, message
        assert rows == [["10101", ""], ["00001", "1011111011"]], rows
        assert headers == ["fixed", "varying"] and tag == "SELECT 2"
        assert type_oids == [1560, 1562], type_oids
        fields = client.row_description_fields(messages)
        assert fields[0][4:] == (-1, 9, 0), fields[0]
        assert fields[1][4:] == (-1, -1, 0), fields[1]

        _, decoded = execute(
            "SELECT B'1010' & B'1100' AS a, B'1010' | B'0101' AS o, "
            "B'1010' # B'1100' AS x, ~B'0011' AS n, "
            "B'1010' << 2 AS l, B'1010' >> 2 AS r, "
            "B'10' || B'01' AS c, get_bit(B'1010', 0) AS g, "
            "set_bit(B'1010', 1, 0) AS s, bit_count(B'101101') AS bc, "
            "octet_length(B'1011111011') AS ol, "
            "substring(B'10101', 2, 3) AS sub;")
        rows, state, message, _, tag, type_oids = decoded
        assert state is None, message
        assert rows == [[
            "1000", "1111", "0110", "1100", "1000", "0010",
            "1001", "1", "1010", "4", "2", "010",
        ]], rows
        assert type_oids == [
            1560, 1560, 1560, 1560, 1560, 1560, 1560, 23, 1560, 20, 23, 1560,
        ], type_oids
        assert tag == "SELECT 1", tag

        expect_error("SELECT B'102';", "22P02")
        expect_error("SELECT B'1' & B'00';", "22026")
        expect_error("CREATE TABLE bad_bit (v BIT(0));", "22023")

        raw = struct.pack("!I", 5) + bytes([0b10101000])
        client.extended_query_binary_parameter(
            server["sock"], "bit", "SELECT $1::bit varying", 1562, raw, raw)
        print("[BIT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
