#!/usr/bin/env python3
"""Network-type OIDs, strict input, and PostgreSQL binary wire formats."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "network_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
            "CREATE TABLE network_wire (id INT PRIMARY KEY, host INET, "
            "net CIDR, mac MACADDR, mac8 MACADDR8);")
        assert decoded[1] is None, decoded
        _, decoded = execute(
            "INSERT INTO network_wire VALUES (1, '192.168.1.7/24', "
            "'192.168.1.0/24', '00:00:00:00:00:00', "
            "'08:00:2b:01:02:03:04:05');")
        assert decoded[1] is None, decoded
        messages, decoded = execute(
            "SELECT host, net, mac, mac8 FROM network_wire;")
        rows, state, message, headers, tag, type_oids = decoded
        assert state is None, message
        assert rows == [["192.168.1.7/24", "192.168.1.0/24",
                         "00:00:00:00:00:00",
                         "08:00:2b:01:02:03:04:05"]], rows
        assert headers == ["host", "net", "mac", "mac8"]
        assert tag == "SELECT 1" and type_oids == [869, 650, 829, 774]
        fields = client.row_description_fields(messages)
        assert [field[4] for field in fields] == [-1, -1, 6, 8], fields

        inet = bytes([2, 24, 0, 4, 192, 168, 1, 7])
        cidr = bytes([2, 24, 1, 4, 192, 168, 1, 0])
        mac = bytes.fromhex("08002b010203")
        mac8 = bytes.fromhex("08002b0102030405")
        client.extended_query_binary_parameter(
            server["sock"], "inet", "SELECT $1::inet", 869, inet, inet)
        client.extended_query_binary_parameter(
            server["sock"], "cidr", "SELECT $1::cidr", 650, cidr, cidr)
        client.extended_query_binary_parameter(
            server["sock"], "mac", "SELECT $1::macaddr", 829, mac, mac)
        client.extended_query_binary_parameter(
            server["sock"], "mac8", "SELECT $1::macaddr8", 774, mac8, mac8)

        bad_parameter(
            "bad_cidr", "SELECT $1::cidr", 650, b"192.168.1.1/24",
            False, b"22P02")
        bad_parameter(
            "bad_mac", "SELECT $1::macaddr", 829, b"not-a-mac",
            False, b"22P02")
        bad_parameter(
            "bad_cidr_binary", "SELECT $1::cidr", 650,
            bytes([2, 24, 1, 4, 192, 168, 1, 1]), True, b"22P03")

        _, decoded = execute(
            "SELECT '192.168.1.7/24'::inet << '192.168.0.0/16'::cidr, "
            "'2001:db8::1/64'::inet && '2001:db8::/48'::cidr;")
        assert decoded[1] is None and decoded[0] == [["t", "t"]], decoded
        assert decoded[5] == [16, 16], decoded[5]
        print("[NETWORK TYPES PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
