#!/usr/bin/env python3
"""Ordinary table scans with repeated binary text and integer inputs."""

import importlib.util
from pathlib import Path
from struct import pack, unpack


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "stored_filter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    typed = client.typed
    table = "stored_filter_inputs"

    def simple(statement, tag):
        frames = client.simple_query(sock, statement)
        rows, state, _, _, actual = runner.decode_wire_result(frames)
        assert (state, actual, frames[-1]) == (None, tag, (b"Z", b"I")), frames
        return rows

    def stats():
        frames = client.simple_query(sock, "SELECT * FROM pg_catalog.pg_stat_tables")
        rows, state, _, _, _ = runner.decode_wire_result(frames)
        assert state is None and frames[-1] == (b"Z", b"I"), frames
        row, = (row for row in rows if row[0] == table)
        return [int(value) for value in row[1:5]]

    def prepare(name, statement):
        sock.sendall(typed(b"P", name + b"\0" + statement.encode() + b"\0" +
                           pack("!HIII", 3, 25, 20, 20)) + typed(b"S"))
        assert client.read_until_ready(sock) == [(b"1", b""), (b"Z", b"I")]

    def probe(shop, lower, upper, expected, name=b"scan"):
        before = stats()
        payload = b"\0" + name + b"\0" + pack("!HHH", 1, 1, 3)
        value = None if shop is None else shop.encode()
        parts = [value] + [None if n is None else pack("!q", n) for n in (lower, upper)]
        for part in parts:
            payload += pack("!i", -1) if part is None else pack("!I", len(part)) + part
        payload += pack("!HH", 1, 1)
        sock.sendall(typed(b"B", payload) + typed(b"D", b"P\0") +
                     typed(b"E", b"\0" + b"\0" * 4) + typed(b"S"))
        frames = client.read_until_ready(sock)
        expected_sequence = [b"2", b"T"] + [b"D"] * len(expected) + [b"C", b"Z"]
        assert [kind for kind, _ in frames] == expected_sequence, frames
        assert frames[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")]
        rows = []
        for kind, body in frames:
            if kind == b"D":
                columns, length, number = unpack("!Hiq", body)
                assert (columns, length) == (1, 8)
                rows.append(number)
        assert rows == expected, (shop, lower, upper, rows, expected)
        return [right - left for left, right in zip(before, stats(), strict=True)]

    try:
        simple(f"CREATE TABLE {table}(shop text, ord bigint)", "CREATE TABLE")
        simple(f"INSERT INTO {table} VALUES ('O''Brien',9007199254740992),"
               "('O''Brien',9007199254740993),('',-9223372036854775808),('NULL',7),"
               "(NULL,8),('青木',9),('青木',NULL)", "INSERT 0 7")
        statement = f"SELECT ord FROM {table} WHERE shop=$1 AND ord>=$2 AND $3>=ord ORDER BY ord"
        prepare(b"scan", statement)
        for shop, lower, upper, expected in (
            ("O'Brien", 9007199254740993, 9007199254740993, [9007199254740993]),
            ("", -(2**63), 0, [-(2**63)]),
            ("NULL", 0, 2**63 - 1, [7]),
            ("青木", 0, 10, [9]),
            ("absent", 0, 10, []),
        ):
            assert probe(shop, lower, upper, expected) == [1, 7, 0, 0]
        probe(None, 0, 10, [])
        probe("青木", None, 10, [])
        assert probe("O'Brien", 9007199254740992, 9007199254740993,
                     [9007199254740992, 9007199254740993]) == [1, 7, 0, 0]

        predicate = f"SELECT ord FROM {table} WHERE 3<length(shop) AND ord>0 ORDER BY ord"
        assert simple(predicate, "SELECT 3") == [["7"], ["9007199254740992"], ["9007199254740993"]]
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + predicate, "EXPLAIN")
        assert any("StoredFilter" in row[0] and "rows=3" in row[0] for row in plan), plan
        assert any("TableScan" in row[0] and "rows=7" in row[0] for row in plan), plan

        prepare(b"length_scan", f"SELECT ord FROM {table} WHERE shop=$1 AND ord>=$2 AND $3>=ord "
                "AND length(shop)>1 ORDER BY ord")
        assert probe("青木", 0, 10, [9], b"length_scan") == [1, 7, 0, 0]
        assert probe("", -(2**63), 10, [], b"length_scan") == [1, 7, 0, 0]
        assert probe("O'Brien", 9007199254740993, 9007199254740993,
                     [9007199254740993], b"length_scan") == [1, 7, 0, 0]
        assert simple(f"SELECT count(*) FROM {table} WHERE shop IS NULL AND ord>0", "SELECT 1") == [["1"]]

        simple("CREATE SCHEMA stored_predicate_routine", "CREATE SCHEMA")
        simple("CREATE FUNCTION stored_predicate_routine.length(s text) RETURNS integer LANGUAGE SQL "
               "IMMUTABLE AS $$SELECT CASE WHEN s IS NULL THEN 1 ELSE 0 END$$", "CREATE FUNCTION")
        stored = f"SELECT ord FROM {table} WHERE stored_predicate_routine.length(shop)>0"
        assert simple(stored, "SELECT 1") == [["8"]]
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + stored, "EXPLAIN")
        assert all("StoredFilter" not in row[0] for row in plan), plan
    finally:
        runner.stop_ours(server)
    print("[STORED LITERAL FILTER PROTOCOL] exact binary inputs and results passed")


if __name__ == "__main__":
    main()
