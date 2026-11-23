#!/usr/bin/env python3
"""Ordinary table scans with repeated binary text and integer inputs."""

import importlib.util
from pathlib import Path
import socket
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

    def simple(statement, tag, ready=b"I", connection=sock):
        frames = client.simple_query(connection, statement)
        rows, state, _, _, actual = runner.decode_wire_result(frames)
        assert (state, actual, frames[-1]) == (None, tag, (b"Z", ready)), frames
        return rows

    def stats():
        frames = client.simple_query(sock, "SELECT * FROM pg_catalog.pg_stat_tables")
        rows, state, _, _, _ = runner.decode_wire_result(frames)
        assert state is None and frames[-1] == (b"Z", b"I"), frames
        row, = (row for row in rows if row[0] == table)
        return [int(value) for value in row[1:5]]

    def prepare(name, statement, types=(25, 20, 20)):
        sock.sendall(typed(b"P", name + b"\0" + statement.encode() + b"\0" +
                           pack("!H" + "I" * len(types), len(types), *types)) + typed(b"S"))
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

        simple("CREATE TABLE stored_filter_ranges(n smallint, ord bigint, qty integer)", "CREATE TABLE")
        prefix = ",".join(f"({n},{2**53 + n},1)" for n in range(64))
        tail = "(-32768,1,1),(NULL,2,1),(32767,NULL,1),(100,3,0),(101,4,NULL)," \
               "(32767,9223372036854775807,2147483647)"
        simple(f"INSERT INTO stored_filter_ranges VALUES {prefix},{tail}", "INSERT 0 70")
        clustered = "SELECT ord FROM stored_filter_ranges WHERE n>=0 AND qty>0 " \
                    "AND ord IS NOT NULL ORDER BY ord OFFSET 0 FETCH FIRST 100 ROWS WITH TIES"
        expected = [[str(2**53 + n)] for n in range(64)] + [[str(2**63 - 1)]]
        assert simple(clustered, "SELECT 65") == expected
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + clustered, "EXPLAIN")
        assert any("StoredFilter" in row[0] and "rows=65" in row[0] for row in plan), plan
        assert any("TableScan" in row[0] and "rows=70" in row[0] for row in plan), plan

        prepare(b"length_scan", f"SELECT ord FROM {table} WHERE shop=$1 AND ord>=$2 AND $3>=ord "
                "AND length(shop)>1 ORDER BY ord")
        assert probe("青木", 0, 10, [9], b"length_scan") == [1, 7, 0, 0]
        assert probe("", -(2**63), 10, [], b"length_scan") == [1, 7, 0, 0]
        assert probe("O'Brien", 9007199254740993, 9007199254740993,
                     [9007199254740993], b"length_scan") == [1, 7, 0, 0]
        assert simple(f"SELECT count(*) FROM {table} WHERE shop IS NULL AND ord>0", "SELECT 1") == [["1"]]

        unfiltered = f"SELECT count(*) AS total, count(shop), count(ord) FROM {table}"
        filtered = f"SELECT count(*), count(ord) FILTER (WHERE shop IS NOT NULL) FROM {table} " \
                   "WHERE 3<length(shop) AND ord>0"
        for sql, expected in ((unfiltered, [["7", "6", "6"]]), (filtered, [["3", "3"]])):
            before = stats()
            assert simple(sql, "SELECT 1") == expected
            assert [right - left for left, right in zip(before, stats(), strict=True)] == [1, 7, 0, 0]
            plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + sql, "EXPLAIN")
            assert any("StoredCount" in row[0] and "rows=1" in row[0] for row in plan), plan
            assert any("TableScan" in row[0] and "rows=7" in row[0] for row in plan), plan
            assert all("TypedFilter" not in row[0] for row in plan), plan

        prepare(b"count", f"SELECT count(*) AS total, count(ord) AS numbers FROM {table} "
                "WHERE shop=$1 AND ord>=$2 AND $3>=ord")
        for shop, lower, upper, expected in (("青木", 9, 9, 1), ("O'Brien", 0, 2**63 - 1, 2),
                                             ("absent", 0, 2**63 - 1, 0), ("青木", 10, 10, 0)):
            value = shop.encode()
            args = pack("!I", len(value)) + value + pack("!IqIq", 8, lower, 8, upper)
            binding = b"\0count\0" + pack("!HHH", 1, 1, 3) + args + pack("!HH", 1, 1)
            before = stats()
            sock.sendall(typed(b"B", binding) + typed(b"D", b"P\0") +
                         typed(b"E", b"\0" * 5) + typed(b"S"))
            frames = client.read_until_ready(sock)
            assert [kind for kind, _ in frames] == [b"2", b"T", b"D", b"C", b"Z"], frames
            _, _, _, fields, _, oids = runner.decode_wire_result([frames[1]], include_types=True)
            assert (fields, oids) == (["total", "numbers"], [20, 20]), frames
            assert frames[2:] == [(b"D", pack("!Hiqiq", 2, 8, expected, 8, expected)),
                                  (b"C", b"SELECT 1\0"), (b"Z", b"I")], frames
            assert [right - left for left, right in zip(before, stats(), strict=True)] == [1, 7, 0, 0]

        for conditions, expected, reads in (
            ("ord<-9223372036854775808", ["0", "0"], False),
            ("ord>7 AND ord<8", ["0", "0"], False),
            ("ord IS NULL AND ord>=0", ["0", "0"], False),
            ("shop<>'青木' AND shop='青木'", ["0", "0"], False),
            ("length(shop)<0", ["0", "0"], False),
            ("ord>70 AND ord<80", ["0", "0"], True),
            ("ord>=9223372036854775806", ["0", "0"], True),
            ("ord>=9007199254740993 AND ord<=9007199254740993", ["1", "1"], True),
            ("ord=-9223372036854775808", ["1", "1"], True),
        ):
            sql = f"SELECT count(*), count(ord) FROM {table} WHERE {conditions}"
            before = stats()
            assert simple(sql, "SELECT 1") == [expected], sql
            assert [right - left for left, right in zip(before, stats(), strict=True)] == [int(reads), 7 * reads, 0, 0], sql

        mixed = f"SELECT count(*) FILTER (WHERE ord<0), count(*) FROM {table} WHERE ord>0"
        all_empty = f"SELECT count(*) FILTER (WHERE ord<0), count(ord) FILTER (WHERE ord IS NULL) FROM {table} WHERE ord>0"
        before = stats()
        assert simple(mixed, "SELECT 1") == [["0", "5"]]
        assert [right - left for left, right in zip(before, stats(), strict=True)] == [1, 7, 0, 0]
        before = stats()
        assert simple(all_empty, "SELECT 1") == [["0", "0"]]
        assert stats() == before
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + all_empty, "EXPLAIN")
        assert any("StoredCount" in row[0] and "emptyInput=true" in row[0] for row in plan), plan

        prepare(b"bounds", f"SELECT count(*) FROM {table} WHERE ord>=$1 AND ord<=$2", (20, 20))
        for lower, upper, expected, reads in ((10, 9, 0, False), (7, 9, 3, True),
                                               (-(2**63), -(2**63), 1, True), (5, 2, 0, False)):
            args = pack("!IqIq", 8, lower, 8, upper)
            binding = b"\0bounds\0" + pack("!HHH", 1, 1, 2) + args + pack("!HH", 1, 1)
            before = stats()
            sock.sendall(typed(b"B", binding) + typed(b"D", b"P\0") +
                         typed(b"E", b"\0" * 5) + typed(b"S"))
            frames = client.read_until_ready(sock)
            assert [kind for kind, _ in frames] == [b"2", b"T", b"D", b"C", b"Z"], frames
            assert frames[2:] == [(b"D", pack("!Hiq", 1, 8, expected)),
                                  (b"C", b"SELECT 1\0"), (b"Z", b"I")], frames
            assert [right - left for left, right in zip(before, stats(), strict=True)] == [int(reads), 7 * reads, 0, 0]

        with socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout()) as writer:
            client.startup(writer, "alice", "info")
            simple("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T")
            assert simple(unfiltered, "SELECT 1", b"T") == [["7", "6", "6"]]
            simple(f"INSERT INTO {table} VALUES ('new',77)", "INSERT 0 1", connection=writer)
            assert simple(unfiltered, "SELECT 1", b"T") == [["7", "6", "6"]]
            simple("SAVEPOINT count_update", "SAVEPOINT", b"T")
            simple(f"UPDATE {table} SET shop=NULL WHERE ord=9", "UPDATE 1", b"T")
            assert simple(unfiltered, "SELECT 1", b"T") == [["7", "5", "6"]]
            simple("ROLLBACK TO SAVEPOINT count_update", "ROLLBACK", b"T")
            assert simple(unfiltered, "SELECT 1", b"T") == [["7", "6", "6"]]
            simple("COMMIT", "COMMIT")
            assert simple(unfiltered, "SELECT 1") == [["8", "7", "7"]]

        simple("CREATE SCHEMA stored_predicate_routine", "CREATE SCHEMA")
        simple("CREATE FUNCTION stored_predicate_routine.length(s text) RETURNS integer LANGUAGE SQL "
               "IMMUTABLE AS $$SELECT CASE WHEN s IS NULL THEN 1 ELSE 0 END$$", "CREATE FUNCTION")
        stored = f"SELECT ord FROM {table} WHERE stored_predicate_routine.length(shop)>0"
        assert simple(stored, "SELECT 1") == [["8"]]
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + stored, "EXPLAIN")
        assert all("StoredFilter" not in row[0] for row in plan), plan
        routine_count = f"SELECT count(*) FROM {table} WHERE stored_predicate_routine.length(shop)>0"
        assert simple(routine_count, "SELECT 1") == [["1"]]
        plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + routine_count, "EXPLAIN")
        assert all("StoredCount" not in row[0] for row in plan), plan

        simple("CREATE TABLE counted_access(id integer, note text)", "CREATE TABLE")
        simple("INSERT INTO counted_access VALUES (1,NULL),(2,'private'),(3,'private')", "INSERT 0 3")
        simple("CREATE ROLE count_reader", "CREATE ROLE")
        simple("GRANT SELECT ON counted_access TO count_reader", "GRANT")
        simple("CREATE POLICY count_visible ON counted_access FOR SELECT TO count_reader USING (id<2)",
               "CREATE POLICY")
        simple("ALTER TABLE counted_access ENABLE ROW LEVEL SECURITY", "ALTER TABLE")
        simple("SET ROLE count_reader", "SET")
        for tail in ("", " WHERE id>=0"):
            sql = "SELECT count(*), count(note) FROM counted_access" + tail
            assert simple(sql, "SELECT 1") == [["1", "0"]]
            plan = simple("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + sql, "EXPLAIN")
            assert any("Aggregate" in row[0] and "rows=1" in row[0] for row in plan), plan
        simple("RESET ROLE", "RESET")

        simple("CREATE TABLE counted_parent(id integer, note text)", "CREATE TABLE")
        simple("CREATE TABLE counted_child() INHERITS (counted_parent)", "CREATE TABLE")
        simple("INSERT INTO counted_parent VALUES (1,'parent')", "INSERT 0 1")
        simple("INSERT INTO counted_child VALUES (2,NULL)", "INSERT 0 1")
        for base, expected in (("counted_parent", [["2", "1"]]), ("ONLY counted_parent", [["1", "1"]])):
            assert simple("SELECT count(*), count(note) FROM " + base, "SELECT 1") == expected
        simple("SET ROLE count_reader", "SET")
        frames = client.simple_query(sock, "SELECT count(*) FROM counted_parent WHERE id>2147483647")
        assert runner.decode_wire_result(frames)[1] == "42501" and frames[-1] == (b"Z", b"I"), frames
        simple("RESET ROLE", "RESET")
    finally:
        runner.stop_ours(server)
    print("[STORED LITERAL FILTER PROTOCOL] exact binary inputs and results passed")


if __name__ == "__main__":
    main()
