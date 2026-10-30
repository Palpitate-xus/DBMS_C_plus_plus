"""Parameterized integer PK access retains values, NULLs and snapshot fallback."""
import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("parameter_index_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    wire = runner.load_protocol_client()
    server = runner.start_ours(wire)
    sock = server["sock"]
    other = None
    controls = 0

    def check(messages, expected=None, ready=b"I", state=None):
        nonlocal controls
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, result
        assert messages[-1] == (b"Z", ready), messages[-1]
        if expected is not None:
            assert result[0] == expected, (result, expected)
        controls += 1
        return result[0]

    def query(sql, expected=None, ready=b"I", connection=None):
        return check(wire.simple_query(connection or sock, sql), expected, ready)

    def prepare(name, sql, oid=23, ready=b"I"):
        body = name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)
        sock.sendall(wire.typed(b"P", body) + wire.typed(b"D", b"S" + name.encode() + b"\0") + wire.typed(b"S"))
        messages = wire.read_until_ready(sock)
        check(messages, [], ready)
        assert sum(kind == b"1" for kind, _ in messages) == 1

    def execute(name, value, expected=None, ready=b"I", state=None, binary=False, width=4):
        raw = None if value is None else (int(value).to_bytes(width, "big", signed=True)
                                         if binary else str(value).encode())
        body = b"\0" + name.encode() + b"\0"
        body += struct.pack("!HHH", 1, int(binary), 1)
        body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        body += struct.pack("!H", 0)
        sock.sendall(wire.typed(b"B", body) + wire.typed(b"D", b"P\0") +
                     wire.typed(b"E", b"\0" + struct.pack("!I", 0)) + wire.typed(b"S"))
        return check(wire.read_until_ready(sock), expected, ready, state)

    def index_scans():
        rows = query("SELECT * FROM pg_catalog.pg_stat_tables")
        return int(next(row for row in rows if row[0] == "parameter_index_rows")[3])

    try:
        query("CREATE TABLE parameter_index_rows(id INTEGER PRIMARY KEY,n INTEGER,t TEXT)")
        query("INSERT INTO parameter_index_rows VALUES(1,NULL,NULL),(2,0,''),(-1,-2147483648,'NULL'),(0,3,'x')")
        prepare("pk", "SELECT id,n,t FROM parameter_index_rows WHERE id=$1")
        before = index_scans()
        execute("pk", 1, [["1", None, None]])
        assert index_scans() == before + 1, "prepared PK lookup did not use its physical index"
        for binary in (False, True):
            for value, expected in ((2, [["2", "0", ""]]), (-1, [["-1", "-2147483648", "NULL"]]),
                                    (0, [["0", "3", "x"]]), (999, []), (None, []), (1, [["1", None, None]])):
                execute("pk", value, expected, binary=binary)
        execute("pk", "invalid", [], state="22P02")
        execute("pk", 2, [["2", "0", ""]])

        prepare("reverse", "SELECT r.id,r.t FROM parameter_index_rows r WHERE $1=r.id")
        execute("reverse", -1, [["-1", "NULL"]])
        prepare("explain", "EXPLAIN ANALYZE SELECT id,n,t FROM parameter_index_rows WHERE id=$1")
        plan = execute("explain", 2)
        assert any("IndexScan" in row[0] for row in plan), plan

        other = socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
        wire.startup(other, "alice", "info")
        query("BEGIN ISOLATION LEVEL REPEATABLE READ", ready=b"T")
        execute("pk", 1, [["1", None, None]], b"T")
        query("UPDATE parameter_index_rows SET id=20 WHERE id=2", connection=other)
        execute("pk", 2, [["2", "0", ""]], b"T")
        execute("pk", 20, [], b"T")
        plan = execute("explain", 2, ready=b"T")
        assert any("TableScan" in row[0] for row in plan), plan
        query("COMMIT")
        execute("pk", 20, [["20", "0", ""]])

        query("BEGIN", ready=b"T")
        query("UPDATE parameter_index_rows SET id=30 WHERE id=20", ready=b"T")
        execute("pk", 30, [["30", "0", ""]], b"T")
        query("ROLLBACK")
        execute("pk", 20, [["20", "0", ""]])
        query("BEGIN", connection=other, ready=b"T")
        before = index_scans()
        execute("pk", 1, [["1", None, None]])
        assert index_scans() == before, "overlapping owner must keep snapshot-safe heap access"
        query("ROLLBACK", connection=other)

        query("BEGIN ISOLATION LEVEL SERIALIZABLE", ready=b"T")
        execute("pk", 1, [["1", None, None]], b"T")
        execute("pk", 999, [], b"T")
        query("COMMIT")

        prepare("wide_parameter", "SELECT id FROM parameter_index_rows WHERE id=$1", 20)
        execute("wide_parameter", 2147483648, [])
        execute("wide_parameter", 20, [["20"]])
        query("CREATE TABLE parameter_index_big(id BIGINT PRIMARY KEY)")
        query("INSERT INTO parameter_index_big VALUES(9007199254740993),(-9223372036854775808)")
        prepare("big", "SELECT id FROM parameter_index_big WHERE id=$1", 20)
        for value in (9007199254740993, -9223372036854775808):
            execute("big", value, [[str(value)]], binary=True, width=8)
        query("CREATE TABLE parameter_index_small(id SMALLINT PRIMARY KEY)")
        query("INSERT INTO parameter_index_small VALUES(-32768),(32767)")
        prepare("small", "SELECT id FROM parameter_index_small WHERE id=$1", 21)
        for value in (-32768, 32767):
            execute("small", value, [[str(value)]], binary=True, width=2)
        query("CREATE TABLE parameter_index_composite(a INTEGER,b INTEGER,PRIMARY KEY(a,b))")
        query("INSERT INTO parameter_index_composite VALUES(1,1),(1,2)")
        prepare("composite", "SELECT b FROM parameter_index_composite WHERE a=$1 ORDER BY b")
        execute("composite", 1, [["1"], ["2"]])
        query("CREATE SEQUENCE parameter_index_calls")
        prepare("volatile_target", "SELECT nextval('parameter_index_calls') FROM parameter_index_rows WHERE id=$1")
        execute("volatile_target", 1, [["1"]])
        execute("volatile_target", 999, [])
        execute("volatile_target", None, [])
        query("SELECT currval('parameter_index_calls')", [["1"]])
        print("[PREPARED PARAMETER INDEX]", controls, "controls passed", flush=True)
    finally:
        if other:
            other.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
