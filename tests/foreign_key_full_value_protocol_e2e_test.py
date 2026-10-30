"""Single-PK foreign-key index hits must match complete visible parent values."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("fk_full_value_runner", root / "tests/compat/pg_diff_runner.py")
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
    checks, failures = 0, []

    def report(good, sql, detail):
        nonlocal checks
        checks += 1
        print("FK_FULL_VALUE_PROTOCOL", checks, sql, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((sql, detail))

    def execute(sql, tag=None, state=None, rows=None, mode="simple"):
        if mode == "simple":
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
            packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, int(mode == "binary")))
            packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
            sock.sendall(packet + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body).get(b"C") for kind, body in messages if kind == b"E"]
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        data = client.data_row_values(messages)
        good = (errors == ([state.encode()] if state else []) and
                tags == ([tag] if tag is not None else []) and
                messages[-1] == (b"Z", b"I"))
        if state:
            good = good and not data
        if rows is not None:
            good = good and data == rows
        if mode != "simple":
            good = good and sum(kind == b"1" for kind, _ in messages) == 1
            good = good and sum(kind == b"2" for kind, _ in messages) == 1
        report(good, mode + " " + sql, (errors, tags, data))

    families = [
        ("text", "abcdefghijklmnopqrstA", "abcdefghijklmnopqrstB"),
        ("varchar(128)", "abcdefghijklmnopqrstA", "abcdefghijklmnopqrstB"),
        ("char(32)", "abcdefghijklmnopqrstA", "abcdefghijklmnopqrstB"),
        ("text", "abcdefghijklmnopqrst", "abcdefghijklmnopqrstZ"),
        ("text", "abcdefghijklmnopqrs", "abcdefghijklmnopqrst"),
        ("text", "汉汉汉汉汉汉abA", "汉汉汉汉汉汉abB"),
        ("text", "a" * 127 + "x", "a" * 127 + "y"),
        ("numeric(38,0)", "123456789012345678901", "123456789012345678902"),
        ("inet", "192.0.2.1", "192.0.2.2"),
        ("cidr", "192.0.2.0/24", "198.51.100.0/24"),
        ("macaddr", "08:00:2b:01:02:03", "08:00:2b:01:02:04"),
        ("uuid", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"),
        ("macaddr8", "08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:06"),
        ("pg_lsn", "0/16B6C50", "0/16B6C51"),
    ]
    try:
        for family, (kind, present, missing) in enumerate(families):
            parent = "full_parent_" + str(family)
            create = "CREATE " + ("TEMP " if reference else "") + "TABLE "
            execute(create + parent + "(v " + kind + " PRIMARY KEY)", "CREATE TABLE")
            execute("INSERT INTO " + parent + " VALUES('" + present + "')", "INSERT 0 1")
            for mode_index, mode in enumerate(("simple", "text", "binary")):
                child = "full_child_" + str(family) + "_" + str(mode_index)
                execute(create + child + "(id integer PRIMARY KEY,v " + kind +
                        " REFERENCES " + parent + "(v))", "CREATE TABLE")
                execute("INSERT INTO " + child + " VALUES(1,'" + missing + "')",
                        state="23503", mode=mode)
                execute("SELECT id FROM " + child, "SELECT 0", rows=[])
                # Keep rejected effects visible; the old implementation must
                # fail this check rather than silently cleaning away the bug.
                execute("INSERT INTO " + child + " VALUES(2,'" + present + "')",
                        "INSERT 0 1", mode=mode)
                execute("INSERT INTO " + child + " VALUES(3,NULL)", "INSERT 0 1", mode=mode)
                execute("SELECT id FROM " + child + " ORDER BY id", "SELECT 2",
                        rows=[[b"2"], [b"3"]])
                execute("UPDATE " + child + " SET v='" + missing + "' WHERE id=2",
                        state="23503", mode=mode)
                execute("SELECT id FROM " + child + " ORDER BY id", "SELECT 2",
                        rows=[[b"2"], [b"3"]])
        print("FK_FULL_VALUE_PROTOCOL_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 364 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
