"""SHOW exposes live parallel configuration, including prepared metadata."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("parallel_show_runner", root / "tests/compat/pg_diff_runner.py")
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
    controls, failures = 0, []
    parameter = "max_parallel_workers_per_gather"
    aliases = ("SHOW " + parameter, "SHOW " + parameter.upper(), 'SHOW "' + parameter.upper() + '";')

    def execute(sql, extended=False):
        if not extended:
            return client.simple_query(sock, sql)
        sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                     client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                     client.typed(b"D", b"P\0") +
                     client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        return client.read_until_ready(sock)

    def setup(sql):
        result = runner.decode_wire_result(execute(sql), include_types=True)
        assert result[1] is None, (sql, result)
        return result

    def check(sql, value=None, extended=False, ready=b"I", state=None, settings=False):
        nonlocal controls
        controls += 1
        messages = execute(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        good = result[1] == state and messages[-1] == (b"Z", ready)
        if state is None:
            expected = ([[str(value), None]], ["setting", "unit"], "SELECT 1", [25, 25]) if settings else (
                [[str(value)]], [parameter], "SHOW", [25])
            good = good and (result[0], result[3], result[4], result[5]) == expected
        print("SHOW_PARALLEL", controls, sql, extended, "pass=", good, "actual=", result, flush=True)
        if not good:
            failures.append((sql, extended, result))

    def describe(sql, ready=b"I"):
        nonlocal controls
        controls += 1
        sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                     client.typed(b"D", b"S\0") + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages, include_types=True)
        good = result[1] is None and result[0] == [] and result[3] == [parameter] and result[5] == [25]
        good = good and messages[-1] == (b"Z", ready) and any(kind == b"T" for kind, _ in messages)
        print("SHOW_PARALLEL_DESCRIBE", controls, sql, "pass=", good, "actual=", result, flush=True)
        if not good:
            failures.append(("describe", sql, result))

    try:
        for workers in (0, 2, 7):
            if reference:
                setup("SET " + parameter + "=" + str(workers))
            else:
                setup("ALTER SYSTEM SET " + parameter + "=" + str(workers))
                reloaded = setup("SELECT pg_reload_conf()")
                assert reloaded[0] == [["t"]] and reloaded[5] == [16], reloaded
            for sql in aliases:
                for extended in (False, True):
                    check(sql, workers, extended)
                describe(sql)
            for extended in (False, True):
                check("SELECT setting,unit FROM pg_catalog.pg_settings WHERE name='" + parameter + "'",
                      workers, extended, settings=True)
        setup("BEGIN")
        for extended in (False, True):
            check(aliases[0], 7, extended, ready=b"T")
        describe(aliases[0], ready=b"T")
        setup("ROLLBACK")
        for extended in (False, True):
            for sql in (aliases[0] + " junk", aliases[2] + " junk"):
                check(sql, extended=extended, state="42601")
        assert controls == 40, controls
        assert not failures, failures
        print("[SHOW PARALLEL WORKERS] all 40 controls passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
