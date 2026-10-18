"""Known scalar SHOW GUCs: describe schema without execution and preserve bytes."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("show_guc_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
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

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("SHOW_GUC_PROTOCOL", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def extended(sql, format_code=0, describe_only=None):
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        if describe_only == "statement":
            packet += client.typed(b"D", b"S\0")
        else:
            packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHh", 0, 0, 1, format_code))
            packet += client.typed(b"D", b"P\0")
            if describe_only is None:
                packet += client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def description(body):
        count = struct.unpack_from("!H", body)[0]
        position, fields = 2, []
        for _ in range(count):
            end = body.index(b"\0", position)
            name = body[position:end].decode()
            position = end + 1
            values = struct.unpack_from("!IhIhih", body, position)
            position += 18
            fields.append((name,) + values)
        assert position == len(body)
        return fields

    def data(body):
        count = struct.unpack_from("!H", body)[0]
        position, values = 2, []
        for _ in range(count):
            length = struct.unpack_from("!i", body, position)[0]
            position += 4
            if length == -1:
                values.append(None)
            else:
                assert length >= 0
                values.append(body[position:position + length])
                position += length
        assert position == len(body)
        return values

    def utility(sql):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[1] is None and result[0] == []
        good = good and [body for kind, body in messages if kind == b"C"] == [b"SET\0"]
        report(good, sql, result)

    def query(name, value, mode, uppercase=False):
        sql = "SHOW " + (name.upper() if uppercase else name)
        messages = client.simple_query(sock, sql) if mode == "simple" else extended(sql, mode)
        kinds = [kind for kind, _ in messages]
        fields = [description(body) for kind, body in messages if kind == b"T"]
        rows = [data(body) for kind, body in messages if kind == b"D"]
        format_code = 0 if mode == "simple" else mode
        expected = [[(name, 0, 0, 25, -1, -1, format_code)]]
        good = fields == expected and rows == [[value.encode()]] and messages[-1] == (b"Z", b"I")
        good = good and not any(kind in (b"E", b"N", b"n", b"s") for kind in kinds)
        good = good and [body for kind, body in messages if kind == b"C"] == [b"SHOW\0"]
        if mode != "simple":
            good = good and kinds.count(b"1") == kinds.count(b"2") == 1
        report(good, (sql, mode), (fields, rows, kinds))

    def describe(name, kind, format_code=0):
        messages = extended("SHOW " + name, format_code, kind)
        kinds = [key for key, _ in messages]
        fields = [description(body) for key, body in messages if key == b"T"]
        expected_format = 0 if kind == "statement" else format_code
        good = fields == [[(name, 0, 0, 25, -1, -1, expected_format)]]
        good = good and messages[-1] == (b"Z", b"I") and kinds.count(b"1") == 1
        good = good and not any(key in (b"D", b"C", b"E", b"N", b"n", b"s") for key in kinds)
        if kind == "statement":
            good = good and [body for key, body in messages if key == b"t"] == [b"\0\0"] and b"2" not in kinds
        else:
            good = good and kinds.count(b"2") == 1 and b"t" not in kinds
        report(good, ("Describe only", name, kind, format_code), (fields, kinds))

    try:
        cases = [("application_name", "'" + value + "'", value) for value in
                 ("", "NULL", "ordinary", "two words", '"quoted"', "ERROR: literal")]
        cases += [("search_path", value, value) for value in
                  ("public", "pg_catalog", '"line\nbreak"', '"tab\tname"',
                   '"中文"', '"two words"', '"a""b"')]
        cases += [("client_encoding", "'UTF8'", "UTF8"), ("lc_monetary", "'C'", "C")]
        for name, literal, value in cases:
            utility("SET " + name + " TO " + literal)
            for uppercase in (False, True):
                for mode in ("simple", 0, 1):
                    query(name, value, mode, uppercase)
            describe(name, "statement")
            describe(name, "portal", 0)
            describe(name, "portal", 1)
        print("SHOW_GUC_PROTOCOL_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 150 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
