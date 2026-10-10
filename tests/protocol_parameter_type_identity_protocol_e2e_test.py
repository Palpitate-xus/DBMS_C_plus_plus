"""Parse OIDs survive Bind SQL lowering for NULL and non-NULL values."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("parameter_identity_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    cases = [(21, b"7", "7", 0), (21, None, None, 0),
             (21, struct.pack("!h", 7), "7", 1), (20, b"7", "7", 0),
             (700, b"1.5", "1.5", 0), (1043, b"", "", 0),
             (1043, None, None, 0), (1007, None, None, 0),
             (1007, b"{1,2}", "{1,2}", 0), (19, None, None, 0),
             (21, b"0x2a", "42", 0), (23, b"  +1_234  ", "1234", 0),
             (20, b"-9223372036854775808", "-9223372036854775808", 0),
             (1560, b"010", "010", 0), (1042, b"abc", "abc", 0)]
    try:
        for oid, raw, expected, format_code in cases:
            parse = b"\0SELECT $1 AS p\0" + struct.pack("!HI", 1, oid)
            datum = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
            bind = b"\0\0" + struct.pack("!HHH", 1, format_code, 1) + datum + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S\0") +
                         client.typed(b"B", bind) + client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            actual = runner.decode_wire_result(messages, include_types=True)
            descriptions = [client.row_description_fields([message]) for message in messages if message[0] == b"T"]
            good = actual[1] is None and actual[0] == [[expected]] and len(descriptions) == 2
            good = good and all(len(fields) == 1 and fields[0][3] == oid for fields in descriptions)
            print("PARAMETER_TYPE_IDENTITY", oid, raw, format_code, actual, "pass=", good, flush=True)
            if not good:
                failures.append((oid, raw, expected, actual, descriptions))
        for oid, raw in [(21, b"32768"), (23, b"2147483648"), (20, b"9223372036854775808")]:
            parse = b"\0SELECT $1 AS p\0" + struct.pack("!HI", 1, oid)
            bind = b"\0\0" + struct.pack("!HH", 0, 1) + struct.pack("!i", len(raw)) + raw + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) + client.typed(b"S"))
            actual = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
            good = actual[1] == "22003" and actual[0] == [] and actual[4] is None
            print("PARAMETER_TYPE_OVERFLOW", oid, raw, actual, "pass=", good, flush=True)
            if not good:
                failures.append((oid, raw, "22003", actual))
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)
    print("PARAMETER_TYPE_IDENTITY_CHECKED=", len(cases) + 3, "FAILED=", len(failures), flush=True)
    assert not failures, failures


if __name__ == "__main__":
    main()
