#!/usr/bin/env python3
"""OVERLAY checks its int4 suffix addition, for text and bytea, on both paths."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "overlay_boundary_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference"]
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

    # These operands individually fit int4. Only their sum may overflow.
    boundaries = (
        ("2147483647", "1", None, "22003"),
        ("2147483646", "2", None, "22003"),
        ("1", "2147483647", None, "22003"),
        ("2", "2147483646", None, "22003"),
        ("2147483647", None, None, "22003"),
        ("2147483646", "1", "abcX", None),
        ("2147483647", "0", "abcX", None),
        ("2147483647", "-1", "abcX", None),
        ("2147483647", "-2147483648", "abcXabc", None),
        ("1", "-2147483648", "Xabc", None),
        ("2", "-1", "aXabc", None),
        ("2", "0", "aXbc", None),
        ("2", "1", "aXc", None),
        ("2", None, "aXc", None),
        ("4", "1", "abcX", None),
        ("0", "2147483647", None, "22011"),
        ("-1", "2147483647", None, "22011"),
        ("NULL::integer", "2147483647", None, None),
        ("2147483647", "NULL::integer", None, None),
    )
    cases = []
    for binary in (False, True):
        source = "'\\x616263'::bytea" if binary else "'abc'::text"
        replacement = "'\\x58'::bytea" if binary else "'X'::text"
        oid = 17 if binary else 25
        for start, count, expected, state in boundaries:
            sql = "SELECT overlay(" + source + " placing " + replacement + " from " + start
            if count is not None:
                sql += " for " + count
            sql += ")"
            value = "\\x" + expected.encode().hex() if binary and expected is not None else expected
            cases.append((sql, value, state, oid))
        for null_source, null_replacement in ((True, False), (False, True)):
            declaration = "bytea" if binary else "text"
            first = "NULL::" + declaration if null_source else source
            second = "NULL::" + declaration if null_replacement else replacement
            cases.append(("SELECT overlay(" + first + " placing " + second +
                          " from 2147483647 for 1)", None, None, oid))
    cases.extend((
        ("SELECT overlay('aé中z' placing '界' from 2)", "a界中z", None, 25),
        ("SELECT overlay('abc' placing '界' from 2147483646)", "abc界", None, 25),
        ("SELECT overlay('abc' placing '界界' from 2147483646)", None, "22003", 25),
        ("SELECT overlay('abc' placing '' from 2147483647)", "abc", None, 25),
        ("SELECT overlay('' placing 'X' from 2147483647)", None, "22003", 25),
    ))
    failures = []
    checks = 0
    try:
        for extended in (False, True):
            for sql, value, state, oid in cases:
                if extended:
                    # Unnamed statement/portal; Sync must recover every error.
                    sock.sendall(
                        client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)) +
                        client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                        client.typed(b"D", b"P\0") +
                        client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                        client.typed(b"S"))
                    messages = client.read_until_ready(sock)
                else:
                    messages = client.simple_query(sock, sql)
                result = runner.decode_wire_result(messages, include_types=True)
                good = result[1] == state and messages[-1] == (b"Z", b"I")
                if state is None:
                    good = good and result[0] == [[value]] and result[5] == [oid]
                    good = good and result[4] == "SELECT 1"
                else:
                    good = good and result[0] == [] and result[4] is None
                    good = good and sum(kind == b"E" for kind, _ in messages) == 1
                checks += 1
                print("OVERLAY_BOUNDARY", "extended" if extended else "simple",
                      sql, result, "PASS", good, flush=True)
                if not good:
                    failures.append((extended, sql, result, value, state, oid))
        assert checks == 94, checks
        print("OVERLAY_BOUNDARY_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
