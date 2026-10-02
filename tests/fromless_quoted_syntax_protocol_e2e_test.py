#!/usr/bin/env python3
"""Quoted data must not split projections, hide aliases, or become WHERE/LIMIT."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fromless_quoted_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    cases = [
        ("SELECT E'it\\'s' AS v;", [["it's"]], ["v"], [25]),
        ("SELECT E'it\\'s, AS WHERE LIMIT OFFSET' AS \"E alias\", 7 AS n;",
         [["it's, AS WHERE LIMIT OFFSET", "7"]], ["E alias", "n"], [25, 23]),
        ("SELECT $Tag$comma, AS WHERE LIMIT OFFSET $tag$ quote ' inside$Tag$ AS \"D alias\", 8 AS n;",
         [["comma, AS WHERE LIMIT OFFSET $tag$ quote ' inside", "8"]], ["D alias", "n"], [25, 23]),
        ("SELECT 'it''s, AS WHERE LIMIT OFFSET' AS \"S alias\", 9 AS n;",
         [["it's, AS WHERE LIMIT OFFSET", "9"]], ["S alias", "n"], [25, 23]),
        ("SELECT 1 AS \"comma, quote\"\" name\", 2 AS n;",
         [["1", "2"]], ['comma, quote" name', "n"], [23, 23]),
        ("SELECT E'it\\'s' AS v WHERE FALSE;", [], ["v"], [25]),
        ("SELECT E'it\\'s, text' AS v WHERE TRUE LIMIT 1 OFFSET 0;",
         [["it's, text"]], ["v"], [25]),
        ("SELECT $$where,as ' literal$$ AS v;", [["where,as ' literal"]], ["v"], [25]),
        ("SELECT E'it\\'s' AS /* outer /* inner */ done */ v, 2 AS n;",
         [["it's", "2"]], ["v", "n"], [25, 23]),
        ("SELECT 42 AS answer, CAST(1 AS text) AS data;",
         [["42", "1"]], ["answer", "data"], [23, 25]),
    ]
    try:
        for extended in (False, True):
            for sql, rows, headers, oids in cases:
                if extended:
                    sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                                 client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                                 client.typed(b"D", b"P\0") +
                                 client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                                 client.typed(b"S", b""))
                    messages = client.read_until_ready(sock)
                else:
                    messages = client.simple_query(sock, sql)
                result = runner.decode_wire_result(messages, include_types=True)
                assert (result[0], result[1], result[3], result[4], result[5]) == (
                    rows, None, headers, "SELECT " + str(len(rows)), oids), (sql, extended, result)
                assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        print("[FROMLESS QUOTED SYNTAX " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
