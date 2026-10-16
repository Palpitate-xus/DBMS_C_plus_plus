#!/usr/bin/env python3
"""Coordinate delimiters are grammar, not a shape obtained by deleting numbers."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


CASES = (
    ("point", 600, "(1,2)", ("(1,2)", "1,2", " ( 1 , 2 ) "),
     ("(1),2", "1,(2)", "()1,2", "1,2()", "((1,2))", "(1,2]")),
    ("line", 628, "{1,-1,1}",
     ("[(1,2),(3,4)]", "((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", "1,2,(3,4)"),
     ("{1},2,3", "{1,2},3", "[(1),2,(3),4]", "(1,2),3,4", "{1,2,3}4")),
    ("lseg", 601, "[(1,2),(3,4)]",
     ("[(1,2),(3,4)]", "((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", "[1,2,3,4]", "1,2,(3,4)"),
     ("[(1),2,(3),4]", "(1),2,(3),4", "(1,2),3,4", "1,(2,3),4")),
    ("box", 603, "(3,4),(1,2)",
     ("((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", "(1,2,3,4)", "1,2,(3,4)"),
     ("(1),2,(3),4", "(1,2),3,4", "[1,2,3,4]", "(1,2),(3,4)()")),
    ("path", 602, "((1,2),(3,4))",
     ("((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", "(1,2,3,4)", "1,2,(3,4)"),
     ("[(1),2,(3),4]", "(1,2),3,4", "[(1,2)),(3,4)]", "[(1,2),]")),
    ("polygon", 604, "((1,2),(3,4))",
     ("((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", "(1,2,3,4)", "1,2,(3,4)"),
     ("(1),2,(3),4", "(1,2),3,4", "[(1,2),(3,4)]", "((1,2),(3,4))()")),
    ("circle", 718, "<(1,2),3>",
     ("<(1,2),3>", "((1,2),3)", "(1,2),3", "1,2,3", "<1,2,3>"),
     ("<(1),2,3>", "<1,2>,3", "1,(2,3)", "(1,2,3)", "<(1,2),3>()")),
)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "geometric_delimiter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures, checks = [], 0

    def check(label, ok, observed):
        nonlocal checks
        checks += 1
        if not ok:
            failures.append((label, observed))
            print("GEOMETRIC_DELIMITER_FAILURE", label, repr(observed), flush=True)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)

    def error(label, sql):
        result = query(sql)
        check(label, result[1] == "22P02" and result[0] == [] and result[4] is None, result)

    def parameter(kind, oid, value, input_type):
        # Send the whole extended cycle, including Sync, so any error can be
        # inspected without leaving this connection in discard-until-Sync.
        sql = ("SELECT $1::" + kind).encode()
        parse = b"\0" + sql + b"\0" + struct.pack("!HI", 1, input_type)
        raw = value.encode()
        bind = (b"\0\0" + struct.pack("!HH", 0, 1) +
                struct.pack("!i", len(raw)) + raw + struct.pack("!H", 0))
        sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                     client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        states = [client.diagnostic_fields(body).get(b"C")
                  for tag, body in messages if tag == b"E"]
        check(kind + " parameter " + value + " oid=" + str(input_type),
              states == [b"22P02"] and not any(tag == b"D" for tag, _ in messages),
              (states, [tag for tag, _ in messages]))

    try:
        suffix = uuid.uuid4().hex[:12]
        for kind, oid, canonical, valid_inputs, invalid_inputs in CASES:
            table = "geometry_delimiter_" + suffix + "_" + kind
            setup = query("CREATE TEMP TABLE " + table + "(id INTEGER,v " + kind + ")")
            assert setup[1] is None, setup
            for value in valid_inputs:
                result = query("SELECT CAST('" + value + "' AS " + kind + ")")
                check(kind + " valid " + value,
                      result[1] is None and result[0] == [[canonical]] and result[5] == [oid], result)
            for value in invalid_inputs:
                literal = "CAST('" + value + "' AS " + kind + ")"
                error(kind + " cast " + value, "SELECT " + literal)
                error(kind + " analysis " + value,
                      "SELECT CASE WHEN true THEN NULL::" + kind + " ELSE " + literal + " END WHERE false")
                error(kind + " insert " + value,
                      "INSERT INTO " + table + " VALUES(1,'" + value + "')")
                parameter(kind, oid, value, oid)
                parameter(kind, oid, value, 25)
            result = query("SELECT count(*) FROM " + table)
            check(kind + " invalid writes absent", result[1] is None and result[0] == [["0"]], result)
            result = query("INSERT INTO " + table + " VALUES(2,'" + valid_inputs[0] + "')")
            check(kind + " healthy write after errors", result[1] is None and result[4] == "INSERT 0 1", result)
            error(kind + " update atomicity",
                  "UPDATE " + table + " SET v='" + invalid_inputs[0] + "' WHERE id=2")
            result = query("SELECT v FROM " + table)
            check(kind + " old value retained", result[1] is None and result[0] == [[canonical]] and result[5] == [oid], result)
        for kind, value, expected in (
                ("line", "{1,2,3}", "{1,2,3}"),
                ("path", "[(1,2),(3,4)]", "[(1,2),(3,4)]"),
                ("path", "(1,2)", "((1,2))"),
                ("path", "[(1,2),(3,4))", "[(1,2),(3,4)]"),
                ("lseg", "[(1,2),(3,4))", "[(1,2),(3,4)]"),
                ("circle", "<(1,2),3)", "<(1,2),3>"),
                ("circle", "((1,2),3>", "<(1,2),3>"),
                ("polygon", "(1,2)", "((1,2))")):
            result = query("SELECT CAST('" + value + "' AS " + kind + ")")
            check(kind + " additional valid " + value, result[1] is None and result[0] == [[expected]], result)
        print("GEOMETRIC_DELIMITER_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert not failures, failures
    finally:
        if reference:
            sock.close()  # All reference relations are session-owned TEMP tables.
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
