#!/usr/bin/env python3
"""Boolean character casts use SQL true/false, not internal t/f cells."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("boolean_character_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    targets = [("text", 25, -1), ("varchar", 1043, -1), ("char", 1042, 5), ("varchar(3)", 1043, 7), ("char(6)", 1042, 10)]
    controls = 0; failures = []
    def output(value, target):
        if value is None: return None
        return value[:1].ljust(1) if target == "char" else value[:3] if target == "varchar(3)" else value[:6].ljust(6) if target == "char(6)" else value
    def check(messages, value, oid, modifier, label):
        nonlocal controls
        controls += 1; result = runner.decode_wire_result(messages, include_types=True)
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        if result[:2] != ([[value]], None) or result[3:] != (["value"], "SELECT 1", [oid]) or len(fields) != 1 or fields[0][3:] != (oid, -1, modifier, 0):
            failures.append((label, result, fields, value, oid, modifier)); print("[BOOLEAN CHARACTER CAST FAIL] " + str(failures[-1]), flush=True)
    try:
        for truth in (True, False, None):
            literal = "NULL::boolean" if truth is None else "true" if truth else "false"
            source = None if truth is None else "true" if truth else "false"
            expressions = [literal, "(" + literal + " AND true)", "(" + literal + " BETWEEN false AND true)", "(" + literal + " NOT BETWEEN false AND true)"]
            for index, expression in enumerate(expressions):
                value = None if truth is None else "true" if index == 2 else "false" if index == 3 else source
                for target, oid, modifier in targets:
                    sql = "SELECT CAST(" + expression + " AS " + target + ") AS value"
                    check(client.simple_query(sock, sql), output(value, target), oid, modifier, sql)
        for source in ("t", "f", "TRUE", " false ", "", None):
            expression = "NULL::text" if source is None else "'" + source + "'::text"
            for target, oid, modifier in targets:
                sql = "SELECT CAST(" + expression + " AS " + target + ") AS value"
                check(client.simple_query(sock, sql), output(source, target), oid, modifier, sql)
        for source in ("t", "f", None):
            for target, oid, modifier in targets:
                sql = "SELECT CAST($1 AS " + target + ") AS value"; name = ("owned_bool_character_" + uuid.uuid4().hex[:12]).encode()
                sock.sendall(client.typed(b"P", name + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 16)) + client.typed(b"D", b"S" + name + b"\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock); fields = client.row_description_fields(messages)
                assert not any(kind in (b"E", b"D") for kind, _ in messages) and fields[0][3:] == (oid, -1, modifier, 0), (sql, messages)
                raw = None if source is None else source.encode(); value = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
                sock.sendall(client.typed(b"B", b"\0" + name + b"\0" + struct.pack("!HH", 0, 1) + value + struct.pack("!H", 0)) + client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                check(client.read_until_ready(sock), output(None if source is None else "true" if source == "t" else "false", target), oid, modifier, (sql, "real BOOLEAN parameter", source))
                sock.sendall(client.typed(b"C", b"S" + name + b"\0") + client.typed(b"S"))
                assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        assert controls == 105, controls
        assert not failures, "%d/%d complete Boolean/character cast controls failed" % (len(failures), controls)
        print("[BOOLEAN CHARACTER CAST PROTOCOL] all %d literal/predicate/NULL/TEXT passthrough/width/real BOOLEAN parameter/OID controls passed" % controls)
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
