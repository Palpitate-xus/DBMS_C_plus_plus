#!/usr/bin/env python3
"""Real Parse/Bind/Describe/Execute checks never execute a writer in analysis."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_demand_parameter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    schema = "owned_between_demand_param_" + uuid.uuid4().hex[:12]; created = False; failures = []; controls = 0
    def ok(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result); return result
    def counter(): return int(ok("SELECT currval('" + schema + ".effects') AS calls")[0][0][0])
    def check(valid, detail):
        if not valid: failures.append(detail); print("[BETWEEN DEMAND PARAMETER FAIL] " + str(detail), flush=True)
    try:
        ok("CREATE SCHEMA " + schema); created = True; ok("CREATE SEQUENCE " + schema + ".effects")
        ok("CREATE FUNCTION " + schema + ".writer(p varbit) RETURNS varbit LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('" + schema + ".effects'); RETURN p; END; $$")
        ok("SELECT nextval('" + schema + ".effects')")
        writer = schema + ".writer"
        expressions = (writer + "($1::varbit) BETWEEN B'01' AND B'11'",
                       "$1::varbit BETWEEN " + writer + "(B'01') AND " + writer + "(B'11')",
                       writer + "($1::varbit) BETWEEN NULL AND B'11'",
                       "$1::varbit BETWEEN NULL AND " + writer + "(B'11')")
        for index, expression in enumerate(expressions):
            for source in ("01", "b01", "2", "", None):
                controls += 1; sql = "SELECT " + expression + " AS value"
                statement = ("owned_between_demand_" + uuid.uuid4().hex[:12]).encode(); portal = statement + b"_p"
                before = counter()
                sock.sendall(client.typed(b"P", statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 0)) +
                             client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True); params = []
                for kind, payload in messages:
                    if kind == b"t":
                        count = struct.unpack("!H", payload[:2])[0]; params = list(struct.unpack("!" + "I" * count, payload[2:]))
                fields = client.row_description_fields(messages)
                check(result[1] is None and params == [1562] and result[3] == ["value"] and result[5] == [16] and
                      len(fields) == 1 and fields[0][3:] == (16, 1, -1, 0) and counter() == before,
                      (sql, source, "pure Parse/Describe", result, params, fields, counter() - before))
                if result[1] is not None: continue
                raw = None if source is None else source.encode()
                parameter = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
                bind = portal + b"\0" + statement + b"\0" + struct.pack("!HH", 0, 1) + parameter + struct.pack("!H", 0)
                before = counter()
                sock.sendall(client.typed(b"B", bind) + client.typed(b"D", b"P" + portal + b"\0") +
                             client.typed(b"E", portal + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True); calls = counter() - before
                bits = None if source is None else "01" if source == "b01" else source
                state = "22P02" if source == "2" else None
                value = None if bits is None or index in (2, 3) else "t" if "01" <= bits <= "11" else "f"
                expected_calls = 0 if state else (1 if bits is not None and bits < "01" else 2) if index == 0 else \
                    (0 if bits is None else 1 if bits < "01" else 2) if index == 1 else 1 if index == 2 else 0 if bits is None else 1
                check(result[:2] == ([] if state else [[value]], state) and calls == expected_calls and
                      (result[3:] == (["value"], "SELECT 1", [16]) if state is None else not any(kind in (b"T", b"D") for kind, _ in messages)),
                      (sql, source, "Bind/Describe/Execute", result, calls, expected_calls, state, value))
                sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
                assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        assert controls == 20, controls
        assert not failures, "%d phase differences in all %d actual demand parameter controls" % (len(failures), controls)
        print("[BETWEEN DEMAND PARAMETER PROTOCOL] all %d real parameter/NULL/body effects/Parse/Bind/Describe/Execute/input-error controls passed" % controls)
    finally:
        if created: ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
