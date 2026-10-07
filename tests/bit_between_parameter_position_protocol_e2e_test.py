#!/usr/bin/env python3
"""SQL parameter positions are not occurrence-ordered bound cell slots."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_positions_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    cases = [("SELECT $2 BETWEEN B'01' AND B'01' AS value", [23, 0]),
             ("SELECT $2 BETWEEN B'01' AND NULL AS value", [23, 0]),
             ("SELECT $2 BETWEEN B'01' AND B'01' AS value", [0, 0]),
             ("SELECT $2 BETWEEN B'01' AND $1 AS value", [1560, 0]),
             ("SELECT $2 BETWEEN B'01' AND $1 AS value", [0, 1560]),
             ("SELECT $1 BETWEEN B'01' AND $2 AS value", [0, 1560]),
             ("SELECT $2 BETWEEN $1 AND B'01' AS value", [0, 0]),
             ("SELECT $2 BETWEEN B'01' AND B'01' AS value", [1562, 0])]
    controls = 0; failures = []
    try:
        for index, (sql, oids) in enumerate(cases):
            for source in ("01", "b01", "2", "", None):
                controls += 1; statement = ("owned_between_positions_" + uuid.uuid4().hex[:12]).encode(); portal = statement + b"_p"
                parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids)) + b"".join(struct.pack("!I", oid) for oid in oids)
                sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                state = "42P18" if index == 2 else "42883" if index == 6 else None
                params = []
                for kind, payload in messages:
                    if kind == b"t":
                        count = struct.unpack("!H", payload[:2])[0]; params = list(struct.unpack("!" + "I" * count, payload[2:]))
                expected_oids = [oid or 1560 for oid in oids]
                mismatch = result[1] != state
                if state is None and result[1] is None:
                    fields = client.row_description_fields(messages)
                    mismatch = mismatch or params != expected_oids or result[3] != ["value"] or result[5] != [16] or len(fields) != 1 or fields[0][3:] != (16, 1, -1, 0)
                else: mismatch = mismatch or any(kind in (b"1", b"T", b"D") for kind, _ in messages)
                if mismatch:
                    failures.append((sql, oids, source, "Parse/Describe", result, params, state, expected_oids))
                    print("[BIT BETWEEN POSITION FAIL] " + str(failures[-1]), flush=True)
                if result[1] is not None: continue
                parameters = []
                for value in ("01", source):
                    raw = None if value is None else value.encode()
                    parameters.append(struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw)
                bind = portal + b"\0" + statement + b"\0" + struct.pack("!HH", 0, 2) + b"".join(parameters) + struct.pack("!H", 0)
                sock.sendall(client.typed(b"B", bind) + client.typed(b"D", b"P" + portal + b"\0") +
                             client.typed(b"E", portal + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                if state is None and source == "2": state = "22P02"
                bits = None if source is None else "01" if source == "b01" else source
                value = None if bits is None else False if index == 1 and bits < "01" else None if index == 1 else bits >= "01" if index == 5 else bits == "01"
                rows = [] if state else [[None if value is None else "t" if value else "f"]]
                mismatch = result[0] != rows or result[1] != state
                if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT 1", [16])
                elif any(kind in (b"T", b"D") for kind, _ in messages): mismatch = True
                if mismatch:
                    failures.append((sql, oids, source, "Bind/Execute", result, rows, state))
                    print("[BIT BETWEEN POSITION FAIL] " + str(failures[-1]), flush=True)
                sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
                assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        assert controls == 40, controls
        assert not failures, "%d phase failures in all %d two-parameter position controls" % (len(failures), controls)
        print("[BIT BETWEEN PARAMETER POSITION PROTOCOL] all %d source-position/occurrence-order/repeated/unused/42P18/NULL/input/OID controls passed" % controls)
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
