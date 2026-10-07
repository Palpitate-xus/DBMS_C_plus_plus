#!/usr/bin/env python3
"""Real BETWEEN parameters retain ordered inference, input and Describe types."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_parameter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    sqls = ["SELECT $1 BETWEEN B'01' AND 1 AS value", "SELECT $1 BETWEEN 1 AND B'01' AS value",
            "SELECT $1 BETWEEN B'01' AND NULL AS value", "SELECT $1 BETWEEN NULL AND B'01' AS value",
            "SELECT $1 BETWEEN B'01' AND 'b0' AS value", "SELECT $1 BETWEEN 'b0' AND B'01' AS value",
            "SELECT B'01' BETWEEN $1 AND B'01' AS value", "SELECT B'01' BETWEEN B'01' AND $1 AS value",
            "SELECT NULL::bit BETWEEN $1 AND B'01' AS value", "SELECT $1::text BETWEEN B'01' AND B'01' AS value"]
    failures = []; controls = 0
    try:
        for index, sql in enumerate(sqls):
            for oid in (0, 1560, 1562, 25, 23):
                for source in ("01", "b01", "2", "", None):
                    controls += 1; statement = ("owned_between_parameter_" + uuid.uuid4().hex[:12]).encode()
                    portal = statement + b"_p"; parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)
                    sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
                    messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                    valid = index not in (0, 1, 9) and (oid in (1560, 1562) or (oid == 0 and index not in (3, 5)))
                    # The first pair converts its genuine UNKNOWN constant
                    # before the second pair's operator is resolved.
                    parse_state = None if valid else "22P02" if index == 5 and oid == 23 else "42883"
                    params = []
                    for kind, payload in messages:
                        if kind == b"t":
                            count = struct.unpack("!H", payload[:2])[0]
                            params = list(struct.unpack("!" + "I" * count, payload[2:]))
                    mismatch = result[1] != parse_state
                    if valid and result[1] is None:
                        mismatch = mismatch or params != [oid or 1560] or result[3] != ["value"] or result[5] != [16]
                        fields = client.row_description_fields(messages)
                        mismatch = mismatch or len(fields) != 1 or fields[0][3:] != (16, 1, -1, 0)
                    else:
                        mismatch = mismatch or any(kind in (b"1", b"T", b"D") for kind, _ in messages)
                    if mismatch:
                        failures.append((sql, oid, source, "Parse/Describe", result, params, parse_state))
                        print("[BIT BETWEEN PARAMETER FAIL] " + str(failures[-1]), flush=True)
                    # Collect the whole matrix even when a candidate wrongly
                    # admits a statement. An errored Parse publishes no object.
                    if result[1] is not None: continue
                    raw = None if source is None else source.encode()
                    parameter = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
                    bind = portal + b"\0" + statement + b"\0" + struct.pack("!HH", 0, 1) + parameter + struct.pack("!H", 0)
                    sock.sendall(client.typed(b"B", bind) + client.typed(b"D", b"P" + portal + b"\0") +
                                 client.typed(b"E", portal + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                    messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                    state = parse_state if not valid else "22P02" if source == "2" else None
                    bits = None if source is None else "01" if source == "b01" else source
                    value = None
                    if bits is not None:
                        if index == 2: value = False if bits < "01" else None
                        elif index == 3: value = False if bits > "01" else None
                        elif index == 4: value = False
                        elif index == 5: value = "0" <= bits <= "01"
                        elif index == 6: value = bits <= "01"
                        elif index == 7: value = bits >= "01"
                    expected = [] if state else [[None if value is None else "t" if value else "f"]]
                    mismatch = result[0] != expected or result[1] != state
                    if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT 1", [16])
                    elif any(kind in (b"T", b"D") for kind, _ in messages): mismatch = True
                    if mismatch:
                        failures.append((sql, oid, source, "Bind/Execute", result, expected, state))
                        print("[BIT BETWEEN PARAMETER FAIL] " + str(failures[-1]), flush=True)
                    sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
                    closed = client.read_until_ready(sock)
                    assert not any(kind == b"E" for kind, _ in closed), closed
        assert controls == 250, controls
        assert not failures, "%d phase failures in all %d real BETWEEN parameter controls" % (len(failures), controls)
        print("[BIT BETWEEN PARAMETER PROTOCOL] all %d ordered inference/actual input/NULL/Parse/Bind/Describe/OID/error controls passed" % controls)
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
