#!/usr/bin/env python3
"""Shared real parameters retain the first INTEGER/TEXT comparison context."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("integer_between_parameter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    sqls = ["SELECT $1 BETWEEN 1 AND 2 AS value", "SELECT $1 BETWEEN NULL AND 1 AS value",
            "SELECT $1 BETWEEN '1' AND 1 AS value", "SELECT $1 BETWEEN 1 AND NULL AS value",
            "SELECT 1 BETWEEN $1 AND 2 AS value", "SELECT 1 BETWEEN 0 AND $1 AS value",
            "SELECT NULL::integer BETWEEN $1 AND 2 AS value", "SELECT $1::smallint BETWEEN 0 AND 2 AS value",
            "SELECT $1::bigint BETWEEN 0 AND 2 AS value", "SELECT $1::text BETWEEN 0 AND 2 AS value"]
    failures = []; controls = 0
    def fail(condition, detail):
        if condition: failures.append(detail); print("[INTEGER BETWEEN PARAMETER FAIL] " + str(detail), flush=True)
    try:
        for index, sql in enumerate(sqls):
            for oid in (0, 21, 23, 20, 25):
                for source in ("01", "b01", "2", "", None, "32768", "2147483648"):
                    controls += 1; statement = ("owned_integer_range_" + uuid.uuid4().hex[:12]).encode(); portal = statement + b"_p"
                    sock.sendall(client.typed(b"P", statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)) +
                                 client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
                    messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True); parameters = []
                    for kind, payload in messages:
                        if kind == b"t":
                            count = struct.unpack("!H", payload[:2])[0]; parameters = list(struct.unpack("!" + "I" * count, payload[2:]))
                    valid = index != 9 and (index in (7, 8) or oid != 25 and (oid != 0 or index not in (1, 2)))
                    inferred = oid or 21 if index == 7 else oid or 20 if index == 8 else oid or 23
                    state = None if valid else "42883"
                    mismatch = result[1] != state
                    if valid and result[1] is None:
                        fields = client.row_description_fields(messages)
                        mismatch = mismatch or parameters != [inferred] or result[3] != ["value"] or result[5] != [16] or len(fields) != 1 or fields[0][3:] != (16, 1, -1, 0)
                    else: mismatch = mismatch or any(kind in (b"1", b"T", b"D") for kind, _ in messages)
                    fail(mismatch, (sql, oid, source, "Parse/Describe", result, parameters, state, inferred))
                    if result[1] is not None: continue
                    raw = None if source is None else source.encode()
                    parameter = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
                    sock.sendall(client.typed(b"B", portal + b"\0" + statement + b"\0" + struct.pack("!HH", 0, 1) + parameter + struct.pack("!H", 0)) +
                                 client.typed(b"D", b"P" + portal + b"\0") + client.typed(b"E", portal + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                    messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                    state = None; value = None
                    if source in ("b01", ""): state = "22P02"
                    elif source is not None:
                        value = int(source)
                        low, high = {21: (-32768, 32767), 23: (-2147483648, 2147483647), 20: (-9223372036854775808, 9223372036854775807), 25: (0, 0)}[inferred]
                        if inferred != 25 and not low <= value <= high: state = "22003"
                        elif index == 7 and not -32768 <= value <= 32767: state = "22003"
                    truth = None
                    if value is not None and state is None:
                        if index in (0, 7, 8): truth = 1 <= value <= 2 if index == 0 else 0 <= value <= 2
                        elif index == 1: truth = False if value > 1 else None
                        elif index in (2, 4): truth = value <= 1 if index == 4 else value == 1
                        elif index == 3: truth = False if value < 1 else None
                        elif index == 5: truth = value >= 1
                    rows = [] if state else [[None if truth is None else "t" if truth else "f"]]
                    mismatch = result[:2] != (rows, state)
                    if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT 1", [16])
                    else: mismatch = mismatch or any(kind in (b"T", b"D") for kind, _ in messages)
                    fail(mismatch, (sql, oid, source, "Bind/Describe/Execute", result, rows, state))
                    sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
                    assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        assert controls == 350, controls
        assert not failures, "%d phase differences in all %d genuine INTEGER BETWEEN parameters" % (len(failures), controls)
        print("[INTEGER BETWEEN PARAMETER PROTOCOL] all %d real first-context/typed/NULL/width/overflow/Parse/Bind/Describe/OID controls passed" % controls)
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
