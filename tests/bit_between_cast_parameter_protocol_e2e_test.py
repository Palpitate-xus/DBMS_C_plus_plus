#!/usr/bin/env python3
"""Explicit casts and shared parameters preserve the first actual input owner."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_cast_parameter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    sqls = ["SELECT $1::bit BETWEEN B'01' AND B'01' AS value", "SELECT $1::bit(2) BETWEEN B'01' AND B'01' AS value",
            "SELECT $1::varbit BETWEEN B'01' AND B'01' AS value", "SELECT CAST($1 AS varbit) BETWEEN B'01' AND B'01' AS value",
            "SELECT $1::text::varbit BETWEEN B'01' AND B'01' AS value", "SELECT B'01' BETWEEN CAST($1 AS varbit) AND B'01' AS value",
            "SELECT B'01' BETWEEN $1::bit AND B'01' AS value", "SELECT $1::varbit BETWEEN NULL AND B'01' AS value",
            "SELECT $1::integer::bit BETWEEN B'01' AND B'01' AS value", "SELECT $1::text BETWEEN B'01' AND B'01' AS value",
            "SELECT $1 BETWEEN NULL AND $1::varbit AS value", "SELECT $1::varbit BETWEEN NULL AND $1 AS value",
            "SELECT $1 BETWEEN B'01' AND $1::varbit AS value", "SELECT $1::varbit BETWEEN B'01' AND $1 AS value",
            "SELECT $1::text::varbit BETWEEN B'01' AND $1::varbit AS value", "SELECT $1::text BETWEEN NULL AND $1::varbit AS value",
            "SELECT B'01' BETWEEN $1::varbit AND $1 AS value", "SELECT B'01' BETWEEN $1 AND $1::varbit AS value"]
    parameter_oids = [1560, 1560, 1562, 1562, 25, 1562, 1560, 1562, 23, None, None, 1562, 1560, 1562, 25, None, 1562, 1560]
    controls = 0; failures = []
    try:
        for index, sql in enumerate(sqls):
            for source in ("01", "b01", "2", "", None):
                controls += 1; statement = ("owned_between_cast_" + uuid.uuid4().hex[:12]).encode(); portal = statement + b"_p"
                parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 0)
                sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S" + statement + b"\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                oid = parameter_oids[index]; state = None if oid else "42883"; params = []
                for kind, payload in messages:
                    if kind == b"t":
                        count = struct.unpack("!H", payload[:2])[0]; params = list(struct.unpack("!" + "I" * count, payload[2:]))
                mismatch = result[1] != state
                if oid and result[1] is None:
                    fields = client.row_description_fields(messages)
                    mismatch = mismatch or params != [oid] or result[3] != ["value"] or result[5] != [16] or len(fields) != 1 or fields[0][3:] != (16, 1, -1, 0)
                else: mismatch = mismatch or any(kind in (b"1", b"T", b"D") for kind, _ in messages)
                if mismatch:
                    failures.append((sql, source, "Parse/Describe", result, params, oid, state))
                    print("[BIT BETWEEN CAST PARAMETER FAIL] " + str(failures[-1]), flush=True)
                if result[1] is not None: continue
                raw = None if source is None else source.encode()
                parameter = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
                bind = portal + b"\0" + statement + b"\0" + struct.pack("!HH", 0, 1) + parameter + struct.pack("!H", 0)
                sock.sendall(client.typed(b"B", bind) + client.typed(b"D", b"P" + portal + b"\0") +
                             client.typed(b"E", portal + b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
                if state is None and (source == "2" and index != 8 or index == 8 and source in ("b01", "")): state = "22P02"
                bits = None if source is None else "01" if source == "b01" else source
                value = None
                if bits is not None:
                    if index in (0, 8): value = False
                    elif index == 1: value = (bits + "00")[:2] == "01"
                    elif index in (2, 3, 4, 16, 17): value = bits == "01"
                    elif index == 5: value = bits <= "01"
                    elif index == 6: value = True
                    elif index in (12, 13, 14): value = bits >= "01"
                rows = [] if state else [[None if value is None else "t" if value else "f"]]
                mismatch = result[0] != rows or result[1] != state
                if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT 1", [16])
                elif any(kind in (b"T", b"D") for kind, _ in messages): mismatch = True
                if mismatch:
                    failures.append((sql, source, "Bind/Execute", result, rows, state))
                    print("[BIT BETWEEN CAST PARAMETER FAIL] " + str(failures[-1]), flush=True)
                sock.sendall(client.typed(b"C", b"S" + statement + b"\0") + client.typed(b"S"))
                assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))
        assert controls == 90, controls
        assert not failures, "%d phase failures in all %d actual CAST/shared-parameter controls" % (len(failures), controls)
        print("[BIT BETWEEN CAST PARAMETER PROTOCOL] all %d real explicit/default/typmod/nested/shared/ordered/NULL/error/OID controls passed" % controls)
    finally:
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
