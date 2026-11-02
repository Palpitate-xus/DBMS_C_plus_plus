#!/usr/bin/env python3
"""Permanent exact52 VALUES count type-modifier and syntax/phase controls.

The fixed PostgreSQL18.6 oracle uses normal Simple and valid unlimited
Extended messages. DBMS_MAIN selects the executable; --reference18 uses
the shared PGREF connection settings. Full raw observations are retained
in a private temporary directory or VALUES_COUNT_TYPE_MODIFIER_OUTPUT_DIR.
"""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
from types import SimpleNamespace

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("modifier_runner", REPO / "tests/compat/pg_diff_runner.py")
runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
wire = runner.load_protocol_client()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def cases():
    base = "VALUES(3),(1),(2) ORDER BY column1 "
    invalid_types = [
        "LIMIT NULL::timestamp WITH TIME ZONE",
        "LIMIT NULL::time WITH TIME ZONE",
        "OFFSET NULL::timestamp WITH TIME ZONE",
        "OFFSET NULL::time WITH TIME ZONE",
        "LIMIT TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00+00'",
        "OFFSET TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00+00'",
        "LIMIT TIME WITH TIME ZONE '00:00:00+00'",
        "OFFSET TIME WITH TIME ZONE '00:00:00+00'",
        "LIMIT NULL::timestamp WITHOUT TIME ZONE",
        "LIMIT NULL::time WITHOUT TIME ZONE",
        "OFFSET NULL::timestamp WITHOUT TIME ZONE",
        "OFFSET NULL::time WITHOUT TIME ZONE",
        "LIMIT (NULL::timestamp WITH TIME ZONE)",
        "LIMIT (NULL::time WITH TIME ZONE)",
        "OFFSET (NULL::timestamp WITH TIME ZONE)",
        "OFFSET (NULL::time WITH TIME ZONE)",
    ]
    result = [{"sql": base + suffix, "state": "42804", "rows": []} for suffix in invalid_types]
    for suffix, rows in [
            ("LIMIT int8 '2'", [["1"], ["2"]]),
            ("OFFSET int8 '1'", [["2"], ["3"]]),
            ("LIMIT NULL::bigint", [["1"], ["2"], ["3"]]),
            ("OFFSET NULL::bigint", [["1"], ["2"], ["3"]])]:
        result.append({"sql": base + suffix, "state": None, "rows": rows})
    for suffix in ("LIMIT ALL+1", "OFFSET ALL", "LIMIT CASE WHEN true THEN 1", "LIMIT CASE WHEN true 1 END"):
        result.append({"sql": base + suffix, "state": "42601", "rows": []})
    assert len(result) == 24
    return result


def observation(messages):
    rows, state, message, headers, tag, oids = runner.decode_wire_result(messages, include_types=True)
    return {"frames": [kind.decode() for kind, _ in messages],
            "ready": [body.decode() for kind, body in messages if kind == b"Z"],
            "rows": rows, "state": state, "message": message, "headers": headers, "tag": tag, "oids": oids,
            "parameter_oids": [[struct.unpack("!I", body[pos:pos+4])[0] for pos in range(2,len(body),4)]
                               for kind, body in messages if kind == b"t"],
            "diagnostics": [{key.decode(): value.decode() for key, value in wire.diagnostic_fields(body).items()}
                            for kind, body in messages if kind == b"E"],
            "raw": [{"kind": kind.decode(), "body_hex": body.hex()} for kind, body in messages]}


@contextmanager
def connection(args, output):
    if args.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        try:
            wire.startup_reference(sock, user, database, password=password)
            runner.verify_reference_version(wire, sock)
            yield SimpleNamespace(sock=sock)
        finally:
            sock.close()
    else:
        server = runner.start_ours(wire)
        try:
            yield SimpleNamespace(sock=server["sock"])
        finally:
            runner.stop_ours(server)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference18", action="store_true")
    args = parser.parse_args()
    supplied_output = os.environ.get("VALUES_COUNT_TYPE_MODIFIER_OUTPUT_DIR")
    output = Path(supplied_output) if supplied_output else Path(tempfile.mkdtemp(prefix="dbms-values-count-type-modifier-"))
    output = output.resolve(); output.mkdir(parents=True, exist_ok=True)
    result = {"complete": False, "mode": "reference" if args.reference18 else "candidate",
              "driver_sha256": sha(__file__), "controls": [], "failures": 0,
              "binary_sha256": None if args.reference18 else sha(Path(runner.DBMS_MAIN)),
              "scope": "Exact normal Simple and Parse/Describe/Sync frames, SQLSTATE and ordinary valid unbounded Bind/Execute. "
                       "No retained plan, portal-format or deferred policy change. All raw protocol bodies retained."}
    def record(index, sql, phase, messages, expected):
        actual = observation(messages)
        valid = all(actual[key] == value for key, value in expected.items())
        item = {"case": index, "sql": sql, "phase": phase, "expected": expected, "actual": actual, "passed": valid}
        result["controls"].append(item); result["failures"] += not valid
        print(json.dumps(item), flush=True)
        (output / "observations.json").write_text(json.dumps(result, indent=2) + "\n")
    try:
        with connection(args, output) as client:
            client.sock.settimeout(15)
            for index, case in enumerate(cases()):
                sql, state, rows = case["sql"], case["state"], case["rows"]
                expected = {"state": state, "ready": ["I"], "rows": rows,
                            "oids": [] if state else [23], "headers": [] if state else ["column1"],
                            "tag": None if state else "SELECT " + str(len(rows)),
                            "frames": ["E", "Z"] if state else ["T"] + ["D"] * len(rows) + ["C", "Z"]}
                record(index, sql, "Simple", wire.simple_query(client.sock, sql), expected)
                name = ("modifier_" + str(index)).encode()
                payload = name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
                client.sock.sendall(wire.typed(b"P", payload) + wire.typed(b"D", b"S" + name + b"\0") + wire.typed(b"S"))
                messages = wire.read_until_ready(client.sock)
                describe = {"state": state, "ready": ["I"], "rows": [], "tag": None,
                            "oids": [] if state else [23], "headers": [] if state else ["column1"],
                            "frames": ["E", "Z"] if state else ["1", "t", "T", "Z"],
                            "parameter_oids": [] if state else [[]]}
                record(index, sql, "ParseDescribe", messages, describe)
                # Preserve all independent observations even after rejection.
                # Only an actually successful Parse can execute that statement.
                if not state and not any(kind == b"E" for kind, _ in messages):
                    bind = b"\0" + name + b"\0" + struct.pack("!HHH", 0, 0, 0)
                    client.sock.sendall(wire.typed(b"B", bind) + wire.typed(b"D", b"P\0") +
                                        wire.typed(b"E", b"\0" + struct.pack("!I", 0)) + wire.typed(b"S"))
                    execute = dict(expected); execute["frames"] = ["2", "T"] + ["D"] * len(rows) + ["C", "Z"]
                    record(index, sql, "BindExecute", wire.read_until_ready(client.sock), execute)
        result["complete"] = True
    except BaseException as error:
        result["error"] = repr(error); raise
    finally:
        (output / "observations.json").write_text(json.dumps(result, indent=2) + "\n")
        print("VALUES_COUNT_TYPE_MODIFIERS_OBSERVATIONS", str(output / "observations.json"), flush=True)
    print("VALUES_COUNT_TYPE_MODIFIERS_COMPLETE", len(result["controls"]), "FAILURES", result["failures"], flush=True)
    assert len(result["controls"]) == 52 and not result["failures"]


if __name__ == "__main__":
    main()
