"""Typed decimal SQRT avoids binary intermediates and preserves display scale."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "sqrt_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
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
    failures = []
    controls = 0

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def check(sql, expected, oid=1700, state=None):
        nonlocal controls
        controls += 1
        if reference and state:
            setup("SAVEPOINT sqrt_error")
        result = query(sql)
        valid = result[1] == state and (result[0] == [] if state else
            result[0] == [[expected]] and result[5] == [oid])
        print("SQRT", sql, "match", valid, "result", result, flush=True)
        if not valid:
            failures.append((sql, expected, oid, state, result))
        if reference and state:
            setup("ROLLBACK TO sqrt_error")
            setup("RELEASE sqrt_error")

    cases = [
        ("0", "0.000000000000000"), ("0.000", "0.000000000000000"),
        ("1", "1.000000000000000"), ("2", "1.414213562373095"),
        ("4", "2.000000000000000"), ("9", "3.000000000000000"),
        ("16", "4.000000000000000"), ("0.1", "0.31622776601683793"),
        ("0.01", "0.10000000000000000"), ("0.0001", "0.01000000000000000"),
        ("1e40", "100000000000000000000"),
        ("10000000000000000000200000000000000000000", "100000000000000000001"),
        ("9999999999999999999999999999999999999999", "100000000000000000000"),
        ("9.0000000000000000000000000000", "3.0000000000000000000000000000"),
        ("2.0000000000000000000000000000", "1.4142135623730950488016887242"),
        ("1e-30", "0.0000000000000010000000000000000"),
        ("1e-1000", "0." + "0" * 499 + "1" + "0" * 500),
        ("1e998", "1" + "0" * 499), ("NaN", "NaN"), ("Infinity", "Infinity")]
    try:
        if reference:
            setup("BEGIN")
        setup("CREATE SCHEMA sqrt_" + uuid.uuid4().hex[:12])
        # TEMP objects are isolated to this connection, also on the reference.
        for value, expected in cases:
            check("SELECT sqrt('" + value + "'::numeric)", expected)
        for value in ("-1", "-0.01", "-Infinity"):
            check("SELECT sqrt('" + value + "'::numeric)", None, state="2201F")
        check("SELECT sqrt(NULL::numeric)", None)
        for sql, expected in [("SELECT sqrt(9)", "3"),
                              ("SELECT sqrt(2::double precision)", "1.4142135623730951"),
                              ("SELECT sqrt('-0'::double precision)", "-0"),
                              ("SELECT sqrt(16::real)", "4"),
                              ("SELECT sqrt('NaN'::double precision)", "NaN"),
                              ("SELECT sqrt('Infinity'::double precision)", "Infinity"),
                              ("SELECT sqrt('4.9406564584124654e-324'::double precision)", "2.2227587494850775e-162"),
                              ("SELECT sqrt('1.401298464324817e-45'::real)", "3.743392130574644e-23"),
                              ("SELECT sqrt('1.7976931348623157e308'::double precision)", "1.3407807929942596e+154"),
                              ("SELECT sqrt(NULL::double precision)", None)]:
            check(sql, expected, 701)
        check("SELECT sqrt(-1::double precision)", None, 701, "2201F")
        setup("CREATE TEMP TABLE sqrt_values(id INTEGER, v NUMERIC)")
        setup("INSERT INTO sqrt_values VALUES(1,'1e40'::numeric),(2,'2.0000000000000000000000000000'::numeric),(3,NULL)")
        for identifier, expected in [(1, cases[10][1]), (2, cases[14][1]), (3, None)]:
            check("SELECT sqrt(v) FROM sqrt_values WHERE id=" + str(identifier), expected)
        setup("CREATE TEMP TABLE sqrt_float_values(f REAL,d DOUBLE PRECISION)")
        setup("INSERT INTO sqrt_float_values VALUES('1.401298464324817e-45'::real,'4.9406564584124654e-324'::double precision)")
        check("SELECT sqrt(f) FROM sqrt_float_values", "3.743392130574644e-23", 701)
        check("SELECT sqrt(d) FROM sqrt_float_values", "2.2227587494850775e-162", 701)
        setup("PREPARE sqrt_numeric(NUMERIC) AS SELECT sqrt($1)")
        check("EXECUTE sqrt_numeric('1e40'::numeric)", cases[10][1])
        check("EXECUTE sqrt_numeric(NULL)", None)
        setup("DEALLOCATE sqrt_numeric")
        for oid, value, expected in [(1700, "1e40", cases[10][1]),
                                     (1700, "2.0000000000000000000000000000", cases[14][1]),
                                     (1700, None, None), (701, "2", "1.4142135623730951")]:
            controls += 1
            name = ("sqrt_" + uuid.uuid4().hex[:10]).encode()
            raw = value.encode() if value is not None else b""
            parameter = struct.pack("!i", len(raw) if value is not None else -1) + raw
            sock.sendall(client.typed(b"P", name + b"\0SELECT sqrt($1)\0" + struct.pack("!HI", 1, oid)) +
                client.typed(b"B", name + b"p\0" + name + b"\0" + struct.pack("!HH", 0, 1) + parameter + struct.pack("!H", 0)) +
                client.typed(b"D", b"P" + name + b"p\0") +
                client.typed(b"E", name + b"p\0" + struct.pack("!I", 0)) + client.typed(b"S"))
            result = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
            if result[1] is not None or result[0] != [[expected]] or result[5] != [oid]:
                failures.append(("bound sqrt", oid, value, expected, result))
            print("SQRT BOUND", oid, value, result, flush=True)
        assert controls == 46, controls
        assert not failures, failures
        print("[NUMERIC SQRT PROTOCOL] all 46 typed controls passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
