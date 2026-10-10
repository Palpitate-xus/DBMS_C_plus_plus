"""version() is an ordinary typed zero-argument builtin, with truthful branding."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("version_function_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checked = 0
    failures = []

    def check(sql, rows=None, oids=None, state=None, describe=False):
        nonlocal checked
        checked += 1
        if describe:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        good = actual[1] == state
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        if state:
            good = good and actual[0] == [] and actual[4] is None
        print("VERSION_FUNCTION", sql, "describe=", describe, actual, "pass=", good, flush=True)
        if not good:
            failures.append((sql, actual, rows, oids, state))
        return actual

    try:
        value = check("SELECT version()", oids=[25])
        if value[1] is None:
            assert len(value[0]) == 1 and len(value[0][0]) == 1
            if reference:
                assert value[0][0][0].startswith("PostgreSQL 18.")
            else:
                assert value[0][0][0] == "DBMS-C++ 0.2.0 (PostgreSQL protocol 3.0)"
        for sql, rows, oids in [
            ("SELECT USER IS NOT NULL,pg_typeof(version())::TEXT,version() IS NOT NULL", [["t", "text", "t"]], [16, 25, 16]),
            ("SELECT pg_catalog.version()=version()", [["t"]], [16]),
            ("SELECT version() WHERE false", [], [25]),
        ]:
            check(sql, rows, oids)
            check(sql, [], oids, describe=True)
        for sql, state in [("SELECT version(1) WHERE false", "42883"),
                           ("SELECT version(a => 1)", "42883"),
                           ("SELECT version() OVER ()", "42809")]:
            check(sql, state=state)
        check("CREATE FUNCTION public.version() RETURNS TEXT LANGUAGE SQL AS $$SELECT 'shadow'::TEXT$$", [])
        check("SELECT public.version(),version()=pg_catalog.version()", [["shadow", "t"]], [25, 16])
        check("SET search_path TO public,pg_catalog", [])
        check("SELECT version(),pg_catalog.version() IS NOT NULL", [["shadow", "t"]], [25, 16])
        check("SET search_path TO public", [])
        check("DROP FUNCTION public.version()", [])
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)
    print("VERSION_FUNCTION_CHECKED=", checked, "FAILED=", len(failures), flush=True)
    assert not failures, failures


if __name__ == "__main__":
    main()
