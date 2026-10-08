"""Catalog-owned OID18 input is distinct from SQL CHAR/bpchar."""
import importlib.util
import socket
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("internal_char_runner", repo / "tests/compat/pg_diff_runner.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
inputs = (("", ""), ("abc", "a"), ("é", "\\303"), ("中", "\\344"),
          ("\\000", ""), ("\\101", "A"), ("\\200", "\\200"), ("\\777", "\\377"))
cases = []
for declaration in ('"char"', 'pg_catalog."char"'):
    for value, expected in inputs:
        for expression in (declaration + " '" + value + "'",
                           "CAST('" + value + "' AS " + declaration + ")",
                           "'" + value + "'::" + declaration):
            cases.append(("SELECT " + expression + " AS value", [[expected]], None))
    for expression in ("CAST(NULL AS " + declaration + ")", "NULL::" + declaration):
        cases.append(("SELECT " + expression + " AS value", [[None]], None))
    for expression in (declaration + "(2) 'abc'", "CAST('abc' AS " + declaration + "(2))",
                       "'abc'::" + declaration + "(2)"):
        cases.append(("SELECT " + expression + " AS value", [], "42601"))
    for expression in (declaration + " 'abc'", "CAST(NULL AS " + declaration + ")"):
        cases.append(("SELECT " + expression + " AS value WHERE false", [], None))
assert len(cases) == 62
reference = "--reference18" in sys.argv
if reference:
    host, port, user, database, password = runner._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
    client.startup_reference(sock, user, database, password)
    runner.verify_reference_version(client, sock)
else:
    server = runner.start_ours(client)
    sock = server["sock"]
checked = failures = 0
try:
    for sql, rows, state in cases:
        actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        good = actual[1] == state and (state is not None or
            (actual[0] == rows and actual[3] == ["value"] and
             actual[4] == "SELECT " + str(len(rows)) and actual[5] == [18]))
        checked += 1
        failures += not good
        print("INTERNAL_CHAR_PROTOCOL", sql, actual, "pass=", good, flush=True)
finally:
    if reference:
        sock.close()
    else:
        runner.stop_ours(server)
print("INTERNAL_CHAR_PROTOCOL_CHECKED", checked, "FAILED", failures, flush=True)
raise SystemExit(0 if checked == 62 and failures == 0 else 1)
