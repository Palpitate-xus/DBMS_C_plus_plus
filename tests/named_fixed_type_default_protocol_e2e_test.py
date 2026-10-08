"""Named fixed types do not borrow SQL keyword default typmods."""
import importlib.util
import socket
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("named_fixed_runner", repo / "tests/compat/pg_diff_runner.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
cases = (
    ("CHAR", "abc", "abc", "a", 1042),
    ("CHARACTER", "abc", "abc", "a", 1042),
    ("bpchar", "abc", "abc", "abc", 1042),
    ("pg_catalog.bpchar", "abc", "abc", "abc", 1042),
    ('"bpchar"', "abc", "abc", "abc", 1042),
    ("BIT", "01", "01", "0", 1560),
    ("pg_catalog.bit", "01", "01", "01", 1560),
    ('"bit"', "01", "01", "01", 1560),
    ("CHAR(2)", "abc", "ab", "ab", 1042),
    ("bpchar(2)", "abc", "ab", "ab", 1042),
    ("pg_catalog.bpchar(2)", "abc", "ab", "ab", 1042),
    ("BIT(2)", "01", "01", "01", 1560),
    ("pg_catalog.bit(2)", "01", "01", "01", 1560),
)
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
    for declaration, value, literal, cast, oid in cases:
        expressions = (
            declaration + " '" + value + "'",
            "CAST('" + value + "' AS " + declaration + ")",
            "'" + value + "'::" + declaration,
        )
        for index, expression in enumerate(expressions):
            sql = "SELECT " + expression + " AS value"
            expected = literal if index == 0 else cast
            actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
            good = (actual[0] == [[expected]] and actual[1] is None and
                    actual[3] == ["value"] and actual[4] == "SELECT 1" and actual[5] == [oid])
            checked += 1
            failures += not good
            print("NAMED_FIXED_PROTOCOL", sql, actual, "pass=", good, flush=True)
finally:
    if reference:
        sock.close()
    else:
        runner.stop_ours(server)
print("NAMED_FIXED_PROTOCOL_CHECKED", checked, "FAILED", failures, flush=True)
raise SystemExit(0 if checked == 39 and failures == 0 else 1)
