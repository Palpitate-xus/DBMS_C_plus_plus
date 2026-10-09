"""Regtype input records copied from actual PostgreSQL 18.6."""
import importlib.util
import json
import socket
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('regtype_input_runner', repo / 'tests/compat/pg_diff_runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
cases = json.loads((repo / 'tests/compat/regtype_input_pg18_expected.json').read_text())
assert len(cases) == 53
reference = '--reference18' in sys.argv
if reference:
    host, port, user, database, password = runner._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
    client.startup_reference(sock, user, database, password)
    runner.verify_reference_version(client, sock)
else:
    server = runner.start_ours(client)
    sock = server['sock']
failures = 0
try:
    for case in cases:
        expected = case['reference']
        actual = runner.decode_wire_result(client.simple_query(sock, case['sql']), include_types=True)
        good = expected[1] == actual[1] and (expected[1] is not None or
            (expected[0] == actual[0] and expected[3:] == list(actual[3:])))
        failures += not good
        print(json.dumps(dict(sql=case['sql'], reference=expected, actual=actual, pass_=good)), flush=True)
finally:
    if reference:
        sock.close()
    else:
        runner.stop_ours(server)
print('REGTYPE_INPUT_CHECKED', len(cases), 'FAILED', failures, flush=True)
raise SystemExit(bool(failures))
