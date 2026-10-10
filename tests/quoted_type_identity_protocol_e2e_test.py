"""Quoted type names keep case, escaped quotes, parentheses and literal []."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("quoted_type_identity_runner", root / "tests/compat/pg_diff_runner.py")
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

    def check(sql, rows=None, oids=None, state=None):
        nonlocal checked
        checked += 1
        actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        good = actual[1] == state
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        if state:
            good = good and actual[0] == [] and actual[4] is None
        print("QUOTED_TYPE_IDENTITY", sql, actual, "pass=", good, flush=True)
        if not good:
            failures.append((sql, actual, rows, oids, state))

    try:
        check('CREATE TYPE "CaseRank" AS ENUM(\'zeta\',\'alpha\')', [])
        check("CREATE TYPE caserank AS ENUM('alpha','zeta')", [])
        check('CREATE TYPE "Ra""nk(Name)" AS ENUM(\'zeta\',\'alpha\')', [])
        check('CREATE TYPE "Rank[]" AS ENUM(\'zeta\',\'alpha\')', [])
        for name in ['"CaseRank"', '"Ra""nk(Name)"', '"Rank[]"']:
            check("SELECT 'zeta'::" + name + " < 'alpha'", [["t"]], [16])
            check("SELECT (CASE WHEN true THEN 'zeta'::" + name + " ELSE 'alpha' END) < 'alpha'", [["t"]], [16])
            spelling = name.replace("'", "''")
            check("SELECT pg_typeof(NULL::" + name + ")::OID='" + spelling + "'::REGTYPE::OID", [["t"]], [16])
        check("SELECT 'zeta'::caserank < 'alpha'", [["f"]], [16])
        check('SELECT \'zeta\'::"CASERANK" < \'alpha\'', state="42704")
        check('DROP TYPE "CaseRank"', [])
        check("SELECT 'zeta'::caserank < 'alpha'", [["f"]], [16])
        check('SELECT NULL::"CaseRank"', state="42704")
        check('DROP TYPE "Ra""nk(Name)"', [])
        check('DROP TYPE "Rank[]"', [])
        check("DROP TYPE caserank", [])
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)
    print("QUOTED_TYPE_IDENTITY_CHECKED=", checked, "FAILED=", len(failures), flush=True)
    assert not failures, failures


if __name__ == "__main__":
    main()
