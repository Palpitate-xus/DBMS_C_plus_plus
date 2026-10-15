"""Boolean DISTINCT arguments preserve their expression and NULL result."""
import importlib.util
import json
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("boolean_distinct_runner", root / "tests/compat/pg_diff_runner.py")
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
    controls, failures = 0, []

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)

    def check(sql, expected, grouped, legacy):
        nonlocal controls
        controls += 1
        print("BOOLEAN_DISTINCT_BEGIN", controls, sql, flush=True)
        result = query(sql)
        width = 2 if grouped else 1
        types = ([23, 16] if grouped else [16]) + ([114] if legacy else [])
        good = result[1] is None and result[5] == types and [row[:width] for row in result[0]] == expected
        good = good and all(len(row) == width + int(legacy) for row in result[0])
        if legacy and good:
            # json_agg is outside this planner's Volcano aggregate set.
            # With no aggregate ORDER BY, only its integer multiset is fixed.
            for row in result[0]:
                try:
                    values = json.loads(row[width])
                    population = [1, 1] if grouped and row[0] == "1" else [int(row[0])] if grouped else [1, 1, 2, 3]
                    good = good and isinstance(values, list) and sorted(values) == population
                except (TypeError, ValueError):
                    good = False
        print("BOOLEAN_DISTINCT", controls, sql, "pass=", good, "actual=", result, flush=True)
        if not good:
            failures.append((sql, expected, result))

    try:
        for sql in ("CREATE TEMP TABLE bd(g INTEGER,v INTEGER,b BOOLEAN)",
                    "INSERT INTO bd VALUES(1,6,true),(1,9,true),(2,NULL,NULL),(3,1,false)"):
            print("BOOLEAN_DISTINCT_SETUP", sql, flush=True)
            result = query(sql)
            assert result[1] is None, (sql, result)
        for legacy, function in ((legacy, function) for legacy in (False, True)
                                 for function in ("bool_and", "bool_or", "every")):
            extra = ",json_agg(g)" if legacy else ""
            for argument in ("b", "v>5", "CAST(b AS BOOLEAN)", "COALESCE(b,NULL::boolean)"):
                for grouped in (False, True):
                    sql = "SELECT " + ("g," if grouped else "") + function + "(DISTINCT " + argument + ")" + extra + " FROM bd"
                    if grouped:
                        sql += " GROUP BY g ORDER BY g"
                    expected = [["1", "t"], ["2", None], ["3", "f"]] if grouped else [
                        ["t" if function == "bool_or" else "f"]]
                    check(sql, expected, grouped, legacy)
            for grouped in (False, True):
                sql = "SELECT " + ("g," if grouped else "") + function + "(DISTINCT v>5) FILTER(WHERE g=1)" + extra + " FROM bd"
                if grouped:
                    sql += " GROUP BY g ORDER BY g"
                expected = [["1", "t"], ["2", None], ["3", None]] if grouped else [["t"]]
                check(sql, expected, grouped, legacy)
        assert controls == 60, controls
        assert not failures, failures
        print("[BOOLEAN DISTINCT] all 60 controls passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
