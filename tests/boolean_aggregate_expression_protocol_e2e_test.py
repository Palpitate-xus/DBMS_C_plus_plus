"""Boolean argument semantics remain typed with parallel planning enabled."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("boolean_argument_runner", root / "tests/compat/pg_diff_runner.py")
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

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql[:160], result)

    def check(sql, rows, types):
        nonlocal controls
        controls += 1
        result = query(sql)
        good = result[1] is None and result[0] == rows and result[5] == types
        print("BOOLEAN_ARGUMENT_WIRE", controls, sql, "pass=", good, "actual=", result, flush=True)
        if not good:
            failures.append((sql, rows, types, result))

    try:
        if reference:
            setup("BEGIN")
        for table, mixed in (("all_true", False), ("mixed", True)):
            setup('CREATE TEMP TABLE ba_' + table + '(id INTEGER,g INTEGER,v INTEGER,b BOOLEAN,t TEXT COLLATE "C")')
            records = []
            for position in range(512):
                kind = position % 4
                group = 2 if kind == 1 else 1
                number = "6" if kind == 0 else ("1" if mixed else "9") if kind == 2 else "NULL"
                boolean = "true" if kind == 0 or kind == 2 and not mixed else "false" if kind == 2 else "NULL"
                text = "''" if kind == 0 else "NULL" if kind == 1 else "'NULL'" if kind == 2 else "'zeta'"
                records.append(f"({position + 1},{group},{number},{boolean},{text})")
            setup("INSERT INTO ba_" + table + " VALUES" + ",".join(records))
        setup("CREATE TEMP TABLE ba_empty(v INTEGER)")
        arguments = ("b", "v>5", "CASE WHEN v IS NULL THEN NULL ELSE v > 5 END",
                     "CAST(b AS BOOLEAN)", "COALESCE(b,NULL::boolean)", "b AND true")
        for workers in (0, 2):
            if reference:
                setup("SET max_parallel_workers_per_gather=" + str(workers))
            else:
                # The owned Root server exposes this process-level setting
                # through persisted configuration plus its real reload path.
                setup("ALTER SYSTEM SET max_parallel_workers_per_gather=" + str(workers))
                reloaded = query("SELECT pg_reload_conf()")
                assert reloaded[1] is None and reloaded[0] == [["t"]] and reloaded[5] == [16], reloaded
            check("SHOW max_parallel_workers_per_gather", [[str(workers)]], [25])
            for table, mixed in (("all_true", False), ("mixed", True)):
                source = "ba_" + table
                for function in ("bool_and", "bool_or", "every"):
                    expected = "t" if not mixed or function == "bool_or" else "f"
                    for argument in arguments:
                        call = function + "(" + argument + ")"
                        check("SELECT " + call + " FROM " + source, [[expected]], [16])
                        check("SELECT g," + call + " FROM " + source + " GROUP BY g ORDER BY g",
                              [["1", expected], ["2", None]], [23, 16])
                check("SELECT bool_or(t=''),bool_or(t='NULL'),bool_and(t=NULL::text) FROM " + source,
                      [["t", "t", None]], [16, 16, 16])
            check("SELECT bool_and(v>5),bool_or(v>5),every(v>5) FROM ba_empty",
                  [[None, None, None]], [16, 16, 16])
        assert controls == 152, controls
        assert not failures, failures
        print("[BOOLEAN AGGREGATE ARGUMENT PROTOCOL] all 152 controls passed")
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
