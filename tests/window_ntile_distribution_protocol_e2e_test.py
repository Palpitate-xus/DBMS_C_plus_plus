#!/usr/bin/env python3
"""NTILE balances leading buckets and never skips bucket IDs."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "window_ntile_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]

    def query(sql):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result)
        return result

    failures = []
    controls = 0
    try:
        query("CREATE TEMP TABLE tile_rows(id INT,p TEXT)")
        query("CREATE TEMP TABLE tile_empty(id INT,p TEXT)")
        values = ",".join(f"({i},'{('a' if i <= 6 else 'b' if i <= 13 else 'c')}')"
                          for i in range(1, 15))
        query("INSERT INTO tile_rows VALUES" + values)
        # Explicit oracle vectors, not a copy of the executor formula.
        cases = [
            (1, [1,1,1,1,1,1], [1,1,1,1,1,1,1]),
            (2, [1,1,1,2,2,2], [1,1,1,1,2,2,2]),
            (3, [1,1,2,2,3,3], [1,1,1,2,2,3,3]),
            (4, [1,1,2,2,3,4], [1,1,2,2,3,3,4]),
            (8, [1,2,3,4,5,6], [1,2,3,4,5,6,7]),
            (2147483647, [1,2,3,4,5,6], [1,2,3,4,5,6,7]),
        ]
        for buckets, six, seven in cases:
            for legacy in [False, True]:
                extra = ",nth_value(id,1) OVER (PARTITION BY p ORDER BY id) AS first" if legacy else ""
                sql = (f"SELECT id,ntile({buckets}) OVER (PARTITION BY p ORDER BY id) AS bucket"
                       f"{extra} FROM tile_rows ORDER BY id")
                expected = [[str(i), str(bucket)] for i, bucket in enumerate(six + seven + [1], 1)]
                if legacy:
                    for i, row in enumerate(expected, 1):
                        row.append("1" if i <= 6 else "7" if i <= 13 else "14")
                result = query(sql)
                valid = (result[0] == expected and result[3] == ["id", "bucket"] + (["first"] if legacy else [])
                         and result[4] == "SELECT 14" and result[5] == [23, 23] + ([23] if legacy else []))
                print("WINDOW_NTILE", sql, result, "match", valid, flush=True)
                if not valid:
                    failures.append((sql, result, expected))
                controls += 1
            sql = (f"SELECT ntile({buckets}) OVER (PARTITION BY p ORDER BY id) AS bucket "
                   "FROM tile_empty")
            result = query(sql)
            valid = result[0] == [] and result[3:] == (["bucket"], "SELECT 0", [23])
            print("WINDOW_NTILE_EMPTY", sql, result, "match", valid, flush=True)
            if not valid:
                failures.append((sql, result))
            controls += 1
        assert controls == 18
        assert not failures, failures
        print("[WINDOW NTILE DISTRIBUTION PROTOCOL] 18 controls passed")
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
