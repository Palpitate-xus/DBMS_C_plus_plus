#!/usr/bin/env python3
"""DELETE ONLY retains typed qualification, RETURNING and its physical target."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("delete_only_runner", root / "tests/compat/pg_diff_runner.py")
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
    failures = []
    controls = 0
    initial = [["1", "one"], ["2", "two"], ["3", None], [None, "nil"]]
    children = [["2", "child-two"], ["5", "child-five"]]

    def query(sql):
        return r.decode_wire_result(c.simple_query(sock, sql), include_types=True)

    def good(sql, rows=None):
        result = query(sql)
        assert result[1] is None, (sql, result)
        if rows is not None:
            assert result[0] == rows, (sql, result)
        return result

    def snapshot(expected):
        parent = query("SELECT id,payload FROM ONLY only_parent ORDER BY id NULLS LAST")
        child = query("SELECT id,payload FROM only_child ORDER BY id")
        if parent[1] is not None or parent[0] != expected or child[1] is not None or child[0] != children:
            failures.append(("physical target snapshot", parent, child, expected))

    def deletion(sql, rows, remaining, oids=None, tag="DELETE 1"):
        nonlocal controls
        controls += 1
        good("BEGIN")
        result = query(sql)
        valid = result[1] is None and result[0] == rows and result[4] == tag
        if oids is not None:
            valid = valid and result[5] == oids
        print("DELETE_ONLY", sql, result, "match", valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, tag, oids))
        if result[1] is None:
            snapshot(remaining)
        good("ROLLBACK")
        snapshot(initial)

    def rejected(sql, state):
        nonlocal controls
        controls += 1
        # Autocommit is essential: a manual rollback must not hide a partial
        # deletion when a later candidate or RETURNING expression fails.
        result = query(sql)
        valid = result[1] == state and result[4] is None
        print("DELETE_ONLY_ERROR", sql, result, "match", valid, flush=True)
        if not valid:
            failures.append((sql, result, state))
        snapshot(initial)

    try:
        good("CREATE TEMP TABLE only_parent(id INTEGER,payload TEXT)")
        good("CREATE TEMP TABLE only_child(note TEXT) INHERITS (only_parent)")
        good("CREATE TEMP TABLE only_filter(id INTEGER)")
        good("INSERT INTO only_parent VALUES(1,'one'),(2,'two'),(3,NULL),(NULL,'nil')")
        good("INSERT INTO only_child VALUES(2,'child-two','x'),(5,'child-five','y')")
        good("INSERT INTO only_filter VALUES(2)")
        snapshot(initial)
        deletion("DELETE FROM ONLY only_parent AS p WHERE p.id=2 RETURNING p.id,p.payload",
                 [["2", "two"]], [initial[0], initial[2], initial[3]], [23, 25])
        deletion("DELETE FROM ONLY only_parent AS p WHERE p.payload IS NULL RETURNING p.id,p.payload",
                 [["3", None]], [initial[0], initial[1], initial[3]], [23, 25])
        deletion("DELETE FROM ONLY only_parent AS p WHERE p.id IS NULL RETURNING p.id,p.payload",
                 [[None, "nil"]], initial[:3], [23, 25])
        deletion('DELETE FROM ONLY (pg_temp.only_parent) AS "P" WHERE "P".id=2 RETURNING "P".*',
                 [["2", "two"]], [initial[0], initial[2], initial[3]], [23, 25])
        deletion("DELETE FROM ONLY pg_temp.only_parent WHERE pg_temp.only_parent.id=1 RETURNING pg_temp.only_parent.id",
                 [["1"]], initial[1:], [23])
        deletion("DELETE FROM ONLY only_parent AS p USING only_filter AS f WHERE p.id=f.id RETURNING p.id",
                 [["2"]], [initial[0], initial[2], initial[3]], [23])
        deletion("DELETE FROM ONLY only_parent WHERE false RETURNING id", [], initial, [23], "DELETE 0")
        deletion("DELETE FROM ONLY only_parent AS p WHERE p.id=2", [],
                 [initial[0], initial[2], initial[3]])
        rejected("DELETE FROM ONLY only_parent AS p WHERE only_parent.id=2 RETURNING p.id", "42P01")
        rejected("DELETE FROM ONLY only_parent AS p WHERE p.missing=2 RETURNING p.id", "42703")
        rejected("DELETE FROM ONLY only_parent AS p WHERE 1 RETURNING p.id", "42804")
        rejected("DELETE FROM ONLY only_parent WHERE id<3 RETURNING 10/(id-2)", "22012")
        assert controls == 12, controls
        assert not failures, failures
        print("[DELETE ONLY QUALIFIED RETURNING] all 12 controls and inheritance snapshots passed")
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()  # all reference objects are connection-local TEMP tables


if __name__ == "__main__":
    main()
