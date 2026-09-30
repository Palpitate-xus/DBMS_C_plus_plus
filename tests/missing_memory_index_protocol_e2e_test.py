#!/usr/bin/env python3
"""Hash/Bloom bitmap reads must report a missing declared index on the wire."""

import importlib.util
from pathlib import Path
import socket


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "memory_index_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    observer = None

    def expect(sql, state=None, sock=None):
        rows, actual_state, message, _, _ = runner.decode_wire_result(
            client.simple_query(sock or server["sock"], sql))
        assert actual_state == state, (sql, rows, actual_state, message)
        return rows

    try:
        observer = socket.create_connection(("127.0.0.1", server["port"]),
                                            timeout=runner.wire_timeout())
        client.startup(observer, "alice", "info")
        expect("SET lock_timeout = 50;", sock=observer)
        for method, suffix in (("hash", "hidx"), ("bloom", "bidx")):
            table = "missing_" + method
            index = table + "_value_idx"
            expect("CREATE TABLE " + table + " (id INT PRIMARY KEY,value INT);")
            expect("INSERT INTO " + table + " VALUES (1,7),(2,7);")
            create = "CREATE INDEX " + index + " ON " + table + " USING " + method + " (value);"
            expect(create)
            bitmap = "SELECT id FROM " + table + " WHERE value=7 AND id=1;"
            disjunction = "SELECT id FROM " + table + " WHERE value=7 OR id=2;"
            assert expect(bitmap) == [["1"]]
            assert sorted(expect(disjunction)) == [["1"], ["2"]]
            # Bitmap EXPLAIN currently renders this node as "Unknown"; that
            # separate instrumentation gap must not replace the row/error
            # assertions for the actual production query path here.
            path = Path(server["dir"]) / "info" / (table + "_value." + suffix)
            assert path.is_file(), path
            path.unlink()
            # The ordinary SELECT currently uses the valid primary index
            # with a heap recheck. EXPLAIN ANALYZE actually runs the bitmap
            # plan and must surface its missing declared mapping as an error.
            assert expect(bitmap) == [["1"]]
            expect("EXPLAIN ANALYZE " + bitmap, "XX001")
            assert not path.exists(), path
            assert expect("SELECT id FROM " + table + ";") == [["1"], ["2"]]
            assert expect("SELECT 42;") == [["42"]]
            # Failure during open() must release the first connection's
            # relation token; another backend must be able to acquire DDL.
            expect("DROP INDEX " + index + ";", sock=observer)
            expect(create)
            assert path.is_file(), path
            assert expect(bitmap) == [["1"]]
            assert sorted(expect(disjunction)) == [["1"], ["2"]]
        print("[MISSING MEMORY INDEX PROTOCOL E2E] passed")
    finally:
        if observer is not None:
            observer.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
