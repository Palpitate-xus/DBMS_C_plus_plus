#!/usr/bin/env python3
"""Integer index writes preserve key identity, access paths and durable results."""

from contextlib import contextmanager
import fcntl
import importlib.util
import json
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "integer_write_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def reply(frames, sql, expected_state=None, tag=None, ready=None, returning=False):
        rows, state, message, names, actual_tag, types = runner.decode_wire_result(frames, True)
        assert state == expected_state, (sql, state, message, rows)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        if ready is not None:
            assert frames[-1] == (b"Z", ready), (sql, frames[-1], ready)
        if returning:
            assert names == ["id", "v"] and types == [23, 23], (sql, names, types)
            assert [kind for kind, _ in frames] == [b"T", *([b"D"] * len(rows)), b"C", b"Z"], sql
        return rows

    def query(sql, expected_state=None, *, tag=None, ready=None, returning=False, connection=None):
        frames = client.simple_query(connection or server["sock"], sql)
        return reply(frames, sql, expected_state, tag, ready, returning)

    def analyzed(select):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + select)))
        assert document["actualRows"] == 1, (select, document)

    def restart_after_crash():
        server["process"].kill()
        assert server["process"].wait(timeout=20) == -signal.SIGKILL
        server["sock"].close()
        server["sock"] = None
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                runner.reconnect_ours(server, client)
                return
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline
                time.sleep(0.05)

    def check_point_writes():
        table = "integer_point_write"
        table_key = "integer_point_table_key"
        pair = "integer_point_pair"
        count = 64
        padding = "x" * 504
        point_keys = {-1, *range(1, 7)}
        keys = (-1, *range(1, count + 1))
        values = ",".join(f"({i},{3 * i},'{'s' if i in point_keys else padding}')" for i in keys)
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY,v INT NOT NULL,pad VARCHAR(504))", tag="CREATE TABLE")
        query(f"CREATE TABLE {table_key}(id INT,v INT NOT NULL,pad VARCHAR(504),PRIMARY KEY(id))", tag="CREATE TABLE")
        query(f"CREATE TABLE {pair}(id INT,v INT,pad VARCHAR(504),PRIMARY KEY(id,v))", tag="CREATE TABLE")
        query(f"INSERT INTO {table} VALUES {values}", tag=f"INSERT 0 {len(keys)}")
        query(f"INSERT INTO {table_key} VALUES {values}", tag=f"INSERT 0 {len(keys)}")
        query(f"INSERT INTO {pair} VALUES {values},(1,9999,'{padding}')", tag=f"INSERT 0 {len(keys) + 1}")
        query("CHECKPOINT", tag="CHECKPOINT")
        database = Path(server["dir"], "info")
        page_size = 8192
        slot_size = 4
        protected_pages = {}

        def heap_page(page):
            sizes = {}
            lower, upper = struct.unpack_from("<HH", page, 12)
            assert 24 <= lower <= upper <= page_size
            for item in range(24, lower, slot_size):
                offset, length_and_flags = struct.unpack_from("<HH", page, item)
                if length_and_flags >> 14 == 1:
                    offset &= 0x7fff
                    header_bytes = page[offset + 22]
                    key = struct.unpack_from("<i", page, offset + header_bytes)[0]
                    sizes[key] = length_and_flags & 0x3fff
            return upper - lower, sizes

        for name in (table, table_key, pair):
            image = (database / f"{name}.dt").read_bytes()
            magic, page_count = struct.unpack_from("<II", image)
            assert magic == 0x44415441 and len(image) == page_count * page_size and page_count > 2
            pages = {page: heap_page(image[page * page_size:(page + 1) * page_size])
                     for page in range(1, page_count)}
            first_free, first_sizes = pages[1]
            assert point_keys <= first_sizes.keys(), (name, first_sizes)
            replacement_space = max(first_sizes[key] + slot_size for key in point_keys)
            # Locate the row and place its new version with the earlier page available.
            assert first_free >= 3 * replacement_space, (name, first_free, replacement_space)
            full_unrelated = [page for page, (free, sizes) in pages.items()
                              if page > 1 and free < replacement_space and sizes.keys().isdisjoint(point_keys)]
            assert full_unrelated, (name, pages)
            protected_pages[name] = max(full_unrelated)

        @contextmanager
        def locked_page(name):
            token = f"page:info:{name}:{protected_pages[name]}".encode().hex()
            with (database / ".lockmgr" / f"page-{token}.lock").open("rb") as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield

        with socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout()) as worker:
            pid, _ = client.startup(worker, "alice", "info")

            def work(sql, **expect):
                return query(sql, connection=worker, **expect)

            def point(sql, tag, rows, ready=b"I", name=table):
                with locked_page(name):
                    assert work(sql, tag=tag, ready=ready, returning=True) == rows, sql

            def fallback(sql, tag, expected, ready=b"T", name=table):
                with locked_page(name):
                    worker.sendall(client.typed(b"Q", sql.encode() + b"\0"))
                    assert not select.select([worker], [], [], 0.25)[0], (sql, "completed with page held")
                    assert query(f"SELECT state,query FROM pg_stat_activity WHERE pid={pid}") == [["active", sql]]
                    assert not select.select([worker], [], [], 0)[0], (sql, "completed with page held")
                rows = reply(client.read_until_ready(worker), sql, tag=tag, ready=ready, returning=True)
                assert sorted(rows) == expected, (sql, rows)

            work("SET lock_timeout=800", tag="SET", ready=b"I")
            point(f"UPDATE {table} SET v=777 WHERE id=1 RETURNING id,v", "UPDATE 1", [["1", "777"]])
            point(f"DELETE FROM {table} WHERE id=2 RETURNING id,v", "DELETE 1", [["2", "6"]])
            point(f"UPDATE {table} SET v=776 WHERE id=-1 RETURNING id,v", "UPDATE 1", [["-1", "776"]])
            work("BEGIN ISOLATION LEVEL READ COMMITTED", tag="BEGIN", ready=b"T")
            point(f"UPDATE {table_key} SET v=778 WHERE id=1 RETURNING id,v",
                  "UPDATE 1", [["1", "778"]], b"T", name=table_key)
            work("COMMIT", tag="COMMIT", ready=b"I")
            point(f"DELETE FROM {table_key} WHERE id=2 RETURNING id,v", "DELETE 1", [["2", "6"]], name=table_key)

            work("BEGIN ISOLATION LEVEL READ COMMITTED", tag="BEGIN", ready=b"T")
            rekey = f"UPDATE {table} SET id=-3 WHERE id=3 RETURNING id,v"
            assert work(rekey, tag="UPDATE 1", ready=b"T", returning=True) == [["-3", "9"]]
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")
            work("SET lock_timeout=0", tag="SET", ready=b"I")
            fallback(f"UPDATE {table} SET v=5 WHERE id=999 RETURNING id,v", "UPDATE 0", [], ready=b"I")
            fallback(f"DELETE FROM {table} WHERE id=999 RETURNING id,v", "DELETE 0", [], ready=b"I")

            work("BEGIN", tag="BEGIN", ready=b"T")
            work(f"UPDATE {table} SET v=501 WHERE id=1", tag="UPDATE 1", ready=b"T")
            fallback(f"UPDATE {table} SET v=502 WHERE id=3 RETURNING id,v", "UPDATE 1", [["3", "502"]])
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")

            work("BEGIN ISOLATION LEVEL REPEATABLE READ", tag="BEGIN", ready=b"T")
            assert work(f"SELECT id,v FROM {table} WHERE id=3", tag="SELECT 1", ready=b"T") == [["3", "9"]]
            query(f"UPDATE {table} SET v=909 WHERE id={count}", tag="UPDATE 1", ready=b"I")
            assert work(f"SELECT id,v FROM {table} WHERE id={count}", tag="SELECT 1", ready=b"T") == [
                [str(count), str(3 * count)]]
            fallback(f"DELETE FROM {table} WHERE id=3 RETURNING id,v", "DELETE 1", [["3", "9"]])
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")

            work("BEGIN", tag="BEGIN", ready=b"T")
            fallback(f"UPDATE {table} SET v=555 WHERE id BETWEEN 4 AND 5 RETURNING id,v",
                     "UPDATE 2", [["4", "555"], ["5", "555"]])
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")
            work("BEGIN", tag="BEGIN", ready=b"T")
            fallback(f"DELETE FROM {table} WHERE v=18 RETURNING id,v", "DELETE 1", [["6", "18"]])
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")
            work("BEGIN", tag="BEGIN", ready=b"T")
            fallback(f"DELETE FROM {pair} WHERE id=1 RETURNING id,v", "DELETE 2",
                     [["1", "3"], ["1", "9999"]], name=pair)
            work("ROLLBACK", tag="ROLLBACK", ready=b"I")

        expected = [["-1", "776"], ["1", "777"], ["3", "9"], ["4", "12"], ["5", "15"], ["6", "18"], [str(count), "909"]]
        statement = f"SELECT id,v FROM {table} WHERE id IN (-3,-1,1,2,3,4,5,6,{count}) ORDER BY id"
        assert query(statement, tag=f"SELECT {len(expected)}", ready=b"I") == expected
        restart_after_crash()
        assert query(statement, tag=f"SELECT {len(expected)}", ready=b"I") == expected
        assert query(f"SELECT id,v FROM {table} WHERE id=-1", tag="SELECT 1", ready=b"I") == [["-1", "776"]]
        assert query(f"SELECT id,v FROM {table} WHERE id=1", tag="SELECT 1", ready=b"I") == [["1", "777"]]
        assert query(f"SELECT id,v FROM {table} WHERE id=2", tag="SELECT 0", ready=b"I") == []
        assert query(f"SELECT id,v FROM {table_key} WHERE id=1", tag="SELECT 1", ready=b"I") == [["1", "778"]]
        assert query(f"SELECT id,v FROM {table_key} WHERE id=2", tag="SELECT 0", ready=b"I") == []
        assert query(f"SELECT id,v FROM {pair} WHERE id=1 ORDER BY v", tag="SELECT 2", ready=b"I") == [["1", "3"], ["1", "9999"]]
        print("[INTEGER POINT WRITE] hit, miss, fallback and cold recovery passed")

    try:
        query("CREATE TABLE integer_write (id BIGINT PRIMARY KEY,value BIGINT);")
        query("CREATE INDEX integer_write_value ON integer_write(value);")
        query("INSERT INTO integer_write VALUES ('0001','+0007');")
        analyzed("SELECT id FROM integer_write WHERE id=1;")
        analyzed("SELECT id FROM integer_write WHERE value=7;")
        query("INSERT INTO integer_write VALUES (1,7);", "23505")
        query("INSERT INTO integer_write VALUES ('-0','0000');")
        query("INSERT INTO integer_write VALUES (0,0);", "23505")
        assert sorted(query("SELECT id FROM integer_write;")) == [["0"], ["1"]]
        query("BEGIN;")
        query("UPDATE integer_write SET value='+0008' WHERE id=1;")
        analyzed("SELECT id FROM integer_write WHERE value=8;")
        query("ROLLBACK;")
        analyzed("SELECT id FROM integer_write WHERE id=1 AND value=7;")
        query("REINDEX TABLE integer_write;")
        analyzed("SELECT id FROM integer_write WHERE id=1;")
        query("CREATE TABLE integer_write_pair (a BIGINT,b BIGINT,PRIMARY KEY(a,b));")
        query("INSERT INTO integer_write_pair VALUES ('+0001','0002');")
        query("INSERT INTO integer_write_pair VALUES (1,2);", "23505")
        assert query("SELECT a,b FROM integer_write_pair;") == [["1", "2"]]
        check_point_writes()
        assert query("SELECT 42;") == [["42"]]
        print("[INTEGER INDEX WRITE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
