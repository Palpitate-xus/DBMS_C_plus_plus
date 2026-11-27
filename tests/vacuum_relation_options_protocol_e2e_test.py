#!/usr/bin/env python3
"""Check VACUUM relation selection through the wire and on disk."""

import importlib.util
from pathlib import Path
import random
import signal
import socket
import struct
import subprocess
import time
from unittest import mock


PAGE_SIZE = 8192
PAGE_HEADER_SIZE = 24
ITEM_ID_SIZE = 4
LP_NORMAL = 1
LP_UNUSED = 0


def heap_pages(image):
    magic, page_count, _, _, version = struct.unpack_from("<IIIII", image)
    assert magic == 0x44415441 and version == 2
    assert len(image) == page_count * PAGE_SIZE
    for block in range(1, page_count):
        page = image[block * PAGE_SIZE:(block + 1) * PAGE_SIZE]
        lower, upper, special = struct.unpack_from("<HHH", page, 12)
        assert PAGE_HEADER_SIZE <= lower <= upper <= special <= PAGE_SIZE
        assert (lower - PAGE_HEADER_SIZE) % ITEM_ID_SIZE == 0
        unused_bytes = 0
        items = []
        for item in range(PAGE_HEADER_SIZE, lower, ITEM_ID_SIZE):
            offset, length_flags = struct.unpack_from("<HH", page, item)
            flags = length_flags >> 14
            length = length_flags & 0x3FFF
            if flags == LP_NORMAL:
                offset &= 0x7FFF
                assert length and upper <= offset and offset + length <= special
                items.append(page[offset:offset + length])
            elif flags == LP_UNUSED:
                unused_bytes += length
        yield upper - lower, unused_bytes, items


def toast_chunks(image):
    keys = set()
    for _, _, items in heap_pages(image):
        for item in items:
            key = struct.unpack_from("<QI", item)
            assert key[0] and key not in keys
            keys.add(key)
    return keys


def main():
    spec = importlib.util.spec_from_file_location(
        "vacuum_relation_options_runner",
        Path(__file__).with_name("compat") / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    mkdtemp = runner.tempfile.mkdtemp

    def configured_directory(*args, **kwargs):
        directory = mkdtemp(*args, **kwargs)
        (Path(directory) / "dbms.conf").write_text(
            "auto_analyze=off\nauto_vacuum=off\n", encoding="ascii")
        return directory

    with mock.patch.object(runner.tempfile, "mkdtemp", side_effect=configured_directory):
        server = runner.start_ours(client)
    database = Path(server["dir"]) / "info"
    old, retained, new = (random.Random(seed).getrandbits(4500 * 8).to_bytes(4500, "little").hex()
                          for seed in (2051, 7907, 3301))
    live_rows = [["1", new], ["2", retained]]

    def query(sql, tag=None, ready=b"I", state=None, rows=None, connection=None):
        conn = server["sock"] if connection is None else connection
        frames = client.simple_query(conn, sql)
        actual_rows, actual_state, message, headers, actual_tag, types = (
            runner.decode_wire_result(frames, include_types=True))
        statement = sql if len(sql) < 180 else sql[:180] + "…"
        assert actual_state == state, (statement, actual_state, message, state)
        assert actual_tag == tag, (statement, actual_tag, tag)
        assert frames[-1] == (b"Z", ready), (statement, frames[-1], ready)
        if state:
            assert [kind for kind, _ in frames] == [b"E", b"Z"], statement
        elif rows is None:
            assert frames == [(b"C", tag.encode() + b"\0"), (b"Z", ready)], statement
        else:
            assert actual_rows == rows, f"Unexpected payload bytes for {statement}"
            assert headers == ["id", "payload"] and types == [23, 25], statement
            assert [kind for kind, _ in frames] == [b"T", *([b"D"] * len(rows)), b"C", b"Z"], statement

    def read_rows(table, rows=live_rows, connection=None):
        ready = b"T" if connection is not None else b"I"
        query(f"SELECT id,payload FROM {table} ORDER BY id", f"SELECT {len(rows)}",
              ready=ready, rows=rows, connection=connection)

    def new_connection():
        conn = socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
        try:
            client.startup(conn, "alice", "info")
        except BaseException:
            conn.close()
            raise
        return conn

    def checkpoint():
        query("CHECKPOINT", "CHECKPOINT")

    def images(table):
        return tuple((database / f"{table}.{suffix}").read_bytes()
                     for suffix in ("dt", "toast.dt", "toast.idx"))

    def seed(table, with_snapshot=False):
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
        query(f"INSERT INTO {table} VALUES (1,'{old}')", "INSERT 0 1")
        checkpoint()
        orphan = toast_chunks(images(table)[1])
        assert len(orphan) > 1 and len({key[0] for key in orphan}) == 1, table
        query(f"INSERT INTO {table} VALUES (2,'{retained}')", "INSERT 0 1")
        update = f"UPDATE {table} SET payload='{new}' WHERE id=1"
        if with_snapshot:
            with new_connection() as reader:
                query("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T", connection=reader)
                before = [["1", old], ["2", retained]]
                read_rows(table, before, reader)
                query(update, "UPDATE 1")
                read_rows(table)
                query(f"VACUUM (PROCESS_MAIN FALSE) {table}", "VACUUM")
                read_rows(table, before, reader)
                query("COMMIT", "COMMIT", connection=reader)
        else:
            query(update, "UPDATE 1")

        # INSERT undo fragments the main heap while the old tuple still
        # physically references its external value.
        query("BEGIN", "BEGIN", b"T")
        query(f"INSERT INTO {table} VALUES (99,'{'x' * 500}')", "INSERT 0 1", b"T")
        query("ROLLBACK", "ROLLBACK")
        checkpoint()
        read_rows(table)
        main_image, toast_image, index_image = images(table)
        [(free_space, unused_bytes, items)] = list(heap_pages(main_image))
        assert len(items) == 3 and unused_bytes > 0, table
        keys = toast_chunks(toast_image)
        assert orphan < keys and len({key[0] for key in keys}) == 3, table
        return (main_image, toast_image, index_image), free_space

    def check_rejected_options(table):
        # SQLSTATEs and EZ packet shape observed on PostgreSQL 18.6.
        for options, state in (
            ("PROCESS_MAIN 2", "42601"),
            ("PROCESS_TOAST = FALSE", "42601"),
            ("FULL, PROCESS_TOAST FALSE", "0A000"),
            ("FULL, PROCESS_MAIN FALSE, PROCESS_TOAST FALSE", "0A000"),
            ("FULL, PARALLEL 1", "0A000"),
            ("PARALLEL 2, FULL", "0A000"),
            ("FULL, PARALLEL 0, PARALLEL 1", "0A000"),
            ("FULL, PARALLEL 1024", "0A000"),
            ("PARALLEL", "42601"),
            ("PARALLEL -1", "42601"),
            ("PARALLEL 1025", "42601"),
            ("PARALLEL 2147483648", "42601"),
            ("PARALLEL '1'", "42601"),
            ("PARALLEL 1.0", "42601"),
            ("FULL, PARALLEL 1025, PARALLEL 0", "42601"),
        ):
            query(f"VACUUM ({options}) {table}", state=state)
        query("VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) vacuum_missing_probe", state="42P01")
        query("BEGIN", "BEGIN", b"T")
        query(f"VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) {table}", ready=b"E", state="25001")
        query("ROLLBACK", "ROLLBACK")
        query("BEGIN", "BEGIN", b"T")
        query(f"VACUUM (FULL, PARALLEL 1) {table}", ready=b"E", state="0A000")
        query("ROLLBACK", "ROLLBACK")

    def check_toast_fork_admission():
        query("CREATE TABLE vacuum_fixed (id INT PRIMARY KEY, value INT)", "CREATE TABLE")
        query("INSERT INTO vacuum_fixed VALUES (1,42)", "INSERT 0 1")
        checkpoint()
        fixed_heap = database / "vacuum_fixed.dt"
        fixed_before = fixed_heap.read_bytes()
        assert sum(len(items) for _, _, items in heap_pages(fixed_before)) == 1
        query("VACUUM (PROCESS_MAIN FALSE) vacuum_fixed", "VACUUM")
        checkpoint()
        assert fixed_heap.read_bytes() == fixed_before
        assert not (database / "vacuum_fixed.toast.dt").exists()
        assert not (database / "vacuum_fixed.toast.idx").exists()

        table = "vacuum_missing_toast_index"
        query(f"CREATE TABLE {table} (id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
        query(f"INSERT INTO {table} VALUES (1,'{retained}')", "INSERT 0 1")
        checkpoint()
        before = images(table)
        assert toast_chunks(before[1]), "missing-partner control needs real TOAST chunks"
        index = database / f"{table}.toast.idx"
        saved = index.with_suffix(".saved")
        index.rename(saved)
        try:
            query(f"VACUUM (PROCESS_TOAST FALSE) {table}", "VACUUM")
            query(f"VACUUM (PROCESS_MAIN FALSE) {table}", state="58030")
            assert not index.exists()
        finally:
            saved.rename(index)
        assert images(table) == before, "failed TOAST-only VACUUM must preserve the existing files"
        read_rows(table, [["1", retained]])

    def cold_restart(crash=False):
        if crash:
            server["process"].kill()
        server["sock"].close()
        server["sock"] = None
        if not crash:
            server["process"].terminate()
        expected_exit = -signal.SIGKILL if crash else 0
        assert server["process"].wait(timeout=20) == expected_exit, "unexpected VACUUM fixture exit status"
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                server["sock"] = new_connection()
                return
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline, "VACUUM fixture server failed to restart"
                time.sleep(0.05)

    def free_blocks(table):
        image = (database / f"{table}.dt").read_bytes()
        blocks = list(heap_pages(image))
        head = struct.unpack_from("<I", image, 8)[0]
        free = set()
        while head:
            assert 1 <= head <= len(blocks) and head not in free, (table, head, free)
            assert blocks[head - 1][1:] == (0, []), (table, "occupied free block", head)
            free.add(head)
            head = struct.unpack_from("<I", image, (head + 1) * PAGE_SIZE - 4)[0]
        return len(blocks), free

    def check_reclaimed_pages():
        payload = "x" * 1700
        originals = [[str(id), payload] for id in range(1, 5)]
        expected_by_table = {}
        for action in ("insert", "update"):
            table = f"vacuum_reuse_{action}"
            query(f"CREATE TABLE {table} (id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
            seed = ",".join(f"({id},'{payload}')" for id in range(1, 5))
            query(f"INSERT INTO {table} VALUES {seed}", "INSERT 0 4")
            query("BEGIN", "BEGIN", b"T")
            extra = ",".join(f"({id},'{payload}')" for id in range(5, 10))
            query(f"INSERT INTO {table} VALUES {extra}", "INSERT 0 5", b"T")
            query("ROLLBACK", "ROLLBACK")
            query(f"VACUUM (PARALLEL 2) {table}", "VACUUM")
            checkpoint()
            assert free_blocks(table) == (3, {2, 3}), table
            read_rows(table, originals)

            if action == "insert":
                query(f"INSERT INTO {table} VALUES (5,'{payload}')", "INSERT 0 1")
                expected_by_table[table] = originals + [["5", payload]]
            else:
                modified = "y" * 1700
                query(f"UPDATE {table} SET payload='{modified}' WHERE id=1", "UPDATE 1")
                expected_by_table[table] = [["1", modified], *originals[1:]]
            checkpoint()
            count, free = free_blocks(table)
            assert count == 3 and len(free) == 1 and free <= {2, 3}, (table, count, free)
            read_rows(table, expected_by_table[table])

        # The committed pages and the last such page selected by an open
        # transaction must remain readable after process recovery.
        query("BEGIN", "BEGIN", b"T")
        pending = ",".join(f"({id},'{payload}')" for id in range(6, 10))
        query(f"INSERT INTO vacuum_reuse_insert VALUES {pending}", "INSERT 0 4", b"T")
        cold_restart(crash=True)
        for table, expected in expected_by_table.items():
            read_rows(table, expected)
        query(f"INSERT INTO vacuum_reuse_update VALUES (5,'{payload}')", "INSERT 0 1")
        read_rows("vacuum_reuse_update", expected_by_table["vacuum_reuse_update"] + [["5", payload]])

    def check_reclaimed_page_search():
        def buffer_requests():
            frames = client.simple_query(server["sock"], "SHOW STATUS")
            result, state, message, _, _ = runner.decode_wire_result(frames)
            assert state is None, message
            stats = dict(result)
            return int(stats["buffer_pool_hits"]) + int(stats["buffer_pool_misses"])

        table = "vacuum_reuse_dense"
        payload = "z" * 1700
        query(f"CREATE TABLE {table} (id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
        query("BEGIN", "BEGIN", b"T")
        for start in range(1, 257, 64):
            batch = ",".join(f"({id},'{payload}')" for id in range(start, start + 64))
            query(f"INSERT INTO {table} VALUES {batch}", "INSERT 0 64", b"T")
        query("COMMIT", "COMMIT")
        query("BEGIN", "BEGIN", b"T")
        query(f"INSERT INTO {table} VALUES (257,'{payload}')", "INSERT 0 1", b"T")
        query("ROLLBACK", "ROLLBACK")
        cases = (
            (f"INSERT INTO {table} VALUES (257,'{payload}')", "INSERT 0 1"),
            (f"UPDATE {table} SET payload='{'y' * 1700}' WHERE id=1", "UPDATE 1"),
        )
        for sql, tag in cases:
            query(f"VACUUM {table}", "VACUUM")
            checkpoint()
            assert free_blocks(table) == (65, {65})
            query("BEGIN", "BEGIN", b"T")
            before = buffer_requests()
            query(sql, tag, b"T")
            # The single write may read metadata and its target. Searching
            # all 64 occupied heap blocks adds at least 64 buffer requests.
            requests = buffer_requests() - before
            assert 0 < requests < 64, (tag, requests)
            query("ROLLBACK", "ROLLBACK")
            read_rows(table, [[str(id), payload] for id in range(1, 257)])
        query(f"VACUUM {table}", "VACUUM")
        query(f"UPDATE {table} SET payload='short' WHERE id=1", "UPDATE 1")
        checkpoint()
        assert free_blocks(table) == (65, {65}), "a fitting current page takes priority"

    def check_full(process_main):
        table = "vacuum_full_both" if process_main else "vacuum_full_toast"
        query(f"CREATE TABLE {table} (id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
        query(f"INSERT INTO {table} VALUES (97,NULL),(98,''),(99,'inline')", "INSERT 0 3")

        def long_text(id):
            return random.Random(899 + id).getrandbits(120000).to_bytes(15000, "little").hex()

        def insert_long(id):
            query(f"INSERT INTO {table} VALUES ({id},'{long_text(id)}')", "INSERT 0 1")

        for id in (1, 2):
            insert_long(id)
        checkpoint()
        remaining = toast_chunks(images(table)[1])
        assert len({key[0] for key in remaining}) == 2
        for id in range(3, 9):
            insert_long(id)
        if process_main:
            inline = "x" * 1000
            values = ",".join(f"({id},'{inline}')" for id in range(100, 140))
            query(f"INSERT INTO {table} VALUES {values}", "INSERT 0 40")
        query("BEGIN", "BEGIN", b"T")
        query(f"DELETE FROM {table} WHERE id BETWEEN 3 AND 8", "DELETE 6", b"T")
        if process_main:
            query(f"DELETE FROM {table} WHERE id >= 100", "DELETE 40", b"T")
        query("COMMIT", "COMMIT")
        checkpoint()

        expected = [[str(id), long_text(id)] for id in (1, 2)] + [["97", None], ["98", ""], ["99", "inline"]]
        read_rows(table, expected)

        def main_files():
            return {path.name: (path.stat().st_ino, path.read_bytes())
                    for path in database.glob(table + ".*")
                    if path.is_file() and not path.name.startswith(table + ".toast")}

        before_main = main_files()
        assert f"{table}.dt" in before_main and f"{table}.idx" in before_main
        _, before_toast, before_index = images(table)
        original = toast_chunks(before_toast)
        assert remaining < original and len({key[0] for key in original}) == 8
        counter = database / f"{table}.toastmeta"
        counter_before = counter.read_bytes()

        command = ("VACUUM FULL" if process_main else
                   "VACUUM (FULL, PROCESS_MAIN FALSE, PARALLEL 1, PARALLEL 0)")
        query(f"{command} {table}", "VACUUM")
        after_heap, compacted, after_index = images(table)
        assert len(compacted) < len(before_toast), f"{command} must shrink the TOAST extent"
        assert toast_chunks(compacted) == remaining, "TOAST rewrite must preserve exactly the live chunk IDs"
        assert after_index != before_index
        after_main = main_files()
        before_heap = before_main[f"{table}.dt"][1]
        if process_main:
            assert len(after_heap) < len(before_heap), "default FULL must also shrink the main heap"
            assert sum(len(items) for _, _, items in heap_pages(after_heap)) == len(expected)
            assert after_main[f"{table}.stc"][1] != before_main[f"{table}.stc"][1]
        else:
            assert after_main == before_main, "TOAST-only FULL must leave all main files and inodes intact"
        assert counter.read_bytes() == counter_before

        # Cold recovery sees the completed rewrite without a checkpoint or a
        # graceful server close after VACUUM acknowledges it.
        cold_restart(crash=True)
        read_rows(table, expected)
        if process_main:
            query(f"SELECT id,payload FROM {table} WHERE id=2", "SELECT 1", rows=[expected[1]])
            query(f"SELECT id,payload FROM {table} WHERE id=3", "SELECT 0", rows=[])
        assert images(table) == (after_heap, compacted, after_index)
        assert main_files() == after_main and counter.read_bytes() == counter_before
        assert not (database / f".{table}.vacuum_full_pending").exists()
        insert_long(9)
        read_rows(table, expected[:2] + [["9", long_text(9)]] + expected[2:])
        checkpoint()
        new_chunks = toast_chunks(images(table)[1])
        added = new_chunks - remaining
        assert remaining < new_chunks and len({key[0] for key in added}) == 1
        assert {key[0] for key in added}.isdisjoint({key[0] for key in original})
        print(f"[{command}] main {len(before_heap)} -> {len(after_heap)} bytes, "
              f"TOAST {len(before_toast)} -> {len(compacted)} bytes, exact cold rows")

    def check_full_without_toast_data():
        fixed = "vacuum_fixed"
        fixed_files = [database / f"{fixed}.{suffix}" for suffix in ("dt", "idx")]
        fixed_before = [path.read_bytes() for path in fixed_files]
        query(f"VACUUM (FULL, PROCESS_MAIN FALSE) {fixed}", "VACUUM")
        assert [path.read_bytes() for path in fixed_files] == fixed_before
        assert not list(database.glob(f"{fixed}.toast*"))
        for workers in ("0", "1", "1024", "+1", "0x1", "0o1", "0b1", "1_0"):
            query(f"VACUUM (PARALLEL {workers}) {fixed}", "VACUUM")
        query(f"VACUUM (PARALLEL = 2) {fixed}", "VACUUM")

        table = "vacuum_inline_toast"
        query(f"CREATE TABLE {table} (id INT PRIMARY KEY, payload TEXT)", "CREATE TABLE")
        query(f"INSERT INTO {table} VALUES (1,'inline'),(2,NULL),(3,'')", "INSERT 0 3")
        checkpoint()
        main_before, toast_before, _ = images(table)
        primary = database / f"{table}.idx"
        primary_before = primary.read_bytes()
        assert len(toast_before) == PAGE_SIZE and not toast_chunks(toast_before)
        query(f"VACUUM (FULL, PROCESS_MAIN FALSE) {table}", "VACUUM")
        main_after, toast_after, _ = images(table)
        assert main_after == main_before and primary.read_bytes() == primary_before
        assert len(toast_after) == PAGE_SIZE and not toast_chunks(toast_after)
        read_rows(table, [["1", "inline"], ["2", None], ["3", ""]])
        query(f"VACUUM (FULL, PARALLEL 0) {table}", "VACUUM")
        read_rows(table, [["1", "inline"], ["2", None], ["3", ""]])

    cases = (
        ("vacuum_main_only", " (PROCESS_TOAST FALSE)", True),
        ("vacuum_toast_only", " (PROCESS_MAIN 'FaLsE', PROCESS_TOAST 'oN')", False),
        ("vacuum_both", "", True),
    )
    try:
        check_toast_fork_admission()
        post_vacuum = {}
        for table, options, process_main in cases:
            before, free_space = seed(table, with_snapshot=not process_main)
            if not process_main:
                check_rejected_options(table)
                checkpoint()
                assert images(table) == before, "rejected options must preserve both relations"
                disabled = f"VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) {table}"
                query(disabled, "VACUUM")
                query(f"VACUUM (PROCESS_MAIN, PROCESS_MAIN FALSE, PROCESS_TOAST 0) {table}", "VACUUM")
                checkpoint()
                assert images(table) == before, "both options disabled must preserve both relations"
                stats = database / ".stats"
                pattern = f"{table} __rows__ 2||".encode()
                assert not stats.exists() or pattern not in stats.read_bytes(), table
                query(f"VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE, ANALYZE) {table}", "VACUUM")
                assert pattern in stats.read_bytes(), "ANALYZE runs with both VACUUM phases disabled"
                checkpoint()
                assert images(table) == before, "ANALYZE only must preserve both relations"

            query(f"VACUUM{options} {table}", "VACUUM")
            checkpoint()
            after = images(table)
            if process_main:
                [(compacted_free_space, unused_bytes, _)] = list(heap_pages(after[0]))
                assert not unused_bytes and compacted_free_space > free_space, f"{table} did not reclaim main heap space"
            else:
                assert after[0] == before[0], f"{table} modified the skipped main heap"
            assert after[1:] == before[1:], f"{table} modified TOAST reachable from physical tuples"
            read_rows(table)
            post_vacuum[table] = after

        query("VACUUM (PROCESS_MAIN, PROCESS_TOAST) vacuum_both", "VACUUM")
        cold_restart()
        for table, after in post_vacuum.items():
            read_rows(table)
            assert images(table) == after, f"{table} did not retain its VACUUM state after reopening"
        check_full(process_main=False)
        check_full(process_main=True)
        check_full_without_toast_data()
        check_reclaimed_pages()
        check_reclaimed_page_search()
        print("[VACUUM RELATION OPTIONS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
