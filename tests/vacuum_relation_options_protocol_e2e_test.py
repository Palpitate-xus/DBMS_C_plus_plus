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

        # The heap rewrite discards the retired tuple. Its real TOAST chunks
        # are now unreferenced. INSERT undo gives the main heap reclaimable space.
        query(f"VACUUM FULL {table}", "VACUUM")
        query("BEGIN", "BEGIN", b"T")
        query(f"INSERT INTO {table} VALUES (99,'{'x' * 500}')", "INSERT 0 1", b"T")
        query("ROLLBACK", "ROLLBACK")
        checkpoint()
        read_rows(table)
        main_image, toast_image, index_image = images(table)
        [(free_space, unused_bytes, items)] = list(heap_pages(main_image))
        assert len(items) == 2 and unused_bytes > 0, table
        keys = toast_chunks(toast_image)
        assert orphan < keys and len({key[0] for key in keys}) == 3, table
        return (main_image, toast_image, index_image), keys - orphan, free_space

    def check_rejected_options(table):
        # SQLSTATEs and EZ packet shape observed on PostgreSQL 18.6.
        for options, state in (
            ("PROCESS_MAIN 2", "42601"),
            ("PROCESS_TOAST = FALSE", "42601"),
            ("FULL, PROCESS_TOAST FALSE", "0A000"),
            ("FULL, PROCESS_MAIN FALSE, PROCESS_TOAST FALSE", "0A000"),
        ):
            query(f"VACUUM ({options}) {table}", state=state)
        query("VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) vacuum_missing_probe", state="42P01")
        query("BEGIN", "BEGIN", b"T")
        query(f"VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) {table}", ready=b"E", state="25001")
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

    def check_toast_only_full():
        table = "vacuum_full_toast"
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
        query("BEGIN", "BEGIN", b"T")
        query(f"DELETE FROM {table} WHERE id BETWEEN 3 AND 8", "DELETE 6", b"T")
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

        query(f"VACUUM (FULL, PROCESS_MAIN FALSE) {table}", "VACUUM")
        _, compacted, after_index = images(table)
        assert len(compacted) < len(before_toast), "TOAST-only FULL must shrink the physical extent"
        assert toast_chunks(compacted) == remaining, "TOAST rewrite must preserve exactly the live chunk IDs"
        assert after_index != before_index
        assert main_files() == before_main, "TOAST-only FULL must leave all main files and inodes intact"
        assert counter.read_bytes() == counter_before

        # Cold recovery sees the completed rewrite without a checkpoint or a
        # graceful server close after VACUUM acknowledges it.
        cold_restart(crash=True)
        read_rows(table, expected)
        assert images(table)[1:] == (compacted, after_index)
        assert main_files() == before_main and counter.read_bytes() == counter_before
        assert not (database / f".{table}.vacuum_full_pending").exists()
        insert_long(9)
        read_rows(table, expected[:2] + [["9", long_text(9)]] + expected[2:])
        checkpoint()
        new_chunks = toast_chunks(images(table)[1])
        added = new_chunks - remaining
        assert remaining < new_chunks and len({key[0] for key in added}) == 1
        assert {key[0] for key in added}.isdisjoint({key[0] for key in original})
        print(f"[VACUUM TOAST FULL] {len(before_toast)} -> {len(compacted)} bytes, exact cold rows")

    def check_full_without_toast_data():
        fixed = "vacuum_fixed"
        fixed_files = [database / f"{fixed}.{suffix}" for suffix in ("dt", "idx")]
        fixed_before = [path.read_bytes() for path in fixed_files]
        query(f"VACUUM (FULL, PROCESS_MAIN FALSE) {fixed}", "VACUUM")
        assert [path.read_bytes() for path in fixed_files] == fixed_before
        assert not list(database.glob(f"{fixed}.toast*"))

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

    cases = (
        ("vacuum_main_only", " (PROCESS_TOAST FALSE)", True, False),
        ("vacuum_toast_only", " (PROCESS_MAIN 'FaLsE', PROCESS_TOAST 'oN')", False, True),
        ("vacuum_both", "", True, True),
    )
    try:
        check_toast_fork_admission()
        post_vacuum = {}
        for table, options, process_main, process_toast in cases:
            before, remaining, free_space = seed(table, with_snapshot=not process_main)
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
            if process_toast:
                assert toast_chunks(after[1]) == remaining, f"{table} did not reclaim exactly the orphan's chunks"
                assert after[2] != before[2], f"{table} did not remove the orphan's index entries"
            else:
                assert after[1:] == before[1:], f"{table} modified the skipped TOAST heap or index"
            read_rows(table)
            post_vacuum[table] = after

        query("VACUUM (PROCESS_MAIN, PROCESS_TOAST) vacuum_both", "VACUUM")
        cold_restart()
        for table, after in post_vacuum.items():
            read_rows(table)
            assert images(table) == after, f"{table} did not retain its VACUUM state after reopening"
        check_toast_only_full()
        check_full_without_toast_data()
        print("[VACUUM RELATION OPTIONS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
