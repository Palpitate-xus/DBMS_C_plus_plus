#!/usr/bin/env python3
"""Observe live VACUUM, ANALYZE and VACUUM FULL through PostgreSQL wire views."""

from contextlib import ExitStack, contextmanager
import fcntl
import importlib.util
from pathlib import Path
import socket
import struct
import time
from unittest import mock


IDENTITY = [("pid", 23), ("datid", 26), ("datname", 19), ("relid", 26)]
VIEWS = {
    "vacuum": IDENTITY + [("phase", 25)] + [(name, 20) for name in (
        "heap_blks_total", "heap_blks_scanned", "heap_blks_vacuumed",
        "index_vacuum_count", "max_dead_tuple_bytes", "dead_tuple_bytes",
        "num_dead_item_ids", "indexes_total", "indexes_processed",
    )] + [("delay_time", 701)],
    "analyze": IDENTITY + [("phase", 25)] + [(name, 20) for name in (
        "sample_blks_total", "sample_blks_scanned", "ext_stats_total",
        "ext_stats_computed", "child_tables_total", "child_tables_done",
    )] + [("current_child_table_relid", 26), ("delay_time", 701)],
    "cluster": IDENTITY + [("command", 25), ("phase", 25), ("cluster_index_relid", 26)] + [
        (name, 20) for name in ("heap_tuples_scanned", "heap_tuples_written",
                               "heap_blks_total", "heap_blks_scanned", "index_rebuild_count")],
}
TYPE_LENGTHS = {19: 64, 20: 8, 23: 4, 25: -1, 26: 4, 701: 8}
TABLE = "maintenance_progress_target"


def main():
    spec = importlib.util.spec_from_file_location(
        "maintenance_progress_runner", Path(__file__).with_name("compat") / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    mkdtemp = runner.tempfile.mkdtemp

    def configured_directory(*args, **kwargs):
        directory = mkdtemp(*args, **kwargs)
        Path(directory, "dbms.conf").write_text(
            "auto_analyze=off\nauto_vacuum=off\n", encoding="ascii")
        return directory

    with mock.patch.object(runner.tempfile, "mkdtemp", side_effect=configured_directory):
        server = runner.start_ours(client)
    admin = server["sock"]

    def check_reply(frames, sql, state=None, tag=None):
        rows, actual_state, message, _, actual_tag, _ = runner.decode_wire_result(frames, True)
        assert actual_state == state, (sql, actual_state, message, state)
        assert frames[-1] == (b"Z", b"I"), (sql, frames[-1])
        if tag is not None or state is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        if state is not None:
            assert [kind for kind, _ in frames] == [b"E", b"Z"], (sql, frames)
        return rows

    def query(sql, connection=admin, state=None, tag=None):
        return check_reply(client.simple_query(connection, sql), sql, state, tag)

    def send_query(connection, sql):
        connection.sendall(client.typed(b"Q", sql.encode() + b"\0"))

    def view(connection, kind, pid=None, columns=None, extended=False, qualified=True):
        name = ("pg_catalog." if qualified else "") + "pg_stat_progress_" + kind
        sql = f"SELECT {', '.join(columns) if columns else '*'} FROM {name}"
        if pid is not None:
            sql += f" WHERE pid = {pid}"
        if extended:
            connection.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                               client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                               client.typed(b"D", b"P\0") +
                               client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                               client.typed(b"S"))
            frames = client.read_until_ready(connection)
        else:
            frames = client.simple_query(connection, sql)
        rows = check_reply(frames, sql)
        kinds = [kind for kind, _ in frames]
        prefix = [b"1", b"2", b"T"] if extended else [b"T"]
        assert kinds == prefix + [b"D"] * len(rows) + [b"C", b"Z"], (sql, kinds)
        assert frames[-2] == (b"C", f"SELECT {len(rows)}\0".encode()), (sql, frames[-2])
        expected = VIEWS[kind] if columns is None else [(col, dict(VIEWS[kind])[col]) for col in columns]
        description = next(body for frame_kind, body in frames if frame_kind == b"T")
        assert struct.unpack_from("!H", description) == (len(expected),), (sql, description)
        offset = 2
        for column, oid in expected:
            end = description.index(b"\0", offset)
            assert description[offset:end].decode() == column, (sql, description)
            fields = struct.unpack_from("!IhIhih", description, end + 1)
            assert fields == (0, 0, oid, TYPE_LENGTHS[oid], -1, 0), (sql, column, fields)
            offset = end + 19
        assert offset == len(description), (sql, description)
        return rows

    def connect(stack, user="alice", database="info", password="secret"):
        connection = stack.enter_context(socket.create_connection(
            ("127.0.0.1", server["port"]), timeout=runner.wire_timeout()))
        pid, secret = client.startup(connection, user, database, password=password)
        return connection, pid, secret

    def empty(pid=None):
        for kind in VIEWS:
            assert view(admin, kind, pid) == [], (kind, pid)

    @contextmanager
    def hold_heap_page(token):
        # This is the server's real cross-process page lock. Blocking page 2
        # leaves relation admission and work on page 1 free to complete.
        with token.open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    try:
        admin.settimeout(min(8, runner.wire_timeout()))
        for kind in VIEWS:
            assert view(admin, kind, extended=True, qualified=kind != "analyze") == [], kind
        shadow = ("WITH pg_stat_progress_vacuum AS (SELECT 1 AS pid) "
                  "SELECT pid FROM pg_stat_progress_vacuum")
        assert query(shadow, tag="SELECT 1") == [["1"]]
        Path(server["dir"], "pg_hba.conf").write_text(
            "host all all 127.0.0.1/32 scram-sha-256\n", encoding="ascii")
        for sql in (
                "CREATE USER progress_plain WITH PASSWORD 'ProgressPass9!'",
                "CREATE USER progress_monitor WITH PASSWORD 'ProgressPass9!'",
                "CREATE ROLE pg_read_all_stats",
                "GRANT pg_read_all_stats TO progress_monitor",
                "CREATE DATABASE progress_observer",
                f"CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, payload CHAR(500))"):
            query(sql)
        values = ",".join(f"({i},'{i:03d}{'x' * 497}')" for i in range(1, 49))
        query(f"INSERT INTO {TABLE} VALUES {values}", tag="INSERT 0 48")
        query("CHECKPOINT", tag="CHECKPOINT")
        [[relid]] = query(f"SELECT oid FROM pg_catalog.pg_class WHERE relname = '{TABLE}'")
        assert int(relid) > 0, relid
        database = Path(server["dir"], "info")
        with (database / f"{TABLE}.dt").open("rb") as heap:
            _, pages_with_header = struct.unpack("<II", heap.read(8))
        heap_blocks = pages_with_header - 1
        assert heap_blocks >= 2, heap_blocks
        token = database / ".lockmgr" / ("page-" + f"page:info:{TABLE}:2".encode().hex() + ".lock")

        def active(kind, pid, completed=1):
            columns = [column for column, _ in VIEWS[kind]]
            phases = {"vacuum": "scanning heap", "analyze": "acquiring sample rows",
                      "cluster": "seq scanning heap"}
            prefix = "sample" if kind == "analyze" else "heap"
            scanned = prefix + "_blks_scanned"
            deadline = time.monotonic() + 8
            while True:
                rows = view(admin, kind, pid)
                assert len(rows) <= 1, (kind, pid, rows)
                if rows:
                    row = rows[0]
                    data = dict(zip(columns, row))
                    if data["phase"] != "initializing" and int(data[scanned] or "0") >= completed:
                        break
                assert time.monotonic() < deadline, ("No completed heap page observed", kind, pid, rows)
                time.sleep(0.01)
            assert row[:4] == [str(pid), None, "info", relid], (kind, row)
            assert data["phase"] == phases[kind], data
            assert data[prefix + "_blks_total"] == str(heap_blocks), data
            assert data[scanned] == str(completed), data
            if kind == "cluster":
                assert data["command"] == "VACUUM FULL", data
                assert 0 < int(data["heap_tuples_scanned"]) < 48, data
                assert data["heap_tuples_written"] == data["index_rebuild_count"] == "0", data
            elif kind == "vacuum":
                assert data["heap_blks_vacuumed"] == data["index_vacuum_count"] == "0", data
            else:
                assert data["ext_stats_total"] == data["ext_stats_computed"] == "0", data
            for other in VIEWS:
                if other != kind:
                    assert view(admin, other, pid) == [], (kind, other, pid)
            return row

        with ExitStack() as stack:
            worker, pid, secret = connect(stack)
            plain, _, _ = connect(stack, "progress_plain", password="ProgressPass9!")
            monitor, _, _ = connect(stack, "progress_monitor", "progress_observer", "ProgressPass9!")

            def check_visibility(kind, row):
                assert view(plain, kind, pid) == [row[:3] + [None] * (len(row) - 3)], (kind, row)
                assert view(monitor, kind, pid) == [row], (kind, row)
                filtered = f"SELECT pid FROM pg_catalog.pg_stat_progress_{kind} WHERE relid = {relid}"
                assert query(filtered, plain, tag="SELECT 0") == [], (kind, row)
                assert query(filtered, monitor, tag="SELECT 1") == [[str(pid)]], (kind, row)

            vacuum = f"VACUUM {TABLE}"
            with hold_heap_page(token):
                send_query(worker, vacuum)
                check_visibility("vacuum", active("vacuum", pid))
            check_reply(client.read_until_ready(worker), vacuum, tag="VACUUM")
            empty(pid)

            analyze = f"ANALYZE {TABLE} (id)"
            with hold_heap_page(token):
                send_query(worker, analyze)
                check_visibility("analyze", active("analyze", pid))
                client.send_cancel_request(server["port"], pid, secret)
                check_reply(client.read_until_ready(worker), analyze, state="57014")
                empty(pid)
            query(analyze, worker, tag="ANALYZE")
            empty(pid)

            analyze_only = f"VACUUM (ANALYZE, PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) {TABLE}"
            with hold_heap_page(token):
                send_query(worker, analyze_only)
                active("analyze", pid)
            check_reply(client.read_until_ready(worker), analyze_only, tag="VACUUM")
            empty(pid)

            full = f"VACUUM FULL {TABLE}"
            with hold_heap_page(token):
                send_query(worker, full)
                check_visibility("cluster", active("cluster", pid))
                assert view(admin, "cluster", pid, ["phase", "command", "relid"], extended=True) == [
                    ["seq scanning heap", "VACUUM FULL", relid]]
            check_reply(client.read_until_ready(worker), full, tag="VACUUM")
            empty(pid)

            query("SET lock_timeout = 3000", worker, tag="SET")
            with hold_heap_page(token):
                send_query(worker, vacuum)
                active("vacuum", pid)
                check_reply(client.read_until_ready(worker), vacuum, state="55P03")
                empty(pid)
            assert query("SELECT 1", worker, tag="SELECT 1") == [["1"]]
            query("SET lock_timeout = 0", worker, tag="SET")

            with hold_heap_page(token):
                send_query(worker, f"VACUUM (PARALLEL 2) {TABLE}")
                active("vacuum", pid, completed=heap_blocks - 1)
                worker.shutdown(socket.SHUT_RDWR)
                worker.close()
            deadline = time.monotonic() + 8
            while query(f"SELECT pid FROM pg_stat_activity WHERE pid = {pid}"):
                assert time.monotonic() < deadline, ("disconnected backend still registered", pid)
                time.sleep(0.01)
            empty()
            assert query(f"SELECT id FROM {TABLE} ORDER BY id") == [[str(i)] for i in range(1, 49)]
        print("[MAINTENANCE PROGRESS PROTOCOL E2E] active work, visibility, and lifecycle passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
