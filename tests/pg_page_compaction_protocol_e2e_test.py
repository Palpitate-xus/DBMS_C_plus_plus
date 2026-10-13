"""Small heap tuples survive DELETE/VACUUM/INSERT and persisted rereads."""
import importlib.util
from pathlib import Path
import socket
import subprocess
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "page_compaction_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        result = runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)
        assert result[1] is None, (sql, result)
        if expected is not None:
            assert result[0] == expected, (sql, result, expected)
        return result

    try:
        for size in (258, 270, 272):
            table = "compact_small_" + str(size)
            query("CREATE TABLE " + table + "(v SMALLINT NOT NULL)")
            query("INSERT INTO " + table + " VALUES " +
                  ",".join("(" + str(i) + ")" for i in range(1, size + 1)))
            query("DELETE FROM " + table + " WHERE v=1")
            query("VACUUM " + table)
            expected = [[str(i)] for i in range(2, size + 1)]
            query("SELECT v FROM " + table + " ORDER BY v", expected)
            query("INSERT INTO " + table + " VALUES(1000),(1001)")
            expected.extend([["1000"], ["1001"]])
            query("SELECT v FROM " + table + " ORDER BY v", expected)
            query("VACUUM " + table)
            query("SELECT v FROM " + table + " ORDER BY v", expected)
            print("[SMALL PAGE COMPACTION] rows=" + str(size) + " passed", flush=True)
        query("CHECKPOINT")
        # Reopen this driver's own data directory in a new process. A fresh
        # connection to the original process could still use its buffer pool.
        server["sock"].close()
        server["sock"] = None
        server["process"].terminate()
        server["process"].wait(timeout=10)
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"],
             "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        fresh = socket.socket()
        fresh.settimeout(runner.wire_timeout())
        server["sock"] = fresh
        deadline = time.monotonic() + 20
        while True:
            try:
                fresh.connect(("127.0.0.1", server["port"]))
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        client.startup(fresh, "alice", "info")
        for size in (258, 270, 272):
            expected = [[str(i)] for i in range(2, size + 1)] + [["1000"], ["1001"]]
            query("SELECT v FROM compact_small_" + str(size) + " ORDER BY v", expected)
        print("[SMALL PAGE COMPACTION PROTOCOL] all controls passed", flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
