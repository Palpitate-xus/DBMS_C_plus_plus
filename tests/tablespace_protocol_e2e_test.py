#!/usr/bin/env python3
"""SQL tablespaces and heap options must drive durable storage metadata."""

import importlib.util
from pathlib import Path
import socket
import subprocess
import time


FILLFACTOR_TABLES = ("fillfactor_created", "fillfactor_altered", "fillfactor_default")


def assert_fillfactor_params(home):
    for table in FILLFACTOR_TABLES[:2]:
        params = home / f"{table}.params"
        assert params.is_file(), params
        assert params.read_bytes() == b"fillfactor=50\n", params
    default = home / "fillfactor_default.params"
    assert not default.exists() or b"fillfactor=" not in default.read_bytes(), default


def assert_rolled_back_fillfactor(execute, home):
    missing = execute("SELECT id FROM fillfactor_rolled;")
    assert missing[1] == "42P01", missing
    for suffix in (".dt", ".stc", ".params"):
        file = home / ("fillfactor_rolled" + suffix)
        assert not file.exists(), file


def prepare_fillfactor_tables(execute, home):
    columns = "(id INT, v INT NOT NULL, pad VARCHAR(504))"
    for table in FILLFACTOR_TABLES:
        options = " WITH (FILLFACTOR = 50)" if table == "fillfactor_created" else ""
        result = execute(f"CREATE TABLE {table} {columns}{options};")
        assert (result[1], result[4]) == (None, "CREATE TABLE"), result
    altered = execute("ALTER TABLE fillfactor_altered SET (fillfactor = 50);")
    assert (altered[1], altered[4]) == (None, "ALTER TABLE"), altered
    assert_fillfactor_params(home)

    for limit in (10, 100):
        table = f"fillfactor_bound_{limit}"
        created = execute(f"CREATE TABLE {table} (id INT) WITH (fillfactor = {limit});")
        assert (created[1], created[4]) == (None, "CREATE TABLE"), created
        params = home / f"{table}.params"
        assert params.read_text(encoding="utf-8") == f"fillfactor={limit}\n", params
    for option, state in (("fillfactor = 9", "22023"), ("fillfactor = 101", "22023"),
                          ("fillfactor = -1", "22023"), ("fillfactor = 'fifty'", "22023"),
                          ("fillfactor =", "42601")):
        rejected = execute(f"CREATE TABLE fillfactor_invalid (id INT) WITH ({option});")
        assert (rejected[1], rejected[4]) == (state, None), (option, rejected)
    assert not list(home.glob("fillfactor_invalid.*"))

    for value in (80, 9):
        existing = execute(
            f"CREATE TABLE IF NOT EXISTS fillfactor_created {columns} WITH (fillfactor = {value});")
        assert (existing[1], existing[4]) == (None, "CREATE TABLE"), existing
        assert_fillfactor_params(home)

    assert execute("BEGIN;")[4] == "BEGIN"
    rolled = execute("CREATE TABLE fillfactor_rolled (id INT) WITH (FILLFACTOR = 50);")
    assert (rolled[1], rolled[4]) == (None, "CREATE TABLE"), rolled
    assert (home / "fillfactor_rolled.params").read_bytes() == b"fillfactor=50\n"
    assert execute("ROLLBACK;")[4] == "ROLLBACK"
    assert_rolled_back_fillfactor(execute, home)


def verify_fillfactor_after_reopen(execute, home):
    assert_fillfactor_params(home)
    assert_rolled_back_fillfactor(execute, home)
    padding = "x" * 504
    values = ",".join(f"({key},{3 * key},'{padding}')" for key in range(1, 37))
    for table in FILLFACTOR_TABLES:
        inserted = execute(f"INSERT INTO {table} VALUES {values};")
        assert (inserted[1], inserted[4]) == (None, "INSERT 0 36"), inserted
    assert execute("CHECKPOINT;")[4] == "CHECKPOINT"

    pages = {}
    for table in FILLFACTOR_TABLES:
        actual = execute(f"SELECT id,v FROM {table} ORDER BY id;")
        expected = [[str(key), str(3 * key)] for key in range(1, 37)]
        assert actual[1] is None and actual[0] == expected, actual
        size = (home / f"{table}.dt").stat().st_size
        assert size % 8192 == 0, (table, size)
        pages[table] = size // 8192 - 1  # The first page stores allocator metadata.
    assert pages["fillfactor_created"] == pages["fillfactor_altered"], pages
    assert pages["fillfactor_altered"] > pages["fillfactor_default"] >= 2, pages
    print("[CREATE FILLFACTOR] passed")


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "tablespace_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql))

    try:
        cluster = Path(server["dir"])
        location = cluster.parent / (cluster.name + "-table space")
        owner_location = cluster.parent / (cluster.name + "-owner")
        option_location = cluster.parent / (cluster.name + "-option")

        result = execute(
            "CREATE TABLESPACE fast_space LOCATION "
            f"'{str(location).replace(chr(39), chr(39) * 2)}';")
        assert result[1] is None, result
        marker = cluster / "info" / "pg_tblspc" / "fast_space.path"
        assert marker.is_file(), marker
        assert marker.read_text(encoding="utf-8") == str(location.resolve()) + "\n"
        assert (location / "info").is_dir()
        assert not (cluster / ".tablespaces").exists()

        assert execute(
            "CREATE TABLE external_items (id INT) TABLESPACE fast_space;")[1] is None
        assert execute("INSERT INTO external_items VALUES (41);")[1] is None
        selected = execute("SELECT id FROM external_items;")
        assert selected[1] is None and selected[0] == [["41"]], selected
        assert (location / "info" / "external_items.dt").is_file()
        prepare_fillfactor_tables(execute, cluster / "info")

        # Offline checksum verification must not bypass the running server's
        # exclusive data-directory lock. Stop it without deleting the runner
        # directory, then restart on the same port after restoring the marker.
        server["sock"].close()
        server["sock"] = None
        server["process"].terminate()
        server["process"].wait(timeout=10)
        checked = subprocess.run(
            [runner.DBMS_MAIN, "--data-dir", str(cluster),
             "--verify-data-checksums"],
            cwd=cluster, capture_output=True, text=True, timeout=10,
            check=False)
        assert checked.returncode == 0, checked
        marker_contents = marker.read_text(encoding="utf-8")
        marker.write_text(marker_contents + "unexpected\n", encoding="utf-8")
        corrupt_check = subprocess.run(
            [runner.DBMS_MAIN, "--data-dir", str(cluster),
             "--verify-data-checksums"],
            cwd=cluster, capture_output=True, text=True, timeout=10,
            check=False)
        assert corrupt_check.returncode == 1, corrupt_check
        assert "invalid tablespace marker" in corrupt_check.stderr, corrupt_check
        marker.write_text(marker_contents, encoding="utf-8")

        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", str(cluster),
             "--server", str(server["port"]), "--insecure"],
            cwd=cluster, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                sock = socket.create_connection(
                    ("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
                client.startup(sock, "alice", "info")
                server["sock"] = sock
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

        verify_fillfactor_after_reopen(execute, cluster / "info")

        busy = execute("DROP TABLESPACE fast_space;")
        assert busy[1] == "22023", busy
        unsupported = execute("ALTER TABLESPACE fast_space RENAME TO other_space;")
        assert unsupported[1] == "0A000", unsupported
        assert marker.is_file()

        relative = execute("CREATE TABLESPACE relative_space LOCATION 'relative';")
        assert relative[1] == "22023", relative
        owner = execute(
            "CREATE TABLESPACE owner_space OWNER alice LOCATION "
            f"'{str(owner_location).replace(chr(39), chr(39) * 2)}';")
        assert owner[1] == "0A000", owner
        options = execute(
            "CREATE TABLESPACE option_space LOCATION "
            f"'{str(option_location).replace(chr(39), chr(39) * 2)}' "
            " WITH (random_page_cost = 1.1);")
        assert options[1] == "0A000", options
        assert not owner_location.exists()
        assert not option_location.exists()

        assert execute("DROP TABLE external_items;")[1] is None
        dropped = execute("DROP TABLESPACE fast_space;")
        assert dropped[1] is None, dropped
        assert not marker.exists()
        assert not (location / "info").exists()
        assert location.is_dir()

        print("[TABLESPACE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)
        for path in (location, owner_location, option_location):
            if path.exists():
                import shutil
                shutil.rmtree(path)


if __name__ == "__main__":
    main()
