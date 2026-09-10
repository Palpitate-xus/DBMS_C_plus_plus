#!/usr/bin/env python3
"""Tablespace SQL must drive the same durable storage metadata as DDL."""

import importlib.util
from pathlib import Path
import subprocess


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
