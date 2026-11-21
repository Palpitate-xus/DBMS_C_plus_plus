#!/usr/bin/env python3
"""DROP TABLE retains other relations and rolls back failed target lists."""

import importlib.util
import subprocess
import time
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "drop_multiple_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows, tag

    def error(sql, expected):
        _, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    def reopen_after_crash():
        process = server["process"]
        process.kill()
        assert process.wait(timeout=10) == -9
        server["sock"].close()
        server["sock"] = None
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"],
             "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            assert server["process"].poll() is None, "server exited during crash recovery"
            try:
                runner.reconnect_ours(server, client)
                return
            except ConnectionRefusedError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def shared_name_prefix():
        payload = "abcdefghij" * 600
        query("CREATE TABLE ownership_cut(id INT PRIMARY KEY, long_key INT, payload TEXT);")
        query("CREATE TABLE ownership_cut_long(id INT PRIMARY KEY, v INT, payload TEXT);")
        query("CREATE INDEX ownership_cut_key_index ON ownership_cut(long_key);")
        query("CREATE INDEX ownership_cut_long_v_index ON ownership_cut_long(v);")
        query("INSERT INTO ownership_cut VALUES(1,10,'short');")
        query(f"INSERT INTO ownership_cut_long VALUES(2,20,'{payload}');")

        def read_neighbor():
            assert query("SELECT id FROM ownership_cut_long WHERE id=2;") == ([["2"]], "SELECT 1")
            assert query("SELECT id,payload FROM ownership_cut_long WHERE v=20;") == (
                [["2", payload]], "SELECT 1")

        read_neighbor()
        home = Path(server["dir"]) / "info"
        # The short table's real secondary index also starts with the long
        # name, but its definition still belongs to the short table.
        target_indexes = [home / name for name in (
            "ownership_cut.idx", "ownership_cut_long_key.idx")]
        assert all(path.is_file() for path in target_indexes)
        neighbor_indexes = [home / name for name in (
            "ownership_cut_long.idx", "ownership_cut_long.toast.idx", "ownership_cut_long_v.idx")]
        before = {path: path.read_bytes() for path in neighbor_indexes}
        assert all(before.values()), "expected non-empty primary, secondary and long-value indexes"

        assert query("DROP TABLE ownership_cut;") == ([], "DROP TABLE")
        error("SELECT id FROM ownership_cut;", "42P01")
        retained = [path.name for path in target_indexes if path.exists()]
        assert not retained, ("DROP retained the target's indexes", retained)
        missing = [path.name for path in neighbor_indexes if not path.is_file()]
        assert not missing, ("DROP deleted files belonging to ownership_cut_long", missing)
        for path, data in before.items():
            assert path.read_bytes() == data, ("DROP modified a neighboring index", path.name)
        read_neighbor()

        reopen_after_crash()
        error("SELECT id FROM ownership_cut;", "42P01")
        read_neighbor()

    def conflicting_index_claims():
        query("CREATE TABLE ownership_claim(id INT PRIMARY KEY, child_v INT);")
        query("CREATE TABLE ownership_claim_child(id INT PRIMARY KEY, v INT);")
        query("INSERT INTO ownership_claim VALUES(100,20);")
        query("INSERT INTO ownership_claim_child VALUES(101,20);")
        # Both CREATEs can succeed in a legacy database even though their
        # physical names resolve to the same path.
        query("CREATE INDEX ownership_claim_short_idx ON ownership_claim(child_v);")
        query("CREATE INDEX ownership_claim_long_idx ON ownership_claim_child(v);")

        home = Path(server["dir"]) / "info"
        assert (home / "ownership_claim.secidx").read_text().splitlines() == ["child_v"]
        assert (home / "ownership_claim_child.secidx").read_text().splitlines() == ["v"]
        assert (home / "ownership_claim_child_v.idx").stat().st_size > 0

        catalog_query = (
            "SELECT relname FROM pg_class WHERE relname IN ('ownership_claim', "
            "'ownership_claim_child', 'ownership_claim_short_idx', 'ownership_claim_long_idx') "
            "ORDER BY relname;")
        catalog_rows = [["ownership_claim"], ["ownership_claim_child"],
                        ["ownership_claim_long_idx"], ["ownership_claim_short_idx"]]
        assert query(catalog_query) == (catalog_rows, "SELECT 4")

        def snapshot():
            files = list(home.glob("ownership_claim*"))
            files.extend(home / name for name in (
                "tlist.lst", "pg_catalog/pg_class.cat", "pg_catalog/pg_attribute.cat",
                "pg_catalog/pg_depend.cat"))
            return {path: path.read_bytes() for path in files}

        before = snapshot()
        for table, behavior, expected_id in (
                ("ownership_claim", "RESTRICT", "100"),
                ("ownership_claim_child", "RESTRICT", "101"),
                ("ownership_claim", "CASCADE", "100"),
                ("ownership_claim_child", "CASCADE", "101")):
            error(f"DROP TABLE {table} {behavior};", "XX000")
            after = snapshot()
            changed = [path.name for path in before.keys() | after.keys()
                       if before.get(path) != after.get(path)]
            assert not changed, (table, behavior, "failed DROP changed relation or catalog files", sorted(changed))
            assert query(catalog_query) == (catalog_rows, "SELECT 4")
            assert query(f"SELECT id FROM {table} WHERE id={expected_id};") == ([[expected_id]], "SELECT 1")

    def preserve_short_name_index():
        query("CREATE TABLE ownership_peer(id INT PRIMARY KEY, child_key INT);")
        query("CREATE TABLE ownership_peer_child(id INT PRIMARY KEY);")
        query("CREATE INDEX ownership_peer_key_index ON ownership_peer(child_key);")
        query("INSERT INTO ownership_peer VALUES (3, 30);")
        index = Path(server["dir"]) / "info" / "ownership_peer_child_key.idx"
        before = index.read_bytes()
        assert before

        assert query("DROP TABLE ownership_peer_child;") == ([], "DROP TABLE")
        error("SELECT id FROM ownership_peer_child;", "42P01")
        assert index.read_bytes() == before
        assert query("SELECT id FROM ownership_peer WHERE child_key=30;") == ([["3"]], "SELECT 1")

    try:
        query("CREATE TABLE drop_many_a(id INT);")
        query("CREATE TABLE drop_many_b(id INT);")
        query("INSERT INTO drop_many_a VALUES(1);")
        query("INSERT INTO drop_many_b VALUES(2);")
        error("DROP TABLE drop_many_a,drop_many_missing;", "42P01")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_b;")[0] == [["2"]]
        query("CREATE VIEW drop_many_view AS SELECT 42 AS id;")
        error("DROP TABLE drop_many_a,drop_many_view;", "42809")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_view;")[0] == [["42"]]
        query("BEGIN;")
        query("SAVEPOINT drop_many_sp;")
        error("DROP TABLE drop_many_a,drop_many_missing;", "42P01")
        query("ROLLBACK TO SAVEPOINT drop_many_sp;")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("DROP TABLE drop_many_a,drop_many_b;")[1] == "DROP TABLE"
        query("ROLLBACK;")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_b;")[0] == [["2"]]
        assert query("DROP TABLE drop_many_a,drop_many_a,public.drop_many_b;")[1] == "DROP TABLE"
        query('CREATE TABLE "comma,a"(id INT);')
        query('CREATE TABLE "comma,b"(id INT);')
        query('DROP TABLE "comma,a","comma,b";')
        query("CREATE TABLE drop_many_shadow(id INT);")
        query("INSERT INTO drop_many_shadow VALUES(9);")
        query("CREATE TEMP TABLE drop_many_shadow(id INT);")
        query("CREATE TEMP TABLE drop_many_temp_b(id INT);")
        query("INSERT INTO drop_many_shadow VALUES(7);")
        error("DROP TABLE drop_many_shadow,drop_many_missing;", "42P01")
        assert query("SELECT id FROM drop_many_shadow;")[0] == [["7"]]
        assert query("SELECT id FROM public.drop_many_shadow;")[0] == [["9"]]
        query("DROP TABLE IF EXISTS drop_many_missing,drop_many_shadow,drop_many_temp_b;")
        assert query("SELECT id FROM drop_many_shadow;")[0] == [["9"]]
        query("CREATE SEQUENCE drop_many_owned_seq;")
        query("CREATE TABLE drop_many_owned(id INT DEFAULT nextval('drop_many_owned_seq') PRIMARY KEY,code TEXT);")
        query("ALTER SEQUENCE drop_many_owned_seq OWNED BY drop_many_owned.id;")
        query("INSERT INTO drop_many_owned(code) VALUES('kept');")
        assert query("SELECT currval('drop_many_owned_seq');")[0] == [["1"]]
        error("DROP TABLE drop_many_owned,drop_many_missing;", "42P01")
        assert query("SELECT id,code FROM drop_many_owned;")[0] == [["1", "kept"]]
        assert query("SELECT currval('drop_many_owned_seq');")[0] == [["1"]]
        assert query("INSERT INTO drop_many_owned(code) VALUES('next') RETURNING id;")[0] == [["2"]]
        shared_name_prefix()
        preserve_short_name_index()
        conflicting_index_claims()
        print("[DROP MULTIPLE TABLES PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
