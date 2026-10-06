#!/usr/bin/env python3
"""Generation-aware sequence allocation across real DDL rollback images."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("seq_ddl_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=15)
        client.startup_reference(sock, user, database, password=password)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def query(sql, rows=None, state=None):
        actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("SEQUENCE DDL", sql, actual, flush=True)
        assert actual[1] == state, (sql, actual, state)
        if rows is not None:
            assert actual[0] == rows, (sql, actual, rows)
        if rows and state is None and ("nextval(" in sql or "currval(" in sql or "lastval(" in sql):
            assert actual[5] == [20], (sql, actual)
        return actual

    def next_value(name, value):
        query(f"SELECT nextval('{name}');", [[str(value)]])

    try:
        if reference:
            query("SHOW server_version_num;", [["180006"]])

        # Original minimal reproduction: TEMP DDL creates a dirty database
        # image, but nextval/currval are not savepoint snapshots.
        query("BEGIN;")
        query("CREATE TEMP TABLE seq_rollback_probe(id INT);")
        query("CREATE TEMP SEQUENCE seq_rollback_private;")
        next_value("seq_rollback_private", 1)
        query("SAVEPOINT q;")
        next_value("seq_rollback_private", 2)
        next_value("seq_rollback_private", 3)
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_rollback_private');", [["3"]])
        next_value("seq_rollback_private", 4)
        query("ROLLBACK;")
        query("SELECT nextval('seq_rollback_private');", [], "42P01")

        query("CREATE TEMP SEQUENCE seq_survivor;")
        next_value("seq_survivor", 1)
        query("BEGIN;")
        query("CREATE TEMP TABLE seq_setval_marker(id INT);")
        query("SAVEPOINT q;")
        query("SELECT setval('seq_survivor',50,true);", [["50"]])
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_survivor');", [["50"]])
        next_value("seq_survivor", 51)
        query("SELECT setval('seq_survivor',12,false);", [["12"]])
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_survivor');", [["51"]])
        next_value("seq_survivor", 12)
        query("ROLLBACK;")
        next_value("seq_survivor", 13)

        query("BEGIN;")
        query("SAVEPOINT q;")
        next_value("seq_survivor", 14)
        query("ALTER SEQUENCE seq_survivor RESTART WITH 100;")
        next_value("seq_survivor", 100)
        query("SELECT setval('seq_survivor',130,true);", [["130"]])
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_survivor');", [["130"]])
        query("SELECT lastval();", [["130"]])
        next_value("seq_survivor", 15)
        query("ROLLBACK;")
        next_value("seq_survivor", 16)

        # DROP/CREATE keeps logical OID identity separate from storage
        # generation and does not resurrect a new object's currval cache.
        query("BEGIN;")
        query("SAVEPOINT q;")
        next_value("seq_survivor", 17)
        query("DROP SEQUENCE seq_survivor;")
        query("CREATE TEMP SEQUENCE seq_survivor START 500;")
        query("SAVEPOINT new_identity;")
        query("SELECT currval('seq_survivor');", [], "55000")
        query("ROLLBACK TO new_identity;")
        next_value("seq_survivor", 500)
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_survivor');", [["17"]])
        query("SAVEPOINT old_identity;")
        query("SELECT lastval();", [], "55000")
        query("ROLLBACK TO old_identity;")
        next_value("seq_survivor", 18)
        query("COMMIT;")

        # Rename preserves generation: allocations through a different name
        # survive rolling the declaration/name back.
        query("BEGIN;")
        query("SAVEPOINT q;")
        query("ALTER SEQUENCE seq_survivor RENAME TO seq_renamed;")
        next_value("seq_renamed", 19)
        query("ROLLBACK TO q;")
        query("SELECT currval('seq_survivor');", [["19"]])
        next_value("seq_survivor", 20)
        query("ROLLBACK;")

        query("CREATE TEMP SEQUENCE seq_metadata MINVALUE 1 MAXVALUE 10;")
        next_value("seq_metadata", 1)
        query("BEGIN;")
        query("SAVEPOINT q;")
        next_value("seq_metadata", 2)
        query("ALTER SEQUENCE seq_metadata INCREMENT -1 CYCLE;")
        next_value("seq_metadata", 1)
        next_value("seq_metadata", 10)
        query("ROLLBACK TO q;")
        next_value("seq_metadata", 3)
        query("ALTER SEQUENCE seq_metadata START 5 CACHE 3;")
        next_value("seq_metadata", 4)
        query("ROLLBACK TO q;")
        next_value("seq_metadata", 4)
        query("ROLLBACK;")

        query("CREATE TEMP SEQUENCE seq_alter_cache CACHE 5;")
        next_value("seq_alter_cache", 1)
        query("ALTER SEQUENCE seq_alter_cache INCREMENT 2;")
        next_value("seq_alter_cache", 7)
        query("CREATE TEMP SEQUENCE seq_alter_uncalled START 5;")
        query("ALTER SEQUENCE seq_alter_uncalled INCREMENT 3;")
        next_value("seq_alter_uncalled", 5)
        query("SELECT setval('seq_alter_uncalled',42,false);", [["42"]])
        query("ALTER SEQUENCE seq_alter_uncalled INCREMENT -1;")
        next_value("seq_alter_uncalled", 42)
        next_value("seq_alter_uncalled", 41)
        query("CREATE TEMP SEQUENCE seq_alter_owned CACHE 5;")
        next_value("seq_alter_owned", 1)
        query("BEGIN;")
        query("SAVEPOINT q;")
        query("ALTER SEQUENCE seq_alter_owned OWNED BY NONE;")
        next_value("seq_alter_owned", 6)
        query("ROLLBACK TO q;")
        next_value("seq_alter_owned", 7)
        query("ROLLBACK;")
        # Equivalent SQL for the retained unversioned file fixture: the
        # next cursor is not the last allocated value after ALTER.
        query("CREATE TEMP SEQUENCE seq_alter_legacy START 5 MINVALUE 1 MAXVALUE 10;")
        next_value("seq_alter_legacy", 5)
        query("ALTER SEQUENCE seq_alter_legacy INCREMENT -1;")
        next_value("seq_alter_legacy", 4)
        query("CREATE TEMP SEQUENCE seq_alter_minimum START 10 INCREMENT 2 MINVALUE 10 MAXVALUE 30;")
        next_value("seq_alter_minimum", 10)
        query("ALTER SEQUENCE seq_alter_minimum INCREMENT -2;")
        query("SELECT nextval('seq_alter_minimum');", [], "2200H")
        query("SELECT nextval('seq_alter_minimum');", [], "2200H")

        for name, definition, values, after in (
            ("seq_cycle", "MINVALUE 1 MAXVALUE 3 CYCLE", [2, 3, 1, 2], 3),
            ("seq_down", "START -1 INCREMENT -1 MINVALUE -10 MAXVALUE -1", [-2, -3, -4], -5),
            ("seq_cache", "CACHE 3", [2, 3], 4),
        ):
            query(f"CREATE TEMP SEQUENCE {name} {definition};")
            next_value(name, -1 if name == "seq_down" else 1)
            query("BEGIN;")
            query("CREATE TEMP TABLE seq_cursor_marker(id INT);")
            query("SAVEPOINT q;")
            for value in values:
                next_value(name, value)
            query("ROLLBACK TO q;")
            next_value(name, after)
            query("ROLLBACK;")

        query("CREATE TEMP SEQUENCE seq_exhaust MAXVALUE 2;")
        next_value("seq_exhaust", 1)
        query("BEGIN;")
        query("CREATE TEMP TABLE seq_exhaust_marker(id INT);")
        query("SAVEPOINT q;")
        next_value("seq_exhaust", 2)
        query("ROLLBACK TO q;")
        query("SELECT nextval('seq_exhaust');", [], "2200H")
        query("ROLLBACK;")
        query("SELECT currval('seq_exhaust');", [["2"]])
        query("SELECT nextval('seq_exhaust');", [], "2200H")

        # A normal SELECT of a writer must still allocate once per real row
        # when the routine and table were created after BEGIN.
        query("CREATE TEMP SEQUENCE seq_calls;")
        query("BEGIN;")
        query("CREATE TEMP TABLE seq_driver(id INT);")
        query("INSERT INTO seq_driver VALUES(1),(2),(3);")
        query("CREATE FUNCTION sequence_ddl_backup_writer(p INT) RETURNS INT LANGUAGE plpgsql "
              "AS $$BEGIN PERFORM nextval('seq_calls'); RETURN p; END$$;")
        next_value("seq_calls", 1)
        query("SAVEPOINT q;")
        query("SELECT sequence_ddl_backup_writer(id) FROM seq_driver ORDER BY id;", [["1"], ["2"], ["3"]])
        query("SELECT currval('seq_calls');", [["4"]])
        query("ROLLBACK TO q;")
        query("SELECT sequence_ddl_backup_writer(id) FROM seq_driver ORDER BY id;", [["1"], ["2"], ["3"]])
        query("SELECT currval('seq_calls');", [["7"]])
        query("ROLLBACK;")
        next_value("seq_calls", 8)
        print("[SEQUENCE DDL ROLLBACK PROTOCOL] passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK;")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
