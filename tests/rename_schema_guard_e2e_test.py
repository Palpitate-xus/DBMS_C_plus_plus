#!/usr/bin/env python3
"""Check successful schema rename and preservation on rejected moves."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "rename_schema_guard_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, expected_state):
        rows, state, message, _, _, _ = query(sql)
        assert state == expected_state, (sql, rows, state, message)
        return rows

    try:
        expect("CREATE SCHEMA old_space;", None)
        expect("CREATE TABLE old_space.items (id INT);", None)
        expect("INSERT INTO old_space.items VALUES (7);", None)
        expect("CREATE SEQUENCE old_space.ticket START WITH 17;", None)
        assert expect("SELECT nextval('old_space.ticket');", None) == [["17"]]
        assert expect("SELECT id FROM old_space.items;", None) == [["7"]]
        data_dir = Path(server["dir"])
        assert (data_dir / "info" / ".schema_old_space").exists()
        assert not (data_dir / "info" / ".schema_new_space").exists()
        assert (data_dir / "info" / "old_space__items.stc").exists()
        assert not (data_dir / "info" / "new_space__items.stc").exists()
        expect("CREATE MATERIALIZED VIEW public.schema_move_mv AS SELECT id FROM old_space.items;", None)
        expect("ALTER SCHEMA old_space RENAME TO new_space;", "0A000")
        assert expect("SELECT id FROM public.schema_move_mv;", None) == [["7"]]
        expect("DROP MATERIALIZED VIEW public.schema_move_mv;", None)
        expect('CREATE TABLE public."new_space__items" (id INT);', None)
        expect('INSERT INTO public."new_space__items" VALUES (9);', None)
        expect("ALTER SCHEMA old_space RENAME TO new_space;", "42P06")
        assert expect('SELECT id FROM public."new_space__items";', None) == [["9"]]
        assert expect("SELECT id FROM old_space.items;", None) == [["7"]]
        expect('DROP TABLE public."new_space__items";', None)
        expect("CREATE TABLE schema_move_seed (id INT);", None)
        expect("INSERT INTO schema_move_seed VALUES (19);", None)
        expect('CREATE MATERIALIZED VIEW public."new_space.reserved" AS SELECT id FROM schema_move_seed;', None)
        expect("ALTER SCHEMA old_space RENAME TO new_space;", "42P06")
        assert expect('SELECT id FROM public."new_space.reserved";', None) == [["19"]]
        assert expect("SELECT id FROM old_space.items;", None) == [["7"]]
        expect('DROP MATERIALIZED VIEW public."new_space.reserved";', None)
        expect("DROP TABLE schema_move_seed;", None)
        expect("ALTER SCHEMA old_space RENAME TO new_space;", None)
        assert expect("SELECT id FROM new_space.items;", None) == [["7"]]
        assert expect("SELECT nextval('new_space.ticket');", None) == [["18"]]
        assert not (data_dir / "info" / ".schema_old_space").exists()
        assert (data_dir / "info" / ".schema_new_space").exists()
        assert not (data_dir / "info" / "old_space__items.stc").exists()
        assert (data_dir / "info" / "new_space__items.stc").exists()
        expect("DROP SEQUENCE new_space.ticket;", None)
        expect("DROP TABLE new_space.items;", None)
        expect("DROP SCHEMA new_space;", None)
        print("[RENAME SCHEMA GUARD E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
