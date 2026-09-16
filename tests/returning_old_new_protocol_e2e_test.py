#!/usr/bin/env python3
"""PostgreSQL 18 OLD/NEW RETURNING row images over the wire protocol."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "returning_images_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def ok(sql):
        result = query(sql)
        assert result[1] is None, (sql, result[1], result[2])
        return result

    try:
        ok("CREATE TABLE returning_images "
           "(id INT PRIMARY KEY, val TEXT);")

        rows, state, message, headers, command_tag, type_oids = query(
            "INSERT INTO returning_images VALUES (1, 'first') "
            "RETURNING old.id AS old_id, new.id AS new_id, "
            "old.val IS NULL AS old_is_null, new.val AS new_val;")
        assert state is None, (state, message)
        assert rows == [[None, "1", "t", "first"]], rows
        assert headers == ["old_id", "new_id", "old_is_null", "new_val"], headers
        assert type_oids == [23, 23, 16, 25], type_oids
        assert command_tag == "INSERT 0 1", command_tag

        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE returning_images SET val = 'second' WHERE id = 1 "
            "RETURNING WITH (OLD AS before_row, NEW AS after_row) "
            "before_row.val AS before, after_row.val AS after, "
            "before_row.id + after_row.id AS id_sum;")
        assert state is None, (state, message)
        assert rows == [["first", "second", "2"]], rows
        assert headers == ["before", "after", "id_sum"], headers
        assert type_oids == [25, 25, 23], type_oids
        assert command_tag == "UPDATE 1", command_tag

        # Renaming OLD hides the default OLD name. Unsupported versioned
        # projections are typed-executor owned and fail before mutation.
        _, state, _, _, _, _ = query(
            "UPDATE returning_images SET val = 'must-not-stick' WHERE id = 1 "
            "RETURNING WITH (OLD AS before_row) old.id;")
        assert state == "0A000", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM returning_images;")
        assert state is None, (state, message)
        assert rows == [["1", "second"]], rows

        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE returning_images AS r SET val = 'alias-ok' WHERE id = 1 "
            "RETURNING r.id, r.val;")
        assert state is None, (state, message)
        assert rows == [["1", "alias-ok"]], rows
        assert headers == ["id", "val"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "UPDATE 1", command_tag

        _, state, _, _, _, _ = query(
            "UPDATE returning_images AS r SET val = 'alias-bad' WHERE id = 1 "
            "RETURNING returning_images.id;")
        assert state == "0A000", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM returning_images;")
        assert state is None, (state, message)
        assert rows == [["1", "alias-ok"]], rows

        rows, state, message, headers, command_tag, type_oids = query(
            "INSERT INTO returning_images VALUES (1, 'upserted') "
            "ON CONFLICT (id) DO UPDATE SET val = excluded.val "
            "RETURNING old.val AS before, new.val AS after;")
        assert state is None, (state, message)
        assert rows == [["alias-ok", "upserted"]], rows
        assert headers == ["before", "after"], headers
        assert type_oids == [25, 25], type_oids
        assert command_tag == "INSERT 0 1", command_tag

        rows, state, message, headers, command_tag, type_oids = query(
            "DELETE FROM returning_images WHERE id = 1 "
            "RETURNING old.id AS old_id, new.id AS new_id, "
            "new.val IS NULL AS new_is_null;")
        assert state is None, (state, message)
        assert rows == [["1", None, "t"]], rows
        assert headers == ["old_id", "new_id", "new_is_null"], headers
        assert type_oids == [23, 23, 16], type_oids
        assert command_tag == "DELETE 1", command_tag

        for sql in [
            "CREATE TABLE returning_merge_target "
            "(id INT PRIMARY KEY, val TEXT);",
            "CREATE TABLE returning_merge_source (id INT, val TEXT);",
            "INSERT INTO returning_merge_target VALUES (1, 'old');",
            "INSERT INTO returning_merge_source VALUES "
            "(1, 'updated'), (2, 'inserted');",
        ]:
            ok(sql)

        rows, state, message, headers, command_tag, type_oids = query(
            "MERGE INTO returning_merge_target AS dst "
            "USING returning_merge_source AS src ON dst.id = src.id "
            "WHEN MATCHED THEN UPDATE SET val = src.val "
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val) "
            "RETURNING WITH (OLD AS before_row, NEW AS after_row) "
            "before_row.val AS before, after_row.val AS after, "
            "merge_action() AS action;")
        assert state is None, (state, message)
        assert rows == [
            ["old", "updated", "UPDATE"],
            [None, "inserted", "INSERT"],
        ], rows
        assert headers == ["before", "after", "action"], headers
        assert type_oids == [25, 25, 25], type_oids
        assert command_tag == "MERGE 2", command_tag

        print("[RETURNING OLD NEW PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
