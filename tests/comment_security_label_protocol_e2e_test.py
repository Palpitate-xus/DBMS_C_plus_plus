#!/usr/bin/env python3
"""COMMENT uses catalog object addresses; SECURITY LABEL fails closed."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "comment_label_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def expect_ok(sql, expected_tag=None):
        rows, state, message, headers, tag, _ = execute(sql)
        assert state is None, (sql, state, message)
        assert rows == [] and headers == [], (sql, rows, headers)
        if expected_tag is not None:
            assert tag == expected_tag, (sql, tag)

    def expect_error(sql, expected_state):
        rows, state, message, headers, _, _ = execute(sql)
        assert rows == [] and headers == [], (sql, rows, headers)
        assert state == expected_state, (sql, state, message)
        rows, state, message, _, tag, _ = execute("SELECT 1;")
        assert state is None, (sql, state, message)
        assert rows == [["1"]] and tag == "SELECT 1", (sql, rows, tag)

    try:
        expect_ok("CREATE TABLE label_notes (id INT, body TEXT);")
        expect_ok("CREATE INDEX label_notes_body_idx ON label_notes (body);")
        expect_ok(
            "CREATE VIEW label_notes_view AS "
            "SELECT id, body FROM label_notes;")
        expect_ok("CREATE SEQUENCE label_notes_seq;")

        for sql in [
                "COMMENT ON TABLE label_notes IS 'wire table comment';",
                "COMMENT ON COLUMN label_notes.body IS 'wire column comment';",
                "COMMENT ON INDEX label_notes_body_idx IS 'wire index comment';",
                "COMMENT ON VIEW label_notes_view IS 'wire view comment';",
                "COMMENT ON COLUMN label_notes_view.body IS 'view column';",
                "COMMENT ON SEQUENCE label_notes_seq IS 'wire sequence';",
                "COMMENT ON TYPE pg_catalog.int4 IS 'wire integer';"]:
            expect_ok(sql, "COMMENT")

        description = (Path(server["dir"]) / "info" / "pg_catalog" /
                       "pg_description.cat")
        assert description.is_file(), description
        stored = description.read_text(encoding="utf-8")
        for value in ["wire table comment", "wire column comment",
                      "wire index comment", "wire view comment",
                      "wire sequence", "wire integer"]:
            assert value in stored, (value, stored)

        expect_ok("ALTER TABLE label_notes RENAME TO label_journal;")
        expect_ok(
            "ALTER TABLE label_journal RENAME COLUMN body TO text;")
        expect_ok("COMMENT ON TABLE label_journal IS NULL;", "COMMENT")
        expect_ok(
            "COMMENT ON COLUMN label_journal.text IS 'renamed column';",
            "COMMENT")

        expect_error(
            "COMMENT ON TABLE missing_relation IS 'orphan';", "42P01")
        expect_error(
            "COMMENT ON COLUMN label_journal.missing IS 'orphan';", "42703")
        expect_error(
            "COMMENT ON TABLE label_notes_view IS 'wrong kind';", "42809")
        expect_error(
            "COMMENT ON DATABASE info IS 'unsupported';", "0A000")
        expect_error(
            "COMMENT ON FUNCTION missing(integer) IS 'unsupported';",
            "0A000")
        expect_error(
            'COMMENT ON TABLE label_journal IS "not a string";', "42601")

        database = Path(server["dir"]) / "info"
        labels = database / ".security_labels"
        assert not labels.exists(), labels
        expect_error(
            "SECURITY LABEL ON TABLE label_journal IS 'policy';", "0A000")
        expect_error(
            "SECURITY LABEL FOR dummy ON TABLE label_journal IS 'policy';",
            "0A000")
        assert not labels.exists(), labels

        # Old versions may have left inert name-keyed rows behind. They remain
        # available to lifecycle cleanup code, but must not be presented as
        # active MAC policy through pg_seclabels.
        legacy = "table label_journal legacy_policy\n"
        labels.write_text(legacy, encoding="utf-8")
        rows, state, message, headers, tag, _ = execute(
            "SELECT * FROM pg_seclabels;")
        assert state is None, (state, message)
        assert rows == [], rows
        assert headers == ["objtype", "objname", "label"], headers
        assert tag == "SELECT 0", tag
        assert labels.read_text(encoding="utf-8") == legacy

        expect_ok("DROP TABLE label_journal CASCADE;")
        expect_ok("CREATE TABLE label_journal (id INT, text TEXT);")
        expect_ok(
            "COMMENT ON TABLE label_journal IS 'fresh object';", "COMMENT")
        stored = description.read_text(encoding="utf-8")
        assert "wire table comment" not in stored, stored
        assert "wire column comment" not in stored, stored
        assert "wire index comment" not in stored, stored
        assert "fresh object" in stored, stored

        print("[COMMENT SECURITY LABEL PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
