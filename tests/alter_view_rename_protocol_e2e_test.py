#!/usr/bin/env python3
"""Rename a stored view without changing its identity or dependencies."""

import os
from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    extended = Wire(client, server["sock"], extended=True)
    fixture = Path(server["dir"])
    blocker = fixture / "info" / "cannot_copy_fifo"

    def view_oid(name):
        sql = f"SELECT '{name}'::regclass::oid AS object_id"
        rows, state, message, _, _, oids = runner.decode_wire_result(
            wire.send_statements(sql), include_types=True)
        assert state is None and oids == [26] and len(rows) == 1, (sql, message, rows)
        return rows[0][0]

    def dependent(expected, ready=b"I"):
        wire.query("SELECT id, value FROM dependent ORDER BY id",
                   tag=f"SELECT {len(expected)}", rows=expected,
                   fields=[("id", 23), ("value", 23)], ready=ready)

    def missing_notice(sql, ready=b"I"):
        frames = extended.send_statements(sql)
        assert frames[:3] == [(b"1", b""), (b"2", b""), (b"n", b"")], frames
        frames = frames[3:]
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        assert b'Mrelation "missing" does not exist, skipping' in frames[0][1].split(b"\0")
        assert frames[1:] == [(b"C", b"ALTER VIEW\0"), (b"Z", ready)], frames

    try:
        wire.command("CREATE TABLE source(id int PRIMARY KEY, value int)", "CREATE TABLE")
        wire.command("INSERT INTO source VALUES (1, 7)", "INSERT 0 1")
        wire.command("CREATE VIEW original AS SELECT id, value FROM source", "CREATE VIEW")
        wire.command("CREATE VIEW dependent AS SELECT original.id, original.value FROM original", "CREATE VIEW")
        wire.command("CREATE VIEW transitive AS SELECT id, value FROM dependent", "CREATE VIEW")
        wire.command("CREATE TYPE occupied_type AS ENUM ('one')", "CREATE TYPE")
        wire.command("COMMENT ON VIEW original IS 'view comment'", "COMMENT")
        wire.command("COMMENT ON COLUMN original.value IS 'column comment'", "COMMENT")
        wire.command("CREATE TRIGGER insert_view INSTEAD OF INSERT ON original FOR EACH ROW "
                     "INSERT INTO source VALUES (NEW.id, NEW.value)", "CREATE TRIGGER")
        oid = view_oid("original")
        original_rows = [("1", "7")]
        dependent(original_rows)

        os.mkfifo(blocker)
        missing_notice("ALTER VIEW IF EXISTS absent_schema.missing RENAME TO source")
        wire.command("BEGIN", "BEGIN", ready=b"T")
        missing_notice("ALTER VIEW IF EXISTS missing RENAME TO source", ready=b"T")
        wire.command("COMMIT", "COMMIT")
        for sql, state in (
            ("ALTER VIEW absent_schema.missing RENAME TO ok", "3F000"),
            ("ALTER VIEW missing RENAME TO ok", "42P01"),
            ("ALTER VIEW IF EXISTS source RENAME TO ok", "42809"),
            ("ALTER VIEW original RENAME TO source", "42P07"),
            ("ALTER VIEW original RENAME TO original", "42P07"),
            ("ALTER VIEW original RENAME TO occupied_type", "42710"),
            ("ALTER VIEW original RENAME TO public.qualified", "42601"),
            ("ALTER VIEW IF EXISTS missing RENAME TO nope extra", "42601"),
            ("ALTER VIEW ONLY original RENAME TO unused", "42601"),
            ("ALTER VIEW original RENAME value nope", "42601"),
        ):
            wire.query(sql, state=state, tag=None)
        assert not list(fixture.glob("info.txn_backup.*"))
        assert not list(fixture.glob("info.ddl_statement_backup.*"))
        blocker.unlink()

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("INSERT INTO source VALUES (2, 8)", "INSERT 0 1", ready=b"T")
        wire.command("SAVEPOINT before_rename", "SAVEPOINT", ready=b"T")
        wire.command("ALTER VIEW original RENAME TO temporary_name", "ALTER VIEW", ready=b"T")
        assert view_oid("temporary_name") == oid
        dependent([("1", "7"), ("2", "8")], ready=b"T")
        wire.command("ROLLBACK TO before_rename", "ROLLBACK", ready=b"T")
        assert view_oid("original") == oid
        wire.command("COMMIT", "COMMIT")
        assert view_oid("original") == oid

        catalog_dir = fixture / "info" / "pg_catalog"
        unchanged = [catalog_dir / "pg_depend.cat", catalog_dir / "pg_attribute.cat"]
        inodes = [path.stat().st_ino for path in unchanged]
        os.mkfifo(blocker)
        wire.command('ALTER /* object type */ VIEW original RENAME TO "New View"', "ALTER VIEW")
        blocker.unlink()
        assert [path.stat().st_ino for path in unchanged] == inodes
        assert not list(fixture.glob("info/.create_table_undo*"))
        assert view_oid('"New View"') == oid
        wire.query("SELECT id FROM original", state="42P01", tag=None)
        dependent([("1", "7"), ("2", "8")])
        wire.query("SELECT value FROM transitive WHERE id=2", rows=[("8",)], fields=[("value", 23)])
        wire.query('''SELECT obj_description('"New View"'::regclass, 'pg_class') AS note''',
                   rows=[("view comment",)], fields=[("note", 25)])
        wire.query('''SELECT col_description('"New View"'::regclass, 2) AS note''',
                   rows=[("column comment",)], fields=[("note", 25)])
        wire.query('DROP VIEW "New View" RESTRICT', state="2BP01", tag=None)
        wire.command('INSERT INTO "New View" VALUES (3, 9)', "INSERT 0 1")
        dependent([("1", "7"), ("2", "8"), ("3", "9")])

        wire.command("CREATE ROLE view_owner SUPERUSER NOINHERIT", "CREATE ROLE")
        wire.command("CREATE ROLE view_outsider", "CREATE ROLE")
        wire.command("SET ROLE view_owner", "SET")
        wire.command("CREATE VIEW owned_view AS SELECT 4 AS id", "CREATE VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.command("ALTER ROLE view_owner NOSUPERUSER", "ALTER ROLE")
        wire.command('CREATE TABLE "schema:public" (id int)', "CREATE TABLE")
        wire.command('ALTER TABLE "schema:public" OWNER TO view_owner', "ALTER TABLE")
        wire.command("SET ROLE view_outsider", "SET")
        wire.query("ALTER VIEW owned_view RENAME TO stolen", state="42501", tag=None)
        wire.command("RESET ROLE", "RESET")
        wire.command("SET ROLE view_owner", "SET")
        wire.query("ALTER VIEW owned_view RENAME TO owned_moved", state="42501", tag=None)
        wire.command("RESET ROLE", "RESET")
        wire.command('DROP TABLE "schema:public"', "DROP TABLE")
        wire.command("GRANT ALL ON SCHEMA public TO view_owner", "GRANT")
        wire.command("SET ROLE view_owner", "SET")
        wire.command("ALTER VIEW owned_view RENAME TO owned_moved", "ALTER VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.query("SELECT id FROM owned_moved", rows=[("4",)], fields=[("id", 23)])

        wire.command("CREATE SCHEMA other", "CREATE SCHEMA")
        wire.command("CREATE TABLE other.renamed(id int)", "CREATE TABLE")
        wire.command("INSERT INTO other.renamed VALUES (20)", "INSERT 0 1")
        wire.command('CREATE VIEW joined AS SELECT "New View".id AS left_id, renamed.id AS right_id '
                     'FROM "New View" CROSS JOIN other.renamed', "CREATE VIEW")
        extended.command('ALTER VIEW "New View" RENAME TO renamed', "ALTER VIEW")
        wire.query("SELECT left_id, right_id FROM joined WHERE left_id=1",
                   rows=[("1", "20")], fields=[("left_id", 23), ("right_id", 23)])

        wire.command("CREATE VIEW former_view AS SELECT id, value FROM source", "CREATE VIEW")
        wire.command("CREATE TRIGGER stale_insert INSTEAD OF INSERT ON former_view FOR EACH ROW "
                     "INSERT INTO source VALUES (NEW.id, 99)", "CREATE TRIGGER")
        wire.command("DROP VIEW former_view", "DROP VIEW")
        wire.command("CREATE VIEW plain_view AS SELECT id, value FROM source", "CREATE VIEW")
        wire.command("ALTER VIEW plain_view RENAME TO former_view", "ALTER VIEW")
        wire.command("INSERT INTO former_view (id, value) VALUES (4, 10)", "INSERT 0 1")
        wire.command('ALTER VIEW former_view RENAME TO "Values View"', "ALTER VIEW")
        wire.command('INSERT INTO public . "Values View" (id, value) VALUES (5, 11)', "INSERT 0 1")
        dependent([("1", "7"), ("2", "8"), ("3", "9"), ("4", "10"), ("5", "11")])
    finally:
        if blocker.exists():
            blocker.unlink()
        runner.stop_ours(server)
    print("[ALTER VIEW RENAME] stable identity, dependent views, trigger and rollback passed")


if __name__ == "__main__":
    main()
