#!/usr/bin/env python3
"""Move a view between schemas and retain its identity and dependencies."""

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

    def check_rows(expected, ready=b"I"):
        wire.query("SELECT id, qty FROM dependent ORDER BY id", rows=expected,
                   tag=f"SELECT {len(expected)}", fields=[("id", 23), ("qty", 23)], ready=ready)

    def view_id(name):
        sql = f"SELECT '{name}'::regclass::oid AS id"
        rows, state, message, _, _, types = runner.decode_wire_result(
            wire.send_statements(sql), include_types=True)
        assert state is None and types == [26] and len(rows) == 1, (sql, message, rows)
        return rows[0][0]

    try:
        wire.command("CREATE SCHEMA origin", "CREATE SCHEMA")
        wire.command('CREATE SCHEMA "Archive West"', "CREATE SCHEMA")
        wire.command("CREATE SCHEMA occupied", "CREATE SCHEMA")
        wire.command("CREATE SCHEMA type_collision", "CREATE SCHEMA")
        wire.command("CREATE TABLE orders (id int PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command("INSERT INTO orders VALUES (1, 7)", "INSERT 0 1")
        wire.command("CREATE TABLE occupied.report (id int)", "CREATE TABLE")
        wire.command("CREATE TYPE type_collision.report AS ENUM ('x')", "CREATE TYPE")
        wire.command("CREATE VIEW origin.report AS SELECT id, qty FROM public.orders", "CREATE VIEW")
        wire.command("CREATE VIEW dependent AS SELECT report.id, report.qty FROM origin.report", "CREATE VIEW")
        wire.command("COMMENT ON VIEW origin.report IS 'sent from origin'", "COMMENT")
        wire.command("COMMENT ON COLUMN origin.report.qty IS 'quantity'", "COMMENT")
        original_id = view_id("origin.report")
        check_rows([("1", "7")])

        os.mkfifo(blocker)
        for sql, state in (
            ("ALTER VIEW origin.report SET SCHEMA absent", "3F000"),
            ("ALTER VIEW IF EXISTS origin.report SET SCHEMA absent", "3F000"),
            ("ALTER VIEW origin.report SET SCHEMA occupied", "42P07"),
            ("ALTER VIEW origin.report SET SCHEMA type_collision", "42710"),
            ("ALTER VIEW IF EXISTS occupied.report SET SCHEMA origin", "42809"),
            ("ALTER VIEW origin.report SET SCHEMA pg_temp", "0A000"),
            ("ALTER VIEW origin.report SET SCHEMA pg_toast", "0A000"),
            ("ALTER VIEW origin.report SET SCHEMA public.qualifier", "42601"),
            ("ALTER VIEW IF EXISTS missing SET SCHEMA origin extra", "42601"),
            ("ALTER VIEW origin.report SET SCHEMA", "42601"),
        ):
            wire.query(sql, state=state, tag=None)
        wire.command("ALTER VIEW origin.report SET SCHEMA origin", "ALTER VIEW")
        frames = wire.send_statements("ALTER VIEW IF EXISTS origin.missing SET SCHEMA absent")
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        assert b'Mrelation "missing" does not exist, skipping' in frames[0][1].split(b"\0")
        assert frames[1:] == [(b"C", b"ALTER VIEW\0"), (b"Z", b"I")], frames
        assert not list(fixture.glob("info.txn_backup.*"))
        blocker.unlink()

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("INSERT INTO orders VALUES (2, 8)", "INSERT 0 1", ready=b"T")
        wire.command("SAVEPOINT before_move", "SAVEPOINT", ready=b"T")
        extended.command('ALTER VIEW origin.report SET SCHEMA "Archive West"', "ALTER VIEW", ready=b"T")
        assert view_id('"Archive West".report') == original_id
        check_rows([("1", "7"), ("2", "8")], ready=b"T")
        wire.command("ROLLBACK TO before_move", "ROLLBACK", ready=b"T")
        assert view_id("origin.report") == original_id
        wire.command("COMMIT", "COMMIT")
        assert view_id("origin.report") == original_id

        catalog_dir = fixture / "info" / "pg_catalog"
        unchanged = [catalog_dir / "pg_attribute.cat", catalog_dir / "pg_description.cat"]
        inodes = [path.stat().st_ino for path in unchanged]
        os.mkfifo(blocker)
        wire.command('ALTER VIEW origin.report SET SCHEMA "Archive West"', "ALTER VIEW")
        blocker.unlink()
        assert [path.stat().st_ino for path in unchanged] == inodes
        assert not list(fixture.glob("info/.create_table_undo*"))
        assert view_id('"Archive West".report') == original_id
        wire.query("SELECT id FROM origin.report", state="42P01", tag=None)
        wire.query('''SELECT obj_description('"Archive West".report'::regclass, 'pg_class') AS note''',
                   rows=[("sent from origin",)], fields=[("note", 25)])
        wire.query('''SELECT col_description('"Archive West".report'::regclass, 2) AS note''',
                   rows=[("quantity",)], fields=[("note", 25)])
        wire.command('DROP SCHEMA origin RESTRICT', "DROP SCHEMA")
        wire.query('DROP SCHEMA "Archive West" RESTRICT', state="2BP01", tag=None)
        wire.query('DROP VIEW "Archive West".report RESTRICT', state="2BP01", tag=None)
        wire.command('CREATE TABLE "Archive West".orders (id int, qty int)', "CREATE TABLE")
        wire.command('SET search_path TO "Archive West", public', "SET")
        wire.command('INSERT INTO "Archive West".report (id, qty) VALUES (3, 9)', "INSERT 0 1")
        check_rows([("1", "7"), ("2", "8"), ("3", "9")])
        wire.query('SELECT id FROM "Archive West".orders', rows=[], tag="SELECT 0", fields=[("id", 23)])
        wire.command('ALTER VIEW "Archive West".report SET SCHEMA public', "ALTER VIEW")
        assert view_id("public.report") == original_id
        check_rows([("1", "7"), ("2", "8"), ("3", "9")])

        wire.command("CREATE SCHEMA landing", "CREATE SCHEMA")
        wire.command("CREATE VIEW public.trigger_view AS SELECT id, qty FROM public.orders", "CREATE VIEW")
        wire.command("CREATE TRIGGER on_insert INSTEAD OF INSERT ON public.trigger_view FOR EACH ROW "
                     "INSERT INTO public.orders VALUES(NEW.id, NEW.qty + 1)", "CREATE TRIGGER")
        wire.command("ALTER VIEW public.trigger_view SET SCHEMA landing", "ALTER VIEW")
        wire.command("INSERT INTO landing.trigger_view VALUES (4, 10)", "INSERT 0 1")
        wire.query("SELECT qty FROM public.orders WHERE id=4", rows=[("11",)], fields=[("qty", 23)])

        wire.command("CREATE ROLE moving_owner SUPERUSER NOINHERIT", "CREATE ROLE")
        wire.command("SET ROLE moving_owner", "SET")
        wire.command("CREATE VIEW landing.owned_view AS SELECT 5 AS id", "CREATE VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.command("ALTER ROLE moving_owner NOSUPERUSER", "ALTER ROLE")
        wire.command("GRANT USAGE ON SCHEMA landing TO moving_owner", "GRANT")
        wire.command("SET ROLE moving_owner", "SET")
        wire.query("ALTER VIEW landing.owned_view SET SCHEMA public", state="42501", tag=None)
        wire.command("RESET ROLE", "RESET")
        wire.command("GRANT ALL ON SCHEMA public TO moving_owner", "GRANT")
        wire.command("SET ROLE moving_owner", "SET")
        wire.command("ALTER VIEW landing.owned_view SET SCHEMA public", "ALTER VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.query("SELECT id FROM public.owned_view", rows=[("5",)], fields=[("id", 23)])

        wire.command("CREATE SCHEMA bound_write", "CREATE SCHEMA")
        wire.command("CREATE TABLE bound_write.orders (id int PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command("CREATE TABLE landing.orders (id int PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command("SET search_path TO bound_write, public", "SET")
        wire.command("CREATE VIEW writable AS SELECT id, qty FROM orders WHERE id > 0 WITH CHECK OPTION",
                     "CREATE VIEW")
        wire.command("ALTER VIEW bound_write.writable SET SCHEMA landing", "ALTER VIEW")
        wire.command("SET search_path TO landing, public", "SET")
        wire.command("INSERT INTO writable (id, qty) VALUES (17, 23)", "INSERT 0 1")
        wire.query("SELECT qty FROM bound_write.orders WHERE id=17", rows=[("23",)], fields=[("qty", 23)])
        wire.query("SELECT id FROM landing.orders", rows=[], tag="SELECT 0", fields=[("id", 23)])
    finally:
        if blocker.exists():
            blocker.unlink()
        runner.stop_ours(server)
    print("[ALTER VIEW SET SCHEMA] identity, dependents, triggers and rollback passed")


if __name__ == "__main__":
    main()
