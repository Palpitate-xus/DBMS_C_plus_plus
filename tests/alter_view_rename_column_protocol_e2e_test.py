#!/usr/bin/env python3
"""Rename a view column without changing its identity or its dependent outputs."""

import os
from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    extended = Wire(client, server["sock"], extended=True)
    database = Path(server["dir"]) / "info"
    blocker = database / "cannot_copy_fifo"
    try:
        wire.command("CREATE SCHEMA origin", "CREATE SCHEMA")
        wire.command("CREATE TABLE original (id int PRIMARY KEY, qty int, amount int DEFAULT 77)", "CREATE TABLE")
        wire.command("INSERT INTO original VALUES (1, 7, 19)", "INSERT 0 1")
        wire.command("CREATE VIEW origin.report AS SELECT id, qty FROM original", "CREATE VIEW")
        wire.command("CREATE VIEW dependent AS SELECT d.qty FROM (SELECT * FROM origin.report) d", "CREATE VIEW")
        wire.command("COMMENT ON COLUMN origin.report.qty IS 'original quantity'", "COMMENT")

        identity = "SELECT 'origin.report'::regclass::oid AS oid"
        note = "SELECT col_description('origin.report'::regclass, 2) AS note"
        oid = client.data_row_values(wire.send_statements(identity))
        assert len(oid) == 1 and oid[0][0] is not None, oid
        stable = [database / name for name in ("pg_catalog/pg_class.cat", "pg_catalog/pg_description.cat",
                                                "pg_catalog/pg_depend.cat", ".views/origin.report.view")]
        before = [(path.stat().st_ino, path.read_bytes()) for path in stable]

        os.mkfifo(blocker)
        frames = wire.send_statements("ALTER VIEW IF EXISTS origin.absent RENAME COLUMN qty TO amount")
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        assert b'Mrelation "absent" does not exist, skipping' in frames[0][1].split(b"\0"), frames
        assert frames[1:] == [(b"C", b"ALTER VIEW\0"), (b"Z", b"I")], frames
        wire.query("ALTER VIEW origin.report RENAME COLUMN qty TO id", tag=None, state="42701")
        wire.command("ALTER VIEW origin.report RENAME COLUMN qty TO amount", "ALTER VIEW")
        blocker.unlink()

        assert before == [(path.stat().st_ino, path.read_bytes()) for path in stable]
        assert not list(database.glob(".create_table_undo*"))
        assert client.data_row_values(wire.send_statements(identity)) == oid
        wire.query(note, rows=[("original quantity",)], fields=[("note", 25)])
        wire.query("SELECT * FROM origin.report", rows=[("1", "7")], fields=[("id", 23), ("amount", 23)])
        wire.query("SELECT * FROM dependent", rows=[("7",)], fields=[("qty", 23)])

        extended.query("UPDATE origin.report SET amount=11 WHERE id=1 RETURNING *",
                       rows=[("1", "11")], fields=[("id", 23), ("amount", 23)], tag="UPDATE 1")
        extended.query("INSERT INTO origin.report (id, amount) VALUES (2, 8) RETURNING amount",
                       rows=[("8",)], fields=[("amount", 23)], tag="INSERT 0 1")
        wire.query("SELECT * FROM original ORDER BY id", tag="SELECT 2", rows=[("1", "11", "19"), ("2", "8", "77")],
                   fields=[("id", 23), ("qty", 23), ("amount", 23)])
        wire.query("UPDATE origin.report SET qty=999 WHERE id=1", tag=None, state="42703")

        # CHECK OPTION evaluates the stored NEW row after name mapping.
        wire.command("CREATE VIEW checked AS SELECT id, qty FROM original WHERE qty > 0 WITH CHECK OPTION", "CREATE VIEW")
        wire.command("ALTER VIEW checked RENAME qty TO amount", "ALTER VIEW")
        wire.query("INSERT INTO checked (id, amount) VALUES (3, -9)", tag=None, state="44000")
        wire.query("SELECT id FROM original WHERE id=3", rows=[], fields=[("id", 23)], tag="SELECT 0")
        wire.command("INSERT INTO checked (id, amount) VALUES (5, 9)", "INSERT 0 1")
        wire.query("SELECT qty, amount FROM original WHERE id=5", rows=[("9", "77")],
                   fields=[("qty", 23), ("amount", 23)])

        wire.command("CREATE VIEW trigger_report AS SELECT id, qty FROM original", "CREATE VIEW")
        wire.command("CREATE TRIGGER write_trigger INSTEAD OF INSERT ON trigger_report FOR EACH ROW "
                     "INSERT INTO original (id, qty) VALUES (NEW.id, 99)", "CREATE TRIGGER")
        wire.command("ALTER VIEW trigger_report RENAME COLUMN qty TO amount", "ALTER VIEW")
        wire.query("INSERT INTO trigger_report (id, qty) VALUES (3, 10)", tag=None, state="42703")
        wire.query("SELECT id FROM original WHERE id=3", rows=[], fields=[("id", 23)], tag="SELECT 0")
        wire.command("INSERT INTO trigger_report (id, amount) VALUES (4, 10)", "INSERT 0 1")
        wire.query("SELECT qty FROM original WHERE id=4", rows=[("99",)], fields=[("qty", 23)])

        wire.command("CREATE ROLE column_owner SUPERUSER NOINHERIT", "CREATE ROLE")
        wire.command("SET ROLE column_owner", "SET")
        wire.command("CREATE VIEW origin.owned_report AS SELECT 1 AS qty", "CREATE VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.command("ALTER ROLE column_owner NOSUPERUSER", "ALTER ROLE")
        wire.command("GRANT USAGE ON SCHEMA origin TO column_owner", "GRANT")
        wire.command("SET ROLE column_owner", "SET")
        wire.query("ALTER VIEW origin.report RENAME amount TO stolen", tag=None, state="42501")
        wire.command('ALTER VIEW origin.owned_report RENAME qty TO "New Name"', "ALTER VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.query('SELECT "New Name" FROM origin.owned_report', rows=[("1",)], fields=[("New Name", 23)])
    finally:
        if blocker.exists():
            blocker.unlink()
        runner.stop_ours(server)
    print("[ALTER VIEW RENAME COLUMN] view identity, dependency, recovery image and writes passed")


if __name__ == "__main__":
    main()
