#!/usr/bin/env python3
"""Check that view options persist without changing dependent relations."""

import os
from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    extended = Wire(client, server["sock"], extended=True)
    database = Path(server["dir"]) / "info"
    blocker = database / "unrelated_fifo"
    definition = database / ".views" / "app.report.view"
    stable = [
        database / "pg_catalog" / "pg_class.cat",
        database / "pg_catalog" / "pg_depend.cat",
        database / "pg_catalog" / "pg_description.cat",
        database / ".views" / "copy_report.view",
    ]
    try:
        wire.command("CREATE SCHEMA app", "CREATE SCHEMA")
        wire.command("CREATE TABLE base(id int PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command("CREATE VIEW app.report AS SELECT b.id,b.qty FROM base b WHERE b.qty>0 WITH CHECK OPTION",
                     "CREATE VIEW")
        wire.command("CREATE VIEW copy_report AS SELECT * FROM app.report", "CREATE VIEW")
        wire.command("COMMENT ON COLUMN app.report.qty IS 'original quantity'", "COMMENT")
        old = definition.read_bytes()
        fence = b"\nBASE_TABLE:\nWITH_CHECK_OPTION:"
        assert old.count(fence) == 1, old
        definition.write_bytes(old.replace(fence, b"WITH_CHECK_OPTION:"))

        identity = "SELECT 'app.report'::regclass::oid"
        original_oid = client.data_row_values(wire.send_statements(identity))
        assert len(original_oid) == 1 and original_oid[0][0] is not None
        before = [(path.stat().st_ino, path.read_bytes()) for path in stable]

        os.mkfifo(blocker)
        wire.query("INSERT INTO copy_report VALUES(1,-2)", tag=None, state="44000")
        wire.command("ALTER VIEW app.report RESET (check_option)", "ALTER VIEW")
        assert b"WITH_CHECK_OPTION" not in definition.read_bytes()
        wire.command("INSERT INTO copy_report VALUES(1,-2)", "INSERT 0 1")
        wire.command("ALTER VIEW app.report SET (check_option = local)", "ALTER VIEW")
        assert definition.read_bytes().endswith(b"\nWITH_CHECK_OPTION:local\n")
        wire.query("INSERT INTO copy_report VALUES(2,-2)", tag=None, state="44000")
        blocker.unlink()

        assert [(path.stat().st_ino, path.read_bytes()) for path in stable] == before
        assert client.data_row_values(wire.send_statements(identity)) == original_oid
        assert not list(database.glob(".create_table_undo*"))
        wire.query("SELECT col_description('app.report'::regclass,2) AS note",
                   rows=[("original quantity",)], fields=[("note", 25)])

        extended.command("BEGIN", "BEGIN", ready=b"T")
        extended.command("SAVEPOINT before_reset", "SAVEPOINT", ready=b"T")
        extended.command("ALTER VIEW app.report RESET (check_option)", "ALTER VIEW", ready=b"T")
        extended.command("INSERT INTO copy_report VALUES(2,-2)", "INSERT 0 1", ready=b"T")
        extended.command("ROLLBACK TO before_reset", "ROLLBACK", ready=b"T")
        extended.command("COMMIT", "COMMIT")
        extended.query("INSERT INTO copy_report VALUES(2,-2)", tag=None, state="44000")
        wire.query("SELECT * FROM base ORDER BY id", rows=[("1", "-2")],
                   fields=[("id", 23), ("qty", 23)])

        name = b"write_report"
        sql = "INSERT INTO copy_report VALUES ($1,$2) RETURNING qty"
        assert wire.exchange(wire.parse(sql, name, (23, 23)) + wire.packet(b"S")) == [
            (b"1", b""), (b"Z", b"I"),
        ]

        def write_report(key):
            return wire.exchange(wire.bind(name, (key, b"-3")) +
                                 wire.describe_portal() + wire.execute() + wire.packet(b"S"))

        wire.check(sql, write_report(b"3"), tag=None, state="44000")
        wire.command("ALTER VIEW app.report RESET (check_option)", "ALTER VIEW")
        wire.check(sql, write_report(b"3"), rows=[("-3",)], tag="INSERT 0 1",
                   fields=[("qty", 23)])
        wire.command("ALTER VIEW app.report SET (check_option=cascaded)", "ALTER VIEW")
        wire.check(sql, write_report(b"4"), tag=None, state="44000")

        wire.command("CREATE ROLE check_owner SUPERUSER NOINHERIT", "CREATE ROLE")
        wire.command("SET ROLE check_owner", "SET")
        wire.command("CREATE VIEW app.owned AS SELECT id, qty FROM base WHERE qty>0", "CREATE VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.command("ALTER ROLE check_owner NOSUPERUSER", "ALTER ROLE")
        wire.command("GRANT USAGE ON SCHEMA app TO check_owner", "GRANT")
        wire.command("SET ROLE check_owner", "SET")
        wire.query("ALTER VIEW app.report RESET (check_option)", tag=None, state="42501")
        wire.command("ALTER VIEW app.owned SET (check_option=local)", "ALTER VIEW")
        wire.command("RESET ROLE", "RESET")
        wire.query("INSERT INTO app.owned VALUES(5,-3)", tag=None, state="44000")
    finally:
        if blocker.exists():
            blocker.unlink()
        runner.stop_ours(server)
    print("[ALTER VIEW CHECK OPTION] identity, file scope, legacy definitions and prepared writes passed")


if __name__ == "__main__":
    main()
