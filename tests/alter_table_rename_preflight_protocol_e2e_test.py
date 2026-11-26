#!/usr/bin/env python3
"""Check ALTER TABLE RENAME diagnostics and its first possible write."""

import os
from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])

    def skipped(sql, ready=b"I"):
        frames = wire.send_statements(sql)
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        fields = frames[0][1].split(b"\0")
        assert b"C00000" in fields, frames
        assert b'Mrelation "missing" does not exist, skipping' in fields, frames
        assert frames[1:] == [(b"C", b"ALTER TABLE\0"), (b"Z", ready)], frames

    try:
        wire.command("CREATE TABLE source_table(id integer PRIMARY KEY)", "CREATE TABLE")
        wire.command("CREATE TABLE taken(id integer PRIMARY KEY)", "CREATE TABLE")
        wire.command("INSERT INTO source_table VALUES (7)", "INSERT 0 1")
        wire.command("INSERT INTO taken VALUES (99)", "INSERT 0 1")

        # A special file cannot be copied into a physical before-image.
        # These statements must reach their own diagnostics without copying.
        fixture = Path(server["dir"])
        blocker = fixture / "info" / "cannot_copy_fifo"
        occupiedPath = fixture / "info" / "stray.stc"
        os.mkfifo(blocker)
        occupiedPath.symlink_to("absent.stc")
        try:
            wire.query("ALTER TABLE missing RENAME TO taken", state="42P01", tag=None)
            skipped("ALTER TABLE IF EXISTS missing RENAME TO taken")
            skipped("ALTER TABLE IF EXISTS no_such_schema.missing RENAME TO taken")
            wire.query("ALTER TABLE no_such_schema.missing RENAME TO taken",
                       state="3F000", tag=None)
            wire.query("ALTER TABLE source_table RENAME TO taken", state="42P07", tag=None)
            wire.query("ALTER TABLE source_table RENAME TO source_table",
                       state="42P07", tag=None)
            wire.query("ALTER TABLE source_table RENAME TO stray", state="42P07", tag=None)
            wire.query("ALTER TABLE pg_temp.missing RENAME TO taken", state="3F000", tag=None)
            for suffix in ("moved unexpected", "moved, ADD COLUMN n int",
                           "moved.other", "123", "SELECT"):
                wire.query("ALTER TABLE IF EXISTS source_table RENAME TO " + suffix,
                           state="42601", tag=None)
            wire.query("ALTER TABLE source_table ADD COLUMN n int, RENAME TO moved",
                       state="42601", tag=None)
            wire.query("ALTER TABLE source_table RENAME TO moved", state="58030", tag=None)
        finally:
            blocker.unlink()
            assert occupiedPath.is_symlink()
            occupiedPath.unlink()
        assert not list(fixture.glob("info.txn_backup.*"))
        wire.query("SELECT id FROM source_table", rows=[("7",)], fields=[("id", 23)])
        wire.query("SELECT id FROM taken", rows=[("99",)], fields=[("id", 23)])
        wire.query("SELECT id FROM moved", state="42P01", tag=None)

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("SAVEPOINT before_rename", "SAVEPOINT", ready=b"T")
        skipped("ALTER TABLE IF EXISTS missing RENAME TO taken", ready=b"T")
        wire.command('ALTER TABLE source_table RENAME TO "New Table"', "ALTER TABLE", ready=b"T")
        wire.query('SELECT id FROM "New Table"', rows=[("7",)], fields=[("id", 23)],
                   ready=b"T")
        wire.command("ROLLBACK TO before_rename", "ROLLBACK", ready=b"T")
        wire.query("SELECT id FROM source_table", rows=[("7",)], fields=[("id", 23)],
                   ready=b"T")
        wire.command("COMMIT", "COMMIT")

        wire.command("CREATE TEMP TABLE scratch(id integer)", "CREATE TABLE")
        wire.command("INSERT INTO scratch VALUES (4)", "INSERT 0 1")
        wire.command("ALTER TABLE pg_temp.scratch RENAME TO taken", "ALTER TABLE")
        wire.query("SELECT id FROM pg_temp.taken", rows=[("4",)], fields=[("id", 23)])
        wire.query("SELECT id FROM public.taken", rows=[("99",)], fields=[("id", 23)])
        wire.query("SELECT id FROM pg_temp.scratch", state="42P01", tag=None)
        wire.query("ALTER TABLE pg_temp.missing RENAME TO taken", state="42P01", tag=None)
        skipped("ALTER TABLE IF EXISTS pg_temp.missing RENAME TO taken")

        wire.command("CREATE TEMP TABLE serial_source(id serial PRIMARY KEY)", "CREATE TABLE")
        wire.command("INSERT INTO serial_source DEFAULT VALUES", "INSERT 0 1")
        wire.query("ALTER TABLE pg_temp.serial_source RENAME TO serial_source_id_seq",
                   state="42P07", tag=None)
        wire.command("ALTER TABLE pg_temp.serial_source RENAME TO serial_moved", "ALTER TABLE")
        wire.command("TRUNCATE pg_temp.serial_moved RESTART IDENTITY", "TRUNCATE TABLE")
        wire.query("INSERT INTO pg_temp.serial_moved DEFAULT VALUES RETURNING id",
                   rows=[("1",)], fields=[("id", 23)], tag="INSERT 0 1")
    finally:
        runner.stop_ours(server)
    print("[ALTER TABLE RENAME] name checks, notices and rollback passed")


if __name__ == "__main__":
    main()
