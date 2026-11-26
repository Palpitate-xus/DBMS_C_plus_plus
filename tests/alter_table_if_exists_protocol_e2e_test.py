#!/usr/bin/env python3
"""Exercise skipped ALTER TABLE actions and deferred rename backups."""

import os
from contextlib import closing
from pathlib import Path
import socket

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    fixture = Path(server["dir"])
    blocker = fixture / "info" / "cannot_copy_fifo"

    def skipped(sql, ready=b"I"):
        frames = wire.send_statements(sql)
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        assert b'Mrelation "missing" does not exist, skipping' in frames[0][1].split(b"\0")
        assert frames[1:] == [(b"C", b"ALTER TABLE\0"), (b"Z", ready)], frames

    def images():
        return set(fixture.glob("info.txn_backup.*")), set(fixture.glob("info.ddl_statement_backup.*"))

    try:
        wire.command("CREATE TABLE source_table(id int PRIMARY KEY, payload int)", "CREATE TABLE")
        wire.command("CREATE TABLE taken(id int)", "CREATE TABLE")
        wire.command("CREATE VIEW existing_view AS SELECT id FROM source_table", "CREATE VIEW")
        wire.command("INSERT INTO source_table VALUES (1, 7)", "INSERT 0 1")

        os.mkfifo(blocker)
        for action in ("ADD COLUMN d int", "DROP COLUMN id", "RENAME COLUMN id TO key2",
                       "ALTER COLUMN payload SET DEFAULT 3", "ADD CONSTRAINT positive CHECK (id > 0)",
                       "OWNER TO nonexistent_role", "SET UNLOGGED"):
            skipped("ALTER TABLE IF EXISTS missing " + action)
        skipped("ALTER TABLE IF EXISTS nonexistent_schema.missing ADD COLUMN d int")
        for action in ("ADD COLUMN", "DROP COLUMN", "ALTER COLUMN payload",
                       "ADD COLUMN d int,", "ADD COLUMN d int, unknown action"):
            wire.query("ALTER TABLE IF EXISTS missing " + action, state="42601", tag=None)
        wire.query("ALTER TABLE IF EXISTS existing_view ADD COLUMN d int", state="42809", tag=None)
        assert images() == (set(), set())

        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("INSERT INTO source_table VALUES (2, 8)", "INSERT 0 1", ready=b"T")
        skipped("ALTER TABLE IF EXISTS missing ADD COLUMN d int", ready=b"T")
        skipped("ALTER TABLE IF EXISTS missing RENAME TO taken", ready=b"T")
        assert images() == (set(), set())
        # A skipped operation does not take an exclusive database lock.
        with closing(socket.create_connection(("127.0.0.1", server["port"]), timeout=4)) as peer:
            client.startup(peer, "alice", "info")
            reader = Wire(client, peer)
            reader.command("BEGIN", "BEGIN", ready=b"T")
            reader.query("SELECT payload FROM source_table WHERE id=1",
                         rows=[("7",)], fields=[("payload", 23)], ready=b"T")
            wire.command("SAVEPOINT peer_active", "SAVEPOINT", ready=b"T")
            wire.command("SET LOCAL lock_timeout = '50ms'", "SET", ready=b"T")
            wire.query("ALTER TABLE source_table RENAME TO moved", state="55P03",
                       tag=None, ready=b"E")
            wire.command("ROLLBACK TO peer_active", "ROLLBACK", ready=b"T")
            reader.command("ROLLBACK", "ROLLBACK")
        wire.command("SAVEPOINT before_rename", "SAVEPOINT", ready=b"T")
        wire.query("ALTER TABLE source_table RENAME TO taken",
                   state="42P07", tag=None, ready=b"E")
        assert images() == (set(), set())
        wire.command("ROLLBACK TO before_rename", "ROLLBACK", ready=b"T")
        wire.query("SELECT payload FROM source_table WHERE id=2",
                   fields=[("payload", 23)], rows=[("8",)], ready=b"T")

        blocker.unlink()
        wire.command("ALTER TABLE IF EXISTS source_table ADD COLUMN extra int DEFAULT 3",
                     "ALTER TABLE", ready=b"T")
        wire.command("ALTER TABLE source_table DROP COLUMN IF EXISTS extra",
                     "ALTER TABLE", ready=b"T")
        wire.command("ALTER TABLE source_table RENAME TO moved", "ALTER TABLE", ready=b"T")
        wire.command("SAVEPOINT after_rename", "SAVEPOINT", ready=b"T")
        prior_images = images()
        assert len(prior_images[0]) == 1
        os.mkfifo(blocker)
        skipped("ALTER TABLE IF EXISTS missing DROP COLUMN payload", ready=b"T")
        assert images() == prior_images and blocker.exists()
        wire.query("ALTER TABLE moved RENAME TO taken", state="42P07", tag=None, ready=b"E")
        # An error can refresh the user savepoint's image. The transaction's
        # original image must survive. A new rename image before the name
        # check would have failed to copy the FIFO and masked the collision.
        assert images()[0] == prior_images[0] and len(images()[1]) == len(prior_images[1])
        if blocker.exists():
            blocker.unlink()
        wire.command("ROLLBACK TO after_rename", "ROLLBACK", ready=b"T")
        wire.query("SELECT payload FROM moved WHERE id=2",
                   fields=[("payload", 23)], rows=[("8",)], ready=b"T")
        wire.command("ALTER TABLE moved RENAME TO moved_twice", "ALTER TABLE", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.query("SELECT payload FROM source_table ORDER BY id",
                   fields=[("payload", 23)], rows=[("7",)])
        wire.query("SELECT payload FROM moved_twice", state="42P01", tag=None)
        assert images() == (set(), set())
    finally:
        if blocker.exists():
            blocker.unlink()
        runner.stop_ours(server)
    print("[ALTER TABLE IF EXISTS] missing sources, lock ownership and rollback passed")


if __name__ == "__main__":
    main()
