#!/usr/bin/env python3
"""Reject writes addressed to read-only views before modifying the base table."""

from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    directory = Path(server["dir"]) / "info"
    try:
        wire = Wire(client, server["sock"])
        wire.command("CREATE TABLE base(id int PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command("INSERT INTO base VALUES(1,11),(2,22)", "INSERT 0 2")
        wire.command("CREATE VIEW totals AS SELECT count(*) AS id FROM base", "CREATE VIEW")
        wire.command("CREATE VIEW report AS SELECT * FROM totals", "CREATE VIEW")
        wire.command("CREATE VIEW ranked AS SELECT id,row_number() OVER (ORDER BY id) AS qty FROM base",
                     "CREATE VIEW")
        wire.command("CREATE SEQUENCE effects", "CREATE SEQUENCE")
        heap = directory / "base.dt"
        before = heap.read_bytes()
        for sql in (
            "INSERT INTO totals VALUES(nextval('effects')::int)",
            "UPDATE ranked SET id=nextval('effects')::int",
            "DELETE FROM report WHERE id IN (SELECT id FROM base)",
        ):
            wire.query(sql, tag=None, state="55000")
        wire.query("SELECT nextval('effects') AS used", rows=[("1",)], fields=[("used", 20)])
        assert heap.read_bytes() == before

        for sql, result in (
            ("SELECT * FROM report", [("2",)]),
            ("SELECT * FROM base ORDER BY id", [("1", "11"), ("2", "22")]),
        ):
            frames = wire.send_statements(sql)
            wire.check(sql, frames, rows=result, tag="SELECT " + str(len(result)))

        # A matching INSTEAD OF trigger makes even a view with no table writable.
        wire.command("CREATE VIEW handled AS SELECT 0 AS id,0 AS qty", "CREATE VIEW")
        wire.command("CREATE TRIGGER handle_insert INSTEAD OF INSERT ON handled FOR EACH ROW "
                     "INSERT INTO base(id,qty) VALUES(NEW.id,NEW.qty)", "CREATE TRIGGER")
        ext = Wire(client, server["sock"], extended=True)
        ext.command("INSERT INTO handled VALUES(3,33)", "INSERT 0 1")
        ext.query("UPDATE handled SET qty=5", tag=None, state="55000")
        ext.query("DELETE FROM handled", tag=None, state="55000")
        ext.query("SELECT qty FROM base WHERE id=3", rows=[("33",)], fields=[("qty", 23)])

        # Reject the whole command before its other CTE can allocate a value.
        sql = ("WITH earlier AS (SELECT nextval('effects') AS id), "
               "rejected AS (DELETE FROM totals RETURNING id) SELECT * FROM earlier")
        wire.query(sql, tag=None, state="55000")
        wire.query("SELECT nextval('effects') AS used", rows=[("2",)], fields=[("used", 20)])

        wire.command("CREATE VIEW checked AS SELECT * FROM base WHERE qty>0 WITH CHECK OPTION",
                     "CREATE VIEW")
        prepared = "UPDATE checked SET qty=$2 WHERE id=$1 RETURNING qty"
        name = b"set_checked"
        assert wire.exchange(wire.parse(prepared, name, (23, 23)) + wire.packet(b"S")) == [
            (b"1", b""), (b"Z", b"I"),
        ]
        failed = wire.exchange(wire.bind(name, (b"1", b"-1")) +
                               wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        wire.check(prepared, failed, tag=None, state="44000")
        wire.query("SELECT qty FROM base WHERE id=1", rows=[("11",)], fields=[("qty", 23)])

        wire.command("PREPARE update_checked(int,int) AS UPDATE checked SET qty=$2 WHERE id=$1",
                     "PREPARE")
        wire.command("EXECUTE update_checked(1,17)", "UPDATE 1")
        wire.command("CREATE OR REPLACE VIEW checked AS SELECT DISTINCT id,qty FROM base",
                     "CREATE VIEW")
        wire.query("EXECUTE update_checked(1,19)", tag=None, state="55000")
        wire.query("SELECT qty FROM base WHERE id=1", rows=[("17",)], fields=[("qty", 23)])
    finally:
        runner.stop_ours(server)
    print("[READ-ONLY VIEW WRITES] no heap or sequence changes on rejected writes")


if __name__ == "__main__":
    main()
