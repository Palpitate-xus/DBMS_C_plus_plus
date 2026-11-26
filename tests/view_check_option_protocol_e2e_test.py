#!/usr/bin/env python3
"""Use an old on-disk view definition with the typed new-row check."""

from pathlib import Path

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    extended = Wire(client, server["sock"], extended=True)
    definition = Path(server["dir"]) / "info/.views/checked.view"
    try:
        wire.command("CREATE TABLE base(id int PRIMARY KEY, qty int DEFAULT 5)", "CREATE TABLE")
        wire.command("INSERT INTO base VALUES(1,10),(2,0)", "INSERT 0 2")
        wire.command("CREATE VIEW checked(k, amount) AS SELECT b.id, b.qty FROM base b "
                     "WHERE b.qty > 0 WITH CHECK OPTION", "CREATE VIEW")
        current = definition.read_bytes()
        fence = b"\nBASE_TABLE:\nWITH_CHECK_OPTION:"
        assert current.count(fence) == 1, current
        # The former writer omitted the newline and the BASE_TABLE line for
        # a view with a FROM alias. Simulate opening that existing catalog.
        stored = current.replace(fence, b"WITH_CHECK_OPTION:")
        definition.write_bytes(stored)

        view_fields = [("k", 23), ("amount", 23)]
        extended.query("SELECT * FROM checked", rows=[("1", "10")], fields=view_fields)
        wire.query("INSERT INTO checked(k) VALUES(3) RETURNING *", rows=[("3", "5")],
                   fields=view_fields, tag="INSERT 0 1")
        wire.query("UPDATE checked SET amount=0 WHERE k=2 RETURNING *", rows=[],
                   fields=view_fields, tag="UPDATE 0")
        wire.query("UPDATE checked SET amount=0 WHERE k=1", tag=None, state="44000")
        extended.query("INSERT INTO checked VALUES(4,40),(5,0)", tag=None, state="44000")
        wire.query("SELECT * FROM base ORDER BY id", rows=[("1", "10"), ("2", "0"), ("3", "5")],
                   fields=[("id", 23), ("qty", 23)], tag="SELECT 3")
        assert definition.read_bytes() == stored
    finally:
        runner.stop_ours(server)
    print("[VIEW CHECK OPTION] old definitions, default values and rollback passed")


if __name__ == "__main__":
    main()
