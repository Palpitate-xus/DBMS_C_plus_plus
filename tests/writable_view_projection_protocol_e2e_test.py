#!/usr/bin/env python3
"""Keep view column names and binary types on prepared write results."""

import struct

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    wire = Wire(client, server["sock"])
    try:
        wire.command("CREATE TABLE original (id int PRIMARY KEY, qty int, other int DEFAULT 77)", "CREATE TABLE")
        wire.command("CREATE VIEW report (amount, k) AS SELECT qty, id FROM original", "CREATE VIEW")
        wire.command("ALTER TABLE original RENAME COLUMN qty TO new_qty", "ALTER TABLE")
        wire.command("ALTER TABLE original RENAME COLUMN other TO qty", "ALTER TABLE")

        for name, sql, args, tag, expected in (
            (b"insert_report", "INSERT INTO report(k,amount) VALUES($1,$2) RETURNING *",
             (b"1", b"7"), b"INSERT 0 1", (7, 1)),
            (b"update_report", "UPDATE report SET amount=amount+$2 WHERE k=$1 RETURNING amount,k",
             (b"1", b"2"), b"UPDATE 1", (9, 1)),
            (b"delete_report", "DELETE FROM report WHERE k=$1+99 RETURNING amount,k",
             (b"1",), b"DELETE 0", ()),
        ):
            prepared = wire.exchange(wire.parse(sql, name, (23,) * len(args)) + wire.packet(b"S"))
            assert prepared == [(b"1", b""), (b"Z", b"I")], (sql, prepared)
            messages = wire.exchange(wire.bind(name, args, result_format=1) + wire.describe_portal()
                                     + wire.execute() + wire.packet(b"S"))
            kinds = [b"2", b"T"] + ([b"D"] if expected else []) + [b"C", b"Z"]
            assert [kind for kind, _ in messages] == kinds, (sql, messages)
            fields = client.row_description_fields(messages)
            assert [(name, oid, fmt) for name, _, _, oid, _, _, fmt in fields] == [
                (b"amount", 23, 1), (b"k", 23, 1),
            ], (sql, fields)
            rows = [[struct.pack("!i", cell) for cell in expected]] if expected else []
            assert client.data_row_values(messages) == rows, (sql, messages)
            assert messages[-2:] == [(b"C", tag + b"\0"), (b"Z", b"I")], (sql, messages)

        wire.query("SELECT id, new_qty, qty FROM original", rows=[("1", "9", "77")],
                   fields=[("id", 23), ("new_qty", 23), ("qty", 23)])

        wire.command("CREATE VIEW calculated AS SELECT id, new_qty AS qty, new_qty*2 AS twice, "
                     "ARRAY[new_qty, NULL, new_qty+1] AS amounts FROM original", "CREATE VIEW")
        sql = "UPDATE calculated SET qty=$1 WHERE twice=$2 RETURNING old.amounts,new.amounts"
        packets = (wire.parse(sql, oids=(23, 23)) + wire.bind(arguments=(b"12", b"18"), result_format=1)
                   + wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        messages = wire.exchange(packets)
        assert [kind for kind, _ in messages] == [b"1", b"2", b"T", b"D", b"C", b"Z"], messages
        fields = client.row_description_fields(messages)
        assert [(field[0], field[3], field[-1]) for field in fields] == [(b"amounts", 1007, 1)] * 2, fields
        assert client.data_row_values(messages) == [[
            struct.pack("!10i", 1, 1, 23, 3, 1, 4, qty, -1, 4, qty + 1) for qty in (9, 12)
        ]], messages
        assert messages[-2:] == [(b"C", b"UPDATE 1\0"), (b"Z", b"I")], messages

        name = b"with_computed"
        sql = ("WITH changed AS (UPDATE calculated SET qty=qty+1 WHERE twice=24 RETURNING id,twice) "
               "SELECT id,twice FROM changed")
        described = wire.exchange(wire.parse(sql, name) + wire.packet(b"D", b"S" + name + b"\0")
                                  + wire.packet(b"S"))
        assert [kind for kind, _ in described] == [b"1", b"t", b"T", b"Z"], described
        fields = client.row_description_fields(described)
        assert [(field[0], field[3]) for field in fields] == [(b"id", 23), (b"twice", 23)], fields
        wire.query("SELECT new_qty FROM original", rows=[("12",)], fields=[("new_qty", 23)])
        messages = wire.exchange(wire.bind(name) + wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        wire.check(sql, messages, rows=[("1", "26")], fields=[("id", 23), ("twice", 23)])
    finally:
        runner.stop_ours(server)
    print("[WRITABLE VIEW PROJECTION] typed prepared writes and result metadata passed")


if __name__ == "__main__":
    main()
