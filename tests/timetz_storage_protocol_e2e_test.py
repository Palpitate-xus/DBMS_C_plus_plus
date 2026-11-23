#!/usr/bin/env python3
"""Exercise stored TIMETZ values through SQL and the PostgreSQL wire protocol."""

import struct

from compat.numeric_type_review import Review


def main():
    review = Review("TIMETZ_STORAGE")
    client, sock = review.client, review.sock

    def check(sql, frames, kinds, tag, rows=(), state=None, ready=b"I", unordered=False):
        errors = [client.diagnostic_fields(data)[b"C"].decode() for kind, data in frames if kind == b"E"]
        tags = [data[:-1].decode() for kind, data in frames if kind == b"C"]
        values = client.data_row_values(frames)
        expected_values = [list(row) for row in rows]
        if unordered:
            values.sort(key=repr)
            expected_values.sort(key=repr)
        actual = (b"".join(kind for kind, _ in frames), tags, values, errors, frames[-1])
        wanted = (kinds, [tag] if tag else [], expected_values, [state] if state else [], (b"Z", ready))
        review.record(sql, actual == wanted, actual, wanted)

    def sql(statement, tag=None, rows=None, state=None, ready=b"I", unordered=False):
        wanted = b"EZ" if state else b"CZ" if rows is None else b"T" + b"D" * len(rows) + b"CZ"
        check(statement, client.simple_query(sock, statement), wanted, tag, rows or (), state, ready, unordered)

    def binary_query(statement, field, precision, rows):
        parse = client.typed(b"P", b"\0" + statement.encode() + b"\0\0\0")
        bind = client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, 1))
        sock.sendall(parse + bind + client.typed(b"D", b"P\0") +
                     client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S", b""))
        frames = client.read_until_ready(sock)
        check(statement + " (binary)", frames, b"12T" + b"D" * len(rows) + b"CZ",
              f"SELECT {len(rows)}", rows)
        headers = client.row_description_fields(frames) if any(kind == b"T" for kind, _ in frames) else []
        correct = len(headers) == 1 and headers[0][0] == field and headers[0][1] > 0 and \
            headers[0][2:] == (2, 1266, 12, precision, 1)
        review.record("Physical TIMETZ description", correct, headers, (field, "table", 2, 1266, 12, precision, 1))

    def copy_rows(raw, tag=None, state=None, options=""):
        statement = "COPY tz_fixed (id,t,n) FROM STDIN" + options
        sock.sendall(client.typed(b"Q", statement.encode() + b"\0"))
        header = client.read_message(sock)
        wanted = (b"G", b"\0" + struct.pack("!HHHH", 3, 0, 0, 0))
        review.record("COPY text input header", header == wanted, header, wanted)
        if header[0] != b"G":
            client.read_until_ready(sock)
            return
        sock.sendall(client.typed(b"d", raw) + client.typed(b"c", b""))
        check(statement, client.read_until_ready(sock), b"EZ" if state else b"CZ", tag, state=state)

    def time_value(micros, west):
        return struct.pack("!qi", micros, west)

    try:
        sql("CREATE TABLE tz_mixed(id integer PRIMARY KEY, t timetz(2), note text DEFAULT 'body')", "CREATE TABLE")
        sql("INSERT INTO tz_mixed VALUES (1, NULL), (2, '12:00:00.115+04:00:07'), "
            "(3, '12:00:00.125+04:00:07'::timetz(6))", "INSERT 0 3")
        mixed = [(b"1", None, b"body"), (b"2", b"12:00:00.12+04:00:07", b"body"),
                 (b"3", b"12:00:00.13+04:00:07", b"body")]
        sql("SELECT id,t::text,note FROM tz_mixed ORDER BY id", "SELECT 3", mixed)
        binary_query("SELECT t FROM tz_mixed ORDER BY id", b"t", 2,
                     [(None,), (time_value(43_200_120_000, -14_407),), (time_value(43_200_130_000, -14_407),)])
        sql("UPDATE tz_mixed SET t='23:59:59.999999-05:30' WHERE id=1 RETURNING id,t::text",
            "UPDATE 1", [(b"1", b"24:00:00-05:30")])
        sql("INSERT INTO tz_mixed VALUES (4,'42')", state="22007")
        sql("UPDATE tz_mixed SET t='12:00:00+16' WHERE id=1", state="22009")
        sql("SELECT id FROM tz_mixed ORDER BY t NULLS LAST, id", "SELECT 3", [(b"2",), (b"3",), (b"1",)])
        sql("ALTER TABLE tz_mixed ALTER COLUMN t TYPE timetz(1)", "ALTER TABLE")
        sql("SELECT id,t::text FROM tz_mixed ORDER BY id", "SELECT 3",
            [(b"1", b"24:00:00-05:30"), (b"2", b"12:00:00.1+04:00:07"), (b"3", b"12:00:00.1+04:00:07")])
        sql("SELECT id, t='12:00:00.10+04:00:07' AS eq, "
            "t IS DISTINCT FROM '12:00:00.10+04:00:07' AS different "
            "FROM tz_mixed ORDER BY id", "SELECT 3",
            [(b"1", b"f", b"t"), (b"2", b"t", b"f"), (b"3", b"t", b"f")])
        sql("SELECT id FROM tz_mixed WHERE '12:00:00.10+04:00:07'=t ORDER BY id",
            "SELECT 2", [(b"2",), (b"3",)])
        sql("SELECT id FROM tz_mixed WHERE t='12:00:00.10+04:00:07'::text", state="42883")
        sql("UPDATE tz_mixed SET note='ordered' WHERE t>'12:00:00.10+04:00:07' RETURNING id",
            "UPDATE 1", [(b"1",)])
        sql("DELETE FROM tz_mixed WHERE t='12:00:00.10+04:00:07' RETURNING id", "DELETE 2",
            [(b"2",), (b"3",)], unordered=True)

        sql("CREATE TABLE tz_fixed(id integer PRIMARY KEY, "
            "t timetz(6) DEFAULT '01:10:00.000002+15:00:01', n integer)", "CREATE TABLE")
        sql("INSERT INTO tz_fixed(id,n) VALUES (1,9),(2,13)", "INSERT 0 2")
        sql("SELECT id,t::text,n FROM tz_fixed ORDER BY id", "SELECT 2",
            [(b"1", b"01:10:00.000002+15:00:01", b"9"), (b"2", b"01:10:00.000002+15:00:01", b"13")])
        copy_rows(b"3\t00:00:00.000001-00:00:07\t8\n4\t\\N\t11\n", tag="COPY 2")
        copy_rows(b"5\t01:10:00+05\t9\n6\tbad+05\t3\n", state="22007")
        binary_query("SELECT t FROM tz_fixed ORDER BY id", b"t", 6,
                     [(time_value(4_200_000_002, -54_001),), (time_value(4_200_000_002, -54_001),),
                      (time_value(1, 7),), (None,)])
        copy_rows(b"5\t01:10:00+05\t9\n6\t12:00:00+16\t3\n", tag="COPY 1",
                  options=" (ON_ERROR ignore, LOG_VERBOSITY silent)")
        sql("SELECT id,t::text FROM tz_fixed WHERE id>=5 ORDER BY id", "SELECT 1",
            [(b"5", b"01:10:00+05")])

        sql("CREATE TABLE tz_key(t time(2) with time zone PRIMARY KEY, id integer)", "CREATE TABLE")
        sql("INSERT INTO tz_key VALUES ('10:00:00.124+02',1),"
            "('09:00:00.124+01',2),('00:00:00+15:59:59',3),('24:00:00-15:59:59',4)", "INSERT 0 4")
        sql("INSERT INTO tz_key VALUES ('10:00:00.12+0200',5)", state="23505")
        sql("SELECT id,t::text FROM tz_key WHERE t='10:00:00.12+02'", "SELECT 1",
            [(b"1", b"10:00:00.12+02")])
        sql("SELECT id FROM tz_key WHERE t='10:00:00.121+02'", "SELECT 0", [])
        sql("SELECT id FROM tz_key WHERE t>='09:00:00.12+01' "
            "AND t<'24:00:00-15:59:59' ORDER BY t", "SELECT 1", [(b"2",)])
        sql("SELECT id FROM tz_key ORDER BY t", "SELECT 4", [(b"3",), (b"1",), (b"2",), (b"4",)])
        sql("SELECT id FROM tz_key ORDER BY t DESC", "SELECT 4", [(b"4",), (b"2",), (b"1",), (b"3",)])
        sql("UPDATE tz_key SET t='23:59:59.999999-15:59:59' WHERE id=2", state="23505")
        sql("REINDEX TABLE tz_key", "REINDEX")
        sql("SELECT id FROM tz_key WHERE t='09:00:00.12+01'", "SELECT 1", [(b"2",)])
        review.complete(39)
    finally:
        review.close()


if __name__ == "__main__":
    main()
