#!/usr/bin/env python3
"""Ordinary physical sorts retain complete typed results at low work memory."""
import struct
from pathlib import Path

from compat.numeric_type_review import Review


def main():
    review = Review("SORT_SPILL_PROTOCOL")
    try:
        # Configure only the helper's private instance. Session SET work_mem
        # remains a separate GUC gap; do not pretend it is implemented here.
        if review.server:
            (Path(review.server["dir"]) / "dbms.conf").write_text("work_mem_kb=64\n")
            messages = review.client.simple_query(review.sock, "SELECT pg_reload_conf()")
        else:
            messages = review.client.simple_query(review.sock, "SET work_mem='64kB'")
        assert not any(kind == b"E" for kind, _ in messages), messages
        review.query("SELECT setting FROM pg_settings WHERE name='work_mem'", [["64"]], [25])
        review.query('CREATE TABLE spill_rows(id INTEGER,wide BIGINT,f DOUBLE PRECISION,d DATE,stamp TIMESTAMP,payload TEXT COLLATE "C")')
        rows = []
        floats = ["-Infinity", "-2.25", "0", "3.5", "Infinity", "NaN", None]
        for i in range(192):
            rows.append([str(i), str(9007199254740993 + i), floats[i % len(floats)],
                         None if i % 11 == 0 else f"2024-02-{1 + i % 28:02d}",
                         None if i % 13 == 0 else f"2024-02-{1 + i % 28:02d} 01:02:{i % 60:02d}",
                         None if i % 17 == 0 else "" if i % 17 == 1 else "NULL" if i % 17 == 2
                         else "row β " + str(i) + " " + "p" * 512])
        literal = lambda value: "NULL" if value is None else "'" + value.replace("'", "''") + "'"
        for begin in range(0, len(rows), 48):
            review.query("INSERT INTO spill_rows VALUES " + ",".join(
                "(" + ",".join(literal(value) for value in row) + ")" for row in rows[begin:begin + 48]))
        columns = [("id", 23), ("wide", 20), ("f", 701), ("d", 1082), ("stamp", 1114), ("payload", 25)]
        for index, (column, oid) in enumerate(columns):
            def key(value):
                if index in (0, 1):
                    return int(value)
                if index == 2:
                    return (value == "NaN", 0 if value == "NaN" else float(value))
                return value.encode()
            for ascending in (True, False):
                for first in (True, False):
                    values = [row[index] for row in rows]
                    present = sorted((value for value in values if value is not None), key=key, reverse=not ascending)
                    nulls = [None] * sum(value is None for value in values)
                    expected = nulls + present if first else present + nulls
                    review.query("SELECT " + column + " FROM spill_rows ORDER BY " + column +
                                 (" ASC" if ascending else " DESC") + (" NULLS FIRST" if first else " NULLS LAST"),
                                 [[value] for value in expected], [oid])
        expected = [[row[0], row[5]] for row in rows]
        review.query("SELECT id,payload FROM spill_rows WHERE id<4 ORDER BY id", expected[:4], [23, 25])
        review.parameter("SELECT id,payload FROM spill_rows WHERE id >= $1 ORDER BY id", ["0"], [23], expected, [23, 25])
        typed = review.client.typed
        sql = "SELECT id,payload FROM spill_rows WHERE id >= $1 ORDER BY id"
        parse = b"spill_binary\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
        bind = b"\0spill_binary\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
        review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) + typed(b"D", b"P\0") +
                            typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
        messages = review.client.read_until_ready(review.sock)
        fields = review.client.row_description_fields(messages)
        binary = [[struct.pack("!i", int(row[0])), None if row[1] is None else row[1].encode()] for row in expected]
        actual = review.client.data_row_values(messages)
        valid = (actual == binary and not any(kind == b"E" for kind, _ in messages)
                 and [field[3] for field in fields] == [23, 25] and [field[-1] for field in fields] == [1, 1]
                 and messages[-1] == (b"Z", b"I"))
        review.record("binary ordinary sort", valid, (len(actual), fields), (len(binary), [23, 25]))
        review.query("SELECT id FROM spill_rows WHERE false ORDER BY id", [], [23])
        review.query("SELECT wide FROM spill_rows ORDER BY wide", [[row[1]] for row in rows], [20])
        review.query("SELECT id FROM spill_rows ORDER BY id LIMIT 3", [["0"], ["1"], ["2"]], [23])
        review.query("SELECT id,payload FROM spill_rows ORDER BY id LIMIT 0", [], [23, 25])
        review.query("SELECT payload,id FROM spill_rows ORDER BY id", [[row[5], row[0]] for row in rows], [25, 23])
        review.query("SELECT id AS value FROM spill_rows ORDER BY value DESC", [[str(i)] for i in range(191, -1, -1)], [23])
        review.query("SELECT id FROM spill_rows ORDER BY 1 DESC", [[str(i)] for i in range(191, -1, -1)], [23])
        review.query("SELECT id FROM spill_rows WHERE id>100 AND id<108 ORDER BY id DESC",
                     [[str(i)] for i in range(107, 100, -1)], [23])
        review.query("SELECT id FROM spill_rows WHERE id<2 OR id>189 ORDER BY id", [["0"], ["1"], ["190"], ["191"]], [23])
        review.query("CREATE TABLE guard_numeric(n NUMERIC(20,0))")
        review.query("INSERT INTO guard_numeric VALUES(9007199254740993),(9007199254740992)")
        review.query("SELECT n FROM guard_numeric ORDER BY n", [["9007199254740992"], ["9007199254740993"]], [1700])
        review.query("SELECT n FROM guard_numeric ORDER BY n DESC", [["9007199254740993"], ["9007199254740992"]], [1700])
        review.query("SELECT DISTINCT id FROM spill_rows ORDER BY id", [[str(i)] for i in range(192)], [23])
        if review.server:
            assert not list(Path(review.server["dir"]).glob(".dbms-sort-*"))
            descriptors = Path("/proc") / str(review.server["process"].pid) / "fd"
            for entry in descriptors.iterdir():
                try:
                    assert ".dbms-sort-" not in str(entry.readlink())
                except FileNotFoundError:
                    pass
        review.complete(47)
    finally:
        review.close()


if __name__ == "__main__":
    main()
