#!/usr/bin/env python3
"""Top-N preserves typed values, effects and errors across real wire modes."""
from functools import cmp_to_key
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("PREPARED_TOPN_PROTOCOL")
    records = []
    for i in range(72):
        key = None if i % 11 == 0 else i % 7
        payload = (None if i % 13 == 0 else "" if i % 17 == 0
                   else "NULL" if i % 19 == 0 else "row β " + str(i))
        records.append([str(i), None if key is None else str(key), payload])

    def wanted(ascending, nulls_first, offset, limit, minimum=0):
        def compare(a, b):
            if (a[1] is None) != (b[1] is None):
                return -1 if (a[1] is None) == nulls_first else 1
            if a[1] is not None and a[1] != b[1]:
                order = (int(a[1]) > int(b[1])) - (int(a[1]) < int(b[1]))
                return order if ascending else -order
            return (int(a[0]) > int(b[0])) - (int(a[0]) < int(b[0]))
        rows = sorted([row for row in records if int(row[0]) >= minimum],
                      key=cmp_to_key(compare))
        return rows[offset:offset + limit]

    try:
        review.query("CREATE TABLE topn_wire(id INTEGER PRIMARY KEY,k INTEGER,payload TEXT)")
        literal = lambda v: "NULL" if v is None else "'" + v.replace("'", "''") + "'"
        review.query("INSERT INTO topn_wire VALUES " + ",".join(
            "(" + row[0] + "," + (row[1] or "NULL") + "," + literal(row[2]) + ")"
            for row in records))
        review.query("CREATE SEQUENCE topn_key_calls")
        review.query("CREATE SEQUENCE topn_projection_calls")
        modes = [(False, False, 0, 7), (True, True, 4, 9), (False, True, 0, 1),
                 (True, False, 0, 0), (False, False, 90, 5), (True, False, 0, 200)]
        for index, (ascending, nulls_first, offset, limit) in enumerate(modes):
            tail = (" ORDER BY k " + ("ASC" if ascending else "DESC") +
                    " NULLS " + ("FIRST" if nulls_first else "LAST") +
                    f",id ASC LIMIT {limit} OFFSET {offset}")
            expected = wanted(ascending, nulls_first, offset, limit)
            review.query("SELECT id,k,payload FROM topn_wire" + tail,
                         expected, [23, 23, 25])
            sql = "SELECT id,k,payload FROM topn_wire WHERE id >= $1" + tail
            review.parameter(sql, ["0"], [23], expected, [23, 23, 25])
            name = ("topn_binary_" + str(index)).encode()
            parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
            bind = b"\0" + name + b"\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
            typed = review.client.typed
            review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) +
                                typed(b"D", b"P\0") + typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
            messages = review.client.read_until_ready(review.sock)
            errors = [review.client.diagnostic_fields(body).get(b"C")
                      for kind, body in messages if kind == b"E"]
            actual = review.client.data_row_values(messages)
            fields = review.client.row_description_fields(messages)
            binary_rows = [[struct.pack("!i", int(row[0])),
                            None if row[1] is None else struct.pack("!i", int(row[1])),
                            None if row[2] is None else row[2].encode()]
                           for row in expected]
            valid = (not errors and actual == binary_rows and messages[-1] == (b"Z", b"I")
                     and [field[3] for field in fields] == [23, 23, 25]
                     and [field[-1] for field in fields] == [1, 1, 1])
            review.record("binary " + str(index), valid, (actual, errors, fields), binary_rows)

        review.query("SELECT k FROM topn_wire ORDER BY k DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
                     [["6"]] * sum(row[1] == "6" for row in records), [23])
        review.query("SELECT DISTINCT k FROM topn_wire ORDER BY k DESC NULLS LAST LIMIT 4",
                     [["6"], ["5"], ["4"], ["3"]], [23])
        review.query("SELECT nextval('topn_key_calls') AS n FROM topn_wire ORDER BY n DESC LIMIT 3",
                     [["72"], ["71"], ["70"]], [20])
        review.query("SELECT currval('topn_key_calls')", [["72"]], [20])
        review.query("SELECT nextval('topn_key_calls') FROM topn_wire ORDER BY nextval('topn_key_calls') LIMIT 0",
                     [], [20])
        review.query("SELECT currval('topn_key_calls')", [["72"]], [20])
        ids = wanted(False, False, 4, 3)
        review.query("SELECT id,nextval('topn_projection_calls') FROM topn_wire "
                     "ORDER BY k DESC NULLS LAST,id ASC LIMIT 3 OFFSET 4",
                     [[row[0], str(i + 5)] for i, row in enumerate(ids)], [23, 20])
        review.query("SELECT currval('topn_projection_calls')", [["7"]], [20])
        review.query("SELECT id FROM topn_wire ORDER BY 1/(id-37) LIMIT 1", state="22012")
        review.query("SELECT 1", [["1"]], [23])
        review.query("SELECT id::oid FROM topn_wire ORDER BY id::oid DESC LIMIT 2",
                     [["71"], ["70"]], [26])
        review.query("CREATE TABLE topn_numeric(id INTEGER,n NUMERIC(30,0),f DOUBLE PRECISION)")
        review.query("INSERT INTO topn_numeric VALUES "
                     "(1,9007199254740993,'NaN'),(2,9007199254740992,'Infinity'),"
                     "(3,-9007199254740993,'-Infinity'),(4,NULL,0),(5,1,'NaN')")
        review.query("SELECT id FROM topn_numeric ORDER BY n ASC NULLS LAST LIMIT 3",
                     [["3"], ["5"], ["2"]], [23])
        review.query("SELECT id FROM topn_numeric ORDER BY f ASC,id ASC LIMIT 3",
                     [["3"], ["4"], ["2"]], [23])
        review.query("SELECT id FROM topn_numeric ORDER BY f DESC,id ASC LIMIT 2",
                     [["1"], ["5"]], [23])
        review.query("CREATE TABLE topn_integer_edges(id INTEGER,s SMALLINT,n INTEGER,b BIGINT)")
        review.query("INSERT INTO topn_integer_edges VALUES "
                     "(1,-32768,-2147483648,-9223372036854775808),"
                     "(2,32767,2147483647,9223372036854775807),"
                     "(3,0,-1,9007199254740993),(4,-1,0,9007199254740992),"
                     "(5,NULL,NULL,NULL),(6,0,-2147483648,-9007199254740993)")
        review.query("SELECT id FROM topn_integer_edges ORDER BY s ASC NULLS LAST,id LIMIT 4",
                     [["1"], ["4"], ["3"], ["6"]], [23])
        review.query("SELECT id FROM topn_integer_edges ORDER BY n DESC NULLS FIRST,id LIMIT 4",
                     [["5"], ["2"], ["4"], ["3"]], [23])
        review.query("SELECT id FROM topn_integer_edges ORDER BY b ASC NULLS LAST LIMIT 4",
                     [["1"], ["6"], ["4"], ["3"]], [23])
        review.parameter("SELECT id FROM topn_integer_edges WHERE id >= $1 "
                         "ORDER BY b DESC NULLS LAST LIMIT 3 OFFSET 1", ["0"], [23],
                         [["3"], ["4"], ["6"]], [23])
        review.query("CREATE TABLE topn_virtual(id INTEGER,v INTEGER GENERATED ALWAYS AS (id*2) VIRTUAL)")
        review.query("INSERT INTO topn_virtual(id) VALUES (1),(3),(2)")
        review.query("SELECT id,v FROM topn_virtual ORDER BY v DESC LIMIT 2",
                     [["3", "6"], ["2", "4"]], [23, 23])
        review.parameter("SELECT id,v FROM topn_virtual WHERE id >= $1 ORDER BY v DESC LIMIT 2",
                         ["0"], [23], [["3", "6"], ["2", "4"]], [23, 23])
        review.complete(48)
    finally:
        review.close()


if __name__ == "__main__":
    main()
