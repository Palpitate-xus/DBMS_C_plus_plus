#!/usr/bin/env python3
"""Physical LIMIT retains projection demand, typed wire values and metadata."""
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("PHYSICAL_LIMIT_DEMAND")
    rows = [[str(i), None if i == 0 else "" if i == 1 else "NULL" if i == 2
             else "row β " + str(i), "tag-" + str(i), str(i) + ".25"]
            for i in range(12)]
    literal = lambda value: "NULL" if value is None else "'" + value.replace("'", "''") + "'"
    try:
        review.query("CREATE TABLE limit_rows(id INTEGER NOT NULL,payload TEXT,tag VARCHAR(12),amount NUMERIC(12,2))")
        review.query("INSERT INTO limit_rows VALUES " + ",".join(
            "(" + row[0] + "," + literal(row[1]) + "," + literal(row[2]) + "," + row[3] + ")"
            for row in rows))
        for index, (limit, offset) in enumerate(((0, 0), (1, 0), (3, 2), (99, 0), (1, 20), (0, 20))):
            tail = f" LIMIT {limit} OFFSET {offset}"
            wanted = rows[offset:offset + limit]
            review.query("SELECT id,payload,tag,amount FROM limit_rows" + tail,
                         wanted, [23, 25, 1043, 1700])
            review.parameter("SELECT id,payload,tag,amount FROM limit_rows WHERE id >= $1" + tail,
                             ["0"], [23], wanted, [23, 25, 1043, 1700])
            sql = "SELECT id,payload,tag FROM limit_rows WHERE id >= $1" + tail
            name = ("physical_limit_" + str(index)).encode()
            parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
            bind = b"\0" + name + b"\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
            typed = review.client.typed
            review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) +
                                typed(b"D", b"P\0") + typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
            messages = review.client.read_until_ready(review.sock)
            actual = review.client.data_row_values(messages)
            errors = [review.client.diagnostic_fields(body).get(b"C")
                      for kind, body in messages if kind == b"E"]
            fields = review.client.row_description_fields(messages)
            expected = [[struct.pack("!i", int(row[0]))] +
                        [None if cell is None else cell.encode() for cell in row[1:3]] for row in wanted]
            valid = (not errors and actual == expected and messages[-1] == (b"Z", b"I")
                     and [field[3] for field in fields] == [23, 25, 1043]
                     and [field[-1] for field in fields] == [1, 1, 1])
            review.record("binary " + str(index), valid, (actual, errors, fields), expected)
        review.query("SELECT 1/(id-5) FROM limit_rows LIMIT 1", [["0"]], [23])
        messages = review.client.simple_query(review.sock, "SELECT 1/(id-5) FROM limit_rows LIMIT 6")
        failed = review.runner.decode_wire_result(messages, include_types=True)
        # PG emits the successful prefix before the sixth row fails. The
        # generic review.query error helper expects no rows and is unsuitable.
        expected_prefix = [["0"], ["0"], ["0"], ["0"], ["-1"]]
        review.record("runtime error prefix", failed[0] == expected_prefix and failed[1] == "22012"
                      and failed[4] is None and failed[5] == [23]
                      and [kind for kind, _ in messages] == [b"T"] + [b"D"] * 5 + [b"E", b"Z"],
                      failed, (expected_prefix, "22012", None, [23]))
        review.query("SELECT 1/(id-5) FROM limit_rows LIMIT 0 OFFSET 5", [], [23])
        review.query("SELECT id FROM limit_rows WHERE 1/(id-5)=0 LIMIT 1", [["0"]], [23])
        review.query("CREATE SEQUENCE physical_limit_calls")
        review.query("SELECT setval('physical_limit_calls',100,true)", [["100"]], [20])
        review.query("SELECT nextval('physical_limit_calls') FROM limit_rows LIMIT 3 OFFSET 2",
                     [["103"], ["104"], ["105"]], [20])
        review.query("SELECT currval('physical_limit_calls')", [["105"]], [20])
        review.query("SELECT setval('physical_limit_calls',100,true)", [["100"]], [20])
        review.query("SELECT nextval('physical_limit_calls') FROM limit_rows LIMIT 0 OFFSET 20", [], [20])
        review.query("SELECT currval('physical_limit_calls')", [["100"]], [20])
        review.query("SELECT setval('physical_limit_calls',100,true)", [["100"]], [20])
        review.query("SELECT nextval('physical_limit_calls') FROM limit_rows WHERE false LIMIT 1", [], [20])
        review.query("SELECT currval('physical_limit_calls')", [["100"]], [20])
        review.query("CREATE SEQUENCE physical_limit_filter_calls")
        review.query("SELECT id FROM limit_rows WHERE nextval('physical_limit_filter_calls')>0 LIMIT 3 OFFSET 2",
                     [["2"], ["3"], ["4"]], [23])
        review.query("SELECT currval('physical_limit_filter_calls')", [["5"]], [20])
        review.query("CREATE TABLE physical_limit_sink(id INTEGER)")
        review.query("CREATE FUNCTION physical_limit_writer(arg INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$ "
                     "BEGIN INSERT INTO physical_limit_sink VALUES(arg); RETURN arg; END; $$")
        review.query("SELECT physical_limit_writer(id) FROM limit_rows LIMIT 1", [["0"]], [23])
        review.query("SELECT count(*) FROM physical_limit_sink", [["1"]], [20])
        review.query("TRUNCATE physical_limit_sink")
        review.query("SELECT physical_limit_writer(id) FROM limit_rows LIMIT 1 OFFSET 2", [["2"]], [23])
        review.query("SELECT count(*) FROM physical_limit_sink", [["3"]], [20])
        review.query("CREATE SEQUENCE physical_limit_distinct_calls")
        review.query("SELECT DISTINCT nextval('physical_limit_distinct_calls')%1 FROM limit_rows LIMIT 1",
                     [["0"]], [20])
        review.query("SELECT currval('physical_limit_distinct_calls')", [["12"]], [20])
        review.query("SELECT count(*) FROM limit_rows LIMIT 1", [["12"]], [20])
        review.query("SELECT sum(id) FROM limit_rows LIMIT 1", [["66"]], [20])
        review.query("SELECT id FROM limit_rows OFFSET 2 FETCH FIRST 3 ROWS ONLY",
                     [["2"], ["3"], ["4"]], [23])
        review.query("SELECT id FROM limit_rows LIMIT -1", state="2201W")
        review.query("SELECT 9007199254740993::bigint FROM limit_rows LIMIT 1",
                     [["9007199254740993"]], [20])
        for parameterized in (False, True):
            sql = "SELECT tag,amount FROM limit_rows" + (" WHERE id >= $1" if parameterized else "") + " LIMIT 1"
            if parameterized:
                parse = b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
                bind = b"\0\0" + struct.pack("!HHi", 0, 1, 1) + b"0" + struct.pack("!H", 0)
                typed = review.client.typed
                review.sock.sendall(typed(b"P", parse) + typed(b"D", b"S\0") + typed(b"B", bind) +
                                    typed(b"D", b"P\0") + typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
                messages = review.client.read_until_ready(review.sock)
            else:
                messages = review.client.simple_query(review.sock, sql)
            descriptions = [review.client.row_description_fields([m]) for m in messages if m[0] == b"T"]
            expected = [(1043, -1, 16, 0), (1700, -1, (12 << 16) + 2 + 4, 0)]
            valid = (len(descriptions) == (2 if parameterized else 1)
                     and all([field[3:] for field in fields] == expected for fields in descriptions)
                     and all([field[2] for field in fields] == [3, 4] and all(field[1] > 0 for field in fields)
                             for fields in descriptions)
                     and review.client.data_row_values(messages) == [[b"tag-0", b"0.25"]]
                     and not any(kind == b"E" for kind, _ in messages))
            review.record("metadata parameter=" + str(parameterized), valid, descriptions, expected)
        review.query("SELECT id FROM limit_rows LIMIT 1 OFFSET -1", state="2201X")
        review.query("SELECT id FROM limit_rows LIMIT -0", [], [23])
        review.query("CREATE TABLE physical_limit_star(i INTEGER)")
        review.query("INSERT INTO physical_limit_star VALUES (1),(2)")
        review.query("SELECT * FROM physical_limit_star LIMIT 1", [["1"]], [23])
        review.query("SELECT NULL FROM limit_rows LIMIT 1", [[None]], [25])
        sql = "SELECT 1/(id-5) FROM limit_rows WHERE id >= $1 LIMIT 6"
        typed = review.client.typed
        parse = b"prefix_error\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
        bind = b"\0prefix_error\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
        review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) + typed(b"D", b"P\0") +
                            typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
        messages = review.client.read_until_ready(review.sock)
        states = [review.client.diagnostic_fields(body).get(b"C") for kind, body in messages if kind == b"E"]
        values = review.client.data_row_values(messages)
        expected = [[struct.pack("!i", i)] for i in (0, 0, 0, 0, -1)]
        fields = review.client.row_description_fields(messages)
        review.record("binary runtime error prefix", states == [b"22012"] and values == expected
                      and not any(kind == b"C" for kind, _ in messages)
                      and messages[-1] == (b"Z", b"I") and [f[3] for f in fields] == [23]
                      and [f[-1] for f in fields] == [1], (values, states, fields), expected)
        review.query("SELECT id FROM limit_rows WHERE false LIMIT -1", state="2201W")
        review.complete(62)
    finally:
        review.close()


if __name__ == "__main__":
    main()
