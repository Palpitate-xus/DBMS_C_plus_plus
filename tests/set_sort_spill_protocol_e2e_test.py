#!/usr/bin/env python3
"""Real UNION ALL global sort: typed values, clause scope and input demand.

Every query is parsed from genuine SQL. No synthetic AST removes branch or
root clauses. All Execute requests use unlimited rows and valid formats.
"""
from functools import cmp_to_key
from pathlib import Path
import struct

from compat.numeric_type_review import Review


ROW_COUNT = 192
BRANCH_COUNT = ROW_COUNT // 2
ENUM_LABELS = ["zeta", "", "NULL", "alpha"]


def fixture():
    numbers = ["9007199254740993", "9007199254740992", "-9007199254740993", "1", "-1", "0"]
    floating = ["-Infinity", "-9.75", "0", "1.5", "Infinity", "NaN", None]
    real = ["1.0000001", "1.0000002", "-1", "-Infinity", "Infinity", "NaN", None]
    rows = []
    for i in range(ROW_COUNT):
        rows.append({"id": str(i), "k": None if i % 17 == 0 else str(i % 5),
                     "n": None if i % 19 == 0 else numbers[i % len(numbers)],
                     "f": floating[i % len(floating)], "r": real[i % len(real)],
                     "payload": None if i % 13 == 0 else "" if i % 17 == 0 else "NULL" if i % 19 == 0
                         else "set row β " + str(i) + " " + "p" * 512,
                     "mood": None if i % 23 == 0 else ENUM_LABELS[i % len(ENUM_LABELS)]})
    return rows


def ordered(rows, keys):
    def key(row, column):
        if column == "regtype":
            return 23 if int(row["id"]) % 2 == 0 else 25
        value = row[column]
        if value is None:
            return None
        if column in ("id", "k", "n"):
            return int(value)
        if column in ("f", "r"):
            number = 0 if value == "NaN" else float(value)
            if column == "r" and value != "NaN":
                number = struct.unpack("!f", struct.pack("!f", number))[0]
            return value == "NaN", number
        if column == "mood":
            return ENUM_LABELS.index(value)
        return value.encode()

    def compare(a, b):
        for name, ascending, nulls_first in keys:
            left, right = key(a, name), key(b, name)
            if (left is None) != (right is None):
                return -1 if (left is None) == nulls_first else 1
            if left is None:
                continue
            difference = (left > right) - (left < right)
            if difference:
                return difference if ascending else -difference
        return 0
    return sorted(rows, key=cmp_to_key(compare))


def cases():
    rows = fixture(); left = rows[:BRANCH_COUNT]; right = rows[BRANCH_COUNT:]
    result = []

    def query(sql, wanted=None, oids=None, state=None, tag=None):
        result.append(("query", sql, wanted, oids, state, tag))

    def union(columns):
        return "SELECT " + columns + " FROM set_spill_left UNION ALL SELECT " + columns + " FROM set_spill_right"

    quote = lambda text: "NULL" if text is None else "'" + text.replace("'", "''") + "'"
    query("CREATE TYPE set_spill_mood AS ENUM ('zeta','','NULL','alpha')", tag="CREATE TYPE")
    for table, kind, values in (("set_spill_left", "INTEGER", left), ("set_spill_right", "BIGINT", right)):
        query("CREATE TABLE " + table + "(id " + kind + ",k INTEGER,n NUMERIC(30,0),f DOUBLE PRECISION,"
              'r REAL,payload TEXT COLLATE "C",mood set_spill_mood)', tag="CREATE TABLE")
        for first in range(0, len(values), 48):
            part = values[first:first + 48]
            query("INSERT INTO " + table + " VALUES " + ",".join(
                "(" + ",".join(quote(row[name]) for name in ("id", "k", "n", "f", "r", "payload", "mood")) + ")"
                for row in part), tag="INSERT 0 " + str(len(part)))
    result.append(("enum_oid", "SELECT oid FROM pg_type WHERE typname='set_spill_mood'", None, [26], None, "SELECT 1"))

    common_keys = [("k", False, True), ("id", True, False)]
    common = ordered(rows, common_keys)
    for suffix, memory in (("low", "64kB"), ("high", "16MB")):
        query("SET work_mem='" + memory + "'", tag="SET")
        query("SHOW work_mem", [[memory]], [25], tag="SHOW")
        query(union("id,k,payload") + " ORDER BY k DESC NULLS FIRST,id ASC",
              [[row["id"], row["k"], row["payload"]] for row in common], [20, 23, 25])
        query(union("id,k,payload") + " ORDER BY 2 DESC NULLS FIRST,1 ASC",
              [[row["id"], row["k"], row["payload"]] for row in common], [20, 23, 25])
        query(union("id,payload") + " ORDER BY id DESC",
              [[row["id"], row["payload"]] for row in reversed(rows)], [20, 25])
        query(union("id,payload") + " ORDER BY id ASC LIMIT 20 OFFSET 31",
              [[row["id"], row["payload"]] for row in rows[31:51]], [20, 25])
        query(union("id,payload") + " ORDER BY id DESC LIMIT 10000",
              [[row["id"], row["payload"]] for row in reversed(rows)], [20, 25])
        query(union("id,payload") + " ORDER BY id ASC LIMIT 13 OFFSET 100",
              [[row["id"], row["payload"]] for row in rows[100:113]], [20, 25])
        query(union("id,payload") + " ORDER BY id ASC LIMIT 0", [], [20, 25])
        for name, oid, ascending, first in (("n", 1700, True, False), ("f", 701, True, False),
                                           ("r", 700, False, True)):
            ordered_rows = ordered(rows, [(name, ascending, first), ("id", True, False)])
            query(union(name + ",id") + " ORDER BY " + name + (" ASC" if ascending else " DESC") +
                  (" NULLS FIRST" if first else " NULLS LAST") + ",id",
                  [[row[name], row["id"]] for row in ordered_rows], [oid, 20])
        words = ordered(rows, [("payload", True, True), ("id", True, False)])
        query('SELECT payload COLLATE "C" AS p,id FROM set_spill_left UNION ALL '
              'SELECT payload COLLATE "C",id FROM set_spill_right ORDER BY p NULLS FIRST,id',
              [[row["payload"], row["id"]] for row in words], [25, 20])
        moods = ordered(rows, [("mood", True, False), ("id", True, False)])
        query(union("mood,id") + " ORDER BY mood ASC NULLS LAST,id",
              [[row["mood"], row["id"]] for row in moods], ["enum", 20])
        query(union("id::oid AS o,payload") + " ORDER BY o DESC",
              [[row["id"], row["payload"]] for row in reversed(rows)], [26, 25])
        types = ordered(rows, [("regtype", False, False), ("id", True, False)])
        query(union("CASE WHEN id%2=0 THEN 'integer'::regtype ELSE 'text'::regtype END AS t,id") + " ORDER BY t DESC,id",
              [["integer" if int(row["id"]) % 2 == 0 else "text", row["id"]] for row in types], [2206, 20])
        query(union("ARRAY[id::oid,NULL::oid,'4294967295'::oid] AS a,id,payload") + " ORDER BY id DESC",
              [["{" + row["id"] + ",NULL,4294967295}", row["id"], row["payload"]] for row in reversed(rows)], [1028, 20, 25])
        selected = rows[17:30]
        query(union("ROW(id,k) AS rec,id") + " ORDER BY id LIMIT 13 OFFSET 17",
              [["(" + row["id"] + "," + (row["k"] or "") + ")", row["id"]] for row in selected], [2249, 20])
        query(union("ROW(id,ARRAY[id::oid,NULL::oid]) AS rec,id") + " ORDER BY id LIMIT 13 OFFSET 17",
              [["(" + row["id"] + ',"{' + row["id"] + ',NULL}")', row["id"]] for row in selected], [2249, 20])
        query(union("ARRAY[id::oid,NULL::oid,'4294967295'::oid] AS a,id") + " ORDER BY a DESC,id",
              [["{" + row["id"] + ",NULL,4294967295}", row["id"]] for row in reversed(rows)], [1028, 20])
        query(union("ROW(id::bigint,k) AS rec,id") + " ORDER BY rec DESC",
              [["(" + row["id"] + "," + (row["k"] or "") + ")", row["id"]] for row in reversed(rows)], [2249, 20])

        ties = sum(row["k"] == "0" for row in rows)
        query(union("k") + " ORDER BY k ASC NULLS LAST FETCH FIRST 1 ROW WITH TIES", [["0"]] * ties, [23])
        query(union("k") + " ORDER BY k ASC NULLS LAST OFFSET 2 ROWS FETCH FIRST 1 ROW WITH TIES",
              [["0"]] * (ties - 2), [23])
        query(union("k") + " ORDER BY k ASC NULLS LAST FETCH FIRST 0 ROWS WITH TIES", [], [23])
        pair_rows = ordered(rows, [("k", True, False), ("n", True, False)])
        boundary = pair_rows[0]["k"], pair_rows[0]["n"]
        query(union("k,n") + " ORDER BY k ASC NULLS LAST,n ASC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
              [list(boundary)] * sum((row["k"], row["n"]) == boundary for row in rows), [23, 1700])
        query(union("f") + " ORDER BY f DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
              [["NaN"]] * sum(row["f"] == "NaN" for row in rows), [701])

        query("VALUES(2,'b'),(NULL,NULL),(1,'') UNION ALL VALUES(4,'NULL'),(3,'two words') "
              "ORDER BY 1 ASC NULLS LAST,2", [["1", ""], ["2", "b"], ["3", "two words"], ["4", "NULL"], [None, None]], [23, 25])
        mix = [{"id": "2147483648", "payload": "big"}, {"id": None, "payload": None}] + left
        mixed = sorted((row for row in mix if row["id"] is not None), key=lambda row: -int(row["id"]))
        mixed += [row for row in mix if row["id"] is None]
        query("VALUES(2147483648,'big'),(NULL,NULL) UNION ALL SELECT id,payload FROM set_spill_left "
              "ORDER BY 1 DESC NULLS LAST LIMIT 11 OFFSET 2",
              [[row["id"], row["payload"]] for row in mixed[2:13]], [20, 25])
        query("SELECT id AS row_id,payload FROM set_spill_left UNION ALL VALUES(2147483648,'big'),(NULL,NULL) "
              "ORDER BY row_id DESC NULLS LAST LIMIT 7 OFFSET 4",
              [[row["id"], row["payload"]] for row in mixed[4:11]], [20, 25])
        query("(VALUES(4),(2),(1) ORDER BY 1 LIMIT 2) UNION ALL VALUES(3),(0) ORDER BY 1 DESC",
              [["3"], ["2"], ["1"], ["0"]], [23])
        query("(SELECT id AS n FROM set_spill_left ORDER BY id DESC LIMIT 3) UNION ALL "
              "(SELECT id FROM set_spill_right ORDER BY id ASC LIMIT 2) ORDER BY n ASC",
              [[str(i)] for i in [93, 94, 95, 96, 97]], [20])
        query("(SELECT id AS n FROM set_spill_left ORDER BY id ASC LIMIT 5 OFFSET 11) UNION ALL "
              "(SELECT id FROM set_spill_right ORDER BY id ASC LIMIT 4 OFFSET 2) ORDER BY n DESC LIMIT 6 OFFSET 1",
              [[str(i)] for i in [100, 99, 98, 15, 14, 13]], [20])
        chain = [{"id": "2147483648", "payload": "tail"}] + list(reversed(rows))
        query(union("id,payload") + " UNION ALL VALUES(2147483648,'tail') ORDER BY 1 DESC OFFSET 7 LIMIT 9",
              [[row["id"], row["payload"]] for row in chain[7:16]], [20, 25])
        query("WITH l(a,b,c) AS(SELECT id,k,payload FROM set_spill_left),"
              "r(d,e,f) AS(SELECT id,k,payload FROM set_spill_right) "
              "SELECT a AS row_id,b AS sort_key,c AS payload FROM l UNION ALL SELECT d,e,f FROM r "
              "ORDER BY sort_key DESC NULLS FIRST,row_id ASC LIMIT 9 OFFSET 11",
              [[row["id"], row["k"], row["payload"]] for row in common[11:20]], [20, 23, 25])
        parameter_sql = ("SELECT id,payload FROM set_spill_left WHERE id >= $1 UNION ALL "
                         "SELECT id,payload FROM set_spill_right WHERE id >= $1 ORDER BY id DESC LIMIT 13 OFFSET 5")
        wanted = [[row["id"], row["payload"]] for row in list(reversed(rows))[5:18]]
        result.append(("parameter", parameter_sql, wanted, [20, 25], None, None))
        result.append(("binary", parameter_sql, wanted, [20, 25], None, None))
        query("SELECT 1/(id-37) AS n FROM set_spill_left UNION ALL SELECT 1/(id-137) FROM set_spill_right ORDER BY n DESC LIMIT 1",
              state="22012")
        query("SELECT id AS n,1/(id-37) AS discarded FROM set_spill_left UNION ALL "
              "SELECT id,1/(id-137) FROM set_spill_right ORDER BY n DESC LIMIT 1", state="22012")
        query("SELECT 1", [["1"]], [23])

        sequence = "set_spill_calls_" + suffix
        query("CREATE SEQUENCE " + sequence, tag="CREATE SEQUENCE")
        # Heap physical order is unspecified and can differ between engines.
        # Branch ORDER fixes the sequence-to-payload association before the
        # global set sort; its source expressions must still run for all rows.
        source = ("(SELECT nextval('" + sequence + "') AS n,payload FROM set_spill_left ORDER BY id ASC) UNION ALL "
                  "(SELECT nextval('" + sequence + "') AS n,payload FROM set_spill_right ORDER BY id ASC)")
        query(source + " ORDER BY n DESC LIMIT 5", [[str(i), rows[i - 1]["payload"]] for i in range(192, 187, -1)], [20, 25])
        query("SELECT currval('" + sequence + "')", [["192"]], [20])
        query(source + " ORDER BY n DESC LIMIT 5 OFFSET 31",
              [[str(i), rows[i - 193]["payload"]] for i in range(353, 348, -1)], [20, 25])
        query("SELECT currval('" + sequence + "')", [["384"]], [20])
        query(source + " ORDER BY n DESC LIMIT 0", [], [20, 25])
        query("SELECT currval('" + sequence + "')", [["384"]], [20])
        query("(SELECT nextval('" + sequence + "') AS n FROM set_spill_left ORDER BY id DESC LIMIT 3) UNION ALL "
              "(SELECT nextval('" + sequence + "') FROM set_spill_right ORDER BY id DESC LIMIT 2) ORDER BY n DESC LIMIT 4",
              [[str(i)] for i in range(389, 385, -1)], [20])
        query("SELECT currval('" + sequence + "')", [["389"]], [20])
        query("(SELECT nextval('" + sequence + "') AS stamp,id FROM set_spill_left ORDER BY id ASC) UNION ALL "
              "(SELECT nextval('" + sequence + "') AS stamp,id FROM set_spill_right ORDER BY id ASC) "
              "ORDER BY id DESC LIMIT 1", [["581", "191"]], [20, 20])
        query("SELECT currval('" + sequence + "')", [["581"]], [20])
        query("(SELECT nextval('" + sequence + "') AS n FROM set_spill_left LIMIT 0) UNION ALL "
              "(SELECT nextval('" + sequence + "') FROM set_spill_right LIMIT 2) ORDER BY n DESC",
              [["583"], ["582"]], [20])
        query("SELECT currval('" + sequence + "')", [["583"]], [20])
    return result


def binary(review, sql, wanted):
    typed = review.client.typed
    name = b"set_spill_binary_" + str(review.controls).encode()
    parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
    bind = b"\0" + name + b"\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
    review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) + typed(b"D", b"P\0") +
                        typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = review.client.read_until_ready(review.sock)
    fields = review.client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
    actual = review.client.data_row_values(messages)
    expected = [[struct.pack("!q", int(row[0])), None if row[1] is None else row[1].encode()] for row in wanted]
    commands = [body.rstrip(b"\0") for kind, body in messages if kind == b"C"]
    good = (actual == expected and not any(kind == b"E" for kind, _ in messages)
            and [field[3] for field in fields] == [20, 25] and [field[-1] for field in fields] == [1, 1]
            and all(field[1] == 0 and field[2] == 0 for field in fields)
            and commands == [b"SELECT 13"] and messages[-1] == (b"Z", b"I"))
    review.record("binary common-BIGINT set sort", good, (actual, fields, commands), expected)


def main():
    review = Review("SET_SORT_SPILL_PROTOCOL")
    enum_oid = None
    try:
        matrix = cases()
        assert len(matrix) == 112, len(matrix)
        for mode, sql, wanted, oids, state, tag in matrix:
            if mode == "enum_oid":
                messages = review.client.simple_query(review.sock, sql)
                actual = review.runner.decode_wire_result(messages, include_types=True)
                try:
                    enum_oid = int(actual[0][0][0]) if len(actual[0]) == 1 and len(actual[0][0]) == 1 else None
                except (TypeError, ValueError):
                    enum_oid = None
                good = (enum_oid is not None and enum_oid > 0 and actual[1] is None and
                        actual[4] == "SELECT 1" and actual[5] == [26])
                review.record(sql, good, actual, "one positive physical enum OID from actual catalog, output26")
            elif mode == "parameter":
                review.parameter(sql, ["0"], [23], wanted, oids)
            elif mode == "binary":
                binary(review, sql, wanted)
            else:
                expected_oids = None if oids is None else [enum_oid if oid == "enum" else oid for oid in oids]
                messages = review.client.simple_query(review.sock, sql)
                actual = review.runner.decode_wire_result(messages, include_types=True)
                valid = actual[1] == state
                if wanted is not None:
                    valid = valid and actual[0] == wanted and actual[4] == (tag or "SELECT " + str(len(wanted)))
                elif tag is not None:
                    valid = valid and actual[0] == [] and actual[4] == tag
                if expected_oids is not None:
                    valid = valid and actual[5] == expected_oids
                if state:
                    valid = valid and actual[0] == [] and actual[4] is None
                review.record(sql, valid, actual, (wanted, expected_oids, state, tag))
        if review.server:
            assert not list(Path(review.server["dir"]).glob(".dbms-sort-*"))
            for entry in (Path("/proc") / str(review.server["process"].pid) / "fd").iterdir():
                try:
                    assert ".dbms-sort-" not in str(entry.readlink())
                except FileNotFoundError:
                    pass
        review.complete(112)
    finally:
        review.close()


if __name__ == "__main__":
    main()
