#!/usr/bin/env python3
"""Ordinary typed sort owns values, contexts and effects across work_mem budgets.

The original 80 controls retain ARRAY[id] for existing typed receiver coverage.
Appended ordinary no-LIMIT controls have no ARRAY admission trigger and cover
the additional base-table multikey/expression/numeric execution entry.
All Execute messages request unlimited rows and valid text/binary formats.
"""
from functools import cmp_to_key
from pathlib import Path
import struct

from compat.numeric_type_review import Review


ENUM_LABELS = ["zeta", "", "NULL", "alpha"]
ROW_COUNT = 192


def fixture():
    rows = []
    floats = ["-Infinity", "-2.25", "0", "3.5", "Infinity", "NaN", None]
    reals = ["1.0000001", "1.0000002", "-1", "-Infinity", "Infinity", "NaN", None]
    numbers = ["9007199254740993", "9007199254740992", "-9007199254740993", "1", "-1", "0"]
    for i in range(ROW_COUNT):
        rows.append({
            "id": str(i), "grp": None if i % 17 == 0 else str(i % 7),
            "n": None if i % 19 == 0 else numbers[i % len(numbers)],
            "f": floats[i % len(floats)], "r": reals[i % len(reals)],
            "d": None if i % 11 == 0 else f"2024-02-{1 + i % 28:02d}",
            "payload": None if i % 17 == 0 else "" if i % 17 == 1 else "NULL" if i % 17 == 2
                else "row β " + str(i) + " " + "p" * 512,
            "label": None if i % 31 == 0 else ["x", "a", "a ", "Z", ""][i % 5],
            "nm": None if i % 29 == 0 else ["z", "aa", "A", "β", ""][i % 5],
            "e": None if i % 23 == 0 else ENUM_LABELS[i % 4],
        })
    return rows


def ordered(rows, keys):
    def value(row, column):
        if column == "expr":
            return int(row["id"]) % 13
        if column == "regtype":
            return 23 if int(row["id"]) % 2 == 0 else 25
        text = row[column]
        if text is None:
            return None
        if column in ("id", "grp", "n"):
            return int(text)
        if column in ("f", "r"):
            number = 0 if text == "NaN" else float(text)
            if column == "r" and text != "NaN":
                number = struct.unpack("!f", struct.pack("!f", number))[0]
            return text == "NaN", number
        if column == "e":
            return ENUM_LABELS.index(text)
        if column == "label":
            return text.rstrip(" ").encode()
        return text.encode()

    def compare(left, right):
        for column, ascending, first in keys:
            a, b = value(left, column), value(right, column)
            if (a is None) != (b is None):
                return -1 if (a is None) == first else 1
            if a is None:
                continue
            result = (a > b) - (a < b)
            if result:
                return result if ascending else -result
        return 0
    return sorted(rows, key=cmp_to_key(compare))


def cases():
    rows = fixture()
    result = []

    def query(sql, wanted=None, oids=None, state=None):
        result.append(("query", sql, wanted, oids, state))

    quote = lambda text: "NULL" if text is None else "'" + text.replace("'", "''") + "'"
    query("CREATE TYPE typed_sort_mood AS ENUM ('zeta','','NULL','alpha')")
    query('CREATE TABLE typed_sort_rows(id INTEGER,grp INTEGER,n NUMERIC(30,0),'
          'f DOUBLE PRECISION,r REAL,d DATE,payload TEXT COLLATE "C",'
          'label CHAR(4),nm TEXT,e typed_sort_mood)')
    columns = ("id", "grp", "n", "f", "r", "d", "payload", "label", "nm", "e")
    for begin in range(0, ROW_COUNT, 48):
        query("INSERT INTO typed_sort_rows VALUES " + ",".join(
            "(" + ",".join(quote(row[column]) for column in columns) + ")"
            for row in rows[begin:begin + 48]))
    multi = [("grp", False, True), ("n", True, False), ("id", True, False)]
    full = ordered(rows, multi)

    for suffix, budget, displayed in (("low", "64kB", "64kB"), ("high", "16MB", "16MB")):
        query("SET work_mem='" + budget + "'")
        query("SHOW work_mem", [[displayed]], [25])

        # The array target admits a real unbounded ordinary typed plan without
        # pretending that all legacy no-LIMIT multikey queries use this node.
        def scan(order, keys, limit=None, offset=0):
            wanted = ordered(rows, keys)
            if offset:
                wanted = wanted[offset:]
            if limit is not None:
                wanted = wanted[:limit]
            tail = " ORDER BY " + order
            if limit is not None:
                tail += " LIMIT " + str(limit)
            if offset:
                tail += " OFFSET " + str(offset)
            query("SELECT id,payload,ARRAY[id] AS ids FROM typed_sort_rows" + tail,
                  [[row["id"], row["payload"], "{" + row["id"] + "}"] for row in wanted], [23, 25, 1007])

        scan("grp DESC NULLS FIRST,n ASC NULLS LAST,id", multi)
        scan("id%13 DESC,id", [("expr", False, False), ("id", True, False)])
        scan("grp DESC NULLS FIRST,n ASC NULLS LAST,id", multi, 20, 31)
        scan("grp DESC NULLS FIRST,n ASC NULLS LAST,id", multi, 10000)
        scan("n DESC NULLS LAST,id", [("n", False, False), ("id", True, False)])
        scan("f ASC NULLS LAST,id", [("f", True, False), ("id", True, False)])
        scan("r ASC NULLS FIRST,id", [("r", True, True), ("id", True, False)])
        scan("d DESC NULLS FIRST,id", [("d", False, True), ("id", True, False)])
        scan('payload COLLATE "C" ASC NULLS LAST,id', [("payload", True, False), ("id", True, False)])
        scan('label COLLATE "C" ASC NULLS LAST,id', [("label", True, False), ("id", True, False)])
        # Physical NAME declaration is a separate observed 42704 gap. The
        # supported cast still creates an actual NAME ordering datum here.
        scan("nm::name ASC NULLS LAST,id", [("nm", True, False), ("id", True, False)])
        scan("e ASC NULLS LAST,id", [("e", True, False), ("id", True, False)])

        distinct = []
        seen = set()
        for row in ordered(rows, [("grp", True, False), ("n", True, True)]):
            key = row["grp"], row["n"]
            if key not in seen:
                seen.add(key)
                distinct.append(list(key))
        query("SELECT DISTINCT grp,n FROM typed_sort_rows ORDER BY grp ASC NULLS LAST,n ASC NULLS FIRST LIMIT 10000",
              distinct, [23, 1700])
        ties = sum(row["grp"] == "6" for row in rows)
        query("SELECT grp FROM typed_sort_rows ORDER BY grp DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
              [["6"]] * ties, [23])
        query("SELECT grp FROM typed_sort_rows ORDER BY grp DESC NULLS LAST OFFSET 2 ROWS FETCH FIRST 1 ROW WITH TIES",
              [["6"]] * (ties - 2), [23])
        query("SELECT f FROM typed_sort_rows ORDER BY f DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
              [["NaN"]] * sum(row["f"] == "NaN" for row in rows), [701])

        selected = full[9:32]
        query("SELECT l.id,l.payload,r.n FROM typed_sort_rows l JOIN typed_sort_rows r ON l.id=r.id "
              "ORDER BY l.grp DESC NULLS FIRST,r.n ASC NULLS LAST,l.id LIMIT 23 OFFSET 9",
              [[row["id"], row["payload"], row["n"]] for row in selected], [23, 25, 1700])
        selected = full[5:36]
        query("SELECT id,ROW(id,grp) AS rec FROM typed_sort_rows "
              "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id LIMIT 31 OFFSET 5",
              [[row["id"], "(" + row["id"] + "," + (row["grp"] or "") + ")"] for row in selected], [23, 2249])
        query("SELECT id,ARRAY[id::oid,NULL::oid,'4294967295'::oid] AS ids FROM typed_sort_rows "
              "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id LIMIT 31 OFFSET 5",
              [[row["id"], "{" + row["id"] + ",NULL,4294967295}"] for row in selected], [23, 1028])
        selected_types = ordered(rows, [("regtype", False, False), ("id", True, False)])[3:16]
        query("SELECT id,CASE WHEN id%2=0 THEN 'integer'::regtype ELSE 'text'::regtype END AS rt "
              "FROM typed_sort_rows ORDER BY rt DESC,id LIMIT 13 OFFSET 3",
              [[row["id"], "integer" if int(row["id"]) % 2 == 0 else "text"] for row in selected_types], [23, 2206])
        query("SELECT id,ROW(id,ARRAY[id::oid,NULL::oid]) AS rec FROM typed_sort_rows "
              "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id LIMIT 31 OFFSET 5",
              [[row["id"], "(" + row["id"] + ',"{' + row["id"] + ',NULL}")'] for row in selected], [23, 2249])

        wanted = [[row["id"], row["payload"]] for row in full[5:24]]
        parameter_sql = ("SELECT id,payload FROM typed_sort_rows WHERE id >= $1 "
                         "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id LIMIT 19 OFFSET 5")
        result.append(("parameter", parameter_sql, wanted, [23, 25], None))
        result.append(("binary", parameter_sql, wanted, [23, 25], None))

        keys, projection = "typed_sort_keys_" + suffix, "typed_sort_projection_" + suffix
        query("CREATE SEQUENCE " + keys)
        query("CREATE SEQUENCE " + projection)
        query("SELECT nextval('" + keys + "') AS k FROM typed_sort_rows ORDER BY k DESC LIMIT 3",
              [["192"], ["191"], ["190"]], [20])
        query("SELECT currval('" + keys + "')", [["192"]], [20])
        query("SELECT nextval('" + keys + "') AS k FROM typed_sort_rows ORDER BY k DESC LIMIT 0", [], [20])
        query("SELECT currval('" + keys + "')", [["192"]], [20])
        query("SELECT id,nextval('" + projection + "') FROM typed_sort_rows "
              "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id LIMIT 5 OFFSET 7",
              [[row["id"], str(i + 8)] for i, row in enumerate(full[7:12])], [23, 20])
        query("SELECT currval('" + projection + "')", [["12"]], [20])
        query("SELECT id FROM typed_sort_rows ORDER BY 1/(id-137),id LIMIT 1", state="22012")
        query("SELECT 1", [["1"]], [23])
        query("SELECT id FROM typed_sort_rows ORDER BY nextval('" + keys + "'),nextval('" + keys + "') DESC LIMIT 3",
              [["0"], ["1"], ["2"]], [23])
        query("SELECT currval('" + keys + "')", [["384"]], [20])

    # Preserve the original80 controls above. These additional ordinary SQL
    # cases use neither ARRAY nor scalar child queries nor a LIMIT trigger.
    for suffix, budget in (("low", "64kB"), ("high", "16MB")):
        query("SET work_mem='" + budget + "'")
        query("SHOW work_mem", [[budget]], [25])
        query("SELECT id,payload FROM typed_sort_rows ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id",
              [[row["id"], row["payload"]] for row in full], [23, 25])
        expression = ordered(rows, [("expr", False, False), ("id", True, False)])
        query("SELECT id,payload FROM typed_sort_rows ORDER BY id%13 DESC,id",
              [[row["id"], row["payload"]] for row in expression], [23, 25])
        numbers = ordered(rows, [("n", True, False), ("id", True, False)])
        query("SELECT n,id,payload FROM typed_sort_rows ORDER BY n ASC NULLS LAST,id",
              [[row["n"], row["id"], row["payload"]] for row in numbers], [1700, 23, 25])
        query("SELECT n FROM typed_sort_rows ORDER BY n DESC NULLS LAST",
              [[row["n"]] for row in ordered(rows, [("n", False, False)])], [1700])
        words = ordered(rows, [("payload", False, True), ("id", True, False)])
        query('SELECT id,payload FROM typed_sort_rows ORDER BY payload COLLATE "C" DESC NULLS FIRST,id',
              [[row["id"], row["payload"]] for row in words], [23, 25])
        # A supported single physical integer key should keep the existing
        # fast path, including output alias resolution and typed metadata.
        query("SELECT id AS value,payload FROM typed_sort_rows ORDER BY value DESC",
              [[row["id"], row["payload"]] for row in reversed(rows)], [23, 25])
        filtered = [row for row in full if 31 < int(row["id"]) < 170]
        query("SELECT id,payload FROM typed_sort_rows WHERE id>31 AND id<170 "
              "ORDER BY grp DESC NULLS FIRST,n ASC NULLS LAST,id",
              [[row["id"], row["payload"]] for row in filtered], [23, 25])
        query("SELECT id FROM typed_sort_rows ORDER BY 1/(id-137),id", state="22012")
        query("SELECT DISTINCT grp,n FROM typed_sort_rows ORDER BY grp ASC NULLS LAST,n ASC NULLS FIRST",
              distinct, [23, 1700])
        sequence = "typed_sort_plain_keys_" + suffix
        query("CREATE SEQUENCE " + sequence)
        query("SELECT nextval('" + sequence + "') AS sort_value FROM typed_sort_rows ORDER BY sort_value DESC",
              [[str(value)] for value in range(ROW_COUNT, 0, -1)], [20])
        query("SELECT currval('" + sequence + "')", [[str(ROW_COUNT)]], [20])
    return result


def binary(review, sql, wanted):
    typed = review.client.typed
    statement = b"typed_sort_binary_" + str(review.controls).encode()
    parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
    bind = b"\0" + statement + b"\0" + struct.pack("!HHHiiHH", 1, 1, 1, 4, 0, 1, 1)
    review.sock.sendall(typed(b"P", parse) + typed(b"B", bind) + typed(b"D", b"P\0") +
                        typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = review.client.read_until_ready(review.sock)
    actual = review.client.data_row_values(messages)
    fields = review.client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
    expected = [[struct.pack("!i", int(row[0])), None if row[1] is None else row[1].encode()] for row in wanted]
    commands = [body.rstrip(b"\0") for kind, body in messages if kind == b"C"]
    good = (actual == expected and not any(kind == b"E" for kind, _ in messages)
            and [field[3] for field in fields] == [23, 25] and [field[-1] for field in fields] == [1, 1]
            and commands == [b"SELECT 19"] and messages[-1] == (b"Z", b"I"))
    review.record("binary owning typed sort", good, (actual, fields, commands), expected)


def main():
    review = Review("TYPED_SORT_SPILL_PROTOCOL")
    try:
        controls = cases()
        assert len(controls) == 108, len(controls)
        for mode, sql, wanted, oids, state in controls:
            if mode == "parameter":
                review.parameter(sql, ["0"], [23], wanted, oids)
            elif mode == "binary":
                binary(review, sql, wanted)
            else:
                review.query(sql, wanted, oids, state, tag="SHOW" if sql == "SHOW work_mem" else None)
        if review.server:
            assert not list(Path(review.server["dir"]).glob(".dbms-sort-*"))
            for descriptor in (Path("/proc") / str(review.server["process"].pid) / "fd").iterdir():
                try:
                    assert ".dbms-sort-" not in str(descriptor.readlink())
                except FileNotFoundError:
                    pass
        review.complete(108)
    finally:
        review.close()


if __name__ == "__main__":
    main()
