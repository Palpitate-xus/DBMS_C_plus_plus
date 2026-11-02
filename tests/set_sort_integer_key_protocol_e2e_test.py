#!/usr/bin/env python3
"""Exact signed integer set sorting and ordinary comparison fallbacks.

Fixed PostgreSQL 18.6 oracle, actual SQL and normal Simple/Extended messages.
Every Execute is unlimited; declared parameter types and formats 0/1 only.
The native resource suite independently verifies actual low-budget spooling.
"""
from functools import cmp_to_key
import json
import os
from pathlib import Path
import struct
import sys
import tempfile

from compat.numeric_type_review import Review


LABEL = "SET_SORT_INTEGER_KEY_PROTOCOL"
EXPECTED_CONTROLS = 66
ROW_COUNT = 128
ENUM_LABELS = ["zeta", "", "NULL", "alpha"]
SMALL = [-32768, -32767, -1, 0, 1, 32766, 32767, None]
INTEGER = [-2147483648, -2147483647, -1, 0, 1, 2147483646, 2147483647, None]
BIGINT = [-9223372036854775808, -9223372036854775807, -9007199254740993,
          -9007199254740992, -1, 0, 1, 9007199254740992, 9007199254740993,
          9223372036854775806, 9223372036854775807, None]
OID = [0, 1, 2147483648, 4294967295, None]


def fixture():
    rows = []
    for witness in range(ROW_COUNT):
        payload = (None if witness % 23 == 0 else "" if witness % 23 == 1 else
                   "NULL" if witness % 23 == 2 else
                   "integer row β " + str(witness) + "\n" + "p" * 1536)
        rows.append(dict(witness=witness, s=SMALL[witness % len(SMALL)],
                         i=INTEGER[(witness * 3) % len(INTEGER)],
                         b=BIGINT[(witness * 5) % len(BIGINT)],
                         n=BIGINT[(witness * 7) % len(BIGINT)],
                         o=OID[witness % len(OID)], mood=ENUM_LABELS[witness % 4],
                         payload=payload))
    return rows


def ordered(rows, keys):
    def compare(left, right):
        for key, ascending, first in keys:
            a, b = key(left), key(right)
            if a is None or b is None:
                if (a is None) != (b is None):
                    return -1 if (a is None) == first else 1
                continue
            order = (a > b) - (a < b)
            if order:
                return order if ascending else -order
        return 0
    return sorted(rows, key=cmp_to_key(compare))


def sql_literal(value):
    return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"


def scalar(value):
    return None if value is None else str(value)


def cases():
    result = []
    rows = fixture()

    def query(label, sql, wanted=None, types=None, tag=None, headers=None):
        result.append(dict(mode="query", label=label, sql=sql, rows=wanted,
                           types=types, tag=tag, headers=headers))

    def set_sql(projection):
        return ("SELECT " + projection + " FROM integer_key_left UNION ALL SELECT " +
                projection + " FROM integer_key_right")

    def output(sequence, key):
        return [[scalar(key(row)), str(row["witness"]), row["payload"]] for row in sequence]

    def control(label, sql, sequence, key, oid, typmod=-1):
        query(label, sql, output(sequence, key), [(oid, 2 if oid == 21 else 4 if oid in (23, 26, "enum") else
              8 if oid == 20 else -1, typmod), (23, 4, -1), (25, -1, -1)],
              headers=["k", "witness", "payload"])

    query("enum declaration", "CREATE TYPE integer_key_mood AS ENUM ('zeta','','NULL','alpha')", tag="CREATE TYPE")
    query("domain declaration", "CREATE DOMAIN integer_key_domain AS BIGINT", tag="CREATE DOMAIN")
    for table, values in (("integer_key_left", rows[:64]), ("integer_key_right", rows[64:])):
        query("create " + table, "CREATE TABLE " + table +
              '(witness INTEGER,s SMALLINT,i INTEGER,b BIGINT,n NUMERIC(30,0),o BIGINT,'
              'mood integer_key_mood,d integer_key_domain,payload TEXT COLLATE "C")', tag="CREATE TABLE")
        for begin in (0, 32):
            batch = values[begin:begin + 32]
            query("insert " + table + " " + str(begin), "INSERT INTO " + table + " VALUES " + ",".join(
                "(" + ",".join(sql_literal(value) for value in
                    (row["witness"], row["s"], row["i"], row["b"], row["n"], row["o"],
                     row["mood"], row["b"], row["payload"])) + ")" for row in batch), tag="INSERT 0 32")
    result.append(dict(mode="enum_oid", label="actual enum catalog identity",
                       sql="SELECT oid FROM pg_type WHERE typname='integer_key_mood'"))
    query("setup row count", "SELECT count(*) FROM integer_key_left", [["64"]], [(20, 8, -1)],
          headers=["count"])

    for budget in ("64kB", "16MB"):
        query("budget " + budget, "SET work_mem='" + budget + "'", tag="SET")
        query("effective budget " + budget, "SHOW work_mem", [[budget]], [(25, -1, -1)],
              tag="SHOW", headers=["work_mem"])
        for ascending, first in ((True, False), (True, True), (False, False), (False, True)):
            suffix = (" ASC" if ascending else " DESC") + (" NULLS FIRST" if first else " NULLS LAST")
            sequence = ordered(rows, [(lambda row: row["b"], ascending, first),
                                      (lambda row: row["witness"], True, False)])
            control("bigint extremes " + budget + suffix,
                    set_sql("b AS k,witness,payload") + " ORDER BY k" + suffix + ",witness", sequence,
                    lambda row: row["b"], 20)
        for column, oid in (("s", 21), ("i", 23)):
            for ascending in (True, False):
                sequence = ordered(rows, [(lambda row, name=column: row[name], ascending, False),
                                          (lambda row: row["witness"], True, False)])
                control(column + " endpoints " + budget + str(ascending),
                        set_sql(column + " AS k,witness,payload") +
                        (" ORDER BY k ASC NULLS LAST,witness" if ascending else
                         " ORDER BY k DESC NULLS LAST,witness"), sequence, lambda row, name=column: row[name], oid)
        ascending = ordered(rows, [(lambda row: row["b"], True, False),
                                  (lambda row: row["witness"], True, False)])
        control("static OFFSET LIMIT " + budget, set_sql("b AS k,witness,payload") +
                " ORDER BY k ASC NULLS LAST,witness LIMIT 17 OFFSET 19", ascending[19:36], lambda row: row["b"], 20)
        control("zero LIMIT " + budget, set_sql("b AS k,witness,payload") +
                " ORDER BY k DESC NULLS FIRST,witness LIMIT 0", [], lambda row: row["b"], 20)
        first_peer = min(row["b"] for row in rows if row["b"] is not None)
        query("integer peers WITH TIES " + budget,
              set_sql("b AS k") + " ORDER BY k ASC NULLS LAST OFFSET 2 ROWS FETCH FIRST 1 ROW WITH TIES",
              [[str(first_peer)]] * (sum(row["b"] == first_peer for row in rows) - 2), [(20, 8, -1)], headers=["k"])
        sequence = ordered(rows, [(lambda row: row["s"], False, True),
                                  (lambda row: row["b"], True, False),
                                  (lambda row: row["witness"], True, False)])
        query("mixed integer key directions " + budget, set_sql("s AS k,b,witness,payload") +
              " ORDER BY k DESC NULLS FIRST,b ASC NULLS LAST,witness",
              [[scalar(row["s"]), scalar(row["b"]), str(row["witness"]), row["payload"]] for row in sequence],
              [(21, 2, -1), (20, 8, -1), (23, 4, -1), (25, -1, -1)], headers=["k", "b", "witness", "payload"])
        for left_column, right_column, oid in (("s", "i", 23), ("i", "b", 20)):
            promoted = [dict(row, promoted=row[left_column] if row["witness"] < 64 else row[right_column]) for row in rows]
            sequence = ordered(promoted, [(lambda row: row["promoted"], True, False),
                                         (lambda row: row["witness"], True, False)])
            sql = ("SELECT " + left_column + " AS k,witness,payload FROM integer_key_left UNION ALL SELECT " +
                   right_column + ",witness,payload FROM integer_key_right ORDER BY k NULLS LAST,witness")
            control("actual common promotion " + left_column + right_column + budget, sql, sequence,
                    lambda row: row["promoted"], oid)
        values = "VALUES" + ",".join("(" + ",".join(sql_literal(value) + "::" + kind for value, kind in
                   ((row["b"], "bigint"), (row["witness"], "integer"), (row["payload"], "text"))) + ")" for row in rows)
        query("owning hidden integer expression " + budget, values +
              " ORDER BY column1+0 DESC NULLS FIRST,column2", output(ordered(rows,
                  [(lambda row: row["b"], False, True), (lambda row: row["witness"], True, False)]), lambda row: row["b"]),
              [(20, 8, -1), (23, 4, -1), (25, -1, -1)], headers=["column1", "column2", "column3"])
        query("hidden key and explicit payload association " + budget, values +
              " ORDER BY column2%5,column1 DESC NULLS LAST,column2", output(ordered(rows,
                  [(lambda row: row["witness"] % 5, True, False), (lambda row: row["b"], False, False),
                   (lambda row: row["witness"], True, False)]), lambda row: row["b"]),
              [(20, 8, -1), (23, 4, -1), (25, -1, -1)], headers=["column1", "column2", "column3"])
        for column, oid, key in (("mood", "enum", lambda row: ENUM_LABELS.index(row["mood"])),
                                 ("o", 26, lambda row: row["o"]), ("d", 20, lambda row: row["b"]),
                                 ("n", 1700, lambda row: row["n"]),
                                 ("payload", 25, lambda row: None if row["payload"] is None else row["payload"].encode())):
            sequence = ordered(rows, [(key, True, False), (lambda row: row["witness"], True, False)])
            value = lambda row, name=column: row["b"] if name == "d" else row[name]
            control("fallback " + column + " " + budget, set_sql(("o::oid" if column == "o" else column) + " AS k,witness,payload") +
                    " ORDER BY k NULLS LAST,witness", sequence, value, oid, 1966084 if column == "n" else -1)
        sequence = ordered(rows, [(lambda row: row["b"], True, False),
                                  (lambda row: row["witness"], True, False)])
        control("owning record comparison fallback " + budget,
                set_sql("ROW(b,witness) AS k,witness,payload") + " ORDER BY k,witness", sequence,
                lambda row: "(" + ("" if row["b"] is None else str(row["b"])) + "," + str(row["witness"]) + ")", 2249)
        for binary in (False, True):
            for promoted in (False, True):
                inputs = ["-32768", None, "99", "wire β"] if promoted else [str(BIGINT[0]), str(BIGINT[-2]), "99", "wire β"]
                first, second = inputs[:2]
                expected = [[first, "99", "wire β"], [second, "2", ""], [None, "3", None]]
                if promoted:
                    expected = [[first, "99", "wire β"], [None, "2", ""], [None, "3", None]]
                sql = ("SELECT $1 AS k,$3 AS witness,$4 AS payload UNION ALL SELECT $2,2,'' "
                       "UNION ALL SELECT NULL,3,NULL ORDER BY k ASC NULLS LAST,witness LIMIT 3")
                result.append(dict(mode="extended", label=("binary" if binary else "text") +
                                   " actual input/common output " + str(promoted) + budget, sql=sql, rows=expected,
                                   input_oids=[21, 23, 23, 25] if promoted else [20, 20, 23, 25], inputs=inputs,
                                   types=[(23 if promoted else 20, 4 if promoted else 8, -1), (23, 4, -1), (25, -1, -1)],
                                   headers=["k", "witness", "payload"], binary=binary))
    return result


def descriptor(fields, case, enum_oid, binary=False):
    expected = [(header.encode(), 0, 0, enum_oid if oid == "enum" else oid, size, modifier, int(binary))
                for header, (oid, size, modifier) in zip(case["headers"], case["types"])]
    return fields == expected


def encoded(value, oid, binary):
    if value is None:
        return None
    if not binary or oid == 25:
        return value.encode()
    return struct.pack({21: "!h", 23: "!i", 20: "!q"}[oid], int(value))


def extended(review, case, enum_oid):
    typed = review.client.typed
    name = b"set_integer_" + str(review.controls).encode()
    parse = name + b"\0" + case["sql"].encode() + b"\0" + struct.pack("!H", len(case["input_oids"]))
    parse += b"".join(struct.pack("!I", oid) for oid in case["input_oids"])
    review.sock.sendall(typed(b"P", parse) + typed(b"D", b"S" + name + b"\0") + typed(b"S"))
    initial = review.client.read_until_ready(review.sock)
    parameters = []
    for kind, body in initial:
        if kind == b"t":
            count = struct.unpack("!H", body[:2])[0]
            parameters.append([struct.unpack("!I", body[2 + 4*i:6 + 4*i])[0] for i in range(count)])
    descriptions = [review.client.row_description_fields([message]) for message in initial if message[0] == b"T"]
    good = (not any(kind in (b"E", b"D", b"C") for kind, _ in initial) and
            sum(kind == b"1" for kind, _ in initial) == 1 and parameters == [case["input_oids"]] and
            len(descriptions) == 1 and descriptor(descriptions[0], case, enum_oid) and initial[-1] == (b"Z", b"I"))
    actual = dict(parse_describe=initial, parameter_oids=parameters)
    if not any(kind == b"E" for kind, _ in initial):
        binary = case["binary"]
        bind = b"\0" + name + b"\0" + struct.pack("!H", int(binary))
        if binary:
            bind += struct.pack("!H", 1)
        bind += struct.pack("!H", len(case["inputs"]))
        for value, oid in zip(case["inputs"], case["input_oids"]):
            raw = encoded(value, oid, binary)
            bind += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        bind += struct.pack("!HH", 1, 1) if binary else struct.pack("!H", 0)
        review.sock.sendall(typed(b"B", bind) + typed(b"D", b"P\0") +
                            typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
        messages = review.client.read_until_ready(review.sock)
        fields = [review.client.row_description_fields([message]) for message in messages if message[0] == b"T"]
        rows = review.client.data_row_values(messages)
        wanted = [[encoded(value, oid, binary) for value, (oid, _, _) in zip(row, case["types"])] for row in case["rows"]]
        commands = [body.rstrip(b"\0") for kind, body in messages if kind == b"C"]
        good = (good and not any(kind == b"E" for kind, _ in messages) and
                sum(kind == b"2" for kind, _ in messages) == 1 and len(fields) == 1 and
                descriptor(fields[0], case, enum_oid, binary) and rows == wanted and
                commands == [b"SELECT 3"] and messages[-1] == (b"Z", b"I"))
        actual["bind_execute"] = dict(messages=messages, fields=fields, rows=rows, wanted=wanted)
    return actual, good


def normalize(value):
    if isinstance(value, bytes):
        return {"hex": value.hex()}
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [normalize(item) for item in value]
    return value


def main():
    review = Review(LABEL)
    observations, enum_oid = [], None
    try:
        matrix = cases()
        assert len(matrix) == EXPECTED_CONTROLS, len(matrix)
        for case in matrix:
            if case["mode"] == "extended":
                actual, good = extended(review, case, enum_oid)
            else:
                messages = review.client.simple_query(review.sock, case["sql"])
                result = review.runner.decode_wire_result(messages, include_types=True)
                actual = dict(result=result, messages=messages)
                good = result[1] is None and messages[-1] == (b"Z", b"I")
                if case["mode"] == "enum_oid":
                    try:
                        enum_oid = int(result[0][0][0]) if len(result[0]) == 1 and len(result[0][0]) == 1 else None
                    except (TypeError, ValueError):
                        enum_oid = None
                    good = good and enum_oid is not None and enum_oid > 0 and result[5] == [26] and result[4] == "SELECT 1"
                else:
                    wanted = case["rows"]
                    good = good and result[0] == (wanted or []) and result[4] == (case["tag"] or "SELECT " + str(len(wanted)))
                    if case["types"] is not None:
                        fields = review.client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
                        if case["headers"] is None:
                            good = False
                        else:
                            good = good and descriptor(fields, case, enum_oid)
            review.record(case["label"], good, actual, case)
            observations.append(dict(control=review.controls, case=case, actual=actual, match=good))
        review.complete(EXPECTED_CONTROLS)
    finally:
        output = os.environ.get("SET_SORT_INTEGER_KEY_OUTPUT_DIR")
        output = Path(output) if output else Path(tempfile.mkdtemp(prefix="dbms-set-integer-"))
        output.mkdir(parents=True, exist_ok=True)
        destination = output / ("reference-observations.json" if "--reference18" in sys.argv else "observations.json")
        destination.write_text(json.dumps(normalize(observations), indent=2) + "\n")
        print(LABEL + "_OBSERVATIONS", str(destination), flush=True)
        review.close()


if __name__ == "__main__":
    main()
