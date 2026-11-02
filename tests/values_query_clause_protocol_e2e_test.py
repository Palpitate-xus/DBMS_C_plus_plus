#!/usr/bin/env python3
"""Ordinary VALUES clauses use their real output scope and typed row values.

The fixed oracle is PostgreSQL 18.6. SQL is parsed normally, including nested
VALUES and local UNION branches. Extended queries use valid text/binary formats
and unlimited Execute; this suite makes no retained-plan or portal phase claim.
"""
from pathlib import Path
import struct

from compat.numeric_type_review import Review


LABEL = "VALUES_QUERY_CLAUSE_PROTOCOL"
EXPECTED_CONTROLS = 143


def cases():
    result = []

    def query(sql, rows=None, oids=None, state=None, tag=None, headers=None):
        if headers is None and oids is not None and sql.startswith(("VALUES", "(VALUES")):
            headers = ["column" + str(i + 1) for i in range(len(oids))]
        result.append({"mode": "query", "sql": sql, "rows": rows, "oids": oids,
                       "state": state, "tag": tag, "headers": headers})

    def parameter(sql, inputs, rows, oids, binary=False):
        result.append({"mode": "binary" if binary else "parameter", "sql": sql,
                       "inputs": inputs, "input_oids": [23] * len(inputs),
                       "rows": rows, "oids": oids})

    query("CREATE TYPE values_clause_mood AS ENUM ('zeta','','NULL','alpha')", tag="CREATE TYPE")
    query("CREATE TABLE values_parent(column1 INTEGER,marker INTEGER)", tag="CREATE TABLE")
    query("INSERT INTO values_parent VALUES(5,50),(2,20),(9,90)", tag="INSERT 0 3")
    result.append({"mode": "enum_oid", "sql": "SELECT oid FROM pg_type WHERE typname='values_clause_mood'"})

    base = "VALUES(3),(1),(2)"
    ascending = [["1"], ["2"], ["3"]]
    descending = list(reversed(ascending))
    # Full payloads, exact NUMERICs above 2^53, empty strings and real NULLs
    # survive both an unbounded sort and offset-plus-limit selection.
    wide = [{"i": i, "n": 9007199254740992 + i % 7,
             "p": None if i % 13 == 0 else "" if i % 17 == 0
                  else "NULL" if i % 19 == 0 else "values β " + str(i) + " " + "v" * 768}
            for i in range(192)]
    quote = lambda text: "NULL" if text is None else "'" + text.replace("'", "''") + "'"
    wide_sql = "VALUES" + ",".join("(" + str(row["i"]) + "," + str(row["n"]) + "::numeric," +
                                    quote(row["p"]) + ")" for row in wide)
    ordered_wide = sorted(wide, key=lambda row: (-row["n"], row["i"]))
    wide_rows = [[str(row["i"]), str(row["n"]), row["p"]] for row in ordered_wide]

    for memory in ("64kB", "16MB"):
        query("SET work_mem='" + memory + "'", tag="SET")
        query("SHOW work_mem", [[memory]], [25], tag="SHOW", headers=["work_mem"])
        query(base + " ORDER BY 1", ascending, [23])
        query(base + " ORDER BY column1 DESC", descending, [23])
        query(base + " ORDER BY -column1", descending, [23])
        query(base + " ORDER BY column1+1 DESC", descending, [23])
        query(base + " ORDER BY column1%2 DESC,column1", [["1"], ["3"], ["2"]], [23])
        query("VALUES(2,'b'),(1,'c'),(1,'a'),(NULL,'z'),(1,NULL) "
              "ORDER BY column1 NULLS FIRST,column2 NULLS LAST",
              [[None, "z"], ["1", "a"], ["1", "c"], ["1", None], ["2", "b"]], [23, 25])
        query("VALUES(2,'b'),(1,'c'),(1,'a'),(NULL,'z'),(1,NULL) "
              "ORDER BY 1 NULLS LAST,2 DESC NULLS FIRST",
              [["1", None], ["1", "c"], ["1", "a"], ["2", "b"], [None, "z"]], [23, 25])
        query("VALUES(NULL),(NULL) ORDER BY column1", [[None], [None]], [25])
        query("VALUES(NULL::integer),(2),(1) ORDER BY column1 NULLS FIRST", [[None], ["1"], ["2"]], [23])
        query("VALUES(1),(2147483648),(NULL) ORDER BY column1 DESC NULLS FIRST",
              [[None], ["2147483648"], ["1"]], [20])
        query("VALUES(9007199254740993::numeric),(9007199254740992::numeric),"
              "(-9007199254740993::numeric),(NULL::numeric) ORDER BY column1",
              [["-9007199254740993"], ["9007199254740992"], ["9007199254740993"], [None]], [1700])
        query("VALUES('NaN'::float8),('-Infinity'::float8),('-1.5'::float8),(0::float8),"
              "('Infinity'::float8),(NULL::float8) ORDER BY column1 ASC NULLS LAST",
              [["-Infinity"], ["-1.5"], ["0"], ["Infinity"], ["NaN"], [None]], [701])
        query("VALUES('1.0000001'::real),('1.0000002'::real),('-Infinity'::real),"
              "('NaN'::real),(NULL::real) ORDER BY column1 DESC NULLS FIRST",
              [[None], ["NaN"], ["1.0000002"], ["1.0000001"], ["-Infinity"]], [700])
        query('VALUES(\'NULL\'),(\'\'),(NULL),(\'two words\'),(\'β\') '
              'ORDER BY column1 COLLATE "C" NULLS FIRST',
              [[None], [""], ["NULL"], ["two words"], ["β"]], [25])
        query("VALUES('alpha'::values_clause_mood),(''::values_clause_mood),"
              "('zeta'::values_clause_mood),('NULL'::values_clause_mood),(NULL::values_clause_mood) "
              "ORDER BY column1 NULLS LAST", [["zeta"], [""], ["NULL"], ["alpha"], [None]], ["enum"])
        query("VALUES('4294967295'::oid),(1::oid),(NULL::oid) ORDER BY column1 DESC NULLS LAST",
              [["4294967295"], ["1"], [None]], [26])
        query("VALUES('text'::regtype),('integer'::regtype),(NULL::regtype) ORDER BY column1",
              [["integer"], ["text"], [None]], [2206])
        query("VALUES(ARRAY[2::oid,NULL::oid,'4294967295'::oid]),"
              "(ARRAY[1::oid,NULL::oid,'4294967295'::oid]) ORDER BY column1 DESC",
              [["{2,NULL,4294967295}"], ["{1,NULL,4294967295}"]], [1028])
        query("VALUES(ROW(2,ARRAY[2::oid,NULL::oid])),(ROW(1,ARRAY[1::oid,NULL::oid])) ORDER BY column1",
              [['(1,"{1,NULL}")'], ['(2,"{2,NULL}")']], [2249])
        query("VALUES(ROW(2,NULL::integer),2::oid),(ROW(1,3),1::oid) ORDER BY column2",
              [["(1,3)", "1"], ["(2,)", "2"]], [2249, 26])
        query(base + " ORDER BY column1 LIMIT 1 OFFSET 1", [["2"]], [23])
        query(base + " ORDER BY column1 OFFSET 1 ROW FETCH NEXT 1 ROW ONLY", [["2"]], [23])
        query(base + " ORDER BY column1 LIMIT ALL", ascending, [23])
        query(base + " ORDER BY column1 LIMIT NULL OFFSET NULL", ascending, [23])
        query(base + " ORDER BY column1 FETCH FIRST NULL ROWS ONLY", ascending, [23])
        query(base + " ORDER BY column1 LIMIT +1", [["1"]], [23])
        query(base + " ORDER BY column1 LIMIT 0", [], [23])
        query(base + " ORDER BY column1 LIMIT -0", [], [23])
        query(base + " ORDER BY column1 OFFSET -0", ascending, [23])
        query(base + " ORDER BY column1 OFFSET 9", [], [23])
        query("VALUES(2),(1),(1),(3) ORDER BY column1 FETCH FIRST ROW WITH TIES", [["1"], ["1"]], [23])
        query("VALUES(2),(1),(1),(3) ORDER BY column1 OFFSET 1 ROW FETCH NEXT 1 ROW WITH TIES", [["1"]], [23])
        query("VALUES(2),(1),(1),(3) ORDER BY column1 FETCH FIRST 0 ROWS WITH TIES", [], [23])
        query("VALUES(1,2),(1,1),(1,1),(2,0) ORDER BY column1,column2 FETCH FIRST 1 ROW WITH TIES",
              [["1", "1"], ["1", "1"]], [23, 23])
        query(wide_sql + " ORDER BY column2 DESC,column1", wide_rows, [23, 1700, 25])
        query(wide_sql + " ORDER BY column2 DESC,column1 LIMIT 11 OFFSET 31", wide_rows[31:42], [23, 1700, 25])

    # Values cells see the real parent; ORDER expressions see the local output.
    query(base + " ORDER BY (SELECT -column1)", descending, [23])
    query(base + " ORDER BY (SELECT (SELECT -column1))", descending, [23])
    query(base + " ORDER BY (SELECT -column1 FROM(VALUES(0)) AS s(column1)),column1", ascending, [23])
    query(base + " ORDER BY (SELECT -s.column1 FROM(VALUES(0)) AS s(column1)),column1", ascending, [23])
    parent_rows = [["2", "22"], ["5", "25"], ["9", "29"]]
    query("SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC LIMIT 1) "
          "FROM values_parent p ORDER BY p.column1", parent_rows, [23, 23], headers=["column1", "column1"])
    query("SELECT p.column1,(VALUES(column1+10),(column1+20) ORDER BY column1 DESC LIMIT 1) "
          "FROM values_parent p ORDER BY p.column1", parent_rows, [23, 23], headers=["column1", "column1"])
    query("SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) "
          "ORDER BY (SELECT -column1+p.column1) LIMIT 1) FROM values_parent p ORDER BY p.column1",
          parent_rows, [23, 23], headers=["column1", "column1"])
    query("VALUES((SELECT column1 FROM values_parent ORDER BY column1 LIMIT 1)),(9) ORDER BY column1",
          [["2"], ["9"]], [23])
    query("WITH v(a,b) AS(VALUES(3,'c'),(1,'a'),(2,'b') ORDER BY column1 DESC LIMIT 2) "
          "SELECT a,b FROM v ORDER BY a", [["2", "b"], ["3", "c"]], [23, 25], headers=["a", "b"])
    query("SELECT a,b FROM(VALUES(3,'c'),(1,'a'),(2,'b') ORDER BY column1 DESC LIMIT 2) AS v(a,b) ORDER BY a",
          [["2", "b"], ["3", "c"]], [23, 25], headers=["a", "b"])
    query("VALUES(3),(1),(2) UNION ALL VALUES(4),(0) ORDER BY 1 DESC LIMIT 3", [["4"], ["3"], ["2"]], [23])
    query("(VALUES(3),(1),(2) ORDER BY 1 DESC LIMIT 2) UNION ALL VALUES(4),(0) ORDER BY 1 DESC",
          [["4"], ["3"], ["2"], ["0"]], [23])
    query("VALUES(3),(1),(2) UNION ALL(VALUES(4),(0),(5) ORDER BY 1 LIMIT 1) ORDER BY 1 DESC",
          [["3"], ["2"], ["1"], ["0"]], [23])
    query("(VALUES(3),(1),(2) ORDER BY 1 DESC LIMIT 2) UNION ALL"
          "(VALUES(4),(0),(5) ORDER BY 1 LIMIT 2) ORDER BY 1 DESC LIMIT 3 OFFSET 1",
          [["3"], ["2"], ["0"]], [23])

    for binary in (False, True):
        parameter("VALUES($1),(1),(5) ORDER BY $1-column1", ["9"], [["9"], ["5"], ["1"]], [23], binary)
        parameter("VALUES($1,$2),(5,$1),(1,$2) ORDER BY column2+$1-column1", ["9", "2"],
                  [["9", "2"], ["1", "2"], ["5", "9"]], [23, 23], binary)
        parameter("VALUES($1),(1),(5) ORDER BY (SELECT $1-column1)", ["9"],
                  [["9"], ["5"], ["1"]], [23], binary)
        parameter("VALUES(3),(1),(2) ORDER BY column1 LIMIT $1 OFFSET $2", ["1", "1"], [["2"]], [23], binary)

    sequence = "values_clause_calls"
    query("CREATE SEQUENCE " + sequence, tag="CREATE SEQUENCE")
    cells = "VALUES(nextval('" + sequence + "')),(nextval('" + sequence + "')),(nextval('" + sequence + "'))"
    def counter(number):
        query("SELECT currval('" + sequence + "')", [[str(number)]], [20], headers=["currval"])
    query(cells + " ORDER BY column1 DESC LIMIT 1", [["3"]], [20]); counter(3)
    query(base + " ORDER BY nextval('" + sequence + "')", [["3"], ["1"], ["2"]], [23]); counter(6)
    query(base + " ORDER BY nextval('" + sequence + "'),nextval('" + sequence + "') DESC",
          [["3"], ["1"], ["2"]], [23]); counter(9)
    query(base + " ORDER BY (SELECT nextval('" + sequence + "')+column1)", [["1"], ["3"], ["2"]], [23]); counter(12)
    query(base + " ORDER BY (SELECT nextval('" + sequence + "')),column1", ascending, [23]); counter(13)
    query(cells + " LIMIT 1", [["14"]], [20]); counter(14)
    query(cells + " ORDER BY nextval('" + sequence + "') LIMIT 0", [], [20]); counter(14)
    query(cells + " ORDER BY nextval('" + sequence + "') FETCH FIRST 0 ROWS WITH TIES", [], [20]); counter(14)
    query(base + " ORDER BY 1/(column1-1) LIMIT 0", [], [23])
    query(base + " ORDER BY 1/(column1-1) LIMIT 1", state="22012")
    # A global LIMIT must still evaluate discarded VALUES input cells.
    query("VALUES(1/(3-1)),(1/(1-1)),(1/(2-1)) ORDER BY column1 DESC LIMIT 1", state="22012")
    query("VALUES(9)", [["9"]], [23])

    for suffix, state in (("ORDER BY 0", "42P10"), ("ORDER BY -1", "42P10"), ("ORDER BY 2", "42P10"),
                          ("ORDER BY 1.5", "42601"), ("ORDER BY 'column1'", "42601"),
                          ("ORDER BY TRUE", "42601"), ("ORDER BY NULL", "42601"),
                          ("ORDER BY column2", "42703"), ("ORDER BY column1 LIMIT -1", "2201W"),
                          ("ORDER BY column1 OFFSET -1", "2201X"),
                          ("ORDER BY column1 FETCH FIRST -1 ROWS ONLY", "2201W"),
                          ("ORDER BY column1 FETCH FIRST -1 ROWS WITH TIES", "2201W"),
                          ("ORDER BY column1 FETCH FIRST NULL ROWS WITH TIES", "2201W"),
                          ("ORDER BY column1 LIMIT column1", "42P10"),
                          ("ORDER BY column1 OFFSET column1", "42P10"),
                          ("FETCH FIRST 1 ROW WITH TIES", "42601")):
        query(base + " " + suffix, state=state)
    query("VALUES(column1),(1) ORDER BY 1", state="42703")
    query("VALUES((SELECT column1)),(2) ORDER BY column1", state="42703")
    query("WITH v(a) AS(VALUES(3),(1),(2) ORDER BY a LIMIT 1) SELECT a FROM v", state="42703")
    query("VALUES(9) ORDER BY column1", [["9"]], [23])
    return result


def execute_parameter(review, case):
    """Valid formats and unlimited Execute, with two real descriptions."""
    binary = case["mode"] == "binary"
    name = b"values_clauses_" + str(review.controls).encode()
    typed = review.client.typed
    parse = name + b"\0" + case["sql"].encode() + b"\0" + struct.pack("!H", len(case["input_oids"]))
    parse += b"".join(struct.pack("!I", oid) for oid in case["input_oids"])
    bind = b"\0" + name + b"\0" + struct.pack("!H", 1 if binary else 0)
    if binary:
        bind += struct.pack("!H", 1)
    bind += struct.pack("!H", len(case["inputs"]))
    for text in case["inputs"]:
        raw = struct.pack("!i", int(text)) if binary else text.encode()
        bind += struct.pack("!i", len(raw)) + raw
    bind += struct.pack("!HH", 1, 1) if binary else struct.pack("!H", 0)
    review.sock.sendall(typed(b"P", parse) + typed(b"D", b"S" + name + b"\0") + typed(b"B", bind) +
                        typed(b"D", b"P\0") + typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = review.client.read_until_ready(review.sock)
    descriptions = [review.client.row_description_fields([message]) for message in messages if message[0] == b"T"]
    commands = [body.rstrip(b"\0") for kind, body in messages if kind == b"C"]
    actual_rows = review.client.data_row_values(messages)
    expected_rows = [[struct.pack("!i", int(value)) if binary else value.encode() for value in row] for row in case["rows"]]
    expected_headers = ["column" + str(i + 1) for i in range(len(case["oids"]))]
    good = (actual_rows == expected_rows and not any(kind == b"E" for kind, _ in messages)
            and len(descriptions) == 2 and messages[-1] == (b"Z", b"I")
            and commands == [("SELECT " + str(len(case["rows"]))).encode()])
    for index, fields in enumerate(descriptions):
        good = (good and [field[0] for field in fields] == [header.encode() for header in expected_headers]
                and [field[3] for field in fields] == case["oids"]
                and [field[-1] for field in fields] == [1 if binary and index == 1 else 0] * len(case["oids"])
                and all(field[1] == 0 and field[2] == 0 for field in fields))
    review.record((case["mode"], case["sql"], case["inputs"]), good,
                  (actual_rows, descriptions, commands, [kind.decode() for kind, _ in messages]),
                  (expected_rows, case["oids"], expected_headers))


def main():
    review = Review(LABEL)
    enum_oid = None
    try:
        matrix = cases()
        assert len(matrix) == EXPECTED_CONTROLS, len(matrix)
        for case in matrix:
            sql = case["sql"]
            if case["mode"] == "enum_oid":
                actual = review.runner.decode_wire_result(review.client.simple_query(review.sock, sql), include_types=True)
                try:
                    enum_oid = int(actual[0][0][0]) if len(actual[0]) == 1 and len(actual[0][0]) == 1 else None
                except (TypeError, ValueError):
                    enum_oid = None
                good = (enum_oid is not None and enum_oid > 0 and actual[1] is None and actual[4] == "SELECT 1" and actual[5] == [26])
                review.record(sql, good, actual, "one positive physical enum OID from actual pg_type, output26")
            elif case["mode"] in ("parameter", "binary"):
                execute_parameter(review, case)
            else:
                messages = review.client.simple_query(review.sock, sql)
                actual = review.runner.decode_wire_result(messages, include_types=True)
                oids = None if case["oids"] is None else [enum_oid if oid == "enum" else oid for oid in case["oids"]]
                good = actual[1] == case["state"] and messages[-1] == (b"Z", b"I")
                if case["rows"] is not None:
                    good = good and actual[0] == case["rows"] and actual[4] == (case["tag"] or "SELECT " + str(len(case["rows"])))
                elif case["tag"] is not None:
                    good = good and actual[0] == [] and actual[4] == case["tag"]
                if oids is not None:
                    good = good and actual[5] == oids
                if case["headers"] is not None:
                    good = good and actual[3] == case["headers"]
                if case["state"]:
                    good = good and actual[0] == [] and actual[4] is None
                review.record(sql, good, actual, (case["rows"], oids, case["state"], case["tag"], case["headers"]))
        if review.server:
            assert not list(Path(review.server["dir"]).glob(".dbms-sort-*"))
            for entry in (Path("/proc") / str(review.server["process"].pid) / "fd").iterdir():
                try:
                    assert ".dbms-sort-" not in str(entry.readlink())
                except FileNotFoundError:
                    pass
        review.complete(EXPECTED_CONTROLS)
    finally:
        review.close()


if __name__ == "__main__":
    main()
