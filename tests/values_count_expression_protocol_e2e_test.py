#!/usr/bin/env python3
"""Ordinary VALUES count expressions, exact supplied OIDs and normal wire I/O.

The oracle is PostgreSQL 18.6. Every Execute is unlimited and every format is
0 or 1. Unknown-OID inference remains separately observed as partial; these
parameter controls declare their actual input types. No retained-plan, invalid
format, portal-quota or constant-planning policy is changed by this fixture.
"""
import json
import os
from pathlib import Path
import struct
import tempfile

from compat.numeric_type_review import Review

LABEL = "VALUES_COUNT_EXPRESSION_PROTOCOL"
EXPECTED_CONTROLS = 59


def cases():
    result = []
    def query(sql, rows=None, oids=None, state=None, tag=None, headers=None):
        if headers is None and oids is not None and sql.startswith("VALUES"):
            headers = ["column" + str(i + 1) for i in range(len(oids))]
        result.append(dict(mode="query", sql=sql, rows=rows, oids=oids,
                           state=state, tag=tag, headers=headers))
    def parameter(sql, inputs, input_oids, rows=None, output_oids=(23,), binary=False,
                  state=None, parse_state=None, sequence=None):
        result.append(dict(mode="parameter", sql=sql, inputs=inputs, input_oids=input_oids,
                           rows=rows, oids=list(output_oids), binary=binary, state=state,
                           parse_state=parse_state, sequence=sequence))
    query("CREATE TABLE count_wire_parent(column1 INTEGER)", tag="CREATE TABLE")
    query("INSERT INTO count_wire_parent VALUES(2),(5),(9)", tag="INSERT 0 3")
    base = "VALUES(3),(1),(2) ORDER BY column1"
    all_rows = [["1"], ["2"], ["3"]]
    query(base + " LIMIT (1+1) OFFSET (1-1)", all_rows[:2], [23])
    query(base + " LIMIT (SELECT 2) OFFSET (SELECT 1)", all_rows[1:], [23])
    query(base + " LIMIT CASE WHEN true THEN 1 ELSE 2 END", all_rows[:1], [23])
    query(base + " LIMIT (1.5::numeric)", all_rows[:2], [23])
    query(base + " LIMIT (NULL::bigint) OFFSET (NULL::bigint)", all_rows, [23])
    query(base + " LIMIT (SELECT 1 WHERE false)", all_rows, [23])
    query(base + " LIMIT (VALUES(1))", all_rows[:1], [23])
    query(base + " LIMIT (SELECT column1 FROM(VALUES(1)) AS s(column1))", all_rows[:1], [23])
    query(base + " LIMIT (SELECT s.column1 FROM(VALUES(1)) AS s(column1))", all_rows[:1], [23])
    maximum = "9223372036854775807"
    query(base + " LIMIT ('" + maximum + "'::bigint+0) OFFSET ('" + maximum + "'::bigint+0)", [], [23])
    for tail, state in ((" LIMIT (0-1)", "2201W"), (" OFFSET (0-1) LIMIT (1-1)", "2201X"),
                        (" OFFSET (0-1) LIMIT (0-1)", "2201X"), (" LIMIT column1+0", "42P10"),
                        (" LIMIT (SELECT column1)", "42P10"),
                        (" LIMIT (SELECT (SELECT column1))", "42P10"),
                        (" LIMIT (SELECT column1 FROM(VALUES(1)) AS s(x))", "42P10"),
                        (" LIMIT CASE WHEN false THEN column1 ELSE 1 END", "42P10")):
        query(base + tail, state=state)
    query("VALUES(2,0),(1,1),(1,1),(1,2),(3,0) ORDER BY column1+0,column2 "
          "OFFSET (SELECT 1) ROW FETCH NEXT 1 ROW WITH TIES", [["1", "1"]], [23, 23])
    query("VALUES(2),(1),(1),(3) ORDER BY column1 OFFSET (NULL::bigint) ROW "
          "FETCH FIRST 1 ROW WITH TIES", [["1"], ["1"]], [23])
    for count in ("p.column1%2", "(SELECT p.column1%2)"):
        query("SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC LIMIT " + count +
              ") FROM count_wire_parent p ORDER BY p.column1", [["2", None], ["5", "25"], ["9", "29"]],
              [23, 23], headers=["column1", "column1"])
    query("SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC "
          "LIMIT 1 OFFSET (p.column1%2)) FROM count_wire_parent p ORDER BY p.column1",
          [["2", "22"], ["5", "15"], ["9", "19"]], [23, 23], headers=["column1", "column1"])

    parameter_sql = base + " LIMIT $1 OFFSET $2"
    for binary in (False, True):
        parameter(parameter_sql, ["1", "1"], [23, 23], [["2"]], binary=binary)
        parameter(parameter_sql, ["2", "1"], [20, 20], [["2"], ["3"]], binary=binary)
        parameter(parameter_sql, [None, None], [20, 20], all_rows, binary=binary)
        parameter(parameter_sql, ["-1", "-1"], [20, 20], state="2201X", binary=binary)
        parameter(parameter_sql, ["-1", "0"], [20, 20], state="2201W", binary=binary)
        parameter(parameter_sql, [maximum, maximum], [20, 20], [], binary=binary)
        parameter("VALUES($1),($2),(3) ORDER BY column1 LIMIT $3 OFFSET $4", ["9", "1", "2", "1"],
                  [23, 23, 23, 23], [["3"], ["9"]], binary=binary)
    # Scalar cardinality belongs to execution; local row variables fail during
    # actual analysis. These use fresh statements and ordinary unlimited I/O.
    parameter(base + " LIMIT (VALUES(1),(2))", [], [], state="21000")
    parameter(base + " LIMIT (SELECT column1)", [], [], parse_state="42P10")

    count, source = "count_wire_calls", "count_wire_source_calls"
    for sequence in (count, source):
        query("CREATE SEQUENCE " + sequence, tag="CREATE SEQUENCE")
        query("SELECT nextval('" + sequence + "')", [["1"]], [20], headers=["nextval"])
    cells = ("VALUES(nextval('" + source + "')),(nextval('" + source + "')),(nextval('" + source +
             "')) ORDER BY column1")
    def counter(sequence, number):
        query("SELECT currval('" + sequence + "')", [[str(number)]], [20], headers=["currval"])
    query(cells + " LIMIT (nextval('" + count + "')-3) OFFSET nextval('" + count + "')", [], [20])
    counter(count, 3); counter(source, 1)
    query(cells + " LIMIT nextval('" + count + "') OFFSET (nextval('" + count + "')-5)", state="2201X")
    counter(count, 4); counter(source, 1)
    query(cells + " LIMIT (nextval('" + count + "')-5) OFFSET (nextval('" + count + "')-5)", [["2"]], [20])
    counter(count, 6); counter(source, 4)
    query(cells + " LIMIT (nextval('" + count + "')::numeric+9223372036854775807)", state="22003")
    counter(count, 7); counter(source, 4)
    call = "nextval('" + count + "')"
    parameter(base + " LIMIT (VALUES(" + call + "),(" + call + ")) OFFSET (VALUES(" + call + "),(" + call + "))",
              [], [], state="21000", sequence=(count, "7"))
    counter(count, 9)
    return result


def execute_parameter(review, case):
    typed = review.client.typed
    name = b"values_count_" + str(review.controls).encode()
    parse = name + b"\0" + case["sql"].encode() + b"\0" + struct.pack("!H", len(case["input_oids"]))
    parse += b"".join(struct.pack("!I", oid) for oid in case["input_oids"])
    review.sock.sendall(typed(b"P", parse) + typed(b"D", b"S" + name + b"\0") + typed(b"S"))
    initial = review.client.read_until_ready(review.sock)
    initial_result = review.runner.decode_wire_result(initial, include_types=True)
    initial_fields = [review.client.row_description_fields([m]) for m in initial if m[0] == b"T"]
    parameters = []
    for kind, body in initial:
        if kind == b"t":
            size = struct.unpack("!H", body[:2])[0]
            parameters.append([struct.unpack("!I", body[2 + 4*i:6 + 4*i])[0] for i in range(size)])
    good = initial_result[1] == case["parse_state"] and initial[-1] == (b"Z", b"I")
    good = good and not any(kind in (b"D", b"C") for kind, _ in initial)
    expected_headers = ["column" + str(i + 1) for i in range(len(case["oids"]))]
    def descriptor(fields, binary):
        return ([(f[0], f[1], f[2], f[3], f[4], f[5], f[6]) for f in fields] ==
                [(header.encode(), 0, 0, oid, 8 if oid == 20 else 4, -1, int(binary))
                 for header, oid in zip(expected_headers, case["oids"])])
    if case["parse_state"]:
        good = good and not any(kind in (b"1", b"t", b"T") for kind, _ in initial)
    else:
        good = (good and sum(kind == b"1" for kind, _ in initial) == 1 and parameters == [case["input_oids"]]
                and len(initial_fields) == 1 and descriptor(initial_fields[0], False))
    actual = {"parse_describe": initial_result, "parse_messages": initial, "parameter_oids": parameters}
    if not initial_result[1]:
        if case["sequence"]:
            sequence, expected = case["sequence"]
            observed = review.runner.decode_wire_result(review.client.simple_query(review.sock,
                "SELECT currval('" + sequence + "')"), include_types=True)
            actual["counter_after_pure_parse_describe"] = observed
            good = good and observed[1] is None and observed[0] == [[expected]] and observed[5] == [20]
        binary = case["binary"]
        bind = b"\0" + name + b"\0" + struct.pack("!H", 1 if binary else 0)
        if binary:
            bind += struct.pack("!H", 1)
        bind += struct.pack("!H", len(case["inputs"]))
        for value, oid in zip(case["inputs"], case["input_oids"]):
            raw = None if value is None else struct.pack("!q" if oid == 20 else "!i", int(value)) if binary else value.encode()
            bind += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        bind += struct.pack("!HH", 1, 1) if binary else struct.pack("!H", 0)
        review.sock.sendall(typed(b"B", bind) + typed(b"D", b"P\0") +
                            typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
        messages = review.client.read_until_ready(review.sock)
        fields = [review.client.row_description_fields([m]) for m in messages if m[0] == b"T"]
        errors = [review.client.diagnostic_fields(body).get(b"C", b"").decode() for kind, body in messages if kind == b"E"]
        rows = review.client.data_row_values(messages)
        commands = [body.rstrip(b"\0") for kind, body in messages if kind == b"C"]
        wanted = [] if case["rows"] is None else [[None if value is None else
            struct.pack("!q" if oid == 20 else "!i", int(value)) if binary else value.encode()
            for value, oid in zip(row, case["oids"])] for row in case["rows"]]
        good = (good and sum(kind == b"2" for kind, _ in messages) == 1 and
                len(fields) == 1 and descriptor(fields[0], binary) and rows == wanted and
                errors == ([case["state"]] if case["state"] else []) and messages[-1] == (b"Z", b"I") and
                commands == ([] if case["state"] else [("SELECT " + str(len(wanted))).encode()]))
        actual["bind_execute"] = dict(rows=rows, descriptions=fields, errors=errors, commands=commands, messages=messages)
    review.record(("binary" if case["binary"] else "text", case["sql"], case["inputs"], case["input_oids"]),
                  good, actual, case)
    return actual, good


def normalize(value):
    if isinstance(value, bytes):
        return {"hex": value.hex()}
    if isinstance(value, dict):
        return {(key.hex() if isinstance(key, bytes) else key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def main():
    review = Review(LABEL)
    observations = []
    try:
        matrix = cases()
        assert len(matrix) == EXPECTED_CONTROLS, len(matrix)
        for case in matrix:
            if case["mode"] == "parameter":
                actual, valid = execute_parameter(review, case)
            else:
                messages = review.client.simple_query(review.sock, case["sql"])
                actual = review.runner.decode_wire_result(messages, include_types=True)
                valid = actual[1] == case["state"] and messages[-1] == (b"Z", b"I")
                if case["rows"] is not None:
                    valid = valid and actual[0] == case["rows"] and actual[4] == (case["tag"] or "SELECT " + str(len(case["rows"])))
                elif case["tag"]:
                    valid = valid and actual[0] == [] and actual[4] == case["tag"]
                if case["oids"] is not None:
                    valid = valid and actual[5] == case["oids"]
                if case["headers"] is not None:
                    valid = valid and actual[3] == case["headers"]
                if case["state"]:
                    valid = valid and actual[0] == [] and actual[4] is None
                review.record(case["sql"], valid, actual,
                              (case["rows"], case["oids"], case["state"], case["tag"], case["headers"]))
                actual = dict(result=actual, messages=messages)
            observations.append(dict(control=review.controls, case=case, actual=actual, match=valid))
        review.complete(EXPECTED_CONTROLS)
    finally:
        output = os.environ.get("VALUES_COUNT_EXPRESSION_OUTPUT_DIR")
        output = Path(output) if output else Path(tempfile.mkdtemp(prefix="dbms-values-count-"))
        output.mkdir(parents=True, exist_ok=True)
        destination = output / ("reference-observations.json" if "--reference18" in __import__("sys").argv else "observations.json")
        destination.write_text(json.dumps(normalize(observations), indent=2) + "\n")
        print(LABEL + "_OBSERVATIONS", str(destination), flush=True)
        review.close()


if __name__ == "__main__":
    main()
