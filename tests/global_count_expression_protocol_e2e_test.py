#!/usr/bin/env python3
"""Ordinary global UNION ALL count semantics and normal text/binary wire I/O.

Fixed PostgreSQL18.6 runtime oracle. Execute quotas are zero; formats are 0/1.
All parameter controls supply true input OIDs. Unknown inference, namespace and
FETCH grammar have independent controls. No retained planning/portal policy is
asserted here, and no existing regression source is changed by this entry.
"""
import json
import os
from pathlib import Path
import tempfile

from compat.numeric_type_review import Review
from values_count_expression_protocol_e2e_test import execute_parameter, normalize

LABEL = "GLOBAL_COUNT_EXPRESSION_PROTOCOL"
EXPECTED_CONTROLS = 75

def matrix():
    result = []
    def query(label, sql, rows=None, oids=None, state=None, tag=None):
        result.append(dict(mode="query", label=label, sql=sql, rows=rows, oids=oids, state=state, tag=tag))
    for name, values in (("runtime_left", "(6),(2),(4)"), ("runtime_right", "(5),(1),(3)"),
                         ("runtime_parent", "(0),(1),(2)")):
        query("create-" + name, "CREATE TABLE " + name + "(v INTEGER)", tag="CREATE TABLE")
        query("insert-" + name, "INSERT INTO " + name + " VALUES" + values, tag="INSERT 0 3")
    branches = [
        ("values", "(VALUES(6),(2),(4) UNION ALL VALUES(5),(1),(3)) ORDER BY column1"),
        ("inline-values", "VALUES(6),(2),(4) UNION ALL VALUES(5),(1),(3) ORDER BY column1"),
        ("physical-select", "SELECT v FROM runtime_left UNION ALL SELECT v FROM runtime_right ORDER BY v"),
        ("mixed", "SELECT v FROM runtime_left UNION ALL VALUES(5),(1),(3) ORDER BY v"),
        ("nested", "((VALUES(6),(2) UNION ALL VALUES(4)) UNION ALL (VALUES(5),(1) UNION ALL VALUES(3))) ORDER BY column1"),
        ("cte", "WITH q AS(VALUES(6),(2),(4)) SELECT column1 AS v FROM q UNION ALL SELECT v FROM runtime_right ORDER BY v"),
        ("derived", "SELECT v FROM(VALUES(6),(2),(4)) AS a(v) UNION ALL SELECT v FROM(VALUES(5),(1),(3)) AS b(v) ORDER BY v"),
    ]
    for label, sql in branches:
        query(label + "-arithmetic", sql + " LIMIT (1+1) OFFSET (1+0)", [["2"], ["3"]], [23])
        query(label + "-scalar", sql + " LIMIT (SELECT 2) OFFSET (SELECT 3)", [["4"], ["5"]], [23])
    base = branches[0][1]
    for label, tail, rows, state in [
        ("null-count", " LIMIT (NULL::bigint) OFFSET (NULL::bigint)", [[str(i)] for i in range(1, 7)], None),
        ("empty-scalar", " LIMIT (SELECT 1 WHERE false)", [[str(i)] for i in range(1, 7)], None),
        ("zero", " LIMIT (1-1) OFFSET (1+0)", [], None),
        ("negative-limit", " LIMIT (0-1)", None, "2201W"),
        ("negative-offset-zero", " LIMIT (1-1) OFFSET (0-1)", None, "2201X"),
        ("offset-before-limit-error", " LIMIT (0-1) OFFSET (0-1)", None, "2201X"),
        ("scalar-limit-cardinality", " LIMIT (VALUES(1),(2))", None, "21000"),
        ("scalar-offset-cardinality", " LIMIT (0-1) OFFSET (VALUES(1),(2))", None, "21000"),
        ("max-count", " LIMIT ('9223372036854775807'::bigint+0) OFFSET ('9223372036854775807'::bigint+0)", [], None),
        ("round-numeric", " LIMIT (1.5::numeric)", [["1"], ["2"]], None),
    ]:
        query(label, base + tail, rows, None if state else [23], state)
    for scalar in (False, True):
        count = "(SELECT p.v%2)" if scalar else "p.v%2"
        query("outer-count-" + str(scalar),
              "SELECT p.v,(VALUES(p.v+10) UNION ALL VALUES(p.v+20) ORDER BY column1 DESC LIMIT " + count +
              ") FROM runtime_parent p ORDER BY p.v", [["0", None], ["1", "21"], ["2", None]], [23, 23])
    query("outer-offset", "SELECT p.v,(VALUES(p.v+10) UNION ALL VALUES(p.v+20) ORDER BY column1 DESC "
          "LIMIT 1 OFFSET (p.v%2)) FROM runtime_parent p ORDER BY p.v", [["0", "20"], ["1", "11"], ["2", "22"]], [23, 23])
    for name in ("runtime_counts", "runtime_source", "runtime_fresh"):
        query("sequence-" + name, "CREATE SEQUENCE " + name, tag="CREATE SEQUENCE")
        query("seed-" + name, "SELECT nextval('" + name + "')", [["1"]], [20])
    count = "nextval('runtime_counts')"
    source = "nextval('runtime_source')"
    volatile = "(VALUES(" + source + "),(" + source + "),(" + source + ") UNION ALL VALUES(" + source + "),(" + source + "),(" + source + ")) ORDER BY column1"
    def counters(cap, cells):
        query("counter-count-" + str(cap), "SELECT currval('runtime_counts')", [[str(cap)]], [20])
        query("counter-source-" + str(cells), "SELECT currval('runtime_source')", [[str(cells)]], [20])
    query("zero-count-before-branches", volatile + " LIMIT (" + count + "-3) OFFSET " + count, [], [20]); counters(3, 1)
    query("offset-error-before-limit-and-branches", volatile + " LIMIT " + count + " OFFSET (" + count + "-5)", state="2201X"); counters(4, 1)
    query("counts-once-full-source", volatile + " LIMIT (" + count + "-5) OFFSET (" + count + "-5)", [["2"]], [20]); counters(6, 7)
    query("runtime-overflow-before-source", volatile + " LIMIT (" + count + "::numeric+9223372036854775807)", state="22003"); counters(7, 7)
    query("offset-child-cardinality-before-limit-child", volatile + " LIMIT (VALUES(" + count + "),(" + count + ")) OFFSET (VALUES(" + count + "),(" + count + "))", state="21000"); counters(9, 7)
    fresh = base + " LIMIT (SELECT nextval('runtime_fresh'))"
    query("fresh-scalar-first", fresh, [["1"], ["2"]], [23])
    query("fresh-scalar-second", fresh, [["1"], ["2"], ["3"]], [23])
    query("fresh-scalar-counter", "SELECT currval('runtime_fresh')", [["3"]], [20])
    def parameter(label, sql, inputs, input_oids, rows=None, binary=False, state=None):
        result.append(dict(mode="parameter", label=label, sql=sql, inputs=inputs, input_oids=input_oids,
                           rows=rows, oids=[23], binary=binary, state=state, parse_state=None, sequence=None))
    for binary in (False, True):
        for label, inputs, input_oids, rows, state in [
            ("int4-finite", ["2", "1"], [23, 23], [["2"], ["3"]], None),
            ("int8-finite", ["1", "2"], [20, 20], [["3"]], None),
            ("null-unlimited", [None, None], [20, 20], [[str(i)] for i in range(1, 7)], None),
            ("zero", ["0", "1"], [20, 20], [], None),
            ("offset-first", ["-1", "-1"], [20, 20], None, "2201X"),
            ("negative", ["-1", "0"], [20, 20], None, "2201W"),
            ("max-both", ["9223372036854775807"] * 2, [20, 20], [], None),
        ]:
            parameter("normal-" + str(binary) + "-" + label, base + " LIMIT $1 OFFSET $2", inputs, input_oids, rows, binary, state)
        parameter("normal-" + str(binary) + "-source-count-owners",
                  "SELECT $1::integer AS column1 UNION ALL VALUES(3),(1) ORDER BY column1 LIMIT $2 OFFSET $3",
                  ["9", "2", "1"], [23, 20, 20], [["3"], ["9"]], binary)
    result.append(dict(mode="parameter", label="pure-parse-describe-scalar-count",
                       sql=base + " LIMIT (SELECT nextval('runtime_fresh'))", inputs=[], input_oids=[],
                       rows=[[str(i)] for i in range(1, 5)], oids=[23], binary=False,
                       state=None, parse_state=None, sequence=("runtime_fresh", "3")))
    query("counter-after-pure-metadata-and-execute", "SELECT currval('runtime_fresh')", [["4"]], [20])
    return result

def main():
    review = Review(LABEL)
    observations = []
    try:
        cases = matrix()
        assert len(cases) == EXPECTED_CONTROLS, len(cases)
        for case in cases:
            if case["mode"] == "parameter":
                actual, valid = execute_parameter(review, case)
            else:
                messages = review.client.simple_query(review.sock, case["sql"])
                result = review.runner.decode_wire_result(messages, include_types=True)
                valid = result[1] == case["state"] and messages[-1] == (b"Z", b"I")
                if case["rows"] is not None:
                    valid = valid and result[0] == case["rows"] and result[4] == (case["tag"] or "SELECT " + str(len(case["rows"])))
                elif case["tag"]:
                    valid = valid and result[0] == [] and result[4] == case["tag"]
                if case["oids"] is not None:
                    valid = valid and result[5] == case["oids"]
                if case["state"]:
                    valid = valid and result[0] == [] and result[4] is None
                actual = dict(result=result, messages=messages)
                review.record(case["label"], valid, actual, case)
            observations.append(dict(control=review.controls, case=case, actual=actual, match=valid))
        review.complete(EXPECTED_CONTROLS)
    finally:
        configured = os.environ.get("GLOBAL_COUNT_EXPRESSION_OUTPUT_DIR")
        output = Path(configured) if configured else Path(tempfile.mkdtemp(prefix="dbms-global-count-"))
        output.mkdir(parents=True, exist_ok=True)
        destination = output / ("reference-observations.json" if "--reference18" in __import__("sys").argv else "observations.json")
        destination.write_text(json.dumps(normalize(observations), indent=2) + "\n")
        print(LABEL + "_OBSERVATIONS", str(destination), flush=True)
        review.close()

if __name__ == "__main__":
    main()
