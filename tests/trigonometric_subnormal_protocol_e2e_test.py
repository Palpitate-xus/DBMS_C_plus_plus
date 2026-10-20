#!/usr/bin/env python3
"""Preserve representable float8 inputs; compare values and signed-zero bits.

The same complete fixture must pass on PostgreSQL 18.6. Python's independent
libm functions are the numerical oracle; no expected value comes from the DBMS.
Range/error and general math semantics remain covered by the existing fixtures.
"""
from angle_conversion_range_protocol_e2e_test import Wire, literal
from compat.numeric_type_review import Review
import math
import sys

FUNCTIONS = ("sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh",
             "tanh", "asinh", "atanh", "cbrt")
INPUTS = ("0", "-0", "1e-300", "-1e-300", "1e-310", "-1e-310",
          "5e-324", "-5e-324", "NaN", None)
PAIRS = (("5e-324", "5e-324"), ("-5e-324", "5e-324"),
         ("5e-324", "-5e-324"), ("-5e-324", "-5e-324"),
         ("5e-324", "1"), ("1", "5e-324"), ("1e-310", "-1e-310"),
         ("-0", "1"), ("-0", "-0"), ("NaN", "5e-324"), (None, "1"), ("1", None))
MODES = ("simple", "extended", "binary")


def expected(fn, value):
    return None if value is None else getattr(math, fn)(float(value))


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("TRIG_SUBNORMAL_PROTOCOL")
    wire = Wire(review)
    try:
        for fn in FUNCTIONS:
            for value in INPUTS:
                for mode in MODES:
                    wire.query(mode, f"SELECT {fn}({literal(value)}) AS v", [expected(fn, value)])
            statement = ("trig_" + fn).encode()
            wire.describe(fn, f"SELECT {fn}($1) AS v", statement, parameter=True)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, "5e-324", binary_input, binary_output)
                    for value in ("5e-324", "-1e-310", "NaN", None):
                        wire.reexecute(statement, value, expected(fn, value), None,
                                       binary_input, binary_output, False)
            wire.close(statement)
        for a, b in PAIRS:
            value = None if a is None or b is None else math.atan2(float(a), float(b))
            for mode in MODES:
                wire.query(mode, f"SELECT atan2({literal(a)},{literal(b)}) AS v", [value])
        for position in (0, 1):
            statement = ("trig_atan2_" + str(position)).encode()
            args = [literal("5e-324"), literal("5e-324")]
            args[position] = "$1"
            wire.describe("atan2 parameter " + str(position), "SELECT atan2(" + ",".join(args) + ") AS v",
                          statement, parameter=True)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, "5e-324", binary_input, binary_output)
                    for value in ("5e-324", "-1e-310", "NaN", None):
                        pair = ["5e-324", "5e-324"]
                        pair[position] = value
                        answer = None if value is None else math.atan2(float(pair[0]), float(pair[1]))
                        wire.reexecute(statement, value, answer, None, binary_input, binary_output, False)
            wire.close(statement)
        table = "r202_trig_rows"
        table_values = ("5e-324", "-1e-310", "-0", "NaN", None)
        wire.command(f"CREATE TABLE {table}(id integer,d double precision)", "CREATE TABLE")
        wire.command(f"INSERT INTO {table} VALUES" + ",".join(
            f"({i},{literal(value)})" for i, value in enumerate(table_values)), "INSERT 0 5")
        for fn in FUNCTIONS:
            for suffix, values in ((" ORDER BY id", [expected(fn, v) for v in table_values]),
                                   (" WHERE false", []), (" LIMIT 0", [])):
                for mode in MODES:
                    wire.query(mode, f"SELECT pg_catalog.{fn}(d) AS v FROM {table}" + suffix, values)
        # Fixed inventory, not the number of passing cases: no early-abort loop.
        expected_controls = len(FUNCTIONS) * (len(INPUTS) * len(MODES) + 22)
        expected_controls += len(PAIRS) * len(MODES) + 2 * 22 + 2 + len(FUNCTIONS) * 3 * len(MODES)
        review.complete(expected_controls)
    finally:
        review.close()


if __name__ == "__main__":
    main()
