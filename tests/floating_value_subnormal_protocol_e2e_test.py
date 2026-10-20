#!/usr/bin/env python3
"""Reached float8 subnormals survive shared conversion before scalar math.

Run unchanged against PostgreSQL 18.6. The native fixture separately preserves
the permissive public conversion API; this fixture only supplies typed SQL data.
This does not assert complete numeric formatting, power or CAST compatibility.
"""
from angle_conversion_range_protocol_e2e_test import Wire, literal
from compat.numeric_type_review import Review
import math
import sys

FUNCTIONS = ("ceil", "ceiling", "floor", "round", "trunc", "abs", "sign")
INPUTS = ("0", "-0", "1.25", "-1.25", "2.5", "-2.5", "3.5", "-3.5",
          "1e-300", "-1e-300", "2.2250738585072014e-308", "-2.2250738585072014e-308",
          "1e-310", "-1e-310", "5e-324", "-5e-324", "Infinity", "-Infinity", "NaN", None)
MODES = ("simple", "extended", "binary")


def expected(fn, value):
    if value is None:
        return None
    n = float(value)
    if fn == "sign":
        return float((n > 0) - (n < 0))
    if fn == "abs":
        return abs(n)
    if not math.isfinite(n):
        return n
    operation = {"ceil": math.ceil, "ceiling": math.ceil, "floor": math.floor,
                 "round": round, "trunc": math.trunc}[fn]
    result = float(operation(n))
    return math.copysign(0.0, n) if result == 0 else result


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("FLOAT_VALUE_SUBNORMAL_PROTOCOL")
    wire = Wire(review)
    try:
        for fn in FUNCTIONS:
            for value in INPUTS:
                for mode in MODES:
                    wire.query(mode, f"SELECT {fn}({literal(value)}) AS v", [expected(fn, value)])
            statement = ("float_value_" + fn).encode()
            wire.describe(fn, f"SELECT {fn}($1) AS v", statement, parameter=True)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, "5e-324", binary_input, binary_output)
                    for value in ("5e-324", "-1e-310", "NaN", None):
                        wire.reexecute(statement, value, expected(fn, value), None,
                                       binary_input, binary_output, False)
            wire.close(statement)
        table = "r203_float_rows"
        values = ("5e-324", "-1e-310", "-0", "NaN", None)
        wire.command(f"CREATE TABLE {table}(id integer,d double precision)", "CREATE TABLE")
        wire.command(f"INSERT INTO {table} VALUES" + ",".join(
            f"({i},{literal(value)})" for i, value in enumerate(values)), "INSERT 0 5")
        for fn in FUNCTIONS:
            for suffix, wanted in ((" ORDER BY id", [expected(fn, v) for v in values]),
                                   (" WHERE false", []), (" LIMIT 0", [])):
                for mode in MODES:
                    wire.query(mode, f"SELECT pg_catalog.{fn}(d) AS v FROM {table}" + suffix, wanted)
        review.complete(len(FUNCTIONS) * (len(INPUTS) * len(MODES) + 22) + 2 + len(FUNCTIONS) * 3 * len(MODES))
    finally:
        review.close()


if __name__ == "__main__":
    main()
