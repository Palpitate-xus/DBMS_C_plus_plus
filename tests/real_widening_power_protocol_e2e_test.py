#!/usr/bin/env python3
"""IEEE floating POWER and REAL in atan2's second input; same PG18 fixture."""
from angle_conversion_range_protocol_e2e_test import Wire
from compat.numeric_type_review import Review
from real_widening_protocol_e2e_test import INPUTS, literal
import math
import struct
import sys

PAIRS = (("NaN","0"),("NaN","1"),("1","NaN"),("-1","NaN"),
         ("0","-1"),("0","-Infinity"),("-2","0.5"),("-Infinity","0.5"),("-Infinity","3"),("-Infinity","-3"),
         ("-Infinity","2"),("2","Infinity"),("0.5","Infinity"),("-1","-Infinity"),("2","1024"),("2","-1075"),
         ("2","-1074"),("0.5","2"),("-0","3"),("-0","2"),("1.1","3"),("2","-1022"),("Infinity","-1"),("-1","1e20"))
MODES = ("simple","extended","binary")


def expected(base, exponent, source):
    x,y = float(base),float(exponent)
    if source == "real":
        x = struct.unpack("!f",struct.pack("!f",x))[0]
    if math.isnan(x) or math.isnan(y):
        return (1.0 if y == 0 or x == 1 else math.nan),None
    if (x == 0 and y < 0) or (x < 0 and math.isfinite(y) and y != math.floor(y)):
        return None,"2201F"
    try:
        value = math.pow(x,y)
    except OverflowError:
        return None,"22003"
    if math.isfinite(x) and math.isfinite(y) and (not math.isfinite(value) or (value == 0 and x != 0)):
        return None,"22003"
    return value,None


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("REAL_POWER_PROTOCOL")
    wire = Wire(review)
    try:
        for source in ("real","double precision"):
            for fn in ("power","pow"):
                for base,exponent in PAIRS:
                    answer,state = expected(base,exponent,source)
                    for mode in MODES:
                        wire.query(mode,f"SELECT {fn}('{base}'::{source},'{exponent}'::double precision) AS v",[answer] if not state else [],state)
        for value in INPUTS:
            answer = None if value is None else math.atan2(1.0,struct.unpack("!f",struct.pack("!f",float(value)))[0])
            for mode in MODES:
                wire.query(mode,f"SELECT atan2(1::double precision,{literal(value)}) AS v",[answer])
        review.complete(2*2*len(PAIRS)*len(MODES)+len(INPUTS)*len(MODES))
    finally:
        review.close()


if __name__ == "__main__":
    main()
