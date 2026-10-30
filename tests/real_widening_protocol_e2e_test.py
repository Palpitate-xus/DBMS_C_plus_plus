#!/usr/bin/env python3
"""Preserve REAL's physical value when widening and consuming float8 data.

Same full fixture on PostgreSQL 18.6. Compare actual binary64 values, signed
zero, SQL NULL, result/parameter metadata and transaction state; not float
display spellings or an assertion that all math overloads are implemented.
"""
from angle_conversion_range_protocol_e2e_test import Wire as FloatWire
from compat.numeric_type_review import Review
import math
import struct
import sys

FUNCTIONS = ("cast","sin","cos","tan","atan","sqrt","cbrt","degrees","radians",
             "round","ceil","ceiling","floor","trunc","sign","abs","power","pow","atan2","asin","acos")
INPUTS = ("0","-0","1.25","-1.25","0.1","-0.1","123456789","-123456789",
          "16777217","-16777217","2147483647","-2147483648","1e-45","-1e-45","1.1754944e-38",
          "-1.1754944e-38","3.4028235e38","-3.4028235e38","Infinity","-Infinity","NaN",None)
MODES = ("simple","extended","binary")
RADIANS_PER_DEGREE = float.fromhex("0x1.1df46a2529d39p-6")


def literal(value):
    return "NULL::real" if value is None else "'"+value+"'::real"


def expression(fn, value):
    if fn == "cast":
        return "CAST("+value+" AS double precision)"
    call = fn+"("+value+(",1::double precision)" if fn in ("power","pow","atan2") else ")")
    return "CAST("+call+" AS double precision)" if fn == "abs" else call


def expected(fn, value):
    if value is None:
        return None,None
    n = struct.unpack("!f",struct.pack("!f",float(value)))[0]
    if fn in ("sin","cos","tan") and math.isinf(n):
        return None,"22003"
    if fn == "sqrt" and n < 0:
        return None,"2201F"
    if fn in ("asin","acos") and not (-1 <= n <= 1 or math.isnan(n)):
        return None,"22003"
    if fn in ("cast","power","pow"):
        return n,None
    if fn == "sign":
        return float((n > 0)-(n < 0)),None
    if fn == "abs":
        return abs(n),None
    if fn == "degrees":
        return n/RADIANS_PER_DEGREE,None
    if fn == "radians":
        return n*RADIANS_PER_DEGREE,None
    if fn == "atan2":
        return math.atan2(n,1.0),None
    if fn in ("round","ceil","ceiling","floor","trunc"):
        if not math.isfinite(n):
            return n,None
        operation = {"round":round,"ceil":math.ceil,"ceiling":math.ceil,"floor":math.floor,"trunc":math.trunc}[fn]
        result = float(operation(n))
        return (math.copysign(0.0,n) if result == 0 else result),None
    return getattr(math,fn)(n),None


class Wire(FloatWire):
    def parse_packet(self, statement, sql, parameter):
        body = statement+b"\0"+sql.encode()+b"\0"+struct.pack("!H",int(parameter))
        if parameter:
            body += struct.pack("!I",700)
        return self.client.typed(b"P",body)+self.client.typed(b"D",b"S"+statement+b"\0")

    def bind_packet(self, statement, parameter, value, binary_input, binary_output, execute=True):
        body = b"\0"+statement+b"\0"+struct.pack("!H",int(parameter))
        if parameter:
            body += struct.pack("!H",int(binary_input))
        body += struct.pack("!H",int(parameter))
        if parameter:
            raw = None if value is None else (struct.pack("!f",float(value)) if binary_input else value.encode())
            body += struct.pack("!i",-1 if raw is None else len(raw))+(raw or b"")
        body += struct.pack("!HH",1,int(binary_output))
        packet = self.client.typed(b"B",body)+self.client.typed(b"D",b"P\0")
        if execute:
            packet += self.client.typed(b"E",b"\0"+struct.pack("!I",0))
        return packet

    def describe(self,label,sql,statement=b"",parameter=False):
        messages = self.exchange(self.parse_packet(statement,sql,parameter))
        wanted = [struct.pack("!HI",1,700) if parameter else struct.pack("!H",0)]
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(False)] and
                [body for kind,body in messages if kind == b"t"] == wanted and
                sum(kind == b"1" for kind,_ in messages) == 1 and
                not any(kind in (b"D",b"C",b"2",b"N") for kind,_ in messages) and messages[-1:] == [(b"Z",self.ready)])
        self.review.record(("DESCRIBE",label),good,messages,(self.fields(False),wanted))


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("REAL_WIDEN_PROTOCOL")
    wire = Wire(review)
    try:
        wire.command("CREATE TABLE r205_real_rows(id integer,r real)","CREATE TABLE")
        table_values = ("123456789","1e-45","-0","-123456789",None)
        wire.command("INSERT INTO r205_real_rows VALUES"+",".join(f"({i},{literal(value)})" for i,value in enumerate(table_values)),"INSERT 0 5")
        for fn in FUNCTIONS:
            for value in INPUTS:
                answer,state = expected(fn,value)
                for mode in MODES:
                    wire.query(mode,"SELECT "+expression(fn,literal(value))+" AS v",[answer] if not state else [],state)
            statement = ("r205_"+fn).encode()
            wire.describe(fn,"SELECT "+expression(fn,"$1")+" AS v",statement,parameter=True)
            for binary_input in (False,True):
                for binary_output in (False,True):
                    wire.bind_describe(statement,"0.1",binary_input,binary_output)
                    for value in ("123456789","1e-45","-0","-123456789","0.1","Infinity","NaN",None):
                        answer,state = expected(fn,value)
                        wire.reexecute(statement,value,answer,state,binary_input,binary_output,False)
            wire.close(statement)
            for i,value in enumerate(table_values):
                answer,state = expected(fn,value)
                for mode in MODES:
                    wire.query(mode,"SELECT "+expression(fn,"r")+f" AS v FROM r205_real_rows WHERE id={i}",[answer] if not state else [],state)
            for suffix in (" WHERE false"," LIMIT 0"):
                for mode in MODES:
                    wire.query(mode,"SELECT "+expression(fn,"r")+" AS v FROM r205_real_rows"+suffix,[])
        arrays = (("123456789",1),("1e-45",2),(None,3))
        for value,index in arrays:
            for mode in MODES:
                wire.query(mode,f"SELECT (ARRAY['123456789'::real,'1e-45'::real,NULL::real]::double precision[])[{index}] AS v",
                           [expected("cast",value)[0]])
        for value in ("123456789","1e-45","-0",None):
            for mode in MODES:
                answer = None if value is None else expected("cast",value)[0]+0.0
                wire.query(mode,f"SELECT {literal(value)}+0::double precision AS v",[answer])
        wire.query("simple","SELECT point '(1,2)' * '123456789'::real AS v",[],"42883")
        review.complete(2+len(FUNCTIONS)*(len(INPUTS)*len(MODES)+38+len(table_values)*len(MODES)+2*len(MODES))+
                        len(arrays)*len(MODES)+4*len(MODES)+1)
    finally:
        review.close()


if __name__ == "__main__":
    main()
