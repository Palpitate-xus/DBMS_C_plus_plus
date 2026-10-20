#!/usr/bin/env python3
"""Float4/float8 to all three integer widths: values, errors and wire metadata.

The identical fixture must pass on PostgreSQL 18.6. Expected values first round
the source to its physical IEEE width, then round ties to even and check the
integer bounds with Python integers. No product output supplies expectations.
"""
from angle_conversion_range_protocol_e2e_test import Wire as FloatWire
from compat.numeric_type_review import Review
from decimal import Decimal
import math
import re
import struct
import sys

INPUTS = (None,"0","-0","0.5","-0.5","1.5","-1.5","2.5","-2.5","3.5","-3.5",
          "0.49999999999999994","-0.49999999999999994","32767","32767.5","-32768","-32768.5","-32768.6",
          "2147483647","2147483647.5","-2147483648","-2147483648.5","123456789","16777217",
          "9223372036854775807","-9223372036854775808","9223372036854774784","-9223372036854777856",
          "5e-324","-5e-324","1e-310","-1e-310","1e-45","-1e-45","Infinity","-Infinity","NaN",
          "bad","1tail","1e999tail","1e999","1e-999")
SOURCES = (("real",700,"f"),("double precision",701,"d"))
TARGETS = (("smallint",21,16,"h"),("integer",23,32,"i"),("bigint",20,64,"q"))
MODES = ("simple","extended","binary")


def literal(value, source):
    return "NULL::" + source if value is None else "'" + value + "'::" + source


def expected(value, source, bits):
    if value is None:
        return None, None
    prefix = re.match(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?",value)
    numeric_text = prefix.group(0) if prefix else value
    try:
        number = float(numeric_text)
    except ValueError:
        return None, "22P02"
    # PG checks conversion range before trailing junk, including numeric
    # overflow that Python float() would otherwise hide behind ValueError.
    if prefix and (math.isinf(number) or (number == 0 and Decimal(numeric_text) != 0)):
        return None, "22003"
    if source == "real":
        try:
            rounded_source = struct.unpack("!f",struct.pack("!f",number))[0]
        except OverflowError:
            return None, "22003"
        if rounded_source == 0 and number != 0:
            return None, "22003"
        number = rounded_source
    if prefix and prefix.end() != len(value):
        return None, "22P02"
    if not math.isfinite(number):
        return None, "22003"
    integer = round(number)
    if integer < -(1 << (bits-1)) or integer >= (1 << (bits-1)):
        return None, "22003"
    return integer, None


class Wire(FloatWire):
    def __init__(self, review, source, target):
        super().__init__(review)
        self.source,self.input_oid,self.input_format = source
        self.target,self.output_oid,self.bits,self.output_format = target

    def fields(self, binary):
        return [(b"v",0,0,self.output_oid,self.bits//8,-1,int(binary))]

    def parse_packet(self, statement, sql, parameter):
        body = statement+b"\0"+sql.encode()+b"\0"+struct.pack("!H",int(parameter))
        if parameter:
            body += struct.pack("!I",self.input_oid)
        return self.client.typed(b"P",body)+self.client.typed(b"D",b"S"+statement+b"\0")

    def bind_packet(self, statement, parameter, value, binary_input, binary_output, execute=True):
        body = b"\0"+statement+b"\0"+struct.pack("!H",int(parameter))
        if parameter:
            body += struct.pack("!H",int(binary_input))
        body += struct.pack("!H",int(parameter))
        if parameter:
            raw = None if value is None else (struct.pack("!"+self.input_format,float(value)) if binary_input else value.encode())
            body += struct.pack("!i",-1 if raw is None else len(raw))+(raw or b"")
        body += struct.pack("!HH",1,int(binary_output))
        packet = self.client.typed(b"B",body)+self.client.typed(b"D",b"P\0")
        if execute:
            packet += self.client.typed(b"E",b"\0"+struct.pack("!I",0))
        return packet

    def result(self,label,messages,values,state=None,binary=False,parsed=False,bound=False,parameter=False,simple=False):
        errors = self.errors(messages)
        rows = self.client.data_row_values(messages)
        tags = [body for kind,body in messages if kind == b"C"]
        parameters = [body for kind,body in messages if kind == b"t"]
        if state:
            good = errors == [state] and not rows and not tags
        else:
            descriptions = ([self.fields(False)] if parsed else [])
            if simple or bound:
                descriptions.append(self.fields(binary))
            wanted_parameters = ([struct.pack("!HI",1,self.input_oid) if parameter else struct.pack("!H",0)] if parsed else [])
            wanted_rows = [[None if value is None else (struct.pack("!"+self.output_format,value) if binary else str(value).encode())]
                           for value in values]
            good = (not errors and rows == wanted_rows and self.descriptions(messages) == descriptions and
                    parameters == wanted_parameters and tags == [("SELECT "+str(len(values))).encode()+b"\0"] and
                    sum(kind == b"1" for kind,_ in messages) == int(parsed) and
                    sum(kind == b"2" for kind,_ in messages) == int(bound))
        good = good and messages[-1:] == [(b"Z",self.ready)] and not any(kind in (b"N",b"s") for kind,_ in messages)
        self.review.record(label,good,(errors,rows,self.descriptions(messages),parameters,tags,messages[-1:]),(values,state,binary,self.ready))

    def describe(self,label,sql,statement=b"",parameter=False):
        messages = self.exchange(self.parse_packet(statement,sql,parameter))
        wanted = [struct.pack("!HI",1,self.input_oid) if parameter else struct.pack("!H",0)]
        good = (not self.errors(messages) and self.descriptions(messages) == [self.fields(False)] and
                [body for kind,body in messages if kind == b"t"] == wanted and
                sum(kind == b"1" for kind,_ in messages) == 1 and
                not any(kind in (b"D",b"C",b"2",b"N") for kind,_ in messages) and messages[-1:] == [(b"Z",self.ready)])
        self.review.record(("DESCRIBE",label),good,messages,(self.fields(False),wanted))


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("FLOAT_INTEGER_CAST_PROTOCOL")
    try:
        setup = Wire(review,SOURCES[0],TARGETS[0])
        setup.command("CREATE TABLE r204_cast_rows(id integer,d double precision,r real)","CREATE TABLE")
        table_values = (("5e-324","1e-45"),("-1e-310","-1e-45"),("123456789","123456789"),
                        ("2147483647","2147483647"),("-9223372036854775808","-9223372036854775808"),("2.5","2.5"),(None,None))
        setup.command("INSERT INTO r204_cast_rows VALUES"+",".join(
            f"({i},{literal(d,'double precision')},{literal(r,'real')})" for i,(d,r) in enumerate(table_values)),"INSERT 0 7")
        for source in SOURCES:
            for target in TARGETS:
                wire = Wire(review,source,target)
                for value in INPUTS:
                    answer,state = expected(value,wire.source,wire.bits)
                    for mode in MODES:
                        wire.query(mode,f"SELECT CAST({literal(value,wire.source)} AS {wire.target}) AS v",[answer] if not state else [],state)
                statement = f"r204_{wire.input_oid}_{wire.output_oid}".encode()
                wire.describe(statement,f"SELECT CAST($1 AS {wire.target}) AS v",statement,parameter=True)
                params = (("1e-45","-1e-45","123456789","2147483647","-9223372036854775808","Infinity","NaN",None)
                          if wire.source == "real" else
                          ("5e-324","-1e-310","2.5","-2.5","9223372036854775807","-9223372036854775808","NaN",None))
                for binary_input in (False,True):
                    for binary_output in (False,True):
                        wire.bind_describe(statement,params[0],binary_input,binary_output)
                        for value in params:
                            answer,state = expected(value,wire.source,wire.bits)
                            wire.reexecute(statement,value,answer,state,binary_input,binary_output,False)
                wire.close(statement)
                column = "r" if wire.source == "real" else "d"
                for i,pair in enumerate(table_values):
                    answer,state = expected(pair[int(column == 'r')],wire.source,wire.bits)
                    for mode in MODES:
                        wire.query(mode,f"SELECT CAST({column} AS {wire.target}) AS v FROM r204_cast_rows WHERE id={i}",
                                   [answer] if not state else [],state)
                safe_values = [expected(table_values[i][int(column == 'r')],wire.source,wire.bits)[0] for i in (0,1,5,6)]
                for suffix,values in ((" WHERE id IN(0,1,5,6) ORDER BY id",safe_values),(" WHERE false",[]),(" LIMIT 0",[])):
                    for mode in MODES:
                        wire.query(mode,f"SELECT CAST({column} AS {wire.target}) AS v FROM r204_cast_rows"+suffix,values)
        for target in TARGETS:
            wire = Wire(review,SOURCES[1],target)
            wire.ready = b"T";wire.command("BEGIN","BEGIN");wire.command("SAVEPOINT r204_sp","SAVEPOINT")
            wire.query("simple",f"SELECT CAST('5e-324'::double precision AS {wire.target}) AS v",[0])
            wire.ready = b"E"
            wire.query("simple",f"SELECT CAST('Infinity'::double precision AS {wire.target}) AS v",[],"22003")
            wire.query("simple",f"SELECT 1::{wire.target} AS v",[],"25P02")
            wire.ready = b"T";wire.command("ROLLBACK TO r204_sp","ROLLBACK")
            wire.query("binary",f"SELECT CAST('-5e-324'::double precision AS {wire.target}) AS v",[0])
            wire.ready = b"I";wire.command("COMMIT","COMMIT")
        review.complete(2+len(SOURCES)*len(TARGETS)*(len(INPUTS)*len(MODES)+38+len(table_values)*len(MODES)+3*len(MODES))+len(TARGETS)*8)
    finally:
        review.close()


if __name__ == "__main__":
    main()
