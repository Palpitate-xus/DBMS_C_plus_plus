#!/usr/bin/env python3
"""Complete BIT numeric-cast relationships, binary widths and typed NULLs."""
import argparse
import importlib.util
import socket
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location("bit_integer_cast_runner",root/"tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host,port,user,database,password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    controls = 0

    def check(sql,expected,state,oid):
        nonlocal controls
        controls += 1
        messages = client.simple_query(sock,sql)
        actual = runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==expected and actual[1]==state, (sql,actual,expected,state)
        if state is None:
            assert actual[3]==["value"] and actual[4]=="SELECT 1" and actual[5]==[oid], (sql,actual,oid)
            fields = client.row_description_fields(messages)
            assert len(fields)==1 and fields[0][3:]==(oid,4 if oid==23 else 8,-1,0), (sql,fields)

    try:
        sources = ["B''","B'1'","B'111'","B'111'::varbit","NULL::bit","NULL::varbit"]
        targets = ["smallint","integer","bigint","numeric","real","double precision","money","boolean","bytea"]
        for source_index,source in enumerate(sources):
            for target in targets:
                permitted = target in ("integer","bigint") and source_index in (0,1,2,4)
                expected = None if source_index==4 else "0" if source_index==0 else "1" if source_index==1 else "7"
                check("SELECT ("+source+")::"+target+" AS value",[[expected]] if permitted else [],
                      None if permitted else "42846",23 if target=="integer" else 20)
        for length in (31,32,33,63,64,65):
            for target,width,oid in (("integer",32,23),("bigint",64,20)):
                expected = "-1" if length==width else str((1<<length)-1)
                check("SELECT B'"+"1"*length+"'::"+target+" AS value",
                      [[expected]] if length<=width else [],None if length<=width else "22003",oid)
        for source in ("1","-1","NULL"):
            for original in ("smallint","integer","bigint"):
                for target in ("bit(5)","bit(65)","varbit","varbit(5)"):
                    if original!="smallint" and target.startswith("bit("):
                        controls += 1
                        length = int(target[4:-1])
                        expected = None if source=="NULL" else "1"*length if source=="-1" else "0"*(length-1)+"1"
                        sql = "SELECT ("+source+"::"+original+")::"+target+" AS value"
                        messages = client.simple_query(sock,sql)
                        actual = runner.decode_wire_result(messages,include_types=True)
                        assert actual[0]==[[expected]] and actual[1] is None, (sql,actual,expected)
                        assert actual[3]==["value"] and actual[4]=="SELECT 1" and actual[5]==[1560], (sql,actual)
                        fields = client.row_description_fields(messages)
                        assert len(fields)==1 and fields[0][3:]==(1560,-1,length,0), (sql,fields)
                        continue
                    check("SELECT ("+source+"::"+original+")::"+target+" AS value",[],"42846",0)
        for original in ("numeric","numeric(8,2)","real","double precision","money","boolean","bytea"):
            for null_value in (False,True):
                for target in ("bit(5)","bit(65)","varbit","varbit(5)"):
                    source = "NULL" if null_value else "'\\x01'" if original=="bytea" else \
                             "true" if original=="boolean" else "'1'" if original=="money" else "1"
                    check("SELECT ("+source+"::"+original+")::"+target+" AS value",[],"42846",0)
        for bits,target,expected,oid in [
                ("1"+"0"*31,"integer","-2147483648",23),
                ("1"+"0"*63,"bigint","-9223372036854775808",20),
                ("0"*65,"bigint",None,20),("00000101","integer","5",23)]:
            check("SELECT B'"+bits+"'::"+target+" AS value",[[expected]] if expected is not None else [],
                  None if expected is not None else "22003",oid)
        for sql in (
                "SELECT CAST(NULL::varbit AS integer) AS value",
                "SELECT CASE WHEN false THEN B'1'::smallint ELSE 0::smallint END AS value",
                "SELECT CAST(B'1'::varbit AS bigint) AS value WHERE false",
                "SELECT (NULL::smallint)::bit(5) AS value WHERE false",
                "SELECT CASE WHEN false THEN B'1'::money ELSE '0'::money END AS value",
                "SELECT CAST(1::numeric AS bit(5)) AS value WHERE false"):
            check(sql,[],"42846",0)
        for expression,expected in (("'111'::integer","111"),("'\\x00000101'::bytea::integer","257"),
                                    ("1.6::numeric::integer","2"),("true::integer","1")):
            check("SELECT "+expression+" AS value",[[expected]],None,23)
        print("[BIT INTEGER CAST PROTOCOL] all %d complete value/NULL/error/OID/descriptor controls passed"%controls)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__=="__main__":
    main()
