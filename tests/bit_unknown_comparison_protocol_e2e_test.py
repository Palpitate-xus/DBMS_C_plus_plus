#!/usr/bin/env python3
"""Implicit UNKNOWN-to-BIT comparison preserves the complete binary datum."""
import argparse
import importlib.util
import socket
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_unknown_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client)
        sock=server["sock"]
    controls=0
    failures=[]

    def check(expression,expected,state=None):
        nonlocal controls
        controls+=1
        sql="SELECT "+expression+" AS value"
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        try:
            assert actual[0]==([] if state else [[expected]]) and actual[1]==state,(sql,actual,expected,state)
            if state is None:
                assert actual[3]==["value"] and actual[4]=="SELECT 1" and actual[5]==[16],(sql,actual)
                fields=client.row_description_fields(messages)
                assert len(fields)==1 and fields[0][3:]==(16,1,-1,0),(sql,fields)
        except AssertionError as error:
            failures.append(str(error))
            print("[BIT UNKNOWN FAIL] "+str(error))

    try:
        # Six complete input/consumer classes: UNKNOWN, leading zeroes,
        # empty, invalid, NULL, and genuine quantified SQL child queries.
        for expression,expected in [
            ("'01'=ANY(ARRAY[B'01'])","t"),("'01'=ALL(ARRAY[B'01'])","t"),
            ("'001'=ANY(ARRAY[B'001'])","t"),("'01'=ANY(ARRAY[B'1'])","f"),
            ("'11'>ALL(ARRAY[B'10'])","t"),("'01'>ANY(ARRAY[B'0'])","t"),
            ("'001'<ALL(ARRAY[B'01'])","t"),("'01'<>ALL(ARRAY[B'00',B'001'])","t"),
            ("''=ANY(ARRAY[B''])","t"),("''<ALL(ARRAY[B'0'])","t"),
            ("'0'=ANY(ARRAY[B''])","f"),("'01'=ANY(ARRAY[]::bit[])","f"),
            ("'01'=ALL(ARRAY[]::bit[])","t"),
            ("'01'=ANY(ARRAY[B'00',NULL,B'01'])","t"),
            ("'01'=ANY(ARRAY[B'00',NULL])",None),("NULL=ANY(ARRAY[B'01'])",None),
            ("'01'=ANY(NULL::bit[])",None),("NULL=ALL(ARRAY[]::bit[])","t"),
            ("'01'=ANY(SELECT B'01')","t"),("'001'=ALL(SELECT B'001')","t"),
            ("''=ANY(SELECT B'')","t"),("'11'>ALL(SELECT B'10')","t"),
            ("'01'=ANY(SELECT B'00')","f"),
            ("'01'=ANY(SELECT B'01' WHERE false)","f"),
            ("'01'=ALL(SELECT B'01' WHERE false)","t"),
            ("'01'=ANY(ARRAY[B'01'::varbit])","t"),
            ("''=ANY(ARRAY[B''::varbit])","t"),
            ("'01'=ANY(SELECT B'01'::varbit)","t")]:
            check(expression,expected)
        for expression in ("'102'=ANY(ARRAY[B'01'])","'102'=ALL(ARRAY[B'01'])",
                           "'102'=ANY(SELECT B'01')","'102'=ALL(SELECT B'01')"):
            check(expression,None,"22P02")
        # Preserve explicit BIT(1)'s value contract without conflating this
        # conversion root with the separately tracked BIT cast descriptors.
        check("(B'01'::bit)=B'0'","t")
        assert not failures,"%d/%d complete controls failed"%(len(failures),controls)
        print("[BIT UNKNOWN COMPARISON PROTOCOL] all %d complete value/NULL/error/OID controls passed"%controls)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__=="__main__":
    main()
