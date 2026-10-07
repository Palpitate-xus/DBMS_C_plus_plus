#!/usr/bin/env python3
"""BIT textual input codecs preserve prefix meaning and typed boundaries."""
import argparse
import importlib.util
import socket
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args();root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_text_input_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    inputs=[("",""),("0","0"),("01","01"),("001","001"),
            ("b",""),("B",""),("b01","01"),("B001","001"),
            ("x",""),("X",""),("x1","0001"),("X0aF","000010101111"),
            ("x0","0000"),("Xff","11111111"),
            ("b02",None),("xg",None),("b 01",None),("x 1",None),("B'01'",None),("x-1",None)]
    controls=0;failures=[]
    def check(expression,expected,oid,state=None):
        nonlocal controls
        controls+=1;sql="SELECT "+expression+" AS value"
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        rows=[] if state else [[expected]]
        difference=result[0]!=rows or result[1]!=state
        if state is None:difference=difference or result[3:]!=(["value"],"SELECT 1",[oid])
        if difference:
            error=(sql,result,rows,state,oid);failures.append(str(error))
            print("[BIT TEXT INPUT FAIL] "+str(error),flush=True)
    truth=lambda value:"t" if value else "f"
    try:
        for source,bits in inputs:
            literal="'"+source.replace("'","''")+"'"
            state=None if bits is not None else "22P02"
            fixed=None if bits is None else (bits+"0000")[:4]
            array=None if bits is None else "{01,"+(bits if bits else '\"\"')+"}"
            for expression,expected,oid in (
                (literal+"::varbit",bits,1562),
                ("CAST("+literal+" AS bit varying)",bits,1562),
                (literal+"::bit",None if bits is None else (bits+"0")[:1],1560),
                (literal+"::bit(4)",fixed,1560),
                (literal+"::varbit(4)",None if bits is None else bits[:4],1562),
                ("B'01'="+literal,truth(bits=="01"),16),
                (literal+"=B'01'",truth(bits=="01"),16),
                ("B'01'<>"+literal,truth(bits!="01"),16),
                ("B'01'<"+literal,None if bits is None else truth("01"<bits),16),
                ("B'01'::varbit="+literal,truth(bits=="01"),16),
                (literal+"=ANY(ARRAY[B'01'])",truth(bits=="01"),16),
                ("ARRAY[B'01',"+literal+"]",array,1561),
                (literal+" IN(B'01')",truth(bits=="01"),16),
                (literal+"::text::varbit",bits,1562),
                ("ARRAY[B'01'::varbit,"+literal+"]",array,1563),
                ("NULL="+literal+"::varbit",None,16)):
                check(expression,expected,oid,state)
            # Text must remain text. Explicit typed comparisons are rejected
            # before trying the BIT textual input codec, even for invalid data.
            check("B'01'="+literal+"::text",None,16,"42883")
            check("B'01'=ANY(ARRAY["+literal+"])",None,16,"42883")
            check(literal+"::text",source,25)
            # The mixed list still prepares an independent INTEGER input;
            # matching a valid prefixed BIT does not conceal its input error.
            numeric=source in ("0","01","001")
            check(literal+" IN(B'01',0)",truth(bits=="01" or source=="0"),16,None if numeric else "22P02")
        for target,oid in (("bit",1560),("varbit",1562)):
            check("NULL::"+target,None,oid)
            check("NULL="+"NULL::"+target,None,16)
        assert not failures,"%d/%d complete BIT input controls failed"%(len(failures),controls)
        print("[BIT TEXT INPUT PROTOCOL] all %d complete prefix/hex/empty/NULL/type/array/width/error/OID controls passed"%controls)
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=="__main__":main()
