#!/usr/bin/env python3
"""Static address CAST input checks without evaluating typed/parameter peers."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("cast_input_analysis_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference="--reference18" in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        runner.verify_reference_version(client,sock)
        server={"sock":sock}
    else:
        server=runner.start_ours(client)
        sock=server["sock"]
    checks,failures=0,[]

    def check(label,ok,result):
        nonlocal checks
        checks+=1
        if not ok:
            failures.append((label,result))
            print("CAST_INPUT_ANALYSIS_FAILURE",label,repr(result),flush=True)

    def literal(value):
        return "'"+value.replace("'","''")+"'"

    def expect(sql,canonical=None,oid=None,state=None):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        ok=result[1]==state
        if state: ok=ok and result[0]==[] and result[4] is None
        else: ok=ok and result[0]==[[canonical]] and result[5]==[oid]
        check(sql,ok,result)

    def parse_only(sql,state=None):
        packet=b"\0"+sql.encode()+b"\0"+struct.pack("!H",0)
        sock.sendall(client.typed(b"P",packet)+client.typed(b"S"))
        messages=client.read_until_ready(sock)
        errors=[client.diagnostic_fields(body).get(b"C",b"").decode() for tag,body in messages if tag==b"E"]
        parsed=sum(tag==b"1" for tag,_ in messages)
        check("Parse/Sync only "+sql,errors==([state] if state else []) and
              parsed==(0 if state else 1) and not any(tag in (b"C",b"D",b"T") for tag,_ in messages),
              (errors,parsed,[tag for tag,_ in messages]))

    def parameter(kind,canonical,oid,input_oid,selected):
        sql="SELECT COALESCE("+("NULL::"+kind if selected else literal(canonical)+"::"+kind)+",$1::"+kind+")"
        raw=b"invalid"
        parse=b"\0"+sql.encode()+b"\0"+struct.pack("!HI",1,input_oid)
        bind=b"\0\0"+struct.pack("!HH",0,1)+struct.pack("!i",len(raw))+raw+struct.pack("!H",0)
        sock.sendall(client.typed(b"P",parse)+client.typed(b"B",bind)+
                     client.typed(b"D",b"P\0")+client.typed(b"E",b"\0"+struct.pack("!I",0))+client.typed(b"S"))
        messages=client.read_until_ready(sock)
        result=runner.decode_wire_result(messages,include_types=True)
        ok=any(tag==b"1" for tag,_ in messages)
        if selected: ok=ok and result[1]=="22P02" and result[0]==[] and result[4] is None
        else: ok=ok and result[1] is None and result[0]==[[canonical]] and result[5]==[oid]
        check("typed parameter "+str((kind,input_oid,selected)),ok,result)

    kinds=(("inet",869,"192.0.2.129",(("invalid","22P02"),("192.0.2.129/33","22P02"))),
           ("cidr",650,"192.0.2.0/24",(("invalid","22P02"),("192.0.2.129/24","22P02"))),
           ("macaddr",829,"08:00:2b:01:02:03",(("invalid","22P02"),("100:00:2b:01:02:03","22003"))),
           ("macaddr8",774,"08:00:2b:ff:fe:01:02:03",(("invalid","22P02"),("08:00:2b:ff:fe:01:02","22P02"))))
    try:
        for kind,oid,canonical,bad_values in kinds:
            for syntax in ("colon","cast"):
                def cast(value,target):
                    return "("+value+")::"+target if syntax=="colon" else "CAST("+value+" AS "+target+")"
                for target in (kind,"pg_catalog."+kind,'"pg_catalog"."'+kind+'"'):
                    good=cast(literal(canonical),target)
                    for value,state in bad_values:
                        bad=cast(literal(value),target)
                        sql="SELECT COALESCE("+good+","+bad+")"
                        expect(sql,state=state)
                        expect(sql+" WHERE false",state=state)
                        expect("SELECT CASE WHEN false THEN "+bad+" ELSE "+good+" END",state=state)
                        expect("SELECT "+bad,state=state)
                        typed=cast(literal(value)+"::text",target)
                        expect("SELECT COALESCE("+good+","+typed+")",canonical,oid)
                        parse_only(sql,state)
                    expect("SELECT COALESCE(NULL::"+target+","+good+")",canonical,oid)
                    expect("SELECT COALESCE("+good+",NULL::"+target+")",canonical,oid)
                    name=cast(literal("invalid")+'::"pg_catalog"."name"',target)
                    expect("SELECT COALESCE("+good+","+name+")",canonical,oid)
                    typed=cast(literal("invalid")+"::text",target)
                    parse_only("SELECT COALESCE("+good+","+typed+")")
                    for source in ("integer","bytea","boolean"):
                        absent=cast("NULL::"+source,target)
                        sql="SELECT COALESCE("+good+","+absent+")"
                        expect(sql,state="42846")
                        expect(sql+" WHERE false",state="42846")
                        expect("SELECT CASE WHEN false THEN "+absent+" ELSE "+good+" END",state="42846")
            parameter(kind,canonical,oid,25,False)
            parameter(kind,canonical,oid,25,True)
            parameter(kind,canonical,oid,19,False)
        print("CAST_INPUT_ANALYSIS_COMPLETE",checks,"FAILURES",len(failures),flush=True)
        assert not failures,failures
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=="__main__":
    main()
