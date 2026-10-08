#!/usr/bin/env python3
"""True Bind NULL can prune; row and computed aggregate NULL cannot."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args();root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("parameter_origin_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner);client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings();sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server["sock"]
    schema="owned_input_origin_"+uuid.uuid4().hex[:12];created=False;failures=[];controls=0
    def ok(sql):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True);assert result[1] is None,(sql,result);return result
    def counter():return int(ok("SELECT currval('"+schema+".effects')")[0][0][0])
    def check(messages,value,effects,before,label):
        nonlocal controls
        controls+=1;result=runner.decode_wire_result(messages,include_types=True)
        fields=client.row_description_fields(messages) if any(kind==b"T" for kind,_ in messages) else []
        after=counter()
        if result!=([[value]],None,"",["value"],"SELECT 1",[16]) or len(fields)!=1 or fields[0][3:]!=(16,1,-1,0) or after-before!=effects:
            failures.append((label,result,fields,after-before,value,effects));print("[PARAMETER INPUT ORIGIN FAIL] "+str(failures[-1]),flush=True)
    try:
        ok("CREATE SCHEMA "+schema);created=True;ok("CREATE SEQUENCE "+schema+".effects");ok("SELECT nextval('"+schema+".effects')")
        for kind,name in (("varbit","writer"),("bigint","int_writer")):
            ok("CREATE FUNCTION "+schema+"."+name+"(p "+kind+") RETURNS "+kind+" LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('"+schema+".effects'); RETURN p; END; $$")
        writer=schema+".writer";integer_writer=schema+".int_writer"
        for path in range(4):
            for operation in (" BETWEEN "," NOT BETWEEN "):
                for oid in (1560,1562):
                    for source in (None,"00","01","11"):
                        if path<2:expression=("$1" if path==0 else "CAST($1 AS varbit)")+operation+writer+"(B'00') AND "+writer+"(B'11')"
                        elif path==2:expression=writer+"(B'01')"+operation+"$1 AND B'11'"
                        else:expression=writer+"(B'01')"+operation+"B'00' AND $1"
                        sql="SELECT "+expression+" AS value";name=("owned_origin_statement_"+uuid.uuid4().hex[:12]).encode();portal=name+b"p";before=counter()
                        sock.sendall(client.typed(b"P",name+b"\0"+sql.encode()+b"\0"+struct.pack("!HI",1,oid))+client.typed(b"D",b"S"+name+b"\0")+client.typed(b"S"))
                        messages=client.read_until_ready(sock);fields=client.row_description_fields(messages);parameters=[]
                        for kind,payload in messages:
                            if kind==b"t":parameters=list(struct.unpack("!"+"I"*struct.unpack("!H",payload[:2])[0],payload[2:]))
                        assert not any(kind in (b"E",b"D") for kind,_ in messages) and parameters==[oid] and fields[0][3:]==(16,1,-1,0) and counter()==before,(sql,messages)
                        raw=None if source is None else source.encode();cell=struct.pack("!i",-1) if raw is None else struct.pack("!i",len(raw))+raw
                        sock.sendall(client.typed(b"B",portal+b"\0"+name+b"\0"+struct.pack("!HH",0,1)+cell+struct.pack("!H",0))+client.typed(b"D",b"P"+portal+b"\0")+client.typed(b"E",portal+b"\0"+struct.pack("!I",0))+client.typed(b"S"))
                        truth=None if source is None else True if path<2 else "01"<=source if path==3 else "01">=source
                        if truth is not None and operation==" NOT BETWEEN ":truth=not truth
                        effects=(0 if path<2 else 1) if source is None else 1 if path==2 and source=="11" else 2
                        check(client.read_until_ready(sock),None if truth is None else "t" if truth else "f",effects,before,(sql,oid,source,"real Bind"))
                        sock.sendall(client.typed(b"C",b"S"+name+b"\0")+client.typed(b"S"));assert not any(kind==b"E" for kind,_ in client.read_until_ready(sock))
        assert controls==64,controls
        table=schema+".rows";ok("CREATE TABLE "+table+"(id integer PRIMARY KEY,b varbit,i integer)")
        ok("INSERT INTO "+table+" VALUES(1,NULL,NULL),(2,B'01',1),(3,NULL,NULL)")
        for reducer in ("bool_and","bool_or","every"):
            for operation in (" BETWEEN "," NOT BETWEEN "):
                for receiver in ("b","CAST(b AS varbit)"):
                    for predicate,calls,value in (("id=1",2,None),("id<>2",4,None),("id=2",2,"t" if operation==" BETWEEN " else "f")):
                        sql="SELECT "+reducer+"("+receiver+operation+writer+"(B'00') AND "+writer+"(B'11')) AS value FROM "+table+" WHERE "+predicate;before=counter()
                        check(client.simple_query(sock,sql),value,calls,before,sql)
        for operation in (" BETWEEN "," NOT BETWEEN "):
            for receiver in ("sum(i+0)","CAST(sum(i+0) AS bigint)"):
                for predicate,value in (("id=1",None),("id<0",None),("id=2","t" if operation==" BETWEEN " else "f")):
                    sql="SELECT "+receiver+operation+integer_writer+"(0) AND "+integer_writer+"(2) AS value FROM "+table+" WHERE "+predicate;before=counter()
                    check(client.simple_query(sock,sql),value,2,before,sql)
        assert controls==112,controls
        assert not failures,"%d/%d actual provenance/value/NULL/writer/OID controls failed"%(len(failures),controls)
        print("[PARAMETER INPUT ORIGIN PROTOCOL] all %d genuine Bind/Parse/Describe/runtime row/computed aggregate/NULL/cast/effects controls passed"%controls)
    finally:
        if created:ok("DROP SCHEMA "+schema+" CASCADE")
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=="__main__":main()
