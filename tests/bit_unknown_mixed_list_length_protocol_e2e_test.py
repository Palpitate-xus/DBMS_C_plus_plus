#!/usr/bin/env python3
"""Mixed BIT comparisons retain widths even when decimal-looking values coincide."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args();root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("mixed_list_length_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server["sock"]
    schema="mixed_bit_length_"+uuid.uuid4().hex[:12];created=False;controls=0;failures=[]
    def check(sql,rows,state=None,name=None):
        messages=client.simple_query(sock,sql);actual=runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==rows and actual[1]==state,(sql,actual,rows,state)
        if state is None and name is not None:
            oid,size=(16,1) if name=="value" else (23,4)
            assert actual[3:]==([name],"SELECT "+str(len(rows)),[oid]),(sql,actual)
            fields=client.row_description_fields(messages)
            assert len(fields)==1 and fields[0][3:]==(oid,size,-1,0),(sql,fields)
    def control(sql,rows,state,name):
        nonlocal controls
        controls+=1
        try:check(sql,rows,state,name)
        except AssertionError as error:failures.append(str(error));print("[MIXED BIT LENGTH FAIL] "+str(error),flush=True)
    try:
        check("CREATE SCHEMA "+schema,[]);created=True
        for name in ("bits","empty_bits"):check("CREATE TABLE "+schema+"."+name+"(id int)",[])
        check("INSERT INTO "+schema+".bits VALUES(1)",[])
        for source in ("1","01","001","0001","0",None,"2",""):
            lhs="NULL" if source is None else "'"+source+"'"
            state="22P02" if source in ("2","") else None
            for literal,bits in (("B'1'","1"),("B'01'","01"),("X'1'","0001")):
                matched=None if source is None else source==bits or source=="0"
                for rhs in (literal+",0","0,"+literal,"NULL,0,"+literal,literal+",0,NULL"):
                    for operation in ("IN","NOT IN"):
                        answer=matched
                        if answer is False and "NULL" in rhs:answer=None
                        if answer is not None and operation=="NOT IN":answer=not answer
                        rows=[] if state else [[None if answer is None else "t" if answer else "f"]]
                        expression=lhs+" "+operation+"("+rhs+")"
                        control("SELECT "+expression+" AS value",rows,state,"value")
                        if source in ("1","01","001","0001") and "NULL" not in rhs:
                            for name in ("bits","empty_bits"):
                                control("SELECT id FROM "+schema+"."+name+" WHERE "+expression,
                                    [["1"]] if answer is True and name=="bits" else [],None,"id")
        assert not failures,"%d/%d complete length controls failed"%(len(failures),controls)
        print("[MIXED BIT LENGTH PROTOCOL] all %d B/X-width/leading-zero/pair/order/NULL/empty/error/OID controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
