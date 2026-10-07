#!/usr/bin/env python3
"""UNKNOWN list inputs resolve independently for heterogeneous BIT comparisons."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("unknown_mixed_list_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="unknown_mixed_list_"+uuid.uuid4().hex[:12]
    created=False;failures=[];controls=0
    inputs=[("'0'",False),("'1'",True),("NULL",None),("'2'","22P02"),
            ("''","22P02"),("'01'",True),("'001'",True),("'11'",False)]
    def check(sql,rows,state=None,name=None):
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
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
        except AssertionError as error:
            failures.append(str(error));print("[BIT UNKNOWN MIXED LIST FAIL] "+str(error),flush=True)
    try:
        check("CREATE SCHEMA "+schema,[]);created=True
        check("CREATE TABLE "+schema+".bits(id int)",[])
        check("CREATE TABLE "+schema+".empty_bits(id int)",[])
        check("INSERT INTO "+schema+".bits VALUES(1)",[])
        for lhs,truth in inputs:
            for rhs in ("B'01',1","1,B'01'","B'01',1,NULL","NULL,1,B'01'"):
                for operation in ("IN","NOT IN"):
                    state=truth if isinstance(truth,str) else None
                    answer=truth
                    if answer is False and "NULL" in rhs:answer=None
                    if state is None and answer is not None and operation=="NOT IN":answer=not answer
                    rows=[] if state else [[None if answer is None else "t" if answer else "f"]]
                    control("SELECT "+lhs+" "+operation+" ("+rhs+") AS value",rows,state,"value")
            for rhs in ("B'01',1","1,B'01'"):
                for operation in ("IN","NOT IN"):
                    state=truth if isinstance(truth,str) else None
                    answer=truth
                    if state is None and answer is not None and operation=="NOT IN":answer=not answer
                    for name in ("bits","empty_bits"):
                        rows=[["1"]] if state is None and answer is True and name=="bits" else []
                        control("SELECT id FROM "+schema+"."+name+" WHERE "+lhs+" "+operation+" ("+rhs+")",rows,state,"id")
        assert not failures,"%d/%d complete mixed-list controls failed"%(len(failures),controls)
        print("[BIT UNKNOWN MIXED LIST PROTOCOL] all %d unknown/leading-zero/NULL/error/per-pair/empty/OID controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
