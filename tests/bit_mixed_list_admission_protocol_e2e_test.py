#!/usr/bin/env python3
"""Real typed list admission is pure; an admitted volatile left operand runs once."""
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
    spec=importlib.util.spec_from_file_location("mixed_list_admission_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="bit_mixed_admission_"+uuid.uuid4().hex[:12]
    created=False;controls=0
    def check(sql,rows,state=None,name=None):
        nonlocal controls
        controls+=1
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==rows and actual[1]==state,(sql,actual,rows,state)
        if state is None and name is not None:
            oid,size=(16,1) if name=="value" else (20,8)
            assert actual[3:]==([name],"SELECT "+str(len(rows)),[oid]),(sql,actual)
            fields=client.row_description_fields(messages)
            assert len(fields)==1 and fields[0][3:]==(oid,size,-1,0),(sql,fields)
    def count(expected):check("SELECT count(*) AS calls FROM "+schema+".calls",[[str(expected)]],name="calls")
    try:
        check("CREATE SCHEMA "+schema,[]);created=True
        check("CREATE TABLE "+schema+".calls(id int)",[])
        check("CREATE TABLE "+schema+".empty_bits(id int,v varbit)",[])
        check("CREATE FUNCTION "+schema+".writer(p varbit) RETURNS varbit LANGUAGE plpgsql AS $$ BEGIN INSERT INTO "+schema+".calls VALUES(1); RETURN p; END; $$",[])
        check("SELECT "+schema+".writer(B'01') IN(B'01',1) AS value",[],"42883");count(0)
        check("SELECT id FROM "+schema+".empty_bits WHERE "+schema+".writer(v) IN(B'01',1)",[],"42883");count(0)
        for index,(argument,value) in enumerate((("B'0'","f"),("B'01'","t"),("NULL::varbit",None)),1):
            check("SELECT "+schema+".writer("+argument+") IN(B'1','01') AS value",[[value]],name="value")
            count(index)
        check("SELECT '01'::text IN("+schema+".writer(B'01'),NULL) AS value",[],"42883");count(3)
        print("[BIT MIXED LIST ADMISSION PROTOCOL] all %d actual routine once/NULL/pure-admission/empty/error/OID controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
