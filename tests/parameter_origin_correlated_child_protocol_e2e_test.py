#!/usr/bin/env python3
"""A correlated NULL is a runtime datum, not a statement input constant."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true");options=parser.parse_args()
    root=Path(__file__).resolve().parent.parent;spec=importlib.util.spec_from_file_location("correlated_origin_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner);client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings();sock=socket.create_connection((host,port),timeout=runner.wire_timeout());client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server["sock"]
    schema="owned_correlated_origin_"+uuid.uuid4().hex[:12];created=False;failures=[];controls=0
    def ok(sql):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True);assert result[1] is None,(sql,result);return result
    def counter():return int(ok("SELECT currval('"+schema+".effects')")[0][0][0])
    try:
        ok("CREATE SCHEMA "+schema);created=True;ok("CREATE SEQUENCE "+schema+".effects");ok("SELECT nextval('"+schema+".effects')")
        ok("CREATE TABLE "+schema+".rows(id integer PRIMARY KEY,i integer,b varbit)");ok("INSERT INTO "+schema+".rows VALUES(1,NULL,NULL),(2,0,B'00'),(3,1,B'01')")
        for kind,name in (("varbit","bit_writer"),("integer","int_writer")):
            ok("CREATE FUNCTION "+schema+"."+name+"(p "+kind+") RETURNS "+kind+" LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('"+schema+".effects'); RETURN p; END; $$")
        for bits in (True,False):
            for identifier in (1,2,3):
                for operation in (" BETWEEN "," NOT BETWEEN "):
                    controls+=1;before=counter()
                    receiver="o.b" if bits else "o.i";writer=schema+(".bit_writer" if bits else ".int_writer")
                    expression=receiver+operation+writer+("(B'00') AND "+writer+"(B'11')" if bits else "(0) AND "+writer+"(2)")
                    sql="SELECT (SELECT "+expression+") AS value FROM "+schema+".rows o WHERE o.id="+str(identifier)
                    messages=client.simple_query(sock,sql);result=runner.decode_wire_result(messages,include_types=True);after=counter()
                    wanted=([[None if identifier==1 else "t" if operation==" BETWEEN " else "f"]],None,"",["value"],"SELECT 1",[16])
                    fields=client.row_description_fields(messages) if any(kind==b"T" for kind,_ in messages) else []
                    if result!=wanted or after-before!=2 or len(fields)!=1 or fields[0][3:]!=(16,1,-1,0):
                        failures.append((sql,result,fields,after-before,wanted,2));print("[CORRELATED PARAMETER ORIGIN FAIL] "+str(failures[-1]),flush=True)
        assert controls==12,controls
        assert not failures,"%d/%d actual correlated row/NULL/value/descriptor/effects controls failed"%(len(failures),controls)
        print("[PARAMETER ORIGIN CORRELATED CHILD PROTOCOL] all %d genuine correlated runtime NULL/rows/descriptors/effects controls passed"%controls)
    finally:
        if created:ok("DROP SCHEMA "+schema+" CASCADE")
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=="__main__":main()
