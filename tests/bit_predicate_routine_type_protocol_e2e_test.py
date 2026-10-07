#!/usr/bin/env python3
"""Owned BIT-returning routines retain types through real predicate preparation."""
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
    spec=importlib.util.spec_from_file_location("bit_routine_predicate_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="bit_routine_predicate_"+uuid.uuid4().hex[:12]
    created=False;failures=[];controls=0
    def check(sql,rows):
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==rows and actual[1] is None,(sql,actual,rows)
        if sql.startswith("SELECT"):
            assert actual[3:]==(["id"],"SELECT "+str(len(rows)),[23]),(sql,actual)
            fields=client.row_description_fields(messages)
            assert len(fields)==1 and fields[0][3:]==(23,4,-1,0),(sql,fields)
    try:
        check("CREATE SCHEMA "+schema,[]);created=True
        for name in ("bits","empty_bits"):
            check("CREATE TABLE "+schema+"."+name+"(id int,v varbit)",[])
        check("INSERT INTO "+schema+".bits VALUES(1,B'01'),(2,NULL)",[])
        check("CREATE FUNCTION "+schema+".identity_bit(p varbit) RETURNS varbit LANGUAGE plpgsql AS $$ BEGIN RETURN p; END; $$",[])
        for name in ("bits","empty_bits"):
            for operation in ("=B'01'"," BETWEEN B'01' AND B'01'"," IN (B'',B'01')"," NOT IN (B'',NULL)"):
                for tail in (""," ORDER BY id"):
                    sql="SELECT id FROM "+schema+"."+name+" WHERE "+schema+".identity_bit(v)"+operation+tail
                    expected=[["1"]] if name=="bits" and "NOT IN" not in operation else []
                    controls+=1
                    try:check(sql,expected)
                    except AssertionError as error:
                        failures.append(str(error));print("[BIT ROUTINE PREDICATE FAIL] "+str(error),flush=True)
        assert not failures,"%d/%d complete routine predicate controls failed"%(len(failures),controls)
        print("[BIT ROUTINE PREDICATE PROTOCOL] all %d real-owned-routine value/NULL/empty/order/OID controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
