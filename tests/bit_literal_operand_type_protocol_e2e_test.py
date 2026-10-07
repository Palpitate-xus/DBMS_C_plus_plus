#!/usr/bin/env python3
"""Typed BIT literals must remain typed through the stored SQL adapter."""
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
    spec=importlib.util.spec_from_file_location("bit_literal_type_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="bit_literal_type_"+uuid.uuid4().hex[:12]
    table=schema+".bits"
    created=False
    controls=0

    def check(sql,rows,state=None,oids=None):
        nonlocal controls
        controls+=1
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        assert actual[0]==rows and actual[1]==state,(sql,actual,rows,state)
        if state is None:
            assert actual[5]==(oids or []),(sql,actual,oids)

    try:
        for expression,rows,state in [
            ("B'01'='01'",[["t"]],None),("B'01'::varbit='01'",[["t"]],None),
            ("B''=''",[["t"]],None),("B'01'='01'::text",[],"42883"),
            ("B'01'=1::integer",[],"42883"),("B'01'::varbit='01'::text",[],"42883"),
            ("B'01'::varbit=1::integer",[],"42883"),
            ("NULL::bit=NULL::text",[],"42883"),("NULL::integer=NULL::varbit",[],"42883")]:
            check("SELECT "+expression+" AS value",rows,state,[16] if state is None else [])
        check("CREATE SCHEMA "+schema,[]);created=True
        check("CREATE TABLE "+table+'(id int,v varbit,t text,i int,"01" varbit)',[])
        check("INSERT INTO "+table+" VALUES "+
              "(1,B'01','01',1,B'1'),(2,B'1','1',1,B'01'),(3,B'','',0,B''),(4,NULL,NULL,NULL,NULL)",[])
        for predicate,rows in [
            ("v=B'01'",[["1"]]),("v='01'",[["1"]]),("t='01'",[["1"]]),
            ("v=B''",[["3"]]),("v=X''",[["3"]]),("v=''",[["3"]]),
            ("v IN (B'')",[["3"]]),("v NOT IN (B'')",[["1"],["2"]]),
            ("v IN (X'')",[["3"]]),("v NOT IN (X'')",[["1"],["2"]]),
            ("v IN (B'',B'01')",[["1"],["3"]]),("v NOT IN (B'',B'01')",[["2"]]),
            ("v IN (B'',NULL)",[["3"]]),("v NOT IN (B'',NULL)",[]),
            ("v IN (B'01')",[["1"]]),("v NOT IN (B'01')",[["2"],["3"]]),
            ("v BETWEEN B'' AND B'01'",[["1"],["3"]]),("v NOT BETWEEN B'' AND B'01'",[["2"]])]:
            check("SELECT id FROM "+table+" WHERE "+predicate+" ORDER BY id",rows,oids=[23])
        for column in ("t","i"):
            for literal in ("B'01'","X'1'","B''","X''"):
                for operation in ("=","<>","<","<=",">",">="):
                    check("SELECT id FROM "+table+" WHERE "+column+operation+literal+" ORDER BY id",[],"42883")
            for predicate in (" IN (B'',B'01',NULL)"," NOT IN (B'',B'01',NULL)",
                              " BETWEEN B'' AND B'01'"," NOT BETWEEN B'' AND B'01'"):
                check("SELECT id FROM "+table+" WHERE "+column+predicate+" ORDER BY id",[],"42883")
        check("CREATE TABLE "+schema+".empty_bits(id int,v varbit,t text,i int)",[])
        for predicate in ("t=B'01'","i=B''","t IN (B'')","i BETWEEN B'' AND B'01'"):
            check("SELECT id FROM "+schema+".empty_bits WHERE "+predicate,[],"42883")
        print("[BIT LITERAL OPERAND TYPE PROTOCOL] all %d SQL type/NULL/empty/list/collision controls passed"%controls)
    finally:
        try:
            if created:
                check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
