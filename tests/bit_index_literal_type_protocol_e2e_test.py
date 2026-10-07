#!/usr/bin/env python3
"""BIT literal signatures cannot disappear when an equality index consumes a predicate."""
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
    spec=importlib.util.spec_from_file_location("bit_index_literal_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="bit_index_literal_"+uuid.uuid4().hex[:12]
    created=False;failures=[];controls=0
    def check(sql,rows,state=None):
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==rows and actual[1]==state,(sql,actual,rows,state)
        if state is None and sql.startswith("SELECT"):
            assert actual[3:]==(["id"],"SELECT "+str(len(rows)),[23]),(sql,actual)
            fields=client.row_description_fields(messages)
            assert len(fields)==1 and fields[0][3:]==(23,4,-1,0),(sql,fields)
    try:
        check("CREATE SCHEMA "+schema,[]);created=True
        for name in ("bits","empty_bits"):
            table=schema+"."+name
            check("CREATE TABLE "+table+"(id int PRIMARY KEY,t text,i int,v varbit)",[])
            check("CREATE INDEX "+name+"_text_idx ON "+table+"(t)",[])
        table=schema+".bits"
        check("INSERT INTO "+table+" VALUES "+
              "(1,'01',1,B'01'),(2,'1',1,B'1'),(3,'',0,B''),(4,NULL,NULL,NULL)",[])
        for name in ("bits","empty_bits"):
            table=schema+"."+name
            for column in ("id","t","i"):
                for literal in ("B'01'","X'1'","B''","X''"):
                    for operation in ("=","<>","<","<=",">",">="):
                        sql="SELECT id FROM "+table+" WHERE "+column+operation+literal+" ORDER BY id"
                        controls+=1
                        try:check(sql,[],"42883")
                        except AssertionError as error:
                            failures.append(str(error));print("[BIT INDEX LITERAL FAIL] "+str(error),flush=True)
            for predicate,rows in (("id=1",[["1"]]),("t='01'",[["1"]]),("t=''",[["3"]]),
                                   ("v=B'01'",[["1"]]),("v IN (B'',B'01')",[["1"],["3"]]),
                                   ("v NOT IN (B'',NULL)",[])):
                sql="SELECT id FROM "+table+" WHERE "+predicate+" ORDER BY id"
                controls+=1
                try:check(sql,rows if name=="bits" else [])
                except AssertionError as error:
                    failures.append(str(error));print("[BIT INDEX LITERAL FAIL] "+str(error),flush=True)
        assert not failures,"%d/%d complete indexed controls failed"%(len(failures),controls)
        print("[BIT INDEX LITERAL PROTOCOL] all %d primary/secondary/nonindexed/empty signature and value controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
