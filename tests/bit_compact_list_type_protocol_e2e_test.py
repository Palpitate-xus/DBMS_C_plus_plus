#!/usr/bin/env python3
"""Typed list members are admitted before first-match, empty and NULL scans."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args();root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("compact_list_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server["sock"]
    schema="compact_list_"+uuid.uuid4().hex[:12];created=False;controls=0;failures=[]
    def query(sql):return runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
    cases=[("b IN(B'01',1)","42883"),("b NOT IN(B'01',1)","42883"),
           ("b IN(1,B'01')","42883"),("b NOT IN(1,B'01')","42883"),
           ("b BETWEEN B'01' AND 1","42883"),("b NOT BETWEEN B'01' AND 1","42883"),
           ("b BETWEEN 1 AND B'01'","42883"),("b NOT BETWEEN 1 AND B'01'","42883"),
           ("b IN(B'01','102')","22P02"),("b NOT IN(B'01','102')","22P02"),
           ("b IN(B'01',NULL,1)","42883"),("b NOT IN(B'01',NULL,1)","42883")]
    try:
        assert query("CREATE SCHEMA "+schema)[1] is None;created=True
        for name in ("populated","empty_bits","null_bits"):
            assert query("CREATE TABLE "+schema+"."+name+"(id int,b bit(2))")[1] is None
        assert query("INSERT INTO "+schema+".populated VALUES(1,B'01')")[1] is None
        assert query("INSERT INTO "+schema+".null_bits VALUES(1,NULL)")[1] is None
        for name in ("populated","empty_bits","null_bits"):
            for predicate,state in cases:
                controls+=1;sql="SELECT id FROM "+schema+"."+name+" WHERE "+predicate
                result=query(sql)
                if result[0]!=[] or result[1]!=state or result[4] is not None:
                    error=(sql,result,state);failures.append(str(error))
                    print("[BIT COMPACT LIST TYPE FAIL] "+str(error),flush=True)
        assert not failures,"%d/%d complete member admission controls failed"%(len(failures),controls)
        print("[BIT COMPACT LIST TYPE PROTOCOL] all %d complete first-hit/empty/NULL/member/input/SQLSTATE controls passed"%controls)
    finally:
        try:
            if created:assert query("DROP SCHEMA "+schema+" CASCADE")[1] is None
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
