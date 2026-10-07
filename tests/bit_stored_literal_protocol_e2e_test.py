#!/usr/bin/env python3
"""Storage predicate adapters decode actual BIT and hexadecimal literals."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    arguments=argparse.ArgumentParser()
    arguments.add_argument("--reference18",action="store_true")
    options=arguments.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_stored_runner",root/"tests/compat/pg_diff_runner.py")
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
    schema="bit_storage_"+uuid.uuid4().hex[:12]
    created=False

    def check(sql,expected,oids,state=None):
        decoded=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        assert decoded[0]==expected and decoded[1]==state and decoded[5]==oids,(sql,decoded,expected,oids,state)

    try:
        check("CREATE SCHEMA "+schema,[],[]);created=True
        check("CREATE TABLE "+schema+".bits(id int,v varbit)",[],[])
        check("INSERT INTO "+schema+".bits VALUES "
              "(1,B'1'),(2,B'01'),(3,B'001'),(4,B'0'),(5,B'00'),(6,B''),"
              "(7,B'01'),(8,B'0001'),(9,NULL)",[],[])
        for predicate,expected in [
                ("v=B'01'",["2","7"]),("v=b'01'",["2","7"]),
                ("v=X'1'",["8"]),("v=x'1'",["8"]),
                ("v=B''",["6"]),("v=X''",["6"]),
                ("v<B'01'",["3","4","5","6","8"]),
                ("v>B'01'",["1"]),("v<=B'01'",["2","3","4","5","6","7","8"]),
                ("v>=B'01'",["1","2","7"]),
                ("v IN(B'01',X'1')",["2","7","8"]),
                ("v IN(B'01',NULL)",["2","7"]),
                ("v BETWEEN B'0001' AND B'01'",["2","3","7","8"]),
                ("v IS NULL",["9"]),("v IS NOT NULL",["1","2","3","4","5","6","7","8"])]:
            check("SELECT id FROM "+schema+".bits WHERE "+predicate+" ORDER BY id",
                  [[value] for value in expected],[23])
        for literal in ["B'02'","X'g'"]:
            check("SELECT id FROM "+schema+".bits WHERE v="+literal,[],[],"22P02")
        check("SELECT count(*) FROM "+schema+".bits",[["9"]],[20])
        print("[BIT STORED LITERAL PROTOCOL] all 21 statement controls passed")
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[],[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
