#!/usr/bin/env python3
"""Bound quoted identifiers must not become numeric predicate constants."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("quoted_bit_column_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="quoted_bit_column_"+uuid.uuid4().hex[:12];table=schema+".bits";created=False
    controls=0

    def check(sql,expected,oids):
        nonlocal controls
        controls+=1
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        assert actual[1] is None and actual[0]==expected and actual[5]==oids,(sql,actual,expected,oids)

    try:
        check("CREATE SCHEMA "+schema,[],[]);created=True
        check("CREATE TABLE "+table+'(id int,"01" varbit)',[],[])
        check("INSERT INTO "+table+" VALUES (1,B'1'),(2,B'01'),(3,B''),(4,NULL)",[],[])
        for predicate,expected in [
            ('"01"=B\'01\'',[['2']]),('"01"<>B\'01\'',[['1'],['3']]),
            ('"01"<B\'01\'',[['3']]),('"01"=X\'1\'',[]),
            ('"01"<>X\'1\'',[['1'],['2'],['3']]),('"01"<X\'1\'',[['3']]),
            ('"01"=B\'\'',[['3']]),('"01"<>B\'\'',[['1'],['2']]),('"01"<B\'\'',[]),
            ('"01"=X\'\'',[['3']]),('"01"<>X\'\'',[['1'],['2']]),('"01"<X\'\'',[]),
            ('"01"=\'01\'',[['2']]),('"01"<>\'01\'',[['1'],['3']]),
            ('"01"<\'01\'',[['3']]),('"01"=\'1\'',[['1']]),
            ('"01"<>\'1\'',[['2'],['3']]),('"01"<\'1\'',[['2'],['3']]),
            ('"01"=\'\'',[['3']]),('"01"<>\'\'',[['1'],['2']]),('"01"<\'\'',[])]:
            check("SELECT id FROM "+table+" WHERE "+predicate+" ORDER BY id",expected,[23])
        check('SELECT d.id FROM '+table+' d WHERE d."01"=B\'01\' ORDER BY d.id',[['2']],[23])
        check('SELECT id FROM '+table+' WHERE "01" IS NULL',[['4']],[23])
        print("[QUOTED BIT COLUMN PREDICATE] all %d bound identifier/literal/empty/NULL controls passed"%controls)
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[],[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
