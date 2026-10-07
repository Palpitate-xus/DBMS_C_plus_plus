#!/usr/bin/env python3
"""Implicit BIT constructor elements retain complete lengths and NULL identity."""
import argparse
import importlib.util
import socket
from pathlib import Path


def main():
    args=argparse.ArgumentParser()
    args.add_argument("--reference18",action="store_true")
    options=args.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_array_runner",root/"tests/compat/pg_diff_runner.py")
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
    try:
        for expression,expected,oid in [
            ("ARRAY[B'01',B'001',B'',NULL]",'{01,001,"",NULL}',1561),
            ("ARRAY[B'01','001']",'{01,001}',1561),
            ("ARRAY[B'01'::bit(3),B'10101'::bit(5)]",'{010,10101}',1561),
            ("ARRAY[ARRAY[B'01',B'001'],ARRAY[B'10',B'101']]",'{{01,001},{10,101}}',1561),
            ("ARRAY[B'01'::varbit,B'001'::varbit]",'{01,001}',1563),
            ("B'01'::bit",'0',1560)]:
            sql="SELECT "+expression+" AS value"
            result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            assert result[1] is None and result[0]==[[expected]] and result[5]==[oid],(sql,result)
        print("[BIT ARRAY CONSTRUCTOR PROTOCOL] six complete value/OID/NULL/typmod controls passed")
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=="__main__":main()
