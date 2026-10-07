#!/usr/bin/env python3
"""Keep the original qualified-cast legacy output-label difference visible."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    directory=Path(__file__).resolve().parent
    spec=importlib.util.spec_from_file_location('geometry_label_runner',directory/'compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference=sys.argv[1:]==['--reference18']
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[]
    try:
        for kind,value,oid in (('point','(1,2)',600),('path','[(0,0),(1,2)]',602)):
            sql="SELECT CAST('"+value+"' AS pg_catalog.\""+kind+'\");'
            result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            print('GEOMETRY CAST LABEL',sql,result,flush=True)
            if (result[0],result[1],result[3],result[5])!=([[value]],None,[kind],[oid]):
                failures.append((sql,result))
        assert not failures,failures
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
