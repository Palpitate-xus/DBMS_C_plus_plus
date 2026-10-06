#!/usr/bin/env python3
"""Original analysis-phase input-conversion regression, kept unchanged."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('unused_cast_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    reference=reference18 or '--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('UNUSED_INPUT_CAST',sql,result,flush=True);return result
    try:
        if reference:assert query('SHOW server_version_num')[0]==[[('180006' if reference18 else '170002')]]
        assert query('CREATE TEMP TABLE unused_input_rows(id INT)')[1] is None
        for source,state,rows in [("CAST('bad' AS INT)",'22P02',[]),('1/0',None,[['1']])]:
            assert query('DELETE FROM unused_input_rows')[1] is None
            result=query('WITH unused AS(SELECT '+source+' AS value) INSERT INTO unused_input_rows VALUES(1) RETURNING id')
            if result[1]!=state or result[0]!=rows:failures.append((source,state,rows,result))
        assert not failures,failures
        print('[WITH UNUSED INPUT CONVERSION] passed')
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
