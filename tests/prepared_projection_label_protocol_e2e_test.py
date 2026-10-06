#!/usr/bin/env python3
"""Prepared CTE projection labels retain CASE/cast/function SQL identities."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('projection_label_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('PROJECTION_LABEL',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('PROJECTION_LABEL_FAILURE',failures[-1],flush=True)
    try:
        if reference18:runner.verify_reference_version(client,sock)
        assert query('CREATE TEMP TABLE projection_labels(v BIGINT);')[1] is None
        cases=(
            ('CASE 1 WHEN 1 THEN CAST(2147483648 AS BIGINT) ELSE 0 END','"case"','2147483648'),
            ('CAST(CASE 1 WHEN 1 THEN 1 ELSE 2 END AS BIGINT)','int8','1'),
            ('(CASE 1 WHEN 1 THEN 1 ELSE 2 END)::INTEGER','int4','1'),
            ('CAST(abs(CASE 1 WHEN 1 THEN 1 ELSE 2 END) AS BIGINT)','abs','1'),
            ('CASE 1 WHEN 1 THEN 1 ELSE 2 END AS "C"','"C"','1'),
        )
        for expression,column,value in cases:
            sql='WITH r AS (SELECT '+expression+') INSERT INTO projection_labels SELECT '+column+' FROM r RETURNING v;'
            result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],[[value]]);check(sql+' OIDs',result[5],[20])
            assert query('DELETE FROM projection_labels;')[1] is None
        assert not failures,'%d projection label failures: %r'%(len(failures),failures)
        print('[PROJECTION LABEL '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
