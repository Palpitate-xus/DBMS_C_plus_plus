#!/usr/bin/env python3
"""CASE inherits a strong ELSE column/function name during pure preparation."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('else_label_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('CASE_ELSE_LABEL',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('CASE_ELSE_LABEL_FAILURE',failures[-1],flush=True)
    try:
        if reference18:runner.verify_reference_version(client,sock)
        assert query('CREATE TEMP TABLE case_else_labels(e INT,"E" INT);')[1] is None
        assert query('INSERT INTO case_else_labels VALUES(7,8);')[1] is None
        for sql,label,oid in (
            ('SELECT CASE true WHEN true THEN 1 ELSE t.e END FROM case_else_labels t;','e',23),
            ('SELECT CASE true WHEN true THEN 1 ELSE t."E" END FROM case_else_labels t;','E',23),
            ('SELECT CASE WHEN true THEN 1 ELSE abs(2) END;','abs',23),
            ('SELECT CAST(CASE WHEN true THEN 1 ELSE abs(2) END AS BIGINT);','abs',20),
            ('SELECT CASE true WHEN true THEN 1 ELSE CAST(t.e AS BIGINT) END FROM case_else_labels t;','e',20),
            ('SELECT CASE true WHEN true THEN 1 ELSE CAST(2 AS BIGINT) END;','case',20),
            ('SELECT CASE true WHEN true THEN 1 ELSE (CASE WHEN false THEN 1 ELSE t.e END) END FROM case_else_labels t;','e',23),
            ('SELECT CASE true WHEN true THEN 1 ELSE t.e END AS "Explicit" FROM case_else_labels t;','Explicit',23)):
            result=query(sql)
            check(sql+' state',result[1],None);check(sql+' rows',result[0],[['1']]);check(sql+' label',result[3],[label]);check(sql+' OID',result[5],[oid])
        assert not failures,'%d CASE ELSE label failures: %r'%(len(failures),failures)
        print('[CASE ELSE LABEL '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
