#!/usr/bin/env python3
"""Prepared VALUES columns have a shared descriptor and genuine input casts."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('values_common_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('VALUES_COMMON',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('VALUES_COMMON_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)
    def value(sql,expected,oids):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],expected);check(sql+' OIDs',result[5],oids)
    try:
        if reference18:runner.verify_reference_version(client,sock)
        setup('CREATE TEMP TABLE values_wide(v BIGINT);')
        value('WITH r AS (VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST(2147483648 AS BIGINT))) INSERT INTO values_wide SELECT column1+1 FROM r RETURNING v;',[['2'],['2147483649']],[20])
        setup('DELETE FROM values_wide;')
        value('WITH r AS (VALUES(CAST(NULL AS INT)),(CAST(1 AS BIGINT))) INSERT INTO values_wide SELECT column1 FROM r RETURNING v;',[[None],['1']],[20])
        setup('DELETE FROM values_wide;')
        value("WITH r AS (VALUES('1'),(CASE 1 WHEN 1 THEN 2 ELSE 3 END)) INSERT INTO values_wide SELECT column1+1 FROM r RETURNING v;",[['2'],['3']],[20])
        setup('DELETE FROM values_wide;')
        setup('CREATE TEMP SEQUENCE values_bad_input;')
        prefix="WITH w AS (INSERT INTO values_wide VALUES(nextval('values_bad_input')) RETURNING v),r AS ("
        for body,state in (
            ("VALUES('bad'),(CASE 1 WHEN 1 THEN 1 ELSE 2 END)",'22P02'),
            ("VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST('2' AS TEXT))",'42804'),
            ("VALUES('bad'),(values_missing_function())",'42883'),
        ):
            check(body+' state',query(prefix+body+') INSERT INTO values_wide SELECT 1;')[1],state)
            check(body+' effects',query("SELECT currval('values_bad_input');")[1],'55000')
            value('SELECT v FROM values_wide;',[],[20])
        assert not failures,'%d VALUES common binding failures: %r'%(len(failures),failures)
        print('[VALUES COMMON '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
