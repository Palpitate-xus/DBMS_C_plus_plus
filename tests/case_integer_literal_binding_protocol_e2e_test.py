#!/usr/bin/env python3
"""Wide integer constants retain their true static types in prepared CASE."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('literal_width_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('CASE_LITERAL_WIDTH',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('CASE_LITERAL_WIDTH_FAILURE',failures[-1],flush=True)
    try:
        if reference18:runner.verify_reference_version(client,sock)
        for expression in (
            'CASE 2147483648 WHEN 2147483648 THEN 1 ELSE 2 END',
            'CASE CAST(2147483648 AS BIGINT) WHEN 2147483648 THEN 1 ELSE 2 END',
            'CASE 2147483648 WHEN CAST(2147483648 AS BIGINT) THEN 1 ELSE 2 END',
            'CASE 9223372036854775807 WHEN 9223372036854775807 THEN 1 ELSE 2 END',
            'CASE 9223372036854775808 WHEN 9223372036854775808 THEN 1 ELSE 2 END',
            'CASE -2147483649 WHEN CAST(-2147483649 AS BIGINT) THEN 1 ELSE 2 END',
            'CASE -9223372036854775808 WHEN CAST(-9223372036854775808 AS BIGINT) THEN 1 ELSE 2 END',
            'CASE CAST(-9223372036854775808 AS BIGINT) WHEN -9223372036854775808 THEN 1 ELSE 2 END',
            'CASE -(-2147483648) WHEN 2147483648 THEN 1 ELSE 2 END',
            'CASE -(-9223372036854775808) WHEN 9223372036854775808 THEN 1 ELSE 2 END'):
            for prefix,suffix,rows in (('SELECT ',';',[['1']]),('VALUES(',');',[['1']]),('SELECT ',' WHERE false;',[])):
                sql=prefix+expression+suffix;result=query(sql)
                check(sql+' state',result[1],None);check(sql+' rows',result[0],rows);check(sql+' OID',result[5],[23])
        for value,oid in (('2147483647',23),('2147483648',20),('9223372036854775807',20),('9223372036854775808',1700),('-2147483648',23),('-2147483649',20),('-9223372036854775808',20),('-9223372036854775809',1700)):
            for sql in ('SELECT CASE true WHEN true THEN '+value+' ELSE 0 END;','VALUES(CASE true WHEN true THEN '+value+' ELSE 0 END);'):
                result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],[[value]]);check(sql+' OID',result[5],[oid])
        result=query('SELECT CASE true WHEN true THEN -(CAST(-2147483648 AS INT)) ELSE 0 END;')
        check('explicit CAST remains runtime overflow',result[1],'22003')
        result=query('SELECT CASE true WHEN true THEN 1 ELSE -(CAST(-2147483648 AS INT)) END;')
        check('discarded explicit overflow state',result[1],None);check('discarded explicit overflow rows',result[0],[['1']])
        result=query("SELECT CASE 2147483648 WHEN CAST('2147483648' AS TEXT) THEN 1 ELSE 2 END WHERE false;")
        check('wide does not make text numerically compatible',result[1],'42883')
        for sql in ('CREATE TEMP SEQUENCE case_literal_preflight;', 'CREATE TEMP TABLE case_literal_rows(id BIGINT);'):
            check(sql,query(sql)[1],None)
        sql="WITH w AS (INSERT INTO case_literal_rows VALUES(nextval('case_literal_preflight')) RETURNING id) SELECT CASE 2147483648 WHEN true THEN 1 ELSE 2 END FROM w;"
        check('wide operator error before writing CTE',query(sql)[1],'42883')
        check('wide operator preflight effects',query("SELECT currval('case_literal_preflight');")[1],'55000')
        assert not failures,'%d literal width failures: %r'%(len(failures),failures)
        print('[CASE INTEGER LITERAL '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
