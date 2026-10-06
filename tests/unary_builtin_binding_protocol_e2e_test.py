#!/usr/bin/env python3
"""Builtin unary signature errors, declared widths, NULL and pure demand."""
import importlib.util,socket,sys,uuid
from pathlib import Path
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('unary_binding_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference18='--reference18' in sys.argv[1:]
    if reference18:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=120);c.startup_reference(sock,u,d,pw);server={'sock':sock}
    else:server=r.start_ours(c)
    failures=[]
    def query(sql):
        if reference18:c.simple_query(server['sock'],'SAVEPOINT unary_case;')
        result=r.decode_wire_result(c.simple_query(server['sock'],sql),include_types=True)
        if reference18:
            if result[1] is not None:c.simple_query(server['sock'],'ROLLBACK TO SAVEPOINT unary_case;')
            c.simple_query(server['sock'],'RELEASE SAVEPOINT unary_case;')
        print('UNARY_BUILTIN',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('UNARY_BUILTIN_FAILURE',failures[-1],flush=True)
    def rows(sql,expected,oid):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],expected);check(sql+' OID',result[5],[oid])
    try:
        if reference18:
            r.verify_reference_version(c,sock);schema='unary_binding_'+uuid.uuid4().hex
            setup=r.decode_wire_result(c.simple_query(sock,'BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;'),include_types=True);assert setup[1] is None,setup
        for expression in ("-(CAST('-92233720368547758.08' AS MONEY))","+CAST(1 AS MONEY)","-CAST(NULL AS MONEY)",
                           "+CAST(NULL AS TEXT)","-CAST(NULL AS BOOLEAN)","-POINT '(1,2)'","+INTERVAL '1 day'","+CAST(1 AS OID)","-ARRAY[1,2]"):
            for sql in ('SELECT '+expression+';','SELECT '+expression+' WHERE false;','VALUES('+expression+');','SELECT CASE true WHEN true THEN 1 ELSE '+expression+' END;'):
                check(sql,query(sql)[1],'42883')
        for expression,state in (("-'1'",'42725'),('-NULL','42725'),("+'bad'",'22P02')):
            check(expression,query('SELECT '+expression+' WHERE false;')[1],state)
        for kind,oid in (('SMALLINT',21),('INT',23),('BIGINT',20),('REAL',700),('DOUBLE PRECISION',701),('NUMERIC',1700)):
            for op in ('+','-'):
                rows('SELECT '+op+'CAST(NULL AS '+kind+');',[[None]],oid)
                rows('SELECT '+op+'CAST(1.5 AS '+kind+');',[['2' if kind in ('SMALLINT','INT','BIGINT') else '1.5']] if op=='+' else [['-2' if kind in ('SMALLINT','INT','BIGINT') else '-1.5']],oid)
        rows("SELECT +'1';",[['1']],701);rows('SELECT +NULL;',[[None]],701)
        for expression,value,oid in (('-2147483648','-2147483648',23),('-9223372036854775808','-9223372036854775808',20),('-(-9223372036854775808)','9223372036854775808',1700)):
            rows('SELECT '+expression+';',[[value]],oid)
        for sql in ('CREATE TEMP TABLE unary_rows(i INT,m MONEY);',"INSERT INTO unary_rows VALUES(3,'1.00'),(NULL,NULL);",'CREATE TEMP SEQUENCE unary_preflight;'):
            check(sql,query(sql)[1],None)
        rows('SELECT -i FROM unary_rows;',[['-3'],[None]],23)
        check('physical Money NULL still has no operator',query('SELECT -m FROM unary_rows WHERE false;')[1],'42883')
        check('logical Money has no operator',query('WITH r AS (SELECT m FROM unary_rows) SELECT -m FROM r WHERE false;')[1],'42883')
        sql="WITH w AS (INSERT INTO unary_rows VALUES(CAST(nextval('unary_preflight') AS INT),'1.00') RETURNING m) SELECT -m FROM w;"
        check('operator binding before writing CTE',query(sql)[1],'42883')
        check('operator preflight has no effect',query("SELECT currval('unary_preflight');")[1],'55000')
        rows('SELECT count(*) FROM unary_rows;',[['2']],20)
        assert not failures,'%d unary binding failures: %r'%(len(failures),failures)
        print('[UNARY BUILTIN '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:c.simple_query(sock,'ROLLBACK;');sock.close()
        else:r.stop_ours(server)
if __name__=='__main__':main()
