#!/usr/bin/env python3
"""Finite interval unary values, checked widths and actual runtime demand."""
import importlib.util,socket,sys,uuid
from pathlib import Path

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('unary_interval_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference='--reference18' in sys.argv[1:]
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=120);c.startup_reference(sock,u,d,pw);server={'sock':sock}
    else:server=r.start_ours(c)
    failures=[]
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('UNARY_INTERVAL_FAILURE',failures[-1],flush=True)
    def query(sql):
        if reference:c.simple_query(sock,'SAVEPOINT unary_interval_case;')
        result=r.decode_wire_result(c.simple_query(server['sock'],sql),include_types=True)
        if reference:
            if result[1] is not None:c.simple_query(sock,'ROLLBACK TO SAVEPOINT unary_interval_case;')
            c.simple_query(sock,'RELEASE SAVEPOINT unary_interval_case;')
        print('UNARY_INTERVAL',sql,result,flush=True);return result
    def rows(sql,expected,oid=1186):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],expected);check(sql+' OID',result[5],[oid])
    try:
        if reference:
            r.verify_reference_version(c,sock);schema='unary_interval_'+uuid.uuid4().hex
            setup=r.decode_wire_result(c.simple_query(sock,'BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;'),include_types=True);assert setup[1] is None,setup
        for value,expected in (
            ('1 month 2 days 03:04:05.000001','-1 mons -2 days -03:04:05.000001'),
            ('-1 month 2 days -03:04:05.000001','1 mon -2 days +03:04:05.000001'),
            ('1.5 months','-1 mons -15 days'),('1.25 seconds','-00:00:01.25'),
            ('-0.000001 seconds','00:00:00.000001'),('0 days','00:00:00'),
            ('2147483647 months','-178956970 years -7 mons'),('2147483647 days','-2147483647 days'),
            ('9223372036854775807 microseconds','-2562047788:00:54.775807'),
            ('-2147483647 months','178956970 years 7 mons'),('-2147483647 days','2147483647 days'),
            ('-9223372036854775807 microseconds','2562047788:00:54.775807')):
            rows("SELECT -INTERVAL '%s';"%value,[[expected]])
        rows('SELECT -CAST(NULL AS INTERVAL);',[[None]])
        rows("SELECT -TIME '01:02:03';",[['-01:02:03']])
        for value in ('-2147483648 months','-2147483648 days','-9223372036854775808 microseconds'):
            for suffix in ('',' WHERE false'):
                sql="SELECT -INTERVAL '%s'%s;"%(value,suffix);check(sql,query(sql)[1],'22008')
            rows("SELECT CASE WHEN true THEN INTERVAL '1 day' ELSE -INTERVAL '%s' END;"%value,[['1 day']])
        for sql in ('CREATE TEMP TABLE unary_interval_rows(id INT,v INTERVAL);',
            "INSERT INTO unary_interval_rows VALUES(1,'1 month -2 days 03:04:05'),(2,NULL);",
            'CREATE TEMP SEQUENCE unary_interval_plan;','CREATE TEMP SEQUENCE unary_interval_runtime;'):
            check(sql,query(sql)[1],None)
        rows('SELECT -v FROM unary_interval_rows ORDER BY id;',[['-1 mons +2 days -03:04:05'],[None]])
        rows('SELECT -v FROM unary_interval_rows WHERE false;',[])
        bad="WITH w AS (INSERT INTO unary_interval_rows VALUES(CAST(nextval('unary_interval_plan') AS INT),INTERVAL '1 day') RETURNING v) SELECT -INTERVAL '-2147483648 months' FROM w;"
        check('pure negation overflow before writer',query(bad)[1],'22008')
        check('no planner writer effects',query("SELECT currval('unary_interval_plan');")[1],'55000')
        runtime="SELECT -(CASE WHEN nextval('unary_interval_runtime')=1 THEN INTERVAL '-2147483648 months' ELSE INTERVAL '0 days' END);"
        check('runtime negation overflow after demanded input',query(runtime)[1],'22008')
        rows("SELECT currval('unary_interval_runtime');",[['1']],20)
        rows('SELECT count(*) FROM unary_interval_rows;',[['2']],20)
        assert not failures,'%d interval unary failures: %r'%(len(failures),failures)
        print('[UNARY INTERVAL '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:c.simple_query(sock,'ROLLBACK;');sock.close()
        else:r.stop_ours(server)
if __name__=='__main__':main()
