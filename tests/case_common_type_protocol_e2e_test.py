#!/usr/bin/env python3
"""CASE common type/input analysis cannot depend on rows or chosen branches."""
import importlib.util
from pathlib import Path
import socket
import sys

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('case_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client(); reference18='--reference18' in sys.argv[1:]
    reference=reference18 or '--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('CASE_COMMON',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('CASE_COMMON_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18: runner.verify_reference_version(client,sock)
        elif reference: assert query('SHOW server_version_num;')[0]==[['170002']]
        setup('CREATE TEMP TABLE case_rows(id INT,v INTERVAL);')
        cases=(
            ('all-unknown-text',"CASE WHEN true THEN '1 day' ELSE '2 days' END",'42804'),
            ('all-unknown-null',"CASE WHEN false THEN NULL ELSE NULL END",'42804'),
            ('unselected-input',"CASE WHEN false THEN '1 fortnight' ELSE INTERVAL '1 day' END",'22007'),
            ('unselected-range',"CASE WHEN false THEN '2147483648 months' ELSE INTERVAL '1 day' END",'22015'),
            ('selected-input',"CASE WHEN true THEN '1 fortnight' ELSE INTERVAL '1 day' END",'22007'),
            ('category-text-interval',"CASE WHEN false THEN CAST('1 day' AS TEXT) ELSE INTERVAL '1 day' END",'42804'),
            ('category-integer-interval',"CASE WHEN false THEN 1 ELSE INTERVAL '1 day' END",'42804'),
            ('nested-unknown-finalized',"CASE WHEN true THEN CASE WHEN true THEN '1 day' ELSE NULL END ELSE INTERVAL '1 day' END",'42804'),
            ('then-transform-before-else',"CASE WHEN true THEN CAST('1 fortnight' AS INTERVAL) ELSE missing_case_function(1) END",'22007'),
            ('when-transform-before-else',"CASE WHEN missing_case_function(1)=1 THEN INTERVAL '1 day' ELSE CAST('1 fortnight' AS INTERVAL) END",'42883'),
            ('searched-case-nonboolean',"CASE WHEN 1 THEN INTERVAL '1 day' ELSE INTERVAL '2 days' END",'42804'),
            ('else-conversion-before-then',"CASE WHEN false THEN '1 fortnight' WHEN false THEN INTERVAL '1 day' ELSE '2147483648 months' END",'22015'),
        )
        for label,expr,state in cases:
            for source in ('SELECT 1,'+expr+' WHERE false','VALUES(1,'+expr+')'):
                check(label+'-'+source,query('INSERT INTO case_rows '+source+';')[1],state)
                check(label+'-no-write',query('SELECT id FROM case_rows;')[0],[])
                setup('DELETE FROM case_rows;')
        setup('CREATE TEMP SEQUENCE case_effect_sequence;')
        expr="CASE WHEN false THEN '1 fortnight' ELSE INTERVAL '1 day' END"
        check('no-effects',query("INSERT INTO case_rows SELECT nextval('case_effect_sequence'),"+expr+';')[1],'22007')
        check('not-executed',query("SELECT currval('case_effect_sequence');")[1],'55000')
        check('still-empty',query('SELECT id FROM case_rows;')[0],[])
        setup('DELETE FROM case_rows;')
        expr="CASE WHEN false THEN '1 fortnight' ELSE INTERVAL '1 day' END"
        check('common-input-before-where',query('INSERT INTO case_rows SELECT 1,'+expr+' WHERE missing_case_function(1)=1;')[1],'22007')
        for source in (
            "SELECT 1,CASE WHEN true THEN '1 us' ELSE INTERVAL '2 days' END WHERE false",
            "SELECT 1,CASE WHEN true THEN NULL ELSE INTERVAL '2 days' END WHERE false",
        ):
            result=query('INSERT INTO case_rows '+source+';')
            check('valid-empty-'+source,(result[1],result[4]),(None,'INSERT 0 0'))
        setup("INSERT INTO case_rows SELECT 1,CASE WHEN true THEN '1 us' ELSE INTERVAL '2 days' END;")
        setup("INSERT INTO case_rows SELECT 2,CASE WHEN true THEN NULL ELSE INTERVAL '2 days' END;")
        setup("INSERT INTO case_rows SELECT 3,CASE 1 WHEN 1 THEN '2 us' ELSE INTERVAL '2 days' END;")
        result=query('SELECT id,v FROM case_rows ORDER BY id;')
        check('values-null-simple',result[0],[['1','00:00:00.000001'],['2',None],['3','00:00:00.000002']])
        check('result-types',result[5],[23,1186])
        setup('CREATE TEMP TABLE case_source(id INT,"V" SMALLINT);')
        setup('INSERT INTO case_source VALUES(1,1),(2,NULL);')
        setup('CREATE TEMP TABLE case_width_result(id INT,n BIGINT);')
        result=query('INSERT INTO case_width_result SELECT s.id,(CASE WHEN s.id>0 THEN s."V" ELSE CAST(9 AS BIGINT) END)+2147483647 FROM case_source s;')
        check('prepared-source-coercion',result[1],None)
        result=query('SELECT id,n FROM case_width_result ORDER BY id;')
        check('prepared-source-coercion-values',result[0],[['1','2147483648'],['2',None]])
        check('prepared-source-coercion-types',result[5],[23,20])
        result=query('INSERT INTO case_width_result VALUES(3,(CASE WHEN true THEN CAST(1 AS SMALLINT) ELSE CAST(9 AS BIGINT) END)+2147483647);')
        check('prepared-values-coercion',result[1],None)
        result=query('SELECT id,n FROM case_width_result ORDER BY id;')
        check('prepared-values-coercion-values',result[0],[['1','2147483648'],['2',None],['3','2147483648']])
        assert not failures,'%d CASE assertions failed: %r'%(len(failures),failures)
        print('[CASE COMMON TYPE '+('PG18.6 REFERENCE' if reference18 else 'PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
