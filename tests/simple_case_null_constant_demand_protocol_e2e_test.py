#!/usr/bin/env python3
"""Constant-NULL strict CASE planning must not execute discarded volatile WHENs."""
import importlib.util
from pathlib import Path
import socket
import sys

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('case_null_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client(); reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('CASE_NULL_CONSTANT',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('CASE_NULL_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18: runner.verify_reference_version(client,sock)
        setup('CREATE TEMP TABLE case_target(v INT);')
        setup('CREATE TEMP SEQUENCE case_null_rhs_sequence;')
        expr="CASE CAST(NULL AS INTEGER) WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 1 WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 2 ELSE 3 END"
        setup('INSERT INTO case_target SELECT '+expr+';')
        check('constant NULL result',query('SELECT v FROM case_target;')[0],[['3']])
        check('constant NULL planner discards volatile WHEN operands',query("SELECT currval('case_null_rhs_sequence');")[1],'55000')
        cases=[
            ("CASE CAST(NULL AS INT) WHEN CAST(nextval('SEQ') AS INT)+1 THEN 1 ELSE 3 END",'3',None,'55000'),
            ("CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN 1 ELSE 3 END",'3',None,'55000'),
            ("CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN CAST(nextval('SEQ') AS INT) ELSE 3 END",'3',None,'55000'),
            ("CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN 1 WHEN 1 THEN 2 ELSE 3 END",'2',None,'1'),
            ('CASE CAST(NULL AS INT) WHEN 1 THEN 1/0 ELSE 3 END','3',None,'55000'),
            ('CASE CAST(NULL AS INT) WHEN 1 THEN CAST(2147483648 AS INT) ELSE 3 END','3',None,'55000'),
            ('CASE CAST(NULL AS INT) WHEN 1/0 THEN 1 ELSE 3 END',None,'22012','55000'),
            ('CASE CAST(NULL AS INT) WHEN CAST(2147483648 AS INT) THEN 1 ELSE 3 END',None,'22003','55000'),
            ('CASE CAST(NULL AS INT) WHEN 1 THEN 1 ELSE 1/0 END',None,'22012','55000'),
            ('CASE 0 WHEN 0 THEN 1 ELSE 1/0 END','1',None,'55000'),
            ('CASE 0 WHEN 1 THEN 1/0 WHEN 0 THEN 2 ELSE 3 END','2',None,'55000'),
            ('CASE WHEN false THEN 1/0 ELSE 3 END','3',None,'55000'),
            ('CASE WHEN true THEN 3 ELSE 1/0 END','3',None,'55000'),
            ("CASE CAST(NULL AS INT) WHEN (SELECT CAST(nextval('SEQ') AS INT)) THEN 1 ELSE 3 END",'3',None,'55000'),
            ("CASE WHEN false AND nextval('SEQ')=1 THEN 1 ELSE 3 END",'3',None,'55000'),
        ]
        for index,(expr,value,state,effects) in enumerate(cases):
            sequence='case_demand_'+str(index);setup('CREATE TEMP SEQUENCE '+sequence+';')
            setup('DELETE FROM case_target;')
            result=query('INSERT INTO case_target SELECT '+expr.replace('SEQ',sequence)+';')
            check('planning state '+str(index),result[1],state)
            check('planning value '+str(index),query('SELECT v FROM case_target;')[0],[] if value is None else [[value]])
            count=query("SELECT currval('"+sequence+"');")
            check('planning effects '+str(index),count[1] if effects=='55000' else count[0],effects if effects=='55000' else [[effects]])
        setup('CREATE TEMP TABLE case_runtime_null(v INT);')
        setup('INSERT INTO case_runtime_null VALUES(NULL);')
        setup('CREATE TEMP SEQUENCE case_runtime_null_sequence;')
        setup('DELETE FROM case_target;')
        setup("INSERT INTO case_target SELECT CASE v WHEN CAST(nextval('case_runtime_null_sequence') AS INT) THEN 1 WHEN CAST(nextval('case_runtime_null_sequence') AS INT) THEN 2 ELSE 3 END FROM case_runtime_null;")
        check('runtime NULL result',query('SELECT v FROM case_target;')[0],[['3']])
        check('runtime NULL WHEN effects retained',query("SELECT currval('case_runtime_null_sequence');")[0],[['2']])
        setup('CREATE TEMP SEQUENCE case_false_demand_sequence;')
        result=query("INSERT INTO case_target SELECT CASE CAST(NULL AS INT) WHEN CAST(nextval('case_false_demand_sequence') AS INT) THEN 1 ELSE 3 END WHERE false;")
        check('zero-row constant demand',result[1],None)
        check('zero-row constant effects',query("SELECT currval('case_false_demand_sequence');")[1],'55000')
        check('zero-row pure WHEN error is still planned',query('INSERT INTO case_target SELECT CASE CAST(NULL AS INT) WHEN 1/0 THEN 1 ELSE 3 END WHERE false;')[1],'22012')
        check('whole binding precedes NULL pruning',query('INSERT INTO case_target SELECT CASE CAST(NULL AS INT) WHEN true THEN 1 ELSE 3 END WHERE false;')[1],'42883')
        assert not failures,'%d constant NULL CASE failures: %r'%(len(failures),failures)
        print('[CASE NULL CONSTANT '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
