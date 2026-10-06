#!/usr/bin/env python3
"""INSERT expression transformation precedes width, but planning/effects do not."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('width_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference18='--reference18' in sys.argv[1:]
    if reference18 and '--reference' in sys.argv[1:]:
        raise ValueError('choose PostgreSQL18.6 reference or PG17.2 diagnostic, not both')
    reference=reference18 or '--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('INSERT_WIDTH',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('INSERT_WIDTH_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18:
            runner.verify_reference_version(client,sock)
        elif reference:
            assert query('SHOW server_version_num;')[0]==[['170002']]
        setup('CREATE TEMP TABLE width_rows(id INT,v INTERVAL);')
        for source,state in (
            ("VALUES(CAST('1 fortnight' AS INTERVAL),2)",'22007'),
            ("VALUES('2147483648 months',missing_width_function(1))",'42883'),
            ("SELECT CAST('1 fortnight' AS INTERVAL),2",'22007'),
            ("SELECT '2147483648 months',missing_width_function(1)",'42883'),
            ("VALUES('2147483648 months',2)",'42601'),
            ("VALUES(CAST(2147483648 AS INTEGER),2)",'42601'),
            ("VALUES(1/0,2)",'42601'),
            ("VALUES(INTERVAL '1 day',2)",'42601'),
            ("VALUES('2147483648 months'),(missing_width_function(1),2)",'22015'),
            ("VALUES('1 day'),(CAST('1 fortnight' AS INTERVAL),2)",'22007'),
            ("VALUES(CAST('1 fortnight' AS INTERVAL),2) (3)",'42601'),
        ):
            check(source,query('INSERT INTO width_rows(v) '+source+';')[1],state)
            check(source+'-no-write',query('SELECT id FROM width_rows;')[0],[])
        setup('CREATE TEMP SEQUENCE width_sequence;')
        sql="INSERT INTO width_rows VALUES(nextval('width_sequence'),'1 day'),(2,'1 day',3);"
        check('no-plan-effects',query(sql)[1],'42601')
        check('sequence-not-called',query("SELECT currval('width_sequence');")[1],'55000')
        setup("INSERT INTO width_rows(v) VALUES('1 us');")
        result=query('SELECT id,v FROM width_rows;')
        check('valid-short-implicit-default',result[0],[[None,'00:00:00.000001']])
        check('valid-types',result[5],[23,1186])
        assert not failures, '%d INSERT width assertions failed: %r' % (len(failures),failures)
        print('[INSERT WIDTH PRIORITY '+('PG18.6 REFERENCE' if reference18 else 'PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
