#!/usr/bin/env python3
"""Simple CASE equality is prepared before row demand or expression effects."""
import importlib.util
from pathlib import Path
import socket
import sys

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('simple_case_runner',root/'tests/compat/pg_diff_runner.py')
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
        print('SIMPLE_CASE_PREPARED',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('SIMPLE_CASE_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18: runner.verify_reference_version(client,sock)
        setup('CREATE TEMP TABLE case_target(v INT);')
        bad=(
            'CASE 1 WHEN true THEN 1 ELSE 2 END',
            "CASE '1' WHEN 1 THEN 1 ELSE 2 END",
            "CASE 1 WHEN CAST('1' AS TEXT) THEN 1 ELSE 2 END",
            "CASE DATE '2026-10-06' WHEN INTERVAL '1 day' THEN 1 ELSE 2 END",
            'CASE CAST(NULL AS INT) WHEN true THEN 1 ELSE 2 END',
            "CASE 1 WHEN true THEN CAST('bad' AS INT) ELSE 2 END",
            "CASE 1 WHEN true THEN 1 ELSE CAST('bad' AS INT) END",
            'CASE 1 WHEN 1 THEN 1 WHEN true THEN 2 ELSE 3 END',
            "CASE true WHEN CAST('true' AS TEXT) THEN 1 ELSE 2 END",
        )
        for expr in bad:
            for source in ('SELECT '+expr+' WHERE false','VALUES('+expr+')'):
                check(source,query('INSERT INTO case_target '+source+';')[1],'42883')
                check(source+'-no-write',query('SELECT v FROM case_target;')[0],[])
                setup('DELETE FROM case_target;')
        positives=(
            ("CASE 1 WHEN '1' THEN 1 ELSE 2 END",'1'),
            ("CASE '1' WHEN '1' THEN 1 ELSE 2 END",'1'),
            ("CASE DATE '2026-10-06' WHEN '2026-10-06' THEN 1 ELSE 2 END",'1'),
            ("CASE DATE '2026-10-06' WHEN TIMESTAMP '2026-10-06 00:00:00' THEN 1 ELSE 2 END",'1'),
            ('CASE 1 WHEN CAST(1 AS BIGINT) THEN 1 ELSE 2 END','1'),
            ('CASE 16777217 WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END','2'),
            ('CASE CAST(16777217 AS NUMERIC) WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END','2'),
            ('CASE CAST(9007199254740993 AS BIGINT) WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END','1'),
            ('CASE CAST(1e20 AS REAL) WHEN CAST(1e20 AS DOUBLE PRECISION) THEN 1 ELSE 2 END','2'),
            ("CASE CAST('a ' AS CHAR(2)) WHEN CAST('a ' AS TEXT) THEN 1 ELSE 2 END",'2'),
            ("CASE CAST('ab' AS CHAR(2)) WHEN CAST('ac' AS CHAR(2)) THEN 1 ELSE 2 END",'2'),
            ("CASE CAST('ab' AS CHAR(2)) WHEN CAST('ac' AS VARCHAR) THEN 1 ELSE 2 END",'2'),
            ("CASE CAST('1.40129846e-45' AS REAL) WHEN CAST('1.40129846e-45' AS REAL) THEN 1 ELSE 2 END",'1'),
            ('CASE CAST(NULL AS BIGINT) WHEN CAST(NULL AS REAL) THEN 1 ELSE 2 END','2'),
            ("CASE CAST('NaN' AS REAL) WHEN CAST('NaN' AS DOUBLE PRECISION) THEN 1 ELSE 2 END",'1'),
        )
        for expr,expected in positives:
            for source in ('SELECT '+expr,'VALUES('+expr+')'):
                check(source+'-success',query('INSERT INTO case_target '+source+';')[1],None)
                result=query('SELECT v FROM case_target;')
                check(source+'-value',result[0],[[expected]])
                check(source+'-type',result[5],[23])
                setup('DELETE FROM case_target;')
        setup('CREATE TEMP SEQUENCE case_switch_sequence;')
        expr="CASE CAST(nextval('case_switch_sequence') AS INTEGER) WHEN CAST(0 AS NUMERIC) THEN 0 WHEN CAST(1 AS BIGINT) THEN 1 ELSE 2 END"
        setup('INSERT INTO case_target SELECT '+expr+' WHERE false;')
        check('empty demand has no effect',query("SELECT currval('case_switch_sequence');")[1],'55000')
        setup('INSERT INTO case_target SELECT '+expr+';')
        check('switch evaluated once',query('SELECT v FROM case_target;')[0],[['1']])
        check('switch effect count',query("SELECT nextval('case_switch_sequence');")[0],[['2']])
        setup('DELETE FROM case_target;')
        setup('CREATE TEMP SEQUENCE case_invalid_sequence;')
        check('invalid equality before switch effect',query("INSERT INTO case_target SELECT CASE CAST(nextval('case_invalid_sequence') AS INTEGER) WHEN true THEN 1 ELSE 2 END;")[1],'42883')
        check('invalid switch not executed',query("SELECT currval('case_invalid_sequence');")[1],'55000')
        check('invalid insert empty',query('SELECT v FROM case_target;')[0],[])
        setup('CREATE TEMP SEQUENCE case_null_rhs_sequence;')
        setup('CREATE TEMP TABLE case_null_source(v INT);')
        setup('INSERT INTO case_null_source VALUES(NULL);')
        expr="CASE v WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 1 WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 2 ELSE 3 END"
        setup('INSERT INTO case_target SELECT '+expr+' FROM case_null_source;')
        check('NULL switch does not match NULL equality',query('SELECT v FROM case_target;')[0],[['3']])
        check('NULL switch retains demanded WHEN effects',query("SELECT currval('case_null_rhs_sequence');")[0],[['2']])
        assert not failures,'%d simple CASE prepared failures: %r'%(len(failures),failures)
        print('[SIMPLE CASE PREPARED '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
