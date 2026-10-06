#!/usr/bin/env python3
"""Ordinary CASE uses whole-query binding and an owned typed AST consumer."""
import importlib.util
from pathlib import Path
import socket
import sys

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('ordinary_case_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('ORDINARY_CASE',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected));print('ORDINARY_CASE_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)
    def value(sql,rows,oids):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],rows);check(sql+' OIDs',result[5],oids)
    try:
        if reference18:runner.verify_reference_version(client,sock)
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
            for sql in ('SELECT '+expr+';','SELECT '+expr+' WHERE false;','VALUES('+expr+');'):
                check(sql,query(sql)[1],'42883')
        for expr,wanted in (
            ("CASE 1 WHEN '1' THEN 1 ELSE 2 END",'1'),
            ('CASE 16777217 WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END','2'),
            ('CASE CAST(16777217 AS NUMERIC) WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END','2'),
            ('CASE CAST(9007199254740993 AS BIGINT) WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END','1'),
            ('CASE CAST(1e20 AS REAL) WHEN CAST(1e20 AS DOUBLE PRECISION) THEN 1 ELSE 2 END','2'),
        ):
            value('SELECT '+expr+';',[[wanted]],[23]);value('VALUES('+expr+');',[[wanted]],[23])
        value('SELECT CASE 1 WHEN 1 THEN CAST(NULL AS BIGINT) ELSE CAST(0 AS BIGINT) END;',[[None]],[20])
        for sql,label in (
            ('SELECT CASE 1 WHEN 1 THEN 1 ELSE 2 END;','case'),
            ('SELECT CAST(CASE 1 WHEN 1 THEN 1 ELSE 2 END AS BIGINT);','int8'),
            ('SELECT (CASE 1 WHEN 1 THEN 1 ELSE 2 END)::INTEGER;','int4'),
            ("SELECT CASE 1 WHEN 1 THEN CAST('a' AS TEXT) ELSE 'b' END COLLATE \"C\";",'case'),
            ('SELECT CAST(abs(CASE 1 WHEN 1 THEN 1 ELSE 2 END) AS BIGINT);','abs'),
        ):check('default projection name '+sql,query(sql)[3],[label])
        value('SELECT CASE 1 WHEN 1 THEN 2 ELSE 3 END ORDER BY "case";',[['2']],[23])
        value('VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST(2147483648 AS BIGINT));',[['1'],['2147483648']],[20])
        value("VALUES('1'),(CASE 1 WHEN 1 THEN 2 ELSE 3 END);",[['1'],['2']],[23])
        check('VALUES unknown input conversion',query("VALUES('bad'),(CASE 1 WHEN 1 THEN 2 ELSE 3 END);")[1],'22P02')
        check('VALUES incompatible categories',query("VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST('2' AS TEXT));")[1],'42804')
        setup('CREATE TEMP TABLE case_source("V" BIGINT,v INT,n INT);')
        setup('INSERT INTO case_source VALUES(9007199254740993,16777217,NULL);')
        value('SELECT CASE "R"."V" WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END, CASE "R".v WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END FROM case_source AS "R";',[['1','2']],[23,23])
        check('empty input still binds operator',query('SELECT CASE v WHEN true THEN 1 ELSE 2 END FROM case_source WHERE false;')[1],'42883')
        value('SELECT v FROM case_source WHERE CASE v WHEN 16777217 THEN true ELSE false END;',[['16777217']],[23])
        value('SELECT CASE v WHEN 16777217 THEN CAST(2147483648 AS BIGINT) ELSE 0 END AS "K" FROM case_source ORDER BY "K";',[['2147483648']],[20])
        setup('CREATE TEMP SEQUENCE case_switch_once;')
        expr="CASE CAST(nextval('case_switch_once') AS INT) WHEN CAST(0 AS NUMERIC) THEN 0 WHEN CAST(1 AS BIGINT) THEN 1 ELSE 2 END"
        value('SELECT '+expr+' WHERE false;',[],[23]);check('empty switch effects',query("SELECT currval('case_switch_once');")[1],'55000')
        value('SELECT '+expr+';',[['1']],[23]);value("SELECT currval('case_switch_once');",[['1']],[20])
        setup('CREATE TEMP SEQUENCE case_runtime_null;')
        value("SELECT CASE n WHEN CAST(nextval('case_runtime_null') AS INT) THEN 1 WHEN CAST(nextval('case_runtime_null') AS INT) THEN 2 ELSE 3 END FROM case_source;",[['3']],[23])
        value("SELECT currval('case_runtime_null');",[['2']],[20])
        setup('CREATE TEMP SEQUENCE case_constant_null;')
        value("SELECT CASE CAST(NULL AS INT) WHEN CAST(nextval('case_constant_null') AS INT) THEN 1 ELSE 3 END;",[['3']],[23])
        check('constant planner effects',query("SELECT currval('case_constant_null');")[1],'55000')
        setup('CREATE TEMP SEQUENCE case_early_projection;')
        check('prepare every target before projection',query("SELECT nextval('case_early_projection'),CASE 1 WHEN true THEN 1 ELSE 2 END;")[1],'42883')
        check('early projection not executed',query("SELECT currval('case_early_projection');")[1],'55000')
        setup('CREATE TEMP TABLE case_writes(id INT);')
        setup('CREATE TEMP SEQUENCE case_bad_cte;')
        sql="WITH w AS (INSERT INTO case_writes VALUES(CAST(nextval('case_bad_cte') AS INT)) RETURNING id) SELECT CASE 1 WHEN true THEN 1 ELSE 2 END FROM w;"
        check('whole binding before writing CTE',query(sql)[1],'42883')
        check('writing CTE has no irreversible effect',query("SELECT currval('case_bad_cte');")[1],'55000')
        value('SELECT id FROM case_writes;',[],[23])
        value('WITH r AS (VALUES(1)) SELECT CASE column1 WHEN 1 THEN CAST(2147483648 AS BIGINT) ELSE 0 END FROM r;',[['2147483648']],[20])
        setup('CREATE TEMP SEQUENCE case_unused_cte;')
        value("WITH w AS (INSERT INTO case_writes VALUES(CAST(nextval('case_unused_cte') AS INT)) RETURNING id) SELECT CASE 1 WHEN 1 THEN 4 ELSE 5 END LIMIT 0;",[],[23])
        value("SELECT currval('case_unused_cte');",[['1']],[20]);value('SELECT id FROM case_writes;',[['1']],[23])
        # Genuine typed sources keep their own occurrences, NULLs and read
        # demand. None of these cases may be replaced by a flattened SQL
        # string or skipped as an unsupported legacy source shape.
        setup('CREATE TEMP TABLE case_read_rows(id INT,"V" BIGINT,v INT,n INT);')
        setup('INSERT INTO case_read_rows VALUES(1,9007199254740993,16777217,NULL),(2,NULL,0,7);')
        setup('CREATE TEMP TABLE case_read_keys(id INT);')
        setup('INSERT INTO case_read_keys VALUES(1),(3);')
        setup('CREATE VIEW case_read_view AS SELECT id,"V",v,n FROM case_read_rows;')
        value('SELECT CASE "R"."V" WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END,CASE "R".v WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END,n FROM case_read_view AS "R" ORDER BY id;', [['1','2',None],['2','2','7']],[23,23,23])
        value('SELECT CASE name WHEN \'statement_timeout\' THEN unit ELSE \'bad\' END FROM pg_settings WHERE name=\'statement_timeout\';', [['ms']],[25])
        value('SELECT CASE "S".name WHEN \'lc_monetary\' THEN "S".unit ELSE \'bad\' END FROM pg_catalog.pg_settings AS "S" WHERE "S".name=\'lc_monetary\';', [[None]],[25])
        value('WITH r AS (SELECT a."V" AS wide,b.v FROM case_read_rows a JOIN case_read_rows b ON a.id=b.id) SELECT CASE wide WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END,CASE v WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END FROM r ORDER BY v DESC;', [['1','2'],['2','2']],[23,23])
        value('SELECT CASE d.v WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END FROM (SELECT a.id,b.v FROM case_read_rows a JOIN case_read_rows b ON a.id=b.id) d ORDER BY d.id;', [['2'],['2']],[23])
        value('SELECT id,CASE id WHEN 3 THEN CAST(NULL AS BIGINT) ELSE a."V" END,CASE b.id WHEN 1 THEN 9 ELSE 0 END FROM case_read_rows a FULL JOIN case_read_keys b USING(id) ORDER BY id;', [['1','9007199254740993','9'],['2',None,'0'],['3',None,'0']],[23,20,23])
        value('SELECT a.*,CASE b.id WHEN 1 THEN true ELSE false END FROM case_read_rows a LEFT JOIN case_read_keys b ON a.id=b.id ORDER BY a.id;', [['1','9007199254740993','16777217',None,'t'],['2',None,'0','7','f']],[23,20,23,23,16])
        check('joined empty input still binds',query('SELECT CASE a.v WHEN true THEN 1 ELSE 2 END FROM case_read_rows a JOIN case_read_keys b ON a.id=b.id WHERE false;')[1],'42883')
        setup('CREATE TEMP SEQUENCE case_join_no_demand;')
        value("SELECT CASE a.id WHEN 1 THEN 1 ELSE 2 END FROM case_read_rows a JOIN case_read_keys b ON nextval('case_join_no_demand')>0 WHERE false;",[],[23])
        check('false qualification never opens volatile JOIN',query("SELECT currval('case_join_no_demand');")[1],'55000')
        setup('CREATE TEMP SEQUENCE case_view_demand;')
        setup("CREATE VIEW case_effect_view AS SELECT id,CAST(nextval('case_view_demand') AS INT) AS tick FROM case_read_rows;")
        value('SELECT CASE tick WHEN 1 THEN 1 ELSE 2 END FROM case_effect_view LIMIT 0;',[],[23])
        check('LIMIT0 never opens volatile view',query("SELECT currval('case_view_demand');")[1],'55000')
        value('SELECT CASE tick WHEN 1 THEN 1 ELSE 2 END FROM case_effect_view LIMIT 1;',[['1']],[23])
        value("SELECT currval('case_view_demand');",[['1']],[20])
        setup('CREATE TEMP SEQUENCE case_join_row_demand;')
        value("SELECT CASE a.id WHEN 1 THEN 1 ELSE 2 END FROM case_read_rows a JOIN case_read_keys b ON nextval('case_join_row_demand')>0 LIMIT 1;",[['1']],[23])
        value("SELECT currval('case_join_row_demand');",[['1']],[20])
        setup("CREATE VIEW case_unknown_view AS SELECT NULL AS n,'' AS e;")
        value("SELECT CASE n WHEN 'x' THEN 'wrong' ELSE e END,n FROM case_unknown_view;",[['',None]],[25,25])
        assert not failures,'%d ordinary CASE failures: %r'%(len(failures),failures)
        print('[ORDINARY CASE '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
