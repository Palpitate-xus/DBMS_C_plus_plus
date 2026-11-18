#!/usr/bin/env python3
"""Ordinary scalar sort children retain errors, typed keys and real sites."""
import importlib.util
import json
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('scalar_order_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=runner.start_ours(client)
    failures=[]
    def query(sql,ready=b'I'):
        messages=client.simple_query(server['sock'],sql)
        result=runner.decode_wire_result(messages,include_types=True)
        assert messages[-1]==(b'Z',ready),(sql,messages[-1])
        return result
    def ok(sql,rows=None,ready=b'I'):
        result=query(sql,ready);assert result[1] is None,(sql,result)
        if rows is not None:assert result[0]==rows,(sql,result)
        return result
    def control(label,sql,state=None,rows=None,oids=None):
        result=query(sql)
        good=result[1]==state and (state is None or result[4] is None)
        if rows is not None:good=good and result[0]==rows
        if oids is not None:good=good and result[5]==oids
        if not good:failures.append((label,sql,'expected',state,rows,oids,'actual',result))
        print('SCALAR_ORDER',label,'PASS' if good else 'FAIL',result,flush=True)
    def calls():return int(ok("SELECT currval('scalar_order_seq');")[0][0][0])
    def effect(label,before,count):
        actual=calls()-before
        if actual!=count:failures.append((label,'calls',actual,'expected',count))
    def page_plan(sql):
        result=ok('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '+sql)
        pending=[json.loads(result[0][0][0])['plan']]
        scans={}
        while pending:
            node=pending.pop()
            scans[node['nodeType']]=node
            pending.extend(node['children'])
        return scans['IntegerPrimaryKeyPage'],scans['TableScan']
    try:
        for sql in (
            'CREATE TABLE scalar_order_rows(id INT,"ID" BIGINT,payload TEXT);',
            "INSERT INTO scalar_order_rows VALUES(1,2147483648,''),(2,2147483649,'null'),(3,2147483650,NULL);",
            'CREATE TABLE scalar_order_sink(id INT);',
            'CREATE SEQUENCE scalar_order_seq START 1;',
            "CREATE FUNCTION scalar_order_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT;BEGIN INSERT INTO scalar_order_sink VALUES(arg);SELECT 'bad' INTO n;RETURN n;END; $$;",
            'CREATE FUNCTION scalar_order_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT;BEGIN INSERT INTO scalar_order_sink VALUES(arg);SELECT 1 INTO STRICT n WHERE false;RETURN n;END; $$;',
            "CREATE FUNCTION scalar_order_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO scalar_order_sink VALUES(arg);PERFORM nextval('scalar_order_seq');RETURN -arg;END; $$;",
        ):ok(sql)
        ok("SELECT nextval('scalar_order_seq');",[['1']])
        control('22P02','SELECT id FROM scalar_order_rows ORDER BY(SELECT scalar_order_cast(61));','22P02',[])
        ok('SELECT id FROM scalar_order_sink;',[])
        control('P0002','SELECT id FROM scalar_order_rows ORDER BY(SELECT scalar_order_strict(62));','P0002',[])
        ok('SELECT id FROM scalar_order_sink;',[])
        control('correlated key','SELECT d.id,d."ID" FROM scalar_order_rows d ORDER BY(SELECT -d.id);',rows=[['3','2147483650'],['2','2147483649'],['1','2147483648']],oids=[23,20])
        control('quoted alias/column','SELECT "D".id FROM scalar_order_rows "D" ORDER BY(SELECT "D"."ID") DESC;',rows=[['3'],['2'],['1']])
        control('two ancestor depths','SELECT d.id FROM scalar_order_rows d ORDER BY(SELECT(SELECT -d.id));',rows=[['3'],['2'],['1']])
        control('NULL/empty/textnull','SELECT d.id,d.payload FROM scalar_order_rows d ORDER BY(SELECT d.payload) COLLATE "C" NULLS FIRST;',rows=[['3',None],['1',''],['2','null']],oids=[23,25])
        control('typed bigint sort','SELECT d."ID" FROM scalar_order_rows d ORDER BY(SELECT d."ID") DESC LIMIT 1;',rows=[['2147483650']],oids=[20])
        control('zero child typed NULL','SELECT id FROM scalar_order_rows ORDER BY(SELECT id FROM scalar_order_rows WHERE id=99),id DESC;',rows=[['3'],['2'],['1']])
        control('cardinality','SELECT id FROM scalar_order_rows ORDER BY(SELECT id FROM scalar_order_rows);','21000',[])
        before=calls()
        control('unknown pre-effect',"SELECT scalar_order_writer(id) FROM scalar_order_rows ORDER BY CASE WHEN false THEN(SELECT no_such_order_function(nextval('scalar_order_seq'))) ELSE id END;",'42883',[])
        effect('unknown pre-effect',before,0);ok('SELECT id FROM scalar_order_sink;',[])
        control('missing column empty input','SELECT id FROM scalar_order_rows WHERE false ORDER BY(SELECT missing_order_column);','42703',[])
        control('width empty input','SELECT id FROM scalar_order_rows WHERE false ORDER BY(SELECT 1,2);','42601',[])
        before=calls()
        control('lazy CASE','SELECT id FROM scalar_order_rows ORDER BY CASE WHEN false THEN(SELECT scalar_order_writer(66)) ELSE -id END;',rows=[['3'],['2'],['1']])
        effect('lazy CASE',before,0)
        control('LIMIT zero','SELECT id FROM scalar_order_rows ORDER BY(SELECT scalar_order_cast(67)) LIMIT 0;',rows=[],oids=[23])
        control('bare NULL predicate','SELECT id FROM scalar_order_rows WHERE NULL ORDER BY(SELECT id);',rows=[],oids=[23])
        control('contextual boolean predicate',"SELECT id FROM scalar_order_rows WHERE 'true' ORDER BY(SELECT -id);",rows=[['3'],['2'],['1']])
        control('typed NULL predicate','SELECT id FROM scalar_order_rows WHERE NULL::integer ORDER BY(SELECT id);','42804',[])
        control('bad literal predicate',"SELECT id FROM scalar_order_rows WHERE 'bad' ORDER BY(SELECT id) LIMIT 0;",'22P02',[])
        before=calls()
        control('correlated writer all keys','SELECT d.id FROM scalar_order_rows d ORDER BY(SELECT scalar_order_writer(d.id)) LIMIT 1;',rows=[['3']])
        effect('correlated writer all keys',before,3);ok('DELETE FROM scalar_order_sink;')
        before=calls()
        control('uncorrelated sort memo','SELECT id FROM scalar_order_rows ORDER BY(SELECT scalar_order_writer(70)),id DESC;',rows=[['3'],['2'],['1']])
        effect('uncorrelated sort memo',before,1);ok('DELETE FROM scalar_order_sink;')
        before=calls()
        control('projection alias sort slot','SELECT(SELECT scalar_order_writer(d.id)) AS value FROM scalar_order_rows d ORDER BY value;',rows=[['-3'],['-2'],['-1']],oids=[23])
        effect('projection alias sort slot',before,3);ok('DELETE FROM scalar_order_sink;')
        before=calls()
        control('projection ordinal sort slot','SELECT(SELECT scalar_order_writer(d.id)) AS value FROM scalar_order_rows d ORDER BY 1;',rows=[['-3'],['-2'],['-1']])
        effect('projection ordinal sort slot',before,3);ok('DELETE FROM scalar_order_sink;')
        before=calls()
        control('two SELECT sites','SELECT(SELECT scalar_order_writer(71)),(SELECT scalar_order_writer(71)) FROM scalar_order_rows ORDER BY id;',rows=[['-71','-71']]*3,oids=[23,23])
        effect('two SELECT sites',before,2);ok('DELETE FROM scalar_order_sink;')
        before=calls()
        control('deferred volatile projection','SELECT scalar_order_writer(id) FROM scalar_order_rows d ORDER BY(SELECT d.id) DESC LIMIT 1;',rows=[['-3']])
        effect('deferred volatile projection',before,1);ok('DELETE FROM scalar_order_sink;')
        ok('BEGIN;',ready=b'T');ok('INSERT INTO scalar_order_sink VALUES(90);',ready=b'T');ok('SAVEPOINT keep;',ready=b'T')
        result=query('SELECT id FROM scalar_order_rows ORDER BY(SELECT scalar_order_strict(91));',b'E')
        if result[1]!='P0002':failures.append(('explicit ORDER error',result))
        ok('ROLLBACK TO keep;',ready=b'T');ok('SELECT id FROM scalar_order_sink;',[['90']],b'T');ok('ROLLBACK;')
        ok('SELECT id FROM scalar_order_sink;',[])
        ok('CREATE TABLE scalar_order_pk_int (k INTEGER PRIMARY KEY, payload TEXT);')
        ok("INSERT INTO scalar_order_pk_int VALUES (10,'ten'),(-2,NULL),(2,''),(0,'zero'),"
           "(-100,'negative'),('2147483647','max'),('-2147483648','min');")
        ascending_page='SELECT k,payload FROM scalar_order_pk_int ORDER BY k LIMIT 4 OFFSET 1;'
        control('integer primary key ascending page',
                ascending_page,
                rows=[['-100','negative'],['-2',None],['0','zero'],['2','']],oids=[23,25])
        selected,scan=page_plan(ascending_page)
        assert selected['usedPrimaryKeyPage']=='true' and selected['orderedIndex']=='true',selected
        assert selected['indexEntries']==selected['heapRowsFetched']=='5',selected
        assert int(selected['indexNodesVisited'])==1,selected
        assert scan.get('actualRows',0)==0,scan
        for label,sql,rows,fetched in (
            ('bounded integer page with offset',
             'SELECT p.k,p.payload FROM scalar_order_pk_int p '
             'WHERE -100 <= p.k AND p.k < 10 ORDER BY p.k LIMIT 2 OFFSET 1;',
             [['-2',None],['0','zero']],3),
            ('descending integer page from reverse comparison',
             'SELECT k,payload FROM scalar_order_pk_int WHERE 10 > k AND k > -100 ORDER BY k DESC LIMIT 2;',
             [['2',''],['0','zero']],2),
            ('short integer page',
             'SELECT k,payload FROM scalar_order_pk_int WHERE k > 2147483646 ORDER BY k LIMIT 3;',
             [['2147483647','max']],1),
            ('contradictory integer range',
             'SELECT k,payload FROM scalar_order_pk_int WHERE k > 10 AND k < -2 ORDER BY k LIMIT 3;',
             [],0),
        ):
            control(label,sql,rows=rows,oids=[23,25])
            selected,scan=page_plan(sql)
            assert selected['indexKeyRange']=='true' and selected['usedPrimaryKeyPage']=='true',selected
            assert int(selected['indexEntries'])==int(selected['heapRowsFetched'])==fetched,selected
            assert scan.get('actualRows',0)==0,scan
        before=calls()
        control('bounded integer page evaluates offset rows',
                'SELECT scalar_order_writer(k) FROM scalar_order_pk_int '
                'WHERE k >= -2 AND k <= 10 ORDER BY k LIMIT 1 OFFSET 1;',rows=[['0']])
        effect('bounded integer page evaluates offset rows',before,2)
        ok('SELECT id FROM scalar_order_sink ORDER BY id;',[['-2'],['0']])
        ok('DELETE FROM scalar_order_sink;')
        control('fractional primary key equality',
                'SELECT k FROM scalar_order_pk_int WHERE k=2.5;',rows=[],oids=[23])
        control('out-of-range primary key equality',
                'SELECT k FROM scalar_order_pk_int WHERE k=9223372036854775808;',rows=[],oids=[23])
        control('integer primary key descending page',
                'SELECT p.k,p.payload FROM scalar_order_pk_int AS p ORDER BY p.k DESC LIMIT 3 OFFSET 1;',
                rows=[['10','ten'],['2',''],['0','zero']],oids=[23,25])
        control('integer primary key zero limit',
                'SELECT k,payload FROM scalar_order_pk_int ORDER BY k LIMIT 0 OFFSET 3;',
                rows=[],oids=[23,25])
        past_end='SELECT k,payload FROM scalar_order_pk_int ORDER BY k DESC LIMIT 3 OFFSET 8;'
        control('integer primary key page past end',past_end,rows=[],oids=[23,25])
        selected,scan=page_plan(past_end)
        assert selected['usedPrimaryKeyPage']=='false' and scan['actualRows']==7,(selected,scan)
        ok('CREATE TABLE scalar_order_pk_big (k BIGINT, payload TEXT, PRIMARY KEY (k));')
        ok("INSERT INTO scalar_order_pk_big VALUES ('9223372036854775807','max'),(11,'eleven'),"
           "('-9223372036854775808','min'),(2,'two'),(-11,'negative');")
        control('table primary key bigint descending page',
                'SELECT k,payload FROM scalar_order_pk_big ORDER BY k DESC LIMIT 4;',
                rows=[['9223372036854775807','max'],['11','eleven'],['2','two'],['-11','negative']],oids=[20,25])
        control('table primary key bigint expression sort',
                'SELECT k,payload FROM scalar_order_pk_big ORDER BY k+0 LIMIT 3;',
                rows=[['-9223372036854775808','min'],['-11','negative'],['2','two']],oids=[20,25])
        assert not failures,failures
        print('[ORDINARY SCALAR ORDER PROTOCOL] passed')
    finally:runner.stop_ours(server)


if __name__=='__main__':main()
