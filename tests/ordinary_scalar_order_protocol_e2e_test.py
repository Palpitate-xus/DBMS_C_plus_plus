#!/usr/bin/env python3
"""Ordinary scalar sort children retain errors, typed keys and real sites."""
import importlib.util
import json
from pathlib import Path
import struct


def parameter_range_controls(runner, client, sock, check_plan=True):
    """Exercise repeated wire Bind inputs on the previously seeded PK tables."""
    def check(messages, rows=None, oids=None, state=None, tag=None):
        result=runner.decode_wire_result(messages,include_types=True)
        assert messages[-1]==(b'Z',b'I') and result[1]==state,(messages[-1],result)
        if rows is not None:assert result[0]==rows,(result,rows)
        if oids is not None:assert result[5]==oids,(result,oids)
        if tag is not None:assert result[4]==tag,(result,tag)
        return result

    def query(sql,rows=None):
        return check(client.simple_query(sock,sql),rows)

    def prepare(name,sql,types):
        label=name.encode()+b'\0'
        body=label+sql.encode()+b'\0'+struct.pack('!H',len(types))
        body+=b''.join(struct.pack('!I',oid) for oid in types)
        sock.sendall(client.typed(b'P',body)+client.typed(b'D',b'S'+label)+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        check(messages,[])
        assert sum(kind==b'1' for kind,_ in messages)==1,messages
        described=[body for kind,body in messages if kind==b't']
        expected=struct.pack('!H',len(types))+b''.join(struct.pack('!I',oid) for oid in types)
        assert described==[expected],(name,described,expected)

    def execute(name,values,rows=None,oids=None,state=None,binary_widths=None):
        body=b'\0'+name.encode()+b'\0'
        widths=binary_widths or (0,)*len(values)
        assert len(widths)==len(values)
        body+=struct.pack('!H',len(widths))+b''.join(struct.pack('!H',bool(width)) for width in widths)
        body+=struct.pack('!H',len(values))
        for value,width in zip(values,widths):
            raw=None if value is None else (int(value).to_bytes(width,'big',signed=True)
                                           if width else str(value).encode())
            body+=struct.pack('!i',-1 if raw is None else len(raw))+(raw or b'')
        sock.sendall(client.typed(b'B',body+struct.pack('!H',0))+client.typed(b'D',b'P\0')+
                     client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        assert sum(kind==b'2' for kind,_ in messages)==(0 if state else 1),messages
        tag=f'SELECT {len(rows)}' if rows is not None and state is None else None
        result=check(messages,rows,oids,state,tag)
        if state:assert result[4] is None,result
        return result

    ranged=('SELECT p.k,p.payload FROM scalar_order_pk_int p '
            'WHERE $1 <= p.k AND p.k < $2 ORDER BY p.k LIMIT 2 OFFSET 1')
    prepare('range_page',ranged,(23,23))
    execute('range_page',(-100,10),[['-2',None],['0','zero']],[23,25])
    execute('range_page',(0,2147483647),[['2',''],['10','ten']],[23,25],binary_widths=(4,4))
    execute('range_page',(-3,1),[['0','zero']],[23,25],binary_widths=(0,4))
    for pair in ((None,10),(-100,None),(None,None),(10,-2)):
        execute('range_page',pair,[],[23,25])
    execute('range_page',('invalid',10),[],state='22P02')
    execute('range_page',(-100,10),[['-2',None],['0','zero']],[23,25])

    prepare('range_reverse','SELECT k,payload FROM scalar_order_pk_int '
            'WHERE $1 > k AND k >= -100 ORDER BY k DESC LIMIT 2',(23,))
    execute('range_reverse',(10,),[['2',''],['0','zero']],[23,25])
    prepare('range_mixed_widths','SELECT k FROM scalar_order_pk_int '
            'WHERE $1 <= k AND k < $2 ORDER BY k LIMIT 3',(21,20))
    execute('range_mixed_widths',(-2,2147483648),[['-2'],['0'],['2']],[23],binary_widths=(2,8))

    prepare('range_big','SELECT k,payload FROM scalar_order_pk_big '
            'WHERE k >= $1 AND k <= $2 ORDER BY k DESC LIMIT 2',(20,20))
    execute('range_big',(-9223372036854775808,-11),
            [['-11','negative'],['-9223372036854775808','min']],[20,25],binary_widths=(8,8))
    execute('range_big',(11,9223372036854775807),
            [['9223372036854775807','max'],['11','eleven']],[20,25])
    prepare('range_strict_max','SELECT k FROM scalar_order_pk_big WHERE k > $1 ORDER BY k LIMIT 1',(20,))
    execute('range_strict_max',(9223372036854775807,),[],[20],binary_widths=(8,))
    prepare('range_strict_min','SELECT k FROM scalar_order_pk_big WHERE $1 > k ORDER BY k LIMIT 1',(20,))
    execute('range_strict_min',(-9223372036854775808,),[],[20])

    query('CREATE TABLE scalar_order_param_small (k SMALLINT PRIMARY KEY)')
    query('INSERT INTO scalar_order_param_small VALUES (-32768),(-11),(0),(11),(32767)')
    prepare('range_small','SELECT k FROM scalar_order_param_small '
            'WHERE k >= $1 AND $2 > k ORDER BY k LIMIT 3',(21,21))
    execute('range_small',(-32768,32767),[['-32768'],['-11'],['0']],[21],binary_widths=(2,2))

    decimal=('SELECT k FROM scalar_order_pk_int WHERE k > $1 ORDER BY k LIMIT 2')
    prepare('range_numeric',decimal,(1700,))
    execute('range_numeric',('2.5',),[['10'],['2147483647']],[23])
    prepare('range_or','SELECT k FROM scalar_order_pk_int WHERE k < $1 OR k > 2 '
            'ORDER BY k LIMIT 3',(23,))
    execute('range_or',(-2,),[['-2147483648'],['-100'],['10']],[23])

    query('CREATE SEQUENCE scalar_order_param_seq')
    query("SELECT nextval('scalar_order_param_seq')",[['1']])
    prepare('range_effect','SELECT nextval(\'scalar_order_param_seq\'),k FROM scalar_order_pk_int '
            'WHERE k >= $1 AND k < $2 ORDER BY k LIMIT 1 OFFSET 1',(23,23))
    execute('range_effect',(-2,10),[['3','0']],[20,23])
    execute('range_effect',(None,10),[],[20,23])
    query("SELECT currval('scalar_order_param_seq')",[['3']])

    if check_plan:
        explain='EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '

        def plan_nodes(result):
            pending=[json.loads(result[0][0][0])['plan']]
            nodes={}
            while pending:
                node=pending.pop()
                nodes[node['nodeType']]=node
                pending.extend(node['children'])
            return nodes

        prepare('range_plan',explain+ranged,(23,23))
        for values,fetched in (((-100,10),3),((-3,1),2)):
            nodes=plan_nodes(execute('range_plan',values,binary_widths=(4,4)))
            assert 'IntegerPrimaryKeyPage' in nodes,nodes
            page=nodes['IntegerPrimaryKeyPage']
            assert page['usedPrimaryKeyPage']=='true' and page['indexKeyRange']=='true',page
            assert int(page['indexEntries'])==int(page['heapRowsFetched'])==fetched,page
            assert nodes['TableScan'].get('actualRows',0)==0,nodes
        prepare('range_plan_numeric',explain+decimal,(1700,))
        nodes=plan_nodes(execute('range_plan_numeric',('2.5',)))
        assert nodes.get('IntegerPrimaryKeyPage',{}).get('usedPrimaryKeyPage')!='true',nodes
    print('[PREPARED INTEGER RANGE PROTOCOL] passed',flush=True)


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
        parameter_range_controls(runner,client,server['sock'])
        print('[ORDINARY SCALAR ORDER PROTOCOL] passed')
    finally:runner.stop_ours(server)


if __name__=='__main__':main()
