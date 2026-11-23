#!/usr/bin/env python3
"""Ordinary scalar sort children retain errors, typed keys and real sites."""
import importlib.util
import json
from pathlib import Path
import struct


def plan_nodes(result):
    pending=[json.loads(result[0][0][0])['plan']]
    nodes={}
    while pending:
        node=pending.pop()
        nodes[node['nodeType']]=node
        pending.extend(node['children'])
    return nodes


def assert_sort_input_order(nodes, spilled=None):
    sort=nodes.get('TypedSort',{})
    if spilled is None:
        assert 'presortedInput' not in sort and 'spilled' not in sort,nodes
    else:
        assert sort.get('presortedInput')=='true',nodes
        assert sort.get('spilled')==str(spilled).lower(),nodes


def complete_range_controls(query):
    table = 'scalar_order_complete_range'
    insertion_order = [8, 2, 6, 1, 4, 7, 3, 5]
    assert query(f'CREATE TABLE {table}(k integer PRIMARY KEY, val integer)')[1] is None
    keys = insertion_order + list(range(9, 1537))
    for start in range(0, len(keys), 128):
        batch = keys[start:start + 128]
        values = ','.join(f'({k},{k if k % 2 else "NULL"})' for k in batch)
        assert query(f'INSERT INTO {table} VALUES {values}')[1] is None

    def check(where, expected, used, fetched=None, ranges=None):
        sql = f'SELECT k FROM {table} WHERE {where} ORDER BY k OFFSET 0 FETCH FIRST 1536 ROWS WITH TIES'
        result = query(sql)
        assert result[1] is None and result[0] == [[str(k)] for k in expected], (sql, result)
        nodes = plan_nodes(query('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) ' + sql))
        scan = nodes.get('IntegerPrimaryKeyRange', {})
        assert (scan.get('usedPrimaryKeyRange') == 'true') == used, nodes
        if used:
            assert int(scan['indexEntries']) == int(scan['heapRowsFetched']) == fetched, scan
            if ranges is not None:
                assert int(scan.get('indexRanges', '1')) == ranges, scan
            assert nodes['TableScan'].get('actualRows', 0) == 0, nodes
        else:
            assert nodes['TableScan']['actualRows'] == len(keys), nodes

    check('k+0<=8', range(1, 9), True, 8)
    check('1*k<=8 AND val IS NULL', (2, 4, 6, 8), True, 8)
    check('k>7 AND k<6', (), True, 0)
    check('k<=256', range(1, 257), True, 256)
    check('k<=257', range(1, 258), False)
    check('k+0 BETWEEN 1 AND 8 AND val IS NULL', (2, 4, 6, 8), True, 8)
    check('k BETWEEN 8 AND 8', (8,), True, 1)
    check('k BETWEEN 8 AND 1', (), True, 0)
    check('k BETWEEN 1 AND 257', range(1, 258), False)
    check('k BETWEEN 1.5 AND 5.5', range(2, 6), False)
    check('k BETWEEN NULL AND 8', (), False)
    check('k NOT BETWEEN 1 AND 1535', (1536,), True, 1, 2)
    check('k NOT BETWEEN -2147483648 AND 1535', (1536,), True, 1)
    check('k NOT BETWEEN 3 AND 6 AND k>=3 AND k<8', (7,), True, 1)
    check('k NOT BETWEEN 1 AND 1536 AND k>=1 AND k<=1536', (), True, 0)
    check('k NOT BETWEEN 3 AND 6 AND k<8', (1, 2, 7), True, 3, 2)
    check('k NOT BETWEEN 2 AND 3 AND k NOT BETWEEN 6 AND 7 AND k<9', (1, 4, 5, 8), True, 4, 3)
    check('k NOT BETWEEN 129 AND 1408', (*range(1, 129), *range(1409, 1537)), True, 256, 2)
    check('k NOT BETWEEN 129 AND 1407', (*range(1, 129), *range(1408, 1537)), False)
    check('k BETWEEN SYMMETRIC 8 AND 1', range(1, 9), True, 8)
    check('k BETWEEN ASYMMETRIC 8 AND 1', (), True, 0)
    check('k NOT BETWEEN SYMMETRIC 1534 AND 3', (1, 2, 1535, 1536), True, 4, 2)
    check('k NOT BETWEEN ASYMMETRIC 8 AND 1 AND k<5', (1, 2, 3, 4), True, 4)
    check('k BETWEEN SYMMETRIC NULL AND 8', (), False)
    check('k<=3 OR 1535<=k', (1, 2, 3, 1535, 1536), True, 5, 2)
    check('k BETWEEN 1 AND 4 OR k BETWEEN 4 AND 8 OR k=9', range(1, 10), True, 9, 1)
    check('(k>1 AND k<4) OR (k=7 OR k=9) OR k>1535', (2, 3, 7, 9, 1536), True, 5, 4)
    check('(k!=8 OR k=2000) AND k>=6 AND k<=8', (6, 7), True, 2, 1)
    check('k< -2147483648 OR k>2147483647', (), True, 0)
    check('k<=128 OR k>=1409', (*range(1, 129), *range(1409, 1537)), True, 256, 2)
    check('k<=129 OR k>=1409', (*range(1, 130), *range(1409, 1537)), False)
    check('k<=3 OR val>1534', (1, 2, 3, 1535), False)
    check('k<2.5 OR k>1535.5', (1, 2, 1536), False)
    check(' OR '.join(['k=2'] * 129), (2,), False)
    for predicate, expected in (('k=4 OR val=11', (4, 11)),
                                ('k<2.5 OR k>1535.5', (1, 2, 1536))):
        result = query(f'SELECT k+0::integer AS v FROM {table} WHERE {predicate} ORDER BY k')
        assert result[1] is None and result[0] == [[str(k)] for k in expected], result

    # The index sources the same heap order when SQL has no sort key.
    for tail, expected in (('LIMIT 4', insertion_order[:4]),
                           ('LIMIT 2 OFFSET 3', [1, 4]),
                           ('AND val IS NULL LIMIT 2 OFFSET 1', [2, 6])):
        result = query(f'SELECT k FROM {table} WHERE k+0<=8 {tail}')
        assert result[1] is None and result[0] == [[str(k)] for k in expected], result
    result = query(f'SELECT k FROM {table} WHERE k BETWEEN 1 AND 8 LIMIT 2 OFFSET 3')
    assert result[1] is None and result[0] == [['1'], ['4']], result
    result = query(f'SELECT k FROM {table} WHERE k NOT BETWEEN 3 AND 6 AND k<=8 LIMIT 3 OFFSET 1')
    assert result[1] is None and result[0] == [['2'], ['1'], ['7']], result
    result = query(f'SELECT k FROM {table} WHERE k<=2 OR (k>=7 AND k<=8) LIMIT 2 OFFSET 1')
    assert result[1] is None and result[0] == [['2'], ['1']], result

    explain = 'EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '
    for condition, order, limit, expected, fetched in (
        ('k NOT BETWEEN 3 AND 1534', 'k ASC', '3 OFFSET 1', [2, 1535, 1536], 4),
        ('k NOT BETWEEN 3 AND 1534', 'k DESC', '3 OFFSET 1', [1535, 2, 1], 4),
        ('k NOT BETWEEN 3 AND 1534', 'k DESC', '1', [1536], 1),
        ('k NOT BETWEEN 128 AND 1408 AND val IS NULL', 'k ASC', '70',
         [*range(2, 128, 2), *range(1410, 1423, 2)], 141),
        ('k<=3 OR k>=1535', 'k ASC', '3 OFFSET 1', [2, 3, 1535], 4),
        ('k<=3 OR k>=1535', 'k DESC', '3 OFFSET 1', [1535, 3, 2], 4),
        ('(k<128 OR k>1408) AND val IS NULL', 'k ASC', '70',
         [*range(2, 128, 2), *range(1410, 1423, 2)], 141),
    ):
        sql = f'SELECT k FROM {table} WHERE {condition} ORDER BY {order} LIMIT {limit}'
        result = query(sql)
        assert result[1] is None and result[0] == [[str(k)] for k in expected], (sql, result)
        nodes = plan_nodes(query(explain + sql))
        scan = nodes['IntegerPrimaryKeyPage']
        assert scan['usedPrimaryKeyPage'] == 'true' and int(scan['indexRanges']) == 2, nodes
        assert int(scan['heapRowsFetched']) == fetched and nodes['TableScan'].get('actualRows', 0) == 0, nodes

    count = f'SELECT count(*) FROM {table} WHERE val BETWEEN 1 AND 8'
    result = query(count)
    assert result[1] is None and result[0] == [['4']], result
    nodes = plan_nodes(query('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) ' + count))
    assert 'StoredCount' in nodes, nodes
    for condition, expected, empty in (
        ('val NOT BETWEEN 1 AND 8', '764', False),
        ('val NOT BETWEEN 8 AND 1', '768', False),
        ('val BETWEEN 1 AND 8 AND val NOT BETWEEN 1 AND 8', '0', True),
        ('(val<2 OR val IS NULL) AND val IS NULL', '768', False),
        ('(val<2 OR val>1536) AND val IS NULL', '0', True),
        ('(val<2 OR val>1534) AND (val=1 OR val=3)', '1', False),
    ):
        count = f'SELECT count(*) FROM {table} WHERE {condition}'
        result = query(count)
        assert result[1] is None and result[0] == [[expected]], result
        nodes = plan_nodes(query('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) ' + count))
        assert (nodes['StoredCount'].get('emptyInput') == 'true') == empty, nodes


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

    def prepare(name,sql,types,described_types=None):
        label=name.encode()+b'\0'
        body=label+sql.encode()+b'\0'+struct.pack('!H',len(types))
        body+=b''.join(struct.pack('!I',oid) for oid in types)
        sock.sendall(client.typed(b'P',body)+client.typed(b'D',b'S'+label)+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        check(messages,[])
        assert sum(kind==b'1' for kind,_ in messages)==1,messages
        described=[body for kind,body in messages if kind==b't']
        described_types = types if described_types is None else described_types
        expected=struct.pack('!H',len(described_types))+b''.join(struct.pack('!I',oid) for oid in described_types)
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
    between = ('SELECT p.k,p.payload FROM scalar_order_pk_int p '
               'WHERE p.k BETWEEN $1 AND $2 ORDER BY p.k DESC LIMIT 2 OFFSET 1')
    prepare('range_between',between,(21,20))
    execute('range_between',(-2,10),[['2',''],['0','zero']],[23,25],binary_widths=(2,8))
    for pair in ((10,-2),(None,10),(-2,None)):
        execute('range_between',pair,[],[23,25])
    execute('range_between',(-2,2),[['0','zero'],['-2',None]],[23,25])
    symmetric = between.replace(' BETWEEN ', ' BETWEEN SYMMETRIC ')
    prepare('range_symmetric', symmetric, (21,20))
    execute('range_symmetric',(10,-2),[['2',''],['0','zero']],[23,25],binary_widths=(2,8))
    execute('range_symmetric',(None,10),[],[23,25])
    execute('range_symmetric',(-2,2),[['0','zero'],['-2',None]],[23,25])
    prepare('range_symmetric_inferred', 'SELECT 2 BETWEEN SYMMETRIC $1 AND $2 AS v', (0,0), (23,23))
    execute('range_symmetric_inferred',(3,1),[['t']],[16],binary_widths=(4,4))
    execute('range_symmetric_inferred',(3,None),[[None]],[16])
    check(client.simple_query(sock, 'CREATE SEQUENCE explicit_range_effects'), tag='CREATE SEQUENCE')
    query("SELECT 2 BETWEEN SYMMETRIC nextval('explicit_range_effects') AND 1 AS v", [['f']])
    query("SELECT currval('explicit_range_effects') AS v", [['1']])
    check(client.simple_query(sock, 'DROP SEQUENCE explicit_range_effects'), tag='DROP SEQUENCE')
    prepare('range_disjunction', 'SELECT k FROM scalar_order_pk_int WHERE k<$1 OR $2<=k ORDER BY k LIMIT 3', (21,20))
    execute('range_disjunction',(-5,10),[['-2147483648'],['-100'],['10']],[23],binary_widths=(2,8))
    execute('range_disjunction',(None,10),[['10'],['2147483647']],[23])
    execute('range_disjunction',(5,-2),[['-2147483648'],['-100'],['-2']],[23])
    outside = ('SELECT k FROM scalar_order_pk_int WHERE k NOT BETWEEN $1 AND $2 '
               'AND k>=-2 ORDER BY k DESC LIMIT 2')
    prepare('range_outside',outside,(21,20))
    execute('range_outside',(-2,10),[['2147483647']],[23],binary_widths=(2,8))
    execute('range_outside',(10,-2),[['2147483647'],['10']],[23])
    execute('range_outside',(None,10),[['2147483647']],[23])
    execute('range_outside',(0,None),[['-2']],[23])
    disjoint = ('SELECT k FROM scalar_order_pk_int WHERE k NOT BETWEEN $1 AND $2 '
                'ORDER BY k LIMIT 4')
    prepare('range_disjoint',disjoint,(21,20))
    execute('range_disjoint',(-5,5),[['-2147483648'],['-100'],['10'],['2147483647']],[23],
            binary_widths=(2,8))
    execute('range_disjoint',(5,-5),[['-2147483648'],['-100'],['-2'],['0']],[23])

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
        prepare('range_plan',explain+ranged,(23,23))
        for values,fetched in (((-100,10),3),((-3,1),2)):
            nodes=plan_nodes(execute('range_plan',values,binary_widths=(4,4)))
            assert 'IntegerPrimaryKeyPage' in nodes,nodes
            page=nodes['IntegerPrimaryKeyPage']
            assert page['usedPrimaryKeyPage']=='true' and page['indexKeyRange']=='true',page
            assert int(page['indexEntries'])==int(page['heapRowsFetched'])==fetched,page
            assert nodes['TableScan'].get('actualRows',0)==0,nodes
            assert_sort_input_order(nodes,spilled=False)
        prepare('range_plan_numeric',explain+decimal,(1700,))
        nodes=plan_nodes(execute('range_plan_numeric',('2.5',)))
        assert nodes.get('IntegerPrimaryKeyPage',{}).get('usedPrimaryKeyPage')!='true',nodes
        prepare('between_plan',explain+between,(21,20))
        for values,fetched in (((-2,10),3),((10,-2),0)):
            nodes=plan_nodes(execute('between_plan',values,binary_widths=(2,8)))
            page=nodes['IntegerPrimaryKeyPage']
            assert page['usedPrimaryKeyPage']=='true' and int(page['heapRowsFetched'])==fetched,nodes
            assert nodes['TableScan'].get('actualRows',0)==0,nodes
        prepare('outside_plan',explain+outside,(21,20))
        for values,fetched in (((-2,10),1),((10,-2),2)):
            nodes=plan_nodes(execute('outside_plan',values,binary_widths=(2,8)))
            page=nodes['IntegerPrimaryKeyPage']
            assert page['usedPrimaryKeyPage']=='true' and int(page['heapRowsFetched'])==fetched,nodes
            assert nodes['TableScan'].get('actualRows',0)==0,nodes
        prepare('disjoint_plan',explain+disjoint,(21,20))
        nodes=plan_nodes(execute('disjoint_plan',(-5,5),binary_widths=(2,8)))
        page=nodes['IntegerPrimaryKeyPage']
        assert page['usedPrimaryKeyPage']=='true' and page['indexRanges']=='2',nodes
        assert int(page['heapRowsFetched'])==4 and nodes['TableScan'].get('actualRows',0)==0,nodes
    print('[PREPARED INTEGER RANGE PROTOCOL] passed',flush=True)


def null_filter_page_controls(query, check_plan=True):
    """NULL filtering must count matched rows while keeping its heap-read budget."""
    def expect(sql,rows=None,oids=None,state=None):
        result=query(sql)
        assert result[1]==state,(sql,result)
        if rows is not None:assert result[0]==rows,(sql,result,rows)
        if oids is not None:assert result[5]==oids,(sql,result,oids)
        if state is not None:assert result[4] is None,(sql,result)
        return result

    def page(sql):
        explain='EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '
        return plan_nodes(expect(explain+sql))

    def accepted(sql,rows,oids,fetched,bounded):
        expect(sql,rows,oids)
        if check_plan:
            nodes=page(sql)
            selected=nodes.get('IntegerPrimaryKeyPage',{})
            assert selected.get('usedPrimaryKeyPage')==selected.get('indexNullFilter')=='true',nodes
            assert (selected.get('indexKeyRange')=='true')==bounded,nodes
            assert int(selected['heapRowsFetched'])==fetched,nodes
            assert nodes['TableScan'].get('actualRows',0)==0,nodes
            assert_sort_input_order(nodes,spilled=False)

    def fallback(sql,rows,oids):
        expect(sql,rows,oids)
        if check_plan:
            nodes=page(sql)
            assert nodes.get('IntegerPrimaryKeyPage',{}).get('usedPrimaryKeyPage')!='true',nodes

    expect('CREATE TABLE scalar_order_null_page(k INTEGER PRIMARY KEY,qty INTEGER,ready BOOLEAN,payload TEXT)')
    qty={-4:'0',5:'51',13:'130',21:'210'}
    ready={-6:'FALSE',3:'TRUE',7:'FALSE',11:'TRUE',20:'FALSE'}
    payload={-7:"'first'",4:"''",14:"'NULL'",23:"'last'"}
    values=[]
    for i in range(32):
        k=(i*13)%32-8
        values.append(f"({k},{qty.get(k,'NULL')},{ready.get(k,'NULL')},{payload.get(k,'NULL')})")
    expect('INSERT INTO scalar_order_null_page VALUES '+','.join(values))

    for sql,rows,oids,fetched,bounded in (
        ('SELECT p.k,p.qty FROM scalar_order_null_page p WHERE p.qty IS NOT NULL '
         'ORDER BY p.k LIMIT 2 OFFSET 1',
         [['5','51'],['13','130']],[23,23],22,False),
        ('SELECT p.k,p.ready FROM scalar_order_null_page p WHERE 20 >= p.k AND p.k > -2 '
         'AND p.ready IS NULL ORDER BY p.k DESC LIMIT 2 OFFSET 1',
         [['18',None],['17',None]],[23,16],4,True),
        ('SELECT p.k AS key_out,p.payload FROM scalar_order_null_page p '
         'WHERE p.payload IS NOT NULL AND p.k >= -1 ORDER BY key_out LIMIT 3 OFFSET 1',
         [['14','NULL'],['23','last']],[23,25],25,True),
        ('SELECT p.k,p.ready FROM scalar_order_null_page p WHERE p.k > 18 '
         'AND p.ready IS NOT NULL ORDER BY p.k DESC LIMIT 1',
         [['20','f']],[23,16],4,True),
        ('SELECT p.k,p.ready FROM scalar_order_null_page p WHERE p.k > 18 '
         'AND p.ready IS NOT NULL ORDER BY p.k DESC LIMIT 3 OFFSET 1',
         [],[23,16],5,True),
    ):
        accepted(sql,rows,oids,fetched,bounded)

    fallback('SELECT k FROM scalar_order_null_page WHERE k >= 0 AND (qty+0) IS NOT NULL '
             'ORDER BY k LIMIT 2 OFFSET 1',[['13'],['21']],[23])
    fallback('SELECT DISTINCT k FROM scalar_order_null_page WHERE qty IS NOT NULL '
             'ORDER BY k LIMIT 2',[['-4'],['5']],[23])

    expect('CREATE SEQUENCE scalar_order_null_calls')
    expect("SELECT nextval('scalar_order_null_calls')",[['1']])
    fallback("SELECT k FROM scalar_order_null_page WHERE qty IS NOT NULL AND "
             "CASE WHEN k IN (13,21) THEN nextval('scalar_order_null_calls') > 0 ELSE false END "
             "ORDER BY k LIMIT 1 OFFSET 1",[['21']],[23])
    expect("SELECT currval('scalar_order_null_calls')",[['5' if check_plan else '3']])
    expect("SELECT k FROM scalar_order_null_page WHERE qty IS NOT NULL AND "
           "CASE WHEN k = 21 THEN ('bad' || k::TEXT)::INTEGER > 0 ELSE false END "
           "ORDER BY k LIMIT 1",[],state='22P02')

    expect('CREATE TABLE scalar_order_null_generated(k INTEGER PRIMARY KEY,n INTEGER,'
           'g INTEGER GENERATED ALWAYS AS (n+1) STORED)')
    expect('INSERT INTO scalar_order_null_generated(k,n) VALUES (2,NULL),(1,2),(3,0)')
    fallback('SELECT k FROM scalar_order_null_generated WHERE g IS NULL ORDER BY k LIMIT 1',[['2']],[23])
    expect('CREATE DOMAIN scalar_order_null_domain AS INTEGER')
    expect('CREATE TABLE scalar_order_null_complex(k INTEGER PRIMARY KEY,q INTEGER,'
           'd scalar_order_null_domain,a INTEGER[])')
    expect('INSERT INTO scalar_order_null_complex VALUES (3,NULL,NULL,ARRAY[3]),'
           '(1,4,1,ARRAY[1]),(2,NULL,2,NULL)')
    fallback('SELECT k FROM scalar_order_null_complex WHERE d IS NOT NULL ORDER BY k LIMIT 1',[['1']],[23])
    fallback('SELECT k FROM scalar_order_null_complex WHERE a IS NULL ORDER BY k LIMIT 1',[['2']],[23])
    fallback('SELECT k FROM scalar_order_null_complex WHERE q IS NULL ORDER BY k LIMIT 1',[['2']],[23])

    # k=57, the first match in the range, is outside the first 32 index entries.
    extra={57:'0',91:'-1',101:'17'}
    expect('INSERT INTO scalar_order_null_page(k,qty) VALUES '+
           ','.join(f"({k},{extra.get(k,'NULL')})" for k in range(24,104)))
    accepted('SELECT k,qty FROM scalar_order_null_page WHERE k >= 22 AND qty IS NOT NULL '
             'ORDER BY k LIMIT 2 OFFSET 1',[['91','-1'],['101','17']],[23,23],80,True)
    print('[INTEGER PRIMARY KEY NULL FILTER PROTOCOL] passed',flush=True)


def presorted_page_controls(query, check_plan=True):
    def expect(sql,rows=None,oids=None,state=None):
        result=query(sql)
        assert result[1]==state,(sql,result)
        if rows is not None:assert result[0]==rows,(sql,result,rows)
        if oids is not None:assert result[5]==oids,(sql,result,oids)
        if state is not None:assert result[4] is None,(sql,result)
        return result

    def page(sql,spilled):
        if check_plan:
            explain='EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '
            nodes=plan_nodes(expect(explain+sql))
            assert_sort_input_order(nodes,spilled)
            used=nodes.get('IntegerPrimaryKeyPage',{}).get('usedPrimaryKeyPage')=='true'
            assert used==(spilled is not None),nodes
            if used:
                assert nodes['TableScan'].get('actualRows',0)==0,nodes

    keys=[-9223372036854775808,-9007199254740993]
    keys+=list(range(9007199254740992,9007199254741029))
    keys.append(9223372036854775807)
    payload=[f'{i:02d}/'+chr(97+i%26)*847 for i in range(len(keys))]
    values=[f"({keys[i]},'{payload[i]}')" for i in (n*17%40 for n in range(40))]
    expect('CREATE TABLE scalar_order_presorted_wide(k BIGINT PRIMARY KEY,payload TEXT)')
    expect('INSERT INTO scalar_order_presorted_wide VALUES '+','.join(values))
    expect('CREATE SEQUENCE scalar_order_presorted_calls')
    expect("SET work_mem='64kB'")

    # The page buffers 39 narrow heap tuples within work_mem; the owning sort
    # contexts exceed that budget and must use the ordered temporary run.
    wide=("SELECT nextval('scalar_order_presorted_calls'),p.k AS key_out,p.payload "
          "FROM scalar_order_presorted_wide p ORDER BY key_out DESC LIMIT 38 OFFSET 1")
    expected=[[str(n),str(keys[i]),payload[i]] for n,i in enumerate(range(38,0,-1),2)]
    expect(wide,expected,[20,20,25])
    expect("SELECT currval('scalar_order_presorted_calls')",[['39']],[20])
    page(wide,spilled=True)
    if check_plan:
        expect("SELECT currval('scalar_order_presorted_calls')",[['78']],[20])

    ordinal='SELECT k,payload FROM scalar_order_presorted_wide ORDER BY 1 LIMIT 3'
    expect(ordinal,[[str(keys[i]),payload[i]] for i in range(3)],[20,25])
    page(ordinal,spilled=False)
    nonkey=('SELECT k FROM scalar_order_presorted_wide ORDER BY k+0 DESC LIMIT 3')
    expect(nonkey,[[str(k)] for k in reversed(keys[-3:])],[20])
    page(nonkey,spilled=None)
    expect("SELECT k FROM scalar_order_presorted_wide ORDER BY "
           "CASE WHEN k = 9007199254741028 THEN (payload || 'bad')::BIGINT ELSE k END LIMIT 1",
           [],state='22P02')

    # Each value is stored externally. Twelve restored values fit inside
    # work_mem, but the extra copied heap page would push them over budget.
    expect('CREATE TABLE scalar_order_external(k INTEGER PRIMARY KEY,payload TEXT)')
    large_values = [f'{k:02d}/' + chr(97+k)*4997 for k in range(1,19)]
    expect('INSERT INTO scalar_order_external VALUES ' +
           ','.join(f"({k},'{value}')" for k,value in enumerate(large_values,1)) +
           ',(19,NULL)')
    for limit, use_index in ((12,True),(14,False)):
        sql = f'SELECT k,payload FROM scalar_order_external ORDER BY k LIMIT {limit}'
        expect(sql, [[str(k),value] for k,value in enumerate(large_values[:limit],1)], [23,25])
        if check_plan:
            nodes = plan_nodes(expect(
                'EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '+sql))
            index = nodes['IntegerPrimaryKeyPage']
            assert (index['usedPrimaryKeyPage'] == 'true') == use_index, index
            if use_index:
                assert int(index['heapRowsFetched']) == limit, index
    expect('SELECT k,payload FROM scalar_order_external WHERE payload IS NOT NULL '
           'ORDER BY k DESC LIMIT 3 OFFSET 2',
           [[str(k),large_values[k-1]] for k in (16,15,14)], [23,25])
    disjoint = 'SELECT k,payload FROM scalar_order_external WHERE k NOT BETWEEN 7 AND 12 ORDER BY k LIMIT 12'
    expect(disjoint, [[str(k),large_values[k-1]] for k in (*range(1,7),*range(13,19))], [23,25])
    if check_plan:
        nodes = plan_nodes(expect('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '+disjoint))
        index = nodes['IntegerPrimaryKeyPage']
        assert index['usedPrimaryKeyPage']=='true' and index['indexRanges']=='2', nodes
        assert int(index['heapRowsFetched']) == 12,nodes
    expect('CREATE TABLE scalar_order_scattered(k INTEGER PRIMARY KEY, payload TEXT)')
    physical_order = [1 + i*223 % 576 for i in range(576)]
    scattered_payload = {k: None if k % 7 == 0 else f'row-{k}' for k in physical_order}
    scattered_payload[11] = 'left'*1250
    scattered_payload[563] = 'right'*1000
    for start in range(0, len(physical_order), 128):
        entries = [(k, scattered_payload[k]) for k in physical_order[start:start+128]]
        values = ','.join(f"({k},{'NULL' if value is None else repr(value)})" for k, value in entries)
        expect('INSERT INTO scalar_order_scattered VALUES '+values)
    for suffix, keys, heap_rows in (
        ('ORDER BY k LIMIT 257 OFFSET 3', range(4, 261), 260),
        ('WHERE k<=19 OR k>=560 ORDER BY k DESC LIMIT 25 OFFSET 6',
         (*range(570, 559, -1), *range(19, 5, -1)), 31),
    ):
        sql = 'SELECT k,payload FROM scalar_order_scattered ' + suffix
        expect(sql, [[str(k), scattered_payload[k]] for k in keys], [23,25])
        if check_plan:
            nodes = plan_nodes(expect('EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, FORMAT JSON) '+sql))
            index = nodes['IntegerPrimaryKeyPage']
            assert index['usedPrimaryKeyPage']=='true' and int(index['heapRowsFetched'])==heap_rows, nodes
    expect('RESET work_mem')
    print('[PRESORTED PRIMARY KEY PROTOCOL] passed',flush=True)


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
        scans=plan_nodes(result)
        assert_sort_input_order(scans,spilled=False if scans['IntegerPrimaryKeyPage']['usedPrimaryKeyPage']=='true' else None)
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
        null_filter_page_controls(query)
        presorted_page_controls(query)
        complete_range_controls(query)
        print('[ORDINARY SCALAR ORDER PROTOCOL] passed')
    finally:runner.stop_ours(server)


if __name__=='__main__':main()
