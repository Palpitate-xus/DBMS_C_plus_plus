"""Scalar IN query lowering keeps typed ANY/equality and NOT's SQL NULLs."""
import struct
import uuid


def cases():
    result=[]
    for label,left in [('one',1),('two',2),('null',None)]:
        for rhs,query,values in [
            ('one','SELECT 1',[1]),('two','SELECT 2',[2]),
            ('null','SELECT CAST(NULL AS INT)',[None]),
            ('empty','SELECT 1 WHERE FALSE',[]),
            ('mixed','SELECT 1 UNION ALL SELECT CAST(NULL AS INT)',[1,None]),
            ('values','VALUES(1),(2)',[1,2]),
            ('cte','WITH q AS(SELECT 1 AS v) SELECT v FROM q',[1])]:
            truth=False if not values else None if left is None else True if left in values else None if None in values else False
            for negated in (False,True):
                value=None if truth is None else ('t' if truth!=negated else 'f')
                sql='SELECT '+('NULL' if left is None else str(left))+(' NOT IN(' if negated else ' IN(')+query+')'
                result.append((label+'_'+rhs+('_not' if negated else ''),sql,[[value]],None,[16]))
    result += [
        ('text_numeric_distinct',"SELECT '001'::TEXT IN(SELECT '1'::TEXT)",[['f']],None,[16]),
        ('text_equal',"SELECT '1'::TEXT IN(SELECT '1'::TEXT)",[['t']],None,[16]),
        ('text_quote',"SELECT 'O''Brien' IN(SELECT 'O''Brien')",[['t']],None,[16]),
        ('literal_null',"SELECT 'NULL' IN(SELECT 'NULL')",[['t']],None,[16]),
        ('true_null',"SELECT CAST(NULL AS TEXT) IN(SELECT 'NULL')",[[None]],None,[16]),
        ('unicode',"SELECT '中文' IN(SELECT '中文')",[['t']],None,[16]),
        ('text_not_equal',"SELECT '001'::TEXT NOT IN(SELECT '1'::TEXT)",[['t']],None,[16]),
        ('unicode_not',"SELECT '中文' NOT IN(SELECT '中文')",[['f']],None,[16]),
    ]
    for negated in (False,True):
        operation=' NOT IN(' if negated else ' IN('
        for label,left,right,value,state in [
            ('unknown_right','1',"SELECT '1'",None,'42883'),
            ('unknown_left',"'1'",'SELECT 1','f' if negated else 't',None),
            ('bad_left',"'bad'",'SELECT 1',None,'22P02')]:
            result.append((label+('_not' if negated else ''),'SELECT '+left+operation+right+')',[] if state else [[value]],state,[] if state else [16]))
    result += [
        ('projection','SELECT l.id,l.v IN(SELECT r.v FROM r282_rhs r) FROM r282_lhs l ORDER BY l.id', [['1',None],['2','t'],['3',None]],None,[23,16]),
        ('projection_not','SELECT l.id,l.v NOT IN(SELECT r.v FROM r282_rhs r) FROM r282_lhs l ORDER BY l.id', [['1',None],['2','f'],['3',None]],None,[23,16]),
        ('correlated','SELECT l.id,l.v IN(SELECT r.v FROM r282_rhs r WHERE r.v=l.v) FROM r282_lhs l ORDER BY l.id',[['1','f'],['2','t'],['3','f']],None,[23,16]),
        ('correlated_not','SELECT l.id,l.v NOT IN(SELECT r.v FROM r282_rhs r WHERE r.v=l.v) FROM r282_lhs l ORDER BY l.id',[['1','t'],['2','f'],['3','t']],None,[23,16]),
        ('where','SELECT id FROM r282_lhs WHERE v IN(SELECT v FROM r282_rhs) ORDER BY id',[['2']],None,[23]),
        ('where_not','SELECT id FROM r282_lhs WHERE v NOT IN(SELECT v FROM r282_rhs) ORDER BY id',[],None,[23]),
        ('trivia',"SELECT 2 IN /*(fake*/ (SELECT 2 /*')'*/)",[['t']],None,[16]),
        ('keyword_text',"SELECT 'SELECT' IN(SELECT 'SELECT')",[['t']],None,[16]),
        ('nested','SELECT 1 IN(SELECT 1 WHERE 1 IN(SELECT 1))',[['t']],None,[16]),
        ('nested_not','SELECT 1 NOT IN(SELECT 1 WHERE 1 NOT IN(SELECT 2))',[['f']],None,[16]),
        ('width','SELECT 1 IN(SELECT 1,2)',[],'42601',[]),
        ('missing_empty','SELECT 1 IN(SELECT missing_column FROM r282_empty)',[],'42703',[]),
        ('division','SELECT 1 IN(SELECT 1/0)',[],'22012',[]),
        ('missing_relation','SELECT 1 IN(SELECT v FROM r282_missing_relation)',[],'42P01',[]),
    ]
    assert len(result)==70
    return result


def effects():
    result=[]
    for negated in (False,True):
        op=' NOT IN(' if negated else ' IN('
        for label,left,right,value,calls in [
            ('empty_left',"nextval('__SEQ__')",'SELECT 1 WHERE FALSE','t' if negated else 'f',0),
            ('hashed_left',"nextval('__SEQ__')",'SELECT 1 UNION ALL SELECT 2','f' if negated else 't',1),
            ('complete_right','0',"SELECT nextval('__SEQ__') FROM r282_lhs",'t' if negated else 'f',3),
            ('filtered_right','0',"SELECT nextval('__SEQ__') FROM r282_lhs WHERE FALSE",'t' if negated else 'f',0)]:
            result.append((label+('_not' if negated else ''),'SELECT '+left+op+right+')',value,calls))
    return result


def run(client,runner,sock):
    controls=failures=0
    def extended(sql):
        parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=b'\0\0'+struct.pack('!HHH',0,0,0)
        sock.sendall(client.typed(b'P',parse)+client.typed(b'B',bind)+client.typed(b'D',b'P\0')+client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        return client.read_until_ready(sock)
    for transport in ('simple','extended'):
        def expect(sql,rows=None,state=None,ready=b'T',oids=None):
            nonlocal controls,failures
            try:messages=client.simple_query(sock,sql) if transport=='simple' else extended(sql)
            except Exception:
                print('TYPED_IN_TRANSPORT_ERROR',transport,controls+1,sql,flush=True);raise
            actual,code,message,_,_,types=runner.decode_wire_result(messages,include_types=True)
            controls+=1
            if actual!=(rows or []) or code!=state or messages[-1]!=(b'Z',ready) or (oids is not None and types!=oids):
                failures+=1;print('TYPED_IN_FAIL',transport,sql,'expected',rows or [],state,ready,oids,'actual',actual,code,messages[-1],types,message,flush=True)
        expect('BEGIN')
        try:
            expect('CREATE TEMP TABLE r282_lhs(id INT,v INT)')
            expect('INSERT INTO r282_lhs VALUES(1,1),(2,2),(3,NULL)')
            expect('CREATE TEMP TABLE r282_rhs(v INT)')
            expect('INSERT INTO r282_rhs VALUES(2),(NULL)')
            expect('CREATE TEMP TABLE r282_empty(v INT)')
            for label,sql,rows,state,oids in cases():
                expect('SAVEPOINT typed_in_case')
                expect(sql,rows,state,b'E' if state else b'T',oids)
                expect('SELECT 1',None if state else [['1']],'25P02' if state else None,b'E' if state else b'T')
                expect('ROLLBACK TO SAVEPOINT typed_in_case')
                expect('RELEASE SAVEPOINT typed_in_case')
            for label,sql,value,calls in effects():
                sequence='r282_seq_'+uuid.uuid4().hex[:16]
                expect('CREATE SEQUENCE '+sequence+' START 1')
                expect('SAVEPOINT typed_in_case')
                expect(sql.replace('__SEQ__',sequence),[[value]],oids=[16])
                expect('SELECT 1',[['1']])
                expect('ROLLBACK TO SAVEPOINT typed_in_case')
                expect('RELEASE SAVEPOINT typed_in_case')
                expect('SAVEPOINT typed_in_counter')
                expect("SELECT currval('"+sequence+"')",[[str(calls)]] if calls else None,None if calls else '55000',b'T' if calls else b'E')
                expect('ROLLBACK TO SAVEPOINT typed_in_counter')
                expect('RELEASE SAVEPOINT typed_in_counter')
                expect("SELECT nextval('"+sequence+"')",[[str(calls+1)]])
        finally:expect('ROLLBACK',ready=b'I')
    print('TYPED_SCALAR_IN_COMPLETE',controls,'FAILURES',failures,flush=True)
    assert controls==890 and failures==0,(controls,failures)
    return controls
