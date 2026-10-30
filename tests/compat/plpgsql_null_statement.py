"""PL/pgSQL NULL is an explicit no-op, not a SQL command or SQL NULL value."""
import struct
import uuid


def cases():
    result = []
    for number, statement in enumerate((
            'NULL;', 'null;', 'NuLl;', 'NULL /* comment */ ;',
            'NULL -- line comment\n;', '/* leading */ NULL/* outer /* nested */ end */;')):
        for site, body, value in (
                ('block', "BEGIN S RETURN 'after'; END;", 'after'),
                ('if', 'DECLARE n INT:=0; BEGIN IF TRUE THEN S ELSE n:=999; END IF; RETURN n; END;', '0'),
                ('while', 'DECLARE n INT:=0; BEGIN WHILE n<3 LOOP S n:=n+1; END LOOP; RETURN n; END;', '3'),
                ('for', 'DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP S n:=n+i; END LOOP; RETURN n; END;', '6'),
                ('loop', 'DECLARE n INT:=0; BEGIN LOOP S n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;', '3')):
            result.append((site+'_'+str(number), body.replace(' S ', ' '+statement+' '), value))
    found="IF FOUND THEN RETURN 't'; ELSE RETURN 'f'; END IF;"
    result += [
        ('repeat', "BEGIN NULL; NULL; NULL; RETURN 'done'; END;", 'done'),
        ('return_null', 'BEGIN NULL; RETURN NULL; END;', None),
        ('text_null', "BEGIN NULL; RETURN 'null'; END;", 'null'),
        ('quoted_null', 'DECLARE "null" INT:=17; BEGIN NULL; RETURN "null"; END;', '17'),
        ('prefix', 'DECLARE nullify INT:=7; BEGIN nullify:=9; NULL; RETURN nullify; END;', '9'),
        ('found_false', 'BEGIN NULL; '+found+' END;', 'f'),
        ('found_true', 'BEGIN FOR i IN 1..1 LOOP NULL; END LOOP; NULL; '+found+' END;', 't'),
        ('found_empty', 'BEGIN FOR i IN 2..1 LOOP NULL; END LOOP; NULL; '+found+' END;', 'f'),
        ('after_return', "BEGIN RETURN 'before'; NULL; END;", 'before'),
        ('unreached', "BEGIN IF FALSE THEN NULL; ELSE RETURN 'reached'; END IF; END;", 'reached'),
    ]
    assert len(result)==40
    return result


def malformed():
    return ['NULL x;', 'NULL::text;', 'NULL+1;', 'NULL := 1;', 'NULL = 1;',
            'NULL(NULL);', 'NULL NULL;', 'NULL THEN;', 'NULL END;', "NULL RETURN 'x';"]


def run(client, runner, sock):
    controls=failures=0
    def extended(sql):
        parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=b'\0\0'+struct.pack('!HHH',0,0,0)
        sock.sendall(client.typed(b'P',parse)+client.typed(b'B',bind)+
                     client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        return client.read_until_ready(sock)
    for transport in ('simple','extended'):
        def expect(sql, rows=None, state=None, ready=b'T'):
            nonlocal controls,failures
            messages=client.simple_query(sock,sql) if transport=='simple' else extended(sql)
            actual_rows,actual_state,message,_,_=runner.decode_wire_result(messages)
            controls+=1
            if actual_rows!=(rows or []) or actual_state!=state or messages[-1]!=(b'Z',ready):
                failures+=1
                print('NULL_STATEMENT_FAIL',transport,sql,'expected',rows or [],state,ready,
                      'actual',actual_rows,actual_state,messages[-1],message,flush=True)
        expect('BEGIN')
        try:
            for label,body,value in cases():
                function='r287_'+uuid.uuid4().hex[:18]
                expect('CREATE FUNCTION '+function+'() RETURNS TEXT LANGUAGE plpgsql AS $r287$'+body+'$r287$')
                expect('SAVEPOINT check_null')
                expect('SELECT '+function+'()',[[value]])
                expect('SELECT 1',[['1']])
                expect('ROLLBACK TO SAVEPOINT check_null')
                expect('RELEASE SAVEPOINT check_null')
        finally:
            expect('ROLLBACK',ready=b'I')
    print('PLPGSQL_NULL_STATEMENT_PROTOCOL_COMPLETE',controls,'FAILURES',failures,flush=True)
    assert controls==484 and failures==0,(controls,failures)
    return controls
