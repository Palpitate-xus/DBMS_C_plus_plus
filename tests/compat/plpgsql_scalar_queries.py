"""Reached procedural expressions retain whole prepared SELECT/query identity."""
import struct
import uuid


def cases():
    result = []
    for label, expression, value, state in (
        ('one', '(SELECT 1)', 'yes', None),
        ('true', '(SELECT TRUE)', 'yes', None),
        ('null', '(SELECT NULL)', 'no', None),
        ('empty', '(SELECT 1 WHERE FALSE)', 'no', None),
        ('invalid_boolean', '(SELECT 2)', None, '22P02'),
        ('division', '(SELECT 1/0)', None, '22012'),
        ('multiple_rows', '(SELECT 1 UNION ALL SELECT 2)', None, '21000'),
    ):
        for site, statement in (
            ('if', "IF " + expression + " THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;"),
            ('elsif', "IF FALSE THEN RETURN 'bad'; ELSIF " + expression + " THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;"),
            ('while', "WHILE " + expression + " LOOP RETURN 'yes'; END LOOP; RETURN 'no';"),
            ('exit', "LOOP EXIT WHEN " + expression + "; RETURN 'no'; END LOOP; RETURN 'yes';"),
        ):
            result.append((site + '_' + label, 'BEGIN ' + statement + ' END;', value, state))
    for label, expression, value, state in (
        ('select', '(SELECT 7)', '7', None),
        ('arithmetic', '1+(SELECT 7)', '8', None),
        ('empty', '(SELECT 7 WHERE FALSE)', None, None),
        ('null', '(SELECT NULL)', None, None),
        ('text_null', "(SELECT 'null')", 'null', None),
        ('text_empty', "(SELECT '')", '', None),
        ('text_quote', "(SELECT 'O''Brien')", "O'Brien", None),
        ('division', '(SELECT 1/0)', None, '22012'),
        ('multiple_rows', '(SELECT 7 UNION ALL SELECT 8)', None, '21000'),
        ('multiple_columns', '(SELECT 7,8)', None, '42601'),
        ('exists', 'EXISTS(SELECT 1)', 'true', None),
        ('exists_empty', 'EXISTS(SELECT 1 WHERE FALSE)', 'false', None),
        ('in', '1 IN(SELECT 1)', 'true', None),
        ('in_null', '1 IN(SELECT CAST(NULL AS INT))', None, None),
        ('in_untyped_null', '1 IN(SELECT NULL)', None, '42883'),
        ('not_in_null', '1 NOT IN(SELECT CAST(NULL AS INT))', None, None),
        ('any', '1=ANY(SELECT 1)', 'true', None),
        ('all', '1=ALL(SELECT 1)', 'true', None),
        ('derived', '(SELECT d.x FROM (SELECT 7 AS x) d)', '7', None),
        ('nested', '(SELECT (SELECT 7))', '7', None),
        ('typed_bigint', '(SELECT CAST(5000000000 AS BIGINT))', '5000000000', None),
        ('table_value', '(SELECT value FROM r281_rows WHERE id=2)', 'b', None),
        ('table_multiple', '(SELECT id FROM r281_rows)', None, '21000'),
        ('missing_empty_column', '(SELECT missing_column FROM r281_empty)', None, '42703'),
    ):
        result.append(('return_' + label, 'BEGIN RETURN ' + expression + '; END;', value, state))
    result += [
        ('default', 'DECLARE x INT:=(SELECT 7); BEGIN RETURN x; END;', '7', None),
        ('assignment', 'DECLARE x INT; BEGIN x:=(SELECT 7); RETURN x; END;', '7', None),
        ('default_null', 'DECLARE x INT:=(SELECT 7 WHERE FALSE); BEGIN RETURN x; END;', None, None),
        ('assignment_text_null', "DECLARE x TEXT; BEGIN x:=(SELECT 'null'); RETURN x; END;", 'null', None),
        ('for_controls', 'BEGIN FOR i IN (SELECT 1)..(SELECT 1) BY (SELECT 1) LOOP RETURN i; END LOOP; RETURN 0; END;', '1', None),
        ('local_parameter', 'DECLARE x INT:=7; BEGIN RETURN (SELECT x); END;', '7', None),
        ('qualified_local', '<<b>> DECLARE x INT:=7; BEGIN RETURN (SELECT b.x); END b;', '7', None),
        ('quoted_local', 'DECLARE "X" INT:=7; x INT:=9; BEGIN RETURN (SELECT "X"+x); END;', '16', None),
        ('dot_label', '<<"b.dot">> DECLARE x INT:=7; BEGIN RETURN (SELECT "b.dot".x); END "b.dot";', '7', None),
        ('shadowed_scope', '<<outer_b>> DECLARE x INT:=7; BEGIN <<inner_b>> DECLARE x INT:=9; BEGIN RETURN (SELECT outer_b.x+inner_b.x); END inner_b; END outer_b;', '16', None),
        ('source_collision', 'DECLARE id INT:=7; BEGIN RETURN (SELECT id FROM r281_empty); END;', None, '42702'),
        ('source_qualified', 'DECLARE id INT:=7; BEGIN RETURN (SELECT r281_rows.id FROM r281_rows WHERE r281_rows.id=2); END;', '2', None),
        ('correlated_inner', 'DECLARE wanted INT:=2; BEGIN RETURN (SELECT t.id FROM r281_rows t WHERE t.id=wanted); END;', '2', None),
        ('unreached', "BEGIN IF FALSE THEN RETURN (SELECT 1/0); END IF; RETURN 'no'; END;", 'no', None),
        ('expression_alias', 'BEGIN RETURN 7 AS result; END;', '7', None),
        ('expression_where_empty', 'BEGIN RETURN 7 WHERE FALSE; END;', None, None),
        ('expression_from', 'BEGIN RETURN id FROM r281_rows WHERE id=2; END;', '2', None),
        ('expression_from_empty', 'BEGIN RETURN id FROM r281_empty; END;', None, None),
        ('expression_from_multiple', 'BEGIN RETURN id FROM r281_rows; END;', None, '21000'),
        ('expression_limit', 'BEGIN RETURN id FROM r281_rows ORDER BY id DESC LIMIT 1; END;', '3', None),
        ('expression_count', 'BEGIN RETURN count(*) FROM r281_rows; END;', '3', None),
        ('condition_count', "BEGIN IF count(*)>0 FROM r281_rows THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
    ]
    assert len(result) == 74
    return result


def effects():
    return [
        ('missing_child_before_effect', "BEGIN RETURN nextval('__SEQ__')+(SELECT missing_column FROM r281_empty); END;", None, '42703', 0),
        ('child_width_before_effect', "BEGIN RETURN (SELECT nextval('__SEQ__'),1); END;", None, '42601', 0),
        ('outer_width_after_effect', "BEGIN RETURN nextval('__SEQ__'),1; END;", None, '42601', 1),
        ('star_width_after_effect', "BEGIN RETURN * FROM (SELECT nextval('__SEQ__') AS x,1 AS y) d; END;", None, '42601', 1),
        ('outer_all_rows_before_error', "BEGIN RETURN nextval('__SEQ__') FROM r281_rows; END;", None, '21000', 3),
        ('child_two_row_demand', "BEGIN RETURN (SELECT nextval('__SEQ__') FROM r281_rows); END;", None, '21000', 2),
        ('unreached_no_effect', "BEGIN IF FALSE THEN RETURN (SELECT nextval('__SEQ__')); END IF; RETURN 'safe'; END;", 'safe', None, 0),
        ('shortcircuit_no_effect', "BEGIN RETURN CASE WHEN TRUE THEN 7 ELSE (SELECT nextval('__SEQ__')) END; END;", '7', None, 0),
        ('default_once', "DECLARE x BIGINT:=(SELECT nextval('__SEQ__')); BEGIN RETURN x; END;", '1', None, 1),
        ('while_per_test_once', "DECLARE n INT:=0; BEGIN WHILE (SELECT nextval('__SEQ__'))<3 LOOP n:=n+1; IF n>5 THEN EXIT; END IF; END LOOP; RETURN n; END;", '2', None, 3),
        ('raise_once', "BEGIN RAISE NOTICE '%',(SELECT nextval('__SEQ__')); RETURN 'done'; END;", 'done', None, 1),
        ('empty_no_effect', "BEGIN RETURN nextval('__SEQ__') FROM r281_empty; END;", None, None, 0),
    ]


def run(client, runner, sock):
    controls = failures = 0
    def extended(sql):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        return client.read_until_ready(sock)
    for transport in ('simple', 'extended'):
        def expect(sql, rows=None, state=None, ready=b'T'):
            nonlocal controls, failures
            try:
                messages = client.simple_query(sock, sql) if transport == 'simple' else extended(sql)
            except Exception:
                print('SCALAR_QUERY_TRANSPORT_ERROR', transport, controls+1, sql, flush=True)
                raise
            actual_rows, actual_state, message, _, _ = runner.decode_wire_result(messages)
            controls += 1
            if actual_rows != (rows or []) or actual_state != state or messages[-1] != (b'Z', ready):
                failures += 1
                print('SCALAR_QUERY_FAIL', transport, sql, 'expected', rows or [], state, ready,
                      'actual', actual_rows, actual_state, messages[-1], message, flush=True)
        expect('BEGIN')
        try:
            expect('CREATE TEMP TABLE r281_rows(id INT,value TEXT)')
            expect("INSERT INTO r281_rows VALUES(1,'a'),(2,'b'),(3,'c')")
            expect('CREATE TEMP TABLE r281_empty(id INT)')
            for label, body, value, state in cases():
                function = 'r281_' + uuid.uuid4().hex[:18]
                expect('CREATE FUNCTION ' + function + '() RETURNS TEXT LANGUAGE plpgsql AS $r281$' + body + '$r281$')
                expect('SAVEPOINT check_scalar_query')
                expect('SELECT ' + function + '()', None if state else [[value]], state, b'E' if state else b'T')
                expect('SELECT 1', None if state else [['1']], '25P02' if state else None, b'E' if state else b'T')
                expect('ROLLBACK TO SAVEPOINT check_scalar_query')
                expect('RELEASE SAVEPOINT check_scalar_query')
            for label, body, value, state, calls in effects():
                sequence = 'r281_seq_' + uuid.uuid4().hex[:16]
                function = 'r281_effect_' + uuid.uuid4().hex[:16]
                expect('CREATE SEQUENCE ' + sequence + ' START 1')
                expect('CREATE FUNCTION ' + function + '() RETURNS TEXT LANGUAGE plpgsql AS $r281$' + body.replace('__SEQ__', sequence) + '$r281$')
                expect('SAVEPOINT check_scalar_query')
                expect('SELECT ' + function + '()', None if state else [[value]], state, b'E' if state else b'T')
                expect('SELECT 1', None if state else [['1']], '25P02' if state else None, b'E' if state else b'T')
                expect('ROLLBACK TO SAVEPOINT check_scalar_query')
                expect('RELEASE SAVEPOINT check_scalar_query')
                expect('SAVEPOINT check_counter')
                expect("SELECT currval('" + sequence + "')", [[str(calls)]] if calls else None,
                       None if calls else '55000', b'T' if calls else b'E')
                expect('ROLLBACK TO SAVEPOINT check_counter')
                expect('RELEASE SAVEPOINT check_counter')
                expect("SELECT nextval('" + sequence + "')", [[str(calls + 1)]])
        finally:
            expect('ROLLBACK', ready=b'I')
    print('PLPGSQL_SCALAR_QUERY_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 1186 and failures == 0, (controls, failures)
    return controls
