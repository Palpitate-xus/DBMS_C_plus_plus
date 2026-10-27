"""PL/pgSQL IF/ELSIF/WHILE/EXIT: nullable boolean input, not truthiness."""
import struct
import uuid


def cases():
    values = [
        ('true', 'TRUE', 'yes', None), ('false', 'FALSE', 'no', None),
        ('null', 'NULL', 'no', None), ('int_zero', '0', 'no', None),
        ('int_one', '1', 'yes', None), ('int_two', '2', None, '22P02'),
        ('negative', '-1', None, '22P02'),
        ('smallint_one', 'CAST(1 AS smallint)', 'yes', None),
        ('bigint_two', 'CAST(2 AS bigint)', None, '22P02'),
        ('numeric_one', 'CAST(1 AS numeric)', 'yes', None),
        ('numeric_zero', 'CAST(0 AS numeric)', 'no', None),
        ('numeric_two', 'CAST(2 AS numeric)', None, '22P02'),
        ('float_one', 'CAST(1 AS double precision)', 'yes', None),
        ('yes', "'yes'", 'yes', None), ('on', "'on'", 'yes', None),
        ('off_prefix', "'of'", 'no', None), ('ambiguous', "'o'", None, '22P02'),
        ('text_true', "'true'", 'yes', None), ('text_one', "'1'", 'yes', None),
        ('text_false', "'false'", 'no', None), ('text_zero', "'0'", 'no', None),
        ('empty', "''", None, '22P02'), ('text_null', "'null'", None, '22P02'),
        ('invalid', "'invalid'", None, '22P02'), ('typed_text', "CAST('t' AS TEXT)", 'yes', None),
        ('null_int', 'CAST(NULL AS INT)', 'no', None),
        ('null_date', 'CAST(NULL AS DATE)', 'no', None),
        ('date', "CAST('2020-01-01' AS DATE)", None, '22P02'),
        ('division_error', '(1/0)', None, '22012'),
        ('cast_error', "CAST('bad' AS INT)", None, '22P02'),
        ('uppercase_prefix', "'Y'", 'yes', None), ('mixed_case', "'YeS'", 'yes', None),
        ('true_prefix', "'TR'", 'yes', None), ('whitespace', "' FALSE '", 'no', None),
        ('off', "'off'", 'no', None), ('no', "'no'", 'no', None),
        ('mixed_on', "'oN'", 'yes', None), ('false_suffix', "'falsely'", None, '22P02'),
    ]
    variables = [
        ('variable_yes', "x TEXT:='yes';", 'x', 'yes', None),
        ('variable_text_null', "x TEXT:='null';", 'x', None, '22P02'),
        ('variable_null', 'x TEXT:=NULL;', 'x', 'no', None),
        ('variable_one', 'x INT:=1;', 'x', 'yes', None),
        ('variable_two', 'x INT:=2;', 'x', None, '22P02'),
        ('variable_bool_null', 'x BOOLEAN:=NULL;', 'x', 'no', None),
        ('found_false', '', 'FOUND', 'no', None),
        ('found_true', '', 'NOT FOUND', 'yes', None),
    ]
    result = []
    for label, declaration, expr, value, state in [
            (label, '', expr, value, state) for label, expr, value, state in values] + variables:
        prefix = ('DECLARE ' + declaration + ' ' if declaration else '') + 'BEGIN '
        for site, statement in (
            ('if', "IF " + expr + " THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;"),
            ('elsif', "IF FALSE THEN RETURN 'wrong'; ELSIF " + expr + " THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;"),
            ('while', "WHILE " + expr + " LOOP RETURN 'yes'; END LOOP; RETURN 'no';"),
            ('exit', "LOOP EXIT WHEN " + expr + "; RETURN 'no'; END LOOP; RETURN 'yes';"),
        ):
            result.append((site + '_' + label, prefix + statement + ' END;', value, state))
    result += [
        ('if_skip_elsif_error', "BEGIN IF 'yes' THEN RETURN 'yes'; ELSIF (1/0) THEN RETURN 'bad'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
        ('elsif_skip_later_error', "BEGIN IF NULL THEN RETURN 'bad'; ELSIF 'on' THEN RETURN 'yes'; ELSIF (1/0) THEN RETURN 'bad'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
        ('while_recheck', "DECLARE i INT:=0; BEGIN WHILE i<3 LOOP i:=i+1; END LOOP; RETURN i; END;", '3', None),
        ('exit_recheck', "DECLARE i INT:=0; BEGIN LOOP i:=i+1; EXIT WHEN i=3; END LOOP; RETURN i; END;", '3', None),
    ]
    assert len(result) == 188
    return result


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
            messages = client.simple_query(sock, sql) if transport == 'simple' else extended(sql)
            actual_rows, actual_state, message, _, _ = runner.decode_wire_result(messages)
            controls += 1
            if actual_rows != (rows or []) or actual_state != state or messages[-1] != (b'Z', ready):
                failures += 1
                print('BOOLEAN_CONDITION_FAIL', transport, sql, 'expected', rows or [], state, ready,
                      'actual', actual_rows, actual_state, messages[-1], message, flush=True)
        expect('BEGIN')
        try:
            for label, body, value, state in cases():
                function = 'r280_' + uuid.uuid4().hex[:18]
                expect('CREATE FUNCTION ' + function + '() RETURNS TEXT LANGUAGE plpgsql AS $r280$' + body + '$r280$')
                expect('SAVEPOINT check_boolean')
                expect('SELECT ' + function + '()', None if state else [[value]], state, b'E' if state else b'T')
                expect('SELECT 1', None if state else [['1']], '25P02' if state else None, b'E' if state else b'T')
                expect('ROLLBACK TO SAVEPOINT check_boolean')
                expect('RELEASE SAVEPOINT check_boolean')
        finally:
            expect('ROLLBACK', ready=b'I')
    print('PLPGSQL_BOOLEAN_CONDITIONS_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 2260 and failures == 0, (controls, failures)
    return controls
