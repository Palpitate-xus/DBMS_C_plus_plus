"""Integer FOR: int4 bounds, ordered coercion, BY, scoped iterator and FOUND."""
import struct
import uuid


def cases():
    result = []
    def sequence(label, interval, expected, declaration=''):
        body = ("DECLARE s TEXT := ''; guard_count INT := 0; " + declaration + ' BEGIN FOR i IN ' + interval +
                " LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;")
        result.append((label, body, expected, None))
    def error(label, interval, state):
        result.append((label, 'BEGIN FOR i IN ' + interval +
                       " LOOP EXIT; END LOOP; RETURN 'done'; END;", None, state))
    sequence('forward', '1..4', '1,2,3,4,')
    sequence('reverse', 'REVERSE 4..1', '4,3,2,1,')
    sequence('empty', '4..1', '')
    sequence('reverse_empty', 'REVERSE 1..4', '')
    sequence('maximum', '2147483647..2147483647', '2147483647,')
    sequence('minimum', 'REVERSE -2147483648..-2147483648', '-2147483648,')
    sequence('step', '1..8 BY 3', '1,4,7,')
    sequence('reverse_step', 'REVERSE 8..1 BY 3', '8,5,2,')
    sequence('positive_overflow_exit', '2147483646..2147483647 BY 2', '2147483646,')
    sequence('negative_overflow_exit', 'REVERSE -2147483647..-2147483648 BY 2', '-2147483647,')
    sequence('full_width_step', '-2147483648..2147483647 BY 2147483647', '-2147483648,-1,2147483646,')
    sequence('full_width_reverse_step', 'REVERSE 2147483647..-2147483648 BY 2147483647', '2147483647,0,-2147483647,')
    sequence('numeric_rounding', '1.6..4.4 BY 1.6', '2,4,')
    sequence('text_input', "'  +2  '..'4' BY '0x2'", '2,4,')
    sequence('trivia', "1 /* .. BY LOOP */ .. 4 /* BY */ BY (1+1)", '1,3,')
    sequence('quoted_by', '1..4 BY "by"', '1,3,', '"by" INT := 2;')
    error('lower_above_int4', '2147483648..2147483648', '22003')
    error('upper_above_int4', '1..2147483648', '22003')
    error('lower_below_int4', '-2147483649..1', '22003')
    error('upper_below_int4', 'REVERSE 1..-2147483649', '22003')
    error('int8_max_not_iterator', '9223372036854775807..9223372036854775807', '22003')
    error('huge_numeric', '999999999999999999999999999999..1', '22003')
    error('step_above_int4', '4..1 BY 2147483648', '22003')
    error('null_lower', 'NULL..1', '22004')
    error('null_upper', '1..NULL', '22004')
    error('null_step', '1..2 BY NULL', '22004')
    error('zero_step_empty', '4..1 BY 0', '22023')
    error('negative_step', '1..4 BY -1', '22023')
    error('reverse_negative_step', 'REVERSE 4..1 BY -1', '22023')
    error('rounded_zero_step', '1..4 BY 0.4', '22023')
    error('invalid_text', "'bad'..1", '22P02')
    error('range_in_literal', "'..'..1", '22P02')
    error('fractional_text', "'1.6'..4", '22P02')
    error('fractional_text_step', "1..4 BY '1.6'", '22P02')
    error('lower_null_before_upper_error', 'NULL..(1/0)', '22004')
    error('lower_error_before_upper_null', '(1/0)..NULL', '22012')
    error('upper_null_before_step_error', '1..NULL BY (1/0)', '22004')
    error('upper_error_before_step_null', '1..(1/0) BY NULL', '22012')
    result += [
        ('evaluate_once', "DECLARE s TEXT := ''; lo INT := 1; hi INT := 4; st INT := 2; BEGIN "
         "FOR i IN lo..hi BY st LOOP s := s || i || ','; lo:=100; hi:=0; st:=99; END LOOP; RETURN s; END;", '1,3,', None),
        ('iterator_assignment', "DECLARE s TEXT := ''; BEGIN FOR i IN 1..3 LOOP s:=s||i||','; i:=99; END LOOP; RETURN s; END;", '1,2,3,', None),
        ('outer_null', 'DECLARE i TEXT := NULL; BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN i; END;', None, None),
        ('outer_literal_null', "DECLARE i TEXT := 'null'; BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN i; END;", 'null', None),
        ('nested_iterator', "DECLARE s TEXT := ''; BEGIN FOR i IN 1..2 LOOP s:=s||i||':'; FOR i IN REVERSE 3..2 LOOP s:=s||i||','; END LOOP; s:=s||i||';'; END LOOP; RETURN s; END;", '1:3,2,1;2:3,2,2;', None),
        ('found_nonempty', "BEGIN FOR i IN 1..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
        ('found_empty', "BEGIN FOUND:=TRUE; FOR i IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'no', None),
        ('found_not_set_inside', "BEGIN FOR i IN 1..1 LOOP IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END LOOP; RETURN 'empty'; END;", 'no', None),
        ('nested_found', "BEGIN FOR i IN 1..1 LOOP FOR j IN 2..1 LOOP EXIT; END LOOP; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
    ]
    result += [
        ('found_iterator_nonempty', "BEGIN FOR found IN 1..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'yes', None),
        ('found_iterator_empty', "BEGIN FOR found IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'no', None),
        ('found_iterator_empty_after_true', "BEGIN FOUND:=TRUE; FOR found IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;", 'no', None),
        ('bound_reads_outer_iterator', "DECLARE i INT:=7; BEGIN FOR i IN i..i LOOP RETURN 'entered'; END LOOP; RETURN 'empty'; END;", 'entered', None),
        ('upper_protected_range', "DECLARE s TEXT:=''; BEGIN FOR i IN 1..LENGTH('..') BY LENGTH('BY') LOOP s:=s||i||','; END LOOP; RETURN s; END;", '1,', None),
        ('lower_protected_range', "DECLARE s TEXT:=''; BEGIN FOR i IN LENGTH('..')..3 LOOP s:=s||i||','; END LOOP; RETURN s; END;", '2,3,', None),
        ('text_null_not_null', "BEGIN FOR i IN 'null'..1 LOOP EXIT; END LOOP; RETURN 'done'; END;", None, '22P02'),
        ('negative_numeric_rounding', "DECLARE s TEXT:=''; BEGIN FOR i IN -1.6..1.6 BY 1.6 LOOP s:=s||i||','; END LOOP; RETURN s; END;", '-2,0,2,', None),
        ('floating_ties_even', "DECLARE s TEXT:=''; BEGIN FOR i IN CAST(1.5 AS double precision)..CAST(4.5 AS double precision) BY CAST(2.5 AS double precision) LOOP s:=s||i||','; END LOOP; RETURN s; END;", '2,4,', None),
    ]
    result += [('upper_quoted_by_field', '<<b>> DECLARE "by" INT:=2; s TEXT:=\'\'; BEGIN FOR i IN 1..b."by" LOOP s:=s||i||\',\'; END LOOP; RETURN s; END b;', '1,2,', None), ('lower_qualified_by_field', '<<b>> DECLARE "by" INT:=2; s TEXT:=\'\'; BEGIN FOR i IN b.by..3 LOOP s:=s||i||\',\'; END LOOP; RETURN s; END b;', '2,3,', None), ('step_qualified_by_field', '<<b>> DECLARE "by" INT:=2; s TEXT:=\'\'; BEGIN FOR i IN 1..3 BY b.by LOOP s:=s||i||\',\'; END LOOP; RETURN s; END b;', '1,3,', None), ('step_qualified_by_field_trivia', '<<b>> DECLARE "by" INT:=2; s TEXT:=\'\'; BEGIN FOR i IN 1..3 BY b /*x*/ . /*y*/ by LOOP s:=s||i||\',\'; END LOOP; RETURN s; END b;', '1,3,', None)]
    assert len(result) == 60
    return result


def run(client, runner, sock):
    controls = failures = 0
    def extended(sql):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        execute = b'\0' + struct.pack('!I', 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'E', execute) + client.typed(b'S'))
        return client.read_until_ready(sock)
    for transport in ('simple', 'extended'):
        def expect(sql, rows=None, state=None, ready=b'T'):
            nonlocal controls, failures
            messages = client.simple_query(sock, sql) if transport == 'simple' else extended(sql)
            actual_rows, actual_state, message, _, _ = runner.decode_wire_result(messages)
            controls += 1
            if actual_rows != (rows or []) or actual_state != state or messages[-1] != (b'Z', ready):
                failures += 1
                print('INTEGER_FOR_FAIL', transport, sql, 'expected', rows or [], state, ready,
                      'actual', actual_rows, actual_state, messages[-1], message, flush=True)
        expect('BEGIN')
        try:
            for label, body, value, state in cases():
                function = 'r277_' + uuid.uuid4().hex[:18]
                expect('CREATE FUNCTION ' + function + '() RETURNS TEXT LANGUAGE plpgsql AS $r277$' + body + '$r277$')
                expect('SAVEPOINT check_for')
                expect('SELECT ' + function + '()', None if state else [[value]], state, b'E' if state else b'T')
                expect('SELECT 1', None if state else [['1']], '25P02' if state else None, b'E' if state else b'T')
                expect('ROLLBACK TO SAVEPOINT check_for')
                expect('RELEASE SAVEPOINT check_for')
        finally:
            expect('ROLLBACK', ready=b'I')
    print('PLPGSQL_INTEGER_FOR_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 724 and failures == 0, (controls, failures)
    return controls
