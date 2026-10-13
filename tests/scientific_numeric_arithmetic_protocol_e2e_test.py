#!/usr/bin/env python3
"""Exact value AND display scale of exponent NUMERIC arithmetic (R34)."""
from decimal import Decimal, localcontext
import importlib.util
from pathlib import Path
import socket
import struct
import sys


PAIRS = (
    ('1e-40', '2e-40'), ('1.250e-2', '2.50e-3'),
    ('1.00e+40', '2.0E+1'), ('-.5E-2', '5.e-3'),
    ('0e-40', '2e-40'), ('-0.00E-2', '-3.0e-2'),
    ('1e400', '1'), ('1E-400', '2e-100'),
)


def expected_arithmetic(left, right, operator):
    # Independent decimal oracle, not the engine's scale implementation.
    # PostgreSQL runs the identical exact-string assertions below.
    with localcontext() as context:
        context.prec = 1100
        a, b = Decimal(left), Decimal(right)
        scale_a = max(0, -a.as_tuple().exponent)
        scale_b = max(0, -b.as_tuple().exponent)
        if operator == '+':
            value = a + b
        elif operator == '-':
            value = a - b
        elif operator == '*':
            value = a * b
        else:
            value = a % b
        scale = scale_a + scale_b if operator == '*' else max(scale_a, scale_b)
        if value.is_zero():
            value = value.copy_abs()
        return format(value, '.' + str(scale) + 'f')


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('scientific_arithmetic_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    failures = []
    controls = 0

    def record(label, valid, actual, expected):
        nonlocal controls
        controls += 1
        print('SCIENTIFIC_ARITHMETIC', label, actual, 'expected', expected, 'match', valid, flush=True)
        if not valid:
            failures.append((label, actual, expected))

    def check(sql, rows=None, oids=None, state=None, tag=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == (tag or 'SELECT '+str(len(rows)))
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and result[0] == [] and result[4] is None
        record(sql, valid, result, (rows, oids, state, tag))

    def describe(sql):
        name = ('scientific_arithmetic_'+str(controls)).encode()
        payload = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H', 0)
        sock.sendall(client.typed(b'P', payload)+client.typed(b'D', b'S'+name+b'\0')+client.typed(b'S'))
        messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        valid = not any(kind in (b'E', b'D', b'C') for kind, _ in messages)
        valid = valid and [field[3] for field in fields] == [1700]
        valid = valid and sum(kind == b'1' for kind, _ in messages) == 1
        record('DESCRIBE '+sql, valid, messages, 'pure metadata, NUMERIC')

    def parameter(sql, inputs, expected):
        parse = b'\0'+sql.encode()+b'\0'+struct.pack('!H', len(inputs))
        parse += b''.join(struct.pack('!I', 1700) for _ in inputs)
        bind = b'\0\0'+struct.pack('!HH', 0, len(inputs))
        for value in inputs:
            raw = None if value is None else value.encode()
            bind += struct.pack('!i', -1 if raw is None else len(raw))+(raw or b'')
        bind += struct.pack('!H', 0)
        sock.sendall(client.typed(b'P', parse)+client.typed(b'D', b'S\0')+
                     client.typed(b'B', bind)+client.typed(b'D', b'P\0')+
                     client.typed(b'E', b'\0'+struct.pack('!I', 0))+client.typed(b'S'))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages, include_types=True)
        descriptions = [client.row_description_fields([message]) for message in messages if message[0] == b'T']
        valid = result[1] is None and result[0] == [[expected]] and result[4] == 'SELECT 1'
        valid = valid and len(descriptions) == 2 and all([field[3] for field in fields] == [1700] for fields in descriptions)
        record(('PARAMETER', sql, inputs), valid, result, expected)

    try:
        # The original R32 failure is deliberately retained as exact text.
        check('SELECT 1e-40+1e-40 AS n', [['0.'+'0'*39+'2']], [1700])
        for a, b in PAIRS:
            for operator in ('+', '-', '*', '%', 'mod'):
                expression = 'mod('+a+','+b+')' if operator == 'mod' else a+' '+operator+' '+b
                check('SELECT '+expression+' AS n', [[expected_arithmetic(a, b, operator)]], [1700])
        for expression, expected in (
            ('1e-40-1e-40', '0.'+'0'*40),
            ('1e-20*1e-20', '0.'+'0'*39+'1'),
            ('1e-40/1e-40', '1.'+'0'*40),
            ('0e-40/1e0', '0.'+'0'*40),
            ('1.250e-2/2.50e-3', '5.0000000000000000'),
            ('1e40/2e0', '5'+'0'*39),
            ('1e-40/2e0', '0.'+'0'*40+'5'+'0'*19),
            ('div(5e-40,2e-40)', '2'),
            ('CAST(gcd(6e-40,4e-40) AS numeric)', '0.'+'0'*39+'2'),
            ('CAST(lcm(6e-40,4e-40) AS numeric)', '0.'+'0'*38+'12'),
            ("'1e-40'::numeric+1e-40", '0.'+'0'*39+'2'),
            ('CAST(1.250e-2 AS NUMERIC(10,5))+2.50e-3', '0.01500'),
            ('CAST(1e-40+1e-40 AS NUMERIC(50,40))', '0.'+'0'*39+'2'),
            ('1.25+2.50', '3.75'), ('1.25*2.50', '3.1250'),
        ):
            check('SELECT '+expression+' AS n', [[expected]], [1700])
        for operator in ('+', '-', '*', '/', '%', 'mod'):
            expression = 'mod(NULL::numeric,1e-40)' if operator == 'mod' else 'NULL::numeric'+operator+'1e-40'
            check('SELECT '+expression+' AS n', [[None]], [1700])
        for special in ('NaN', 'Infinity', '-Infinity'):
            for operator in ('+', '*', '%', 'mod'):
                left = "'"+special+"'::numeric"
                expression = 'mod('+left+',1e-40)' if operator == 'mod' else left+operator+'1e-40'
                expected = 'NaN' if operator in ('%', 'mod') else special
                check('SELECT '+expression+' AS n', [[expected]], [1700])
        for expression in ('1e-40/0e-40', '1e-40%0e-40', 'mod(1e-40,0e-40)'):
            check('SELECT '+expression+' AS n', state='22012')
            describe('SELECT '+expression+' AS n')
        check("SELECT '1e-40'::text", [['1e-40']], [25])
        for operator in ('+', '-', '*', '%', 'mod'):
            sql = 'SELECT '+('mod($1,$2)' if operator == 'mod' else '$1'+operator+'$2')+' AS n'
            for a, b in (('1e-40', '2e-40'), ('1.250e-2', '2.50e-3'), ('0e-40', '2e-40')):
                parameter(sql, (a, b), expected_arithmetic(a, b, operator))
            parameter(sql, (None, '1e-40'), None)
        parameter('SELECT $1/$2 AS n', ('1e-40', '1e-40'), '1.'+'0'*40)
        check('CREATE TEMP TABLE scientific_arithmetic_rows(id INT PRIMARY KEY,a NUMERIC,b NUMERIC)')
        for index, (a, b) in enumerate(PAIRS[:6], 1):
            check('INSERT INTO scientific_arithmetic_rows VALUES('+str(index)+','+a+','+b+')')
        for operator in ('+', '-', '*', '%', 'mod'):
            expression = 'mod(a,b)' if operator == 'mod' else 'a'+operator+'b'
            expected = [[expected_arithmetic(a, b, operator)] for a, b in PAIRS[:6]]
            check('SELECT '+expression+' AS n FROM scientific_arithmetic_rows ORDER BY id', expected, [1700])
        check('UPDATE scientific_arithmetic_rows SET a=1e-40+1e-40 WHERE id=1 RETURNING a',
              [['0.'+'0'*39+'2']], [1700], tag='UPDATE 1')
        check('INSERT INTO scientific_arithmetic_rows VALUES(7,1e-20*1e-20,1) RETURNING a',
              [['0.'+'0'*39+'1']], [1700], tag='INSERT 0 1')
        check('SELECT a+CAST(1e-40 AS numeric) AS n FROM scientific_arithmetic_rows WHERE id=1', [['0.'+'0'*39+'3']], [1700])
        check('SELECT 1e-40+1e-40 AS n FROM scientific_arithmetic_rows WHERE false', [], [1700])
        check('SELECT 1e-40+1e-40 AS n LIMIT 0', [], [1700])
        check('CREATE TEMP SEQUENCE scientific_arithmetic_calls')
        describe("SELECT 1e-40+nextval('scientific_arithmetic_calls') AS n")
        check("SELECT currval('scientific_arithmetic_calls')", state='55000')
        check('SELECT 42', [['42']], [23])
        assert controls == 123, controls
        print('SCIENTIFIC_ARITHMETIC_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
