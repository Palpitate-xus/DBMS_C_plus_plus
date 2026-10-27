"""Strict EXCLUDE violation class for separate INSERT and UPDATE statements.

This intentionally does not assert correctness of same-command duplicate
visibility, empty-text keys, opclasses, or concurrent exclusion enforcement.
"""
import struct
import uuid


def run(client, runner, sock):
    controls = failures = 0
    cases = [
        ('text', 'TEXT', 'btree', '=', "'x'", "'y'", "'x'", "'z'", 'x', 'y', 'z'),
        ('varchar', 'VARCHAR(32)', 'btree', '=', "'aa'", "'bb'", "'aa'", "'cc'", 'aa', 'bb', 'cc'),
        ('integer', 'INT', 'btree', '=', '1', '2', '1', '3', '1', '2', '3'),
        ('range', 'int4range', 'gist', '&&', "'[1,5)'", "'[10,15)'", "'[2,3)'", "'[20,25)'", '[1,5)', '[10,15)', '[20,25)'),
        ('literal_null', 'TEXT', 'btree', '=', "'NULL'", "'other'", "'NULL'", "'third'", 'NULL', 'other', 'third'),
    ]

    def extended(sql):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        execute = b'\0' + struct.pack('!I', 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'E', execute) + client.typed(b'S'))
        return client.read_until_ready(sock)

    for transport in ('simple', 'extended'):
        for label, datatype, method, operator, a, b, bad, good, text_a, text_b, text_good in cases:
            table = 'r275_' + uuid.uuid4().hex[:18]
            name = 'ex_' + uuid.uuid4().hex[:16]

            def expect(sql, rows=None, state=None, ready=b'T'):
                nonlocal controls, failures
                messages = (client.simple_query(sock, sql) if transport == 'simple'
                            else extended(sql))
                actual_rows, actual_state, message, _, _ = runner.decode_wire_result(messages)
                controls += 1
                if (actual_rows != (rows or []) or actual_state != state or
                        messages[-1] != (b'Z', ready)):
                    failures += 1
                    print('EXCLUSION_SQLSTATE_FAIL', transport, label, sql,
                          'expected=', rows or [], state, ready,
                          'actual=', actual_rows, actual_state, messages[-1], message, flush=True)

            expect('BEGIN')
            try:
                expect('CREATE TABLE ' + table + '(id INT PRIMARY KEY,v ' + datatype +
                       ',CONSTRAINT ' + name + ' EXCLUDE USING ' + method +
                       '(v WITH ' + operator + '))')
                expect('INSERT INTO ' + table + ' VALUES(1,' + a + ')')
                expect('INSERT INTO ' + table + ' VALUES(2,' + b + ')')
                before = [['1', text_a], ['2', text_b]]
                select = 'SELECT id,v FROM ' + table + ' ORDER BY id'
                expect(select, before)
                expect('SAVEPOINT insert_error')
                expect('INSERT INTO ' + table + ' VALUES(3,' + bad + ')', state='23P01', ready=b'E')
                expect(select, state='25P02', ready=b'E')
                expect('ROLLBACK TO SAVEPOINT insert_error')
                expect(select, before)
                expect('SAVEPOINT update_error')
                expect('UPDATE ' + table + ' SET v=' + bad + ' WHERE id=2', state='23P01', ready=b'E')
                expect(select, state='25P02', ready=b'E')
                expect('ROLLBACK TO SAVEPOINT update_error')
                expect(select, before)
                expect('UPDATE ' + table + ' SET v=' + good + ' WHERE id=2')
                expect(select, [['1', text_a], ['2', text_good]])
                expect('INSERT INTO ' + table + ' VALUES(4,NULL)')
                expect('INSERT INTO ' + table + ' VALUES(5,NULL)')
                expect(select, [['1', text_a], ['2', text_good], ['4', None], ['5', None]])
            finally:
                expect('ROLLBACK', ready=b'I')
    print('EXCLUSION_SQLSTATE_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 210 and failures == 0, (controls, failures)
    return controls
