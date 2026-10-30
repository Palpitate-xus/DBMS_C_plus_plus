"""Immediate EXCLUDE checks see own current-command writes, not SELECTs."""
import struct
import uuid


def run(client, runner, sock):
    controls = failures = 0
    cases = [
        ('text', 'TEXT', 'btree', '=', "'x'", "'y'", "'z'", "'p'", "'q'", 'x', 'y', 'z', 'p', 'q'),
        ('varchar', 'VARCHAR(32)', 'btree', '=', "'aa'", "'bb'", "'cc'", "'dd'", "'ee'", 'aa', 'bb', 'cc', 'dd', 'ee'),
        ('integer', 'INT', 'btree', '=', '1', '2', '3', '4', '5', '1', '2', '3', '4', '5'),
        ('range', 'int4range', 'gist', '&&', "'[1,5)'", "'[10,15)'", "'[20,25)'", "'[30,35)'", "'[40,45)'", '[1,5)', '[10,15)', '[20,25)', '[30,35)', '[40,45)'),
        ('composite', 'TEXT', 'btree', '=', "'x'", "'y'", "'z'", "'p'", "'q'", 'x', 'y', 'z', 'p', 'q'),
        ('partial', 'TEXT', 'btree', '=', "'x'", "'y'", "'z'", "'p'", "'q'", 'x', 'y', 'z', 'p', 'q'),
    ]
    def extended(sql):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
        execute = b'\0' + struct.pack('!I', 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'E', execute) + client.typed(b'S'))
        return client.read_until_ready(sock)

    for transport in ('simple', 'extended'):
        for label, datatype, method, op, a, b, duplicate, e, f, ta, tb, td, te, tf in cases:
            table = 'r276_' + uuid.uuid4().hex[:18]
            name = 'ex_' + uuid.uuid4().hex[:16]
            def expect(sql, rows=None, state=None, ready=b'T'):
                nonlocal controls, failures
                messages = client.simple_query(sock, sql) if transport == 'simple' else extended(sql)
                actual_rows, actual_state, message, _, _ = runner.decode_wire_result(messages)
                controls += 1
                if actual_rows != (rows or []) or actual_state != state or messages[-1] != (b'Z', ready):
                    failures += 1
                    print('EXCLUSION_COMMAND_FAIL', transport, label, sql,
                          'expected=', rows or [], state, ready,
                          'actual=', actual_rows, actual_state, messages[-1], message, flush=True)
            expect('BEGIN')
            try:
                elements = ('k WITH =,' if label == 'composite' else '') + 'v WITH ' + op
                predicate = ' WHERE(enabled)' if label == 'partial' else ''
                expect('CREATE TABLE ' + table + '(id INT PRIMARY KEY,k INT,v ' + datatype +
                       ',enabled BOOLEAN,CONSTRAINT ' + name + ' EXCLUDE USING ' + method +
                       '(' + elements + ')' + predicate + ' NOT DEFERRABLE)')
                expect('INSERT INTO ' + table + ' VALUES(1,1,' + a + ',TRUE)')
                expect('INSERT INTO ' + table + ' VALUES(2,1,' + b + ',TRUE)')
                select = 'SELECT id,v FROM ' + table + ' ORDER BY id'
                before = [['1', ta], ['2', tb]]
                expect(select, before)
                expect('SAVEPOINT values_error')
                expect('INSERT INTO ' + table + ' VALUES(3,1,' + duplicate + ',TRUE),(4,1,' + duplicate + ',TRUE)', state='23P01', ready=b'E')
                expect(select, state='25P02', ready=b'E')
                expect('ROLLBACK TO SAVEPOINT values_error')
                expect(select, before)
                expect('SAVEPOINT select_error')
                expect('INSERT INTO ' + table + '(id,k,v,enabled) SELECT id+10,1,' + duplicate +
                       ',TRUE FROM ' + table + ' WHERE id<=2', state='23P01', ready=b'E')
                expect(select, state='25P02', ready=b'E')
                expect('ROLLBACK TO SAVEPOINT select_error')
                expect(select, before)
                expect('SAVEPOINT update_error')
                expect('UPDATE ' + table + ' SET v=' + duplicate + ' WHERE id IN (1,2)', state='23P01', ready=b'E')
                expect(select, state='25P02', ready=b'E')
                expect('ROLLBACK TO SAVEPOINT update_error')
                expect(select, before)
                expect('INSERT INTO ' + table + '(id,k,v,enabled) SELECT id+10,k,' + duplicate +
                       ',TRUE FROM ' + table + ' WHERE id=1')
                three = before + [['11', td]]
                expect(select, three)
                expect('SAVEPOINT null_values')
                expect('INSERT INTO ' + table + ' VALUES(20,1,NULL,TRUE)')
                expect('INSERT INTO ' + table + ' VALUES(21,1,NULL,TRUE)')
                expect(select, three + [['20', None], ['21', None]])
                expect('ROLLBACK TO SAVEPOINT null_values')
                expect(select, three)
                if label == 'composite':
                    expect('INSERT INTO ' + table + ' VALUES(30,2,' + duplicate + ',TRUE),(31,3,' + duplicate + ',TRUE)')
                    extra = [['30', td], ['31', td]]
                elif label == 'partial':
                    expect('INSERT INTO ' + table + ' VALUES(30,1,' + duplicate + ',FALSE),(31,1,' + duplicate + ',FALSE)')
                    extra = [['30', td], ['31', td]]
                else:
                    expect('INSERT INTO ' + table + ' VALUES(30,1,' + e + ',TRUE),(31,1,' + f + ',TRUE)')
                    extra = [['30', te], ['31', tf]]
                expect(select, three + extra)
            finally:
                expect('ROLLBACK', ready=b'I')
    print('EXCLUSION_COMMAND_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 372 and failures == 0, (controls, failures)
    return controls
