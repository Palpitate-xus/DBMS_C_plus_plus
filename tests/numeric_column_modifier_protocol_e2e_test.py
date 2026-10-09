"""Declared numeric modifiers govern storage, arrays and wire metadata."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('numeric_column_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    failures = []
    checked = 0

    def check(sql, rows=None, oids=None, state=None, modifiers=None, tag=None):
        nonlocal checked
        checked += 1
        if reference and state:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT numeric_error'))[1] is None
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == (tag or 'SELECT ' + str(len(rows)))
        if oids is not None:
            valid = valid and result[5] == oids
        if modifiers is not None:
            actual = [field[5] for field in client.row_description_fields(messages)]
            valid = valid and actual == modifiers
            print('NUMERIC_MODIFIERS', actual, 'expected', modifiers, flush=True)
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print('NUMERIC_COLUMN', sql, result, 'pass=', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids, state, modifiers))
        if reference and state:
            assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO numeric_error'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE numeric_error'))[1] is None

    try:
        if reference:
            check('BEGIN')
        check('CREATE TABLE numeric_modifier_rows(id INT PRIMARY KEY,v NUMERIC(5,2),n NUMERIC(3,-2),s NUMERIC(2,4),a NUMERIC(4,1)[],u NUMERIC)')
        check("INSERT INTO numeric_modifier_rows VALUES(1,12.345,12345,0.001234,'{12.35,NULL,-12.35}',12)")
        check('SELECT v,n,s,a,u FROM numeric_modifier_rows', [['12.35','12300','0.0012','{12.4,NULL,-12.4}','12']], [1700,1700,1700,1231,1700], modifiers=[(5<<16)+2+4,(3<<16)+2046+4,(2<<16)+4+4,(4<<16)+1+4,-1])
        check("UPDATE numeric_modifier_rows SET v=10,n=-12345,s=0.00125,a='{1.25,NULL}' WHERE id=1")
        check('SELECT v,n,s,a FROM numeric_modifier_rows', [['10.00','-12300','0.0013','{1.3,NULL}']], [1700,1700,1700,1231])
        check('INSERT INTO numeric_modifier_rows(id,v) SELECT 2,20')
        check('SELECT (SELECT sum(v) FROM numeric_modifier_rows)', [['30.00']], [1700])
        check('CREATE TABLE numeric_modifier_copy(LIKE numeric_modifier_rows)')
        check('INSERT INTO numeric_modifier_copy(id,v) VALUES(1,7) RETURNING v', [['7.00']], [1700], tag='INSERT 0 1')
        check('CREATE SEQUENCE numeric_modifier_effects')
        check("SELECT nextval('numeric_modifier_effects')", [['1']], [20])
        check("INSERT INTO numeric_modifier_rows(id,v) VALUES(nextval('numeric_modifier_effects'),'1000')", state='22003')
        check("SELECT currval('numeric_modifier_effects')", [['1']], [20])
        check("UPDATE numeric_modifier_rows SET v='1000' WHERE false", state='22003')
        check("INSERT INTO numeric_modifier_rows(id,a) VALUES(3,'{1000}')", state='22003')
        check('SELECT id FROM numeric_modifier_rows ORDER BY id', [['1'],['2']], [23])
        check('ALTER TABLE numeric_modifier_rows ALTER COLUMN v TYPE NUMERIC(6,3)')
        check('SELECT v FROM numeric_modifier_rows ORDER BY id', [['10.000'],['20.000']], [1700], modifiers=[(6<<16)+3+4])
        check('ALTER TABLE numeric_modifier_rows RENAME COLUMN v TO "x=y"')
        check('SELECT "x=y" FROM numeric_modifier_rows WHERE id=1', [['10.000']], [1700], modifiers=[(6<<16)+3+4])
        check('INSERT INTO numeric_modifier_rows(id,"x=y",n,s,a,u) VALUES(4,NULL,NULL,NULL,NULL,NULL)')
        check('SELECT "x=y",n,s,a,u FROM numeric_modifier_rows WHERE id=4', [[None,None,None,None,None]], [1700,1700,1700,1231,1700])
        check('INSERT INTO numeric_modifier_rows(id,a) VALUES(5,ARRAY[[1.25,NULL],[2.55,-1.25]])')
        check('SELECT a FROM numeric_modifier_rows WHERE id=5', [['{{1.3,NULL},{2.6,-1.3}}']], [1231], modifiers=[(4<<16)+1+4])
        check('SELECT "x=y",n,s,a FROM numeric_modifier_rows WHERE false', [], [1700,1700,1700,1231], modifiers=[(6<<16)+3+4,(3<<16)+2046+4,(2<<16)+4+4,(4<<16)+1+4])
        print('NUMERIC_COLUMN_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try:
                client.simple_query(sock, 'ROLLBACK')
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
