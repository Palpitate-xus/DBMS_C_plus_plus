"""Interval modifiers survive assignment, column identity and catalog metadata."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('interval_column_runner', root / 'tests/compat/pg_diff_runner.py')
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
        if reference and state is not None:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT modifier_error'))[1] is None
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        print('INTERVAL_COLUMN', sql, result, flush=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == (tag or 'SELECT ' + str(len(rows)))
        if oids is not None:
            valid = valid and result[5] == oids
        if modifiers is not None:
            actual = [field[5] for field in client.row_description_fields(messages)]
            print('INTERVAL_COLUMN_MODIFIERS', actual, flush=True)
            valid = valid and actual == modifiers
        if state is not None:
            valid = valid and result[0] == [] and result[4] is None
        if not valid:
            failures.append((sql, result, rows, oids, state, modifiers))
        if reference and state is not None:
            assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO modifier_error'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE modifier_error'))[1] is None

    try:
        if reference:
            check('BEGIN')
        check('CREATE TABLE interval_precision_rows(id INT PRIMARY KEY, v INTERVAL DAY TO SECOND(3), y INTERVAL YEAR, m INTERVAL DAY TO MINUTE, a INTERVAL SECOND(2)[])')
        check("INSERT INTO interval_precision_rows VALUES(1,'2.3456 seconds','2','2','{2.345 seconds,NULL,-2.345 seconds}')")
        check('SELECT id,v,y,m,a FROM interval_precision_rows',
              [['1','00:00:02.346','2 years','00:02:00','{00:00:02.35,NULL,-00:00:02.35}']],
              [23,1186,1186,1186,1187], modifiers=[-1,(7176<<16)|3,(4<<16)|65535,(3080<<16)|65535,(4096<<16)|2])
        check("UPDATE interval_precision_rows SET v='3.4567 seconds',y='3',m='2 hours 3 minutes 4 seconds' WHERE id=1")
        check('SELECT id,v,y,m FROM interval_precision_rows', [['1','00:00:03.457','3 years','02:03:00']], [23,1186,1186,1186])
        check("INSERT INTO interval_precision_rows(id,v,y,m) SELECT 2,'4.5678 seconds','4','4'")
        check('SELECT id,v,y,m FROM interval_precision_rows WHERE id=2', [['2','00:00:04.568','4 years','00:04:00']], [23,1186,1186,1186])
        check("INSERT INTO interval_precision_rows(id,v) VALUES(3,INTERVAL '23.4567 seconds') RETURNING v",
              [['00:00:23.457']], [1186], tag='INSERT 0 1')
        check('CREATE SEQUENCE interval_modifier_effects')
        check("SELECT nextval('interval_modifier_effects')", [['1']], [20])
        check("INSERT INTO interval_precision_rows(id,v) VALUES(nextval('interval_modifier_effects'),'9223372036854775807 microseconds')", state='22008')
        check("SELECT currval('interval_modifier_effects')", [['1']], [20])
        check("UPDATE interval_precision_rows SET v='9223372036854775807 microseconds' WHERE false", state='22008')
        check('SELECT v FROM interval_precision_rows WHERE id=1', [['00:00:03.457']], [1186])
        check("INSERT INTO interval_precision_rows(id,a) SELECT 5,'{2 seconds}'::TEXT WHERE false", state='42804')
        check('SELECT id FROM interval_precision_rows WHERE id=5', [], [23])
        check("INSERT INTO interval_precision_rows(id,v,y,m,a) VALUES(4,NULL,NULL,NULL,NULL)")
        check('SELECT v,y,m,a FROM interval_precision_rows WHERE id=4', [[None,None,None,None]], [1186,1186,1186,1187])
        check('CREATE TABLE interval_precision_copy (LIKE interval_precision_rows)')
        check("INSERT INTO interval_precision_copy(id,v,y,m) VALUES(1,'2.3456 seconds','2','2')")
        check('SELECT v,y,m FROM interval_precision_copy', [['00:00:02.346','2 years','00:02:00']], [1186,1186,1186])
        check('ALTER TABLE interval_precision_rows RENAME COLUMN v TO "a=b"')
        check('ALTER TABLE interval_precision_rows RENAME TO interval_precision_renamed')
        check("UPDATE interval_precision_renamed SET \"a=b\"='5.6789 seconds' WHERE id=1")
        check('SELECT "a=b" FROM interval_precision_renamed WHERE id=1', [['00:00:05.679']], [1186], modifiers=[(7176<<16)|3])
        check('ALTER TABLE interval_precision_renamed ALTER COLUMN "a=b" TYPE INTERVAL SECOND(1)')
        check('SELECT "a=b" FROM interval_precision_renamed WHERE id=1', [['00:00:05.7']], [1186], modifiers=[(4096<<16)|1])
        print('INTERVAL_COLUMN_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
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
