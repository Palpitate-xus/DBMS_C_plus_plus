"""Strict PG18.6 live exported-snapshot oracle, never a candidate substitute."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid

assert sys.argv[1:] == ['--reference18'], 'requires explicit --reference18 and PGREF settings'
root = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('snapshot_reference_runner', root/'tests/compat/pg_diff_runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
host, port, user, database, password = runner._reference_connection_settings()
sockets = []

def query(index, sql, expected=None, state=None):
    result = runner.decode_wire_result(client.simple_query(sockets[index], sql), include_types=True)
    print('STRICT180006_SNAPSHOT', index, sql, result, flush=True)
    assert result[1] == state, (index, sql, result)
    if expected is not None:
        assert result[0] == expected, (index, sql, result, expected)
    return result

try:
    for _ in range(4):
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
        sockets.append(sock)
    schema = 'snapshot_xid_' + uuid.uuid4().hex
    query(0, 'CREATE SCHEMA ' + schema)
    for index in range(4):
        query(index, 'SET search_path=' + schema + ',public')
    query(0, 'CREATE TABLE t(id integer PRIMARY KEY,val text)')
    query(0, "INSERT INTO t VALUES(1,'old'),(2,'deleted'),(3,NULL)")
    old = [['1','old'], ['2','deleted'], ['3',None]]
    for outcome in ('COMMIT', 'ROLLBACK'):
        query(0, 'TRUNCATE t')
        query(0, "INSERT INTO t VALUES(1,'old'),(2,'deleted'),(3,NULL)")
        query(3, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
        query(3, "INSERT INTO t VALUES(20,'foreign')")
        foreign_xid = query(3, 'SELECT pg_current_xact_id()::text')[0][0][0]
        query(0, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
        query(0, "UPDATE t SET val='new' WHERE id=1")
        query(0, 'DELETE FROM t WHERE id=2')
        query(0, 'INSERT INTO t VALUES(4,NULL)')
        query(0, 'SAVEPOINT released')
        query(0, "INSERT INTO t VALUES(5,'released')")
        query(0, 'RELEASE SAVEPOINT released')
        query(0, 'SAVEPOINT undone')
        query(0, "INSERT INTO t VALUES(6,'undone')")
        query(0, 'ROLLBACK TO SAVEPOINT undone')
        query(0, 'RELEASE SAVEPOINT undone')
        own = [['1','new'], ['3',None], ['4',None], ['5','released']]
        query(0, 'SELECT id,val FROM t ORDER BY id', own)
        xid = query(0, 'SELECT pg_current_xact_id()::text')[0][0][0]
        exported = query(0, 'SELECT pg_export_snapshot()')[0][0][0]
        print('EXPORTED_REAL_XIDS', xid, foreign_xid, exported, flush=True)
        # Import the same still-live snapshot in two independent transactions.
        for index in (1, 2):
            query(index, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
            query(index, "SET TRANSACTION SNAPSHOT '" + exported + "'")
            query(index, 'SELECT id,val FROM t ORDER BY id', old)
        query(0, 'SELECT id,val FROM t ORDER BY id', own)
        query(3, 'COMMIT')
        query(0, 'SELECT id,val FROM t ORDER BY id', own)
        for index in (1, 2):
            query(index, 'SELECT id,val FROM t ORDER BY id', old)
        query(0, outcome)
        for index in (1, 2):
            query(index, 'SELECT id,val FROM t ORDER BY id', old)
        fresh = (own if outcome == 'COMMIT' else old) + [['20','foreign']]
        query(0, 'SELECT id,val FROM t ORDER BY id', fresh)
        query(1, "INSERT INTO t VALUES(7,'importer own')")
        query(1, 'SELECT id,val FROM t ORDER BY id', old + [['7','importer own']])
        query(2, 'SELECT id,val FROM t ORDER BY id', old)
        query(1, 'ROLLBACK')
        query(2, 'ROLLBACK')
        query(0, 'SELECT id,val FROM t ORDER BY id', fresh)
    query(0, 'DROP SCHEMA ' + schema + ' CASCADE')
    print('STRICT180006_EXPORTER_XID_SNAPSHOT_ORACLE=0', flush=True)
finally:
    for sock in sockets:
        try:
            client.simple_query(sock, 'ROLLBACK')
        finally:
            sock.close()

