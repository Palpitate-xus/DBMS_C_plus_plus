"""Typed view-trigger UPDATE/DELETE values; legitimate strict PG18 oracle."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('typed_view_trigger_runner', repo/'tests/compat/pg_diff_runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
reference = '--reference18' in sys.argv[1:]
server = None
if reference:
    host, port, user, database, password = runner._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=15)
    client.startup_reference(sock, user, database, password=password)
else:
    server = runner.start_ours(client)
    sock = server['sock']

def query(sql):
    # A ROLLBACK TO an outer test savepoint removes younger savepoints.
    # Transaction-control probes therefore cannot have a younger wrapper.
    isolated = reference and sql.split(None, 1)[0].lower() not in {
        'begin', 'savepoint', 'release', 'rollback'}
    if isolated:
        client.simple_query(sock, 'SAVEPOINT typed_view_case')
    result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    if isolated:
        if result[1] is not None:
            client.simple_query(sock, 'ROLLBACK TO typed_view_case')
        client.simple_query(sock, 'RELEASE typed_view_case')
    print('TYPED_VIEW_TRIGGER', sql, result, flush=True)
    return result

def ok(sql):
    result = query(sql)
    assert result[1] is None, (sql, result)
    return result

def rows(sql, expected, oids=None):
    result = ok(sql)
    assert result[0] == expected, (sql, result, expected)
    if oids is not None:
        assert result[5] == oids, (sql, result, oids)
    return result

def update(sql, count):
    result = ok(sql)
    assert result[4] == 'UPDATE '+str(count), (sql, result)

try:
    if reference:
        assert runner.decode_wire_result(client.simple_query(sock, 'SHOW server_version_num'))[0] == [['180006']]
        client.simple_query(sock, 'BEGIN')
        schema = 'typed_view_' + uuid.uuid4().hex
        ok('CREATE SCHEMA ' + schema)
        ok('SET search_path=' + schema + ',public')
    for sql in (
        'CREATE TABLE view_a(aid INT PRIMARY KEY,tag TEXT)',
        'CREATE TABLE view_b(bid INT PRIMARY KEY,aid INT,val TEXT,other TEXT,wide BIGINT,nums INT[],flag BOOL,"Case" INT)',
        "INSERT INTO view_a VALUES(1,'a tag with spaces')",
        "INSERT INTO view_b VALUES(10,1,'old space','another old',2147483648,ARRAY[1,2],true,5),(11,1,NULL,'OLD.val',2147483649,NULL,NULL,NULL)",
        'CREATE VIEW typed_view AS SELECT b.bid,a.tag,b.val,b.other,b.wide,b.nums,b.flag,b."Case" FROM view_b b JOIN view_a a ON b.aid=a.aid ORDER BY b.bid',
        'CREATE SEQUENCE typed_view_calls',
    ): ok(sql)
    action = 'UPDATE view_b SET val=NEW.val,other=NEW.other,wide=NEW.wide,nums=NEW.nums,flag=NEW.flag,"Case"=NEW."Case" WHERE bid=OLD.bid'
    if reference:
        ok('CREATE FUNCTION typed_view_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN '+action+'; RETURN NEW; END $$')
        ok('CREATE TRIGGER typed_view_update INSTEAD OF UPDATE ON typed_view FOR EACH ROW EXECUTE FUNCTION typed_view_update()')
    else:
        ok('CREATE TRIGGER typed_view_update INSTEAD OF UPDATE ON typed_view FOR EACH ROW '+action)
    update("UPDATE typed_view SET val='v2' WHERE bid=10", 1)
    rows('SELECT val FROM view_b WHERE bid=10', [['v2']], [25])
    update("UPDATE typed_view SET val='O''Brien, NEW.val; OLD.bid',other='literal OLD.val',nums=ARRAY[3,NULL,4],flag=false,wide=9223372036854775807 WHERE bid=10", 1)
    rows('SELECT val,other,wide,nums,flag,"Case" FROM view_b WHERE bid=10',
        [["O'Brien, NEW.val; OLD.bid",'literal OLD.val','9223372036854775807','{3,NULL,4}','f','5']], [25,25,20,1007,16,23])
    update("UPDATE typed_view SET val='',other='NULL',wide=2147483648,flag=NULL,nums=NULL WHERE bid=10", 1)
    rows('SELECT val,other,wide,nums,flag FROM view_b WHERE bid=10', [['','NULL','2147483648',None,None]], [25,25,20,1007,16])
    update("UPDATE typed_view SET val=other,other=val WHERE bid=10", 1)
    rows('SELECT val,other FROM view_b WHERE bid=10', [['NULL','']], [25,25])
    update("UPDATE typed_view SET val=CASE WHEN bid=10 THEN 'computed, value' ELSE NULL END,"+
        "wide=wide+2,\"Case\"=COALESCE(\"Case\",0)+1", 2)
    rows('SELECT bid,val,wide,"Case" FROM view_b ORDER BY bid',
        [['10','computed, value','2147483650','6'],['11',None,'2147483651','1']], [23,25,20,23])
    update("UPDATE typed_view AS v SET val=v.other WHERE v.bid=10", 1)
    rows('SELECT val FROM view_b WHERE bid=10', [['']], [25])
    update("UPDATE typed_view SET wide=CAST(nextval('typed_view_calls') AS BIGINT)", 2)
    rows("SELECT currval('typed_view_calls')", [['2']], [20])
    rows('SELECT bid,wide FROM view_b ORDER BY bid', [['10','1'],['11','2']], [23,20])
    update("UPDATE typed_view SET wide=CAST(nextval('typed_view_calls') AS BIGINT) WHERE false", 0)
    rows("SELECT currval('typed_view_calls')", [['2']], [20])
    result = query("UPDATE typed_view SET missing=nextval('typed_view_calls')")
    assert result[1] == '42703' and result[0] == [] and result[4] is None, result
    rows("SELECT currval('typed_view_calls')", [['2']], [20])
    ok("ALTER TABLE view_b ADD CONSTRAINT typed_view_check CHECK(val <> 'reject')")
    before = rows('SELECT bid,val,other,wide,nums,flag,"Case" FROM view_b ORDER BY bid',
        [['10','','','1',None,None,'6'],['11',None,'OLD.val','2',None,None,'1']])
    result = query("UPDATE typed_view SET val=CASE WHEN bid=10 THEN 'first changed' ELSE 'reject' END,wide=CAST(nextval('typed_view_calls') AS BIGINT)")
    assert result[1] == '23514' and result[0] == [] and result[4] is None, result
    assert ok('SELECT bid,val,other,wide,nums,flag,"Case" FROM view_b ORDER BY bid')[0] == before[0]
    rows("SELECT currval('typed_view_calls')", [['4']], [20])
    rows('SELECT 1', [['1']], [23])
    # Reuse an explicit caller's transaction/savepoint; view SQL actions must
    # not commit that owner or discard its rollback boundary.
    if not reference:
        ok('BEGIN')
    ok('SAVEPOINT typed_view_parent')
    update("UPDATE typed_view SET other='parent rollback',wide=42", 2)
    rows('SELECT bid,other,wide FROM view_b ORDER BY bid',
        [['10','parent rollback','42'],['11','parent rollback','42']], [23,25,20])
    ok('ROLLBACK TO SAVEPOINT typed_view_parent')
    assert ok('SELECT bid,val,other,wide,nums,flag,"Case" FROM view_b ORDER BY bid')[0] == before[0]
    ok('RELEASE SAVEPOINT typed_view_parent')
    if not reference:
        ok('ROLLBACK')
    action = 'DELETE FROM view_b WHERE bid=OLD.bid AND val IS NOT DISTINCT FROM OLD.val AND other=OLD.other'
    if reference:
        ok('CREATE FUNCTION typed_view_delete() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN '+action+'; RETURN OLD; END $$')
        ok('CREATE TRIGGER typed_view_delete INSTEAD OF DELETE ON typed_view FOR EACH ROW EXECUTE FUNCTION typed_view_delete()')
    else:
        ok('CREATE TRIGGER typed_view_delete INSTEAD OF DELETE ON typed_view FOR EACH ROW '+action)
    result = ok('DELETE FROM typed_view WHERE bid=10')
    assert result[4] == 'DELETE 1', result
    rows('SELECT bid FROM view_b ORDER BY bid', [['11']], [23])
    result = ok('DELETE FROM typed_view WHERE bid=11')
    assert result[4] == 'DELETE 1', result
    rows('SELECT bid FROM view_b', [], [23])
    print('[TYPED VIEW TRIGGER '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:
        runner.stop_ours(server)
    else:
        client.simple_query(sock, 'ROLLBACK')
        sock.close()
