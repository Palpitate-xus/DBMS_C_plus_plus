"""Trigger commands cannot replace the caller wire result (PG wrapper oracle)."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('r98_runner', root / 'tests/compat/pg_diff_runner.py')
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
prefix = 'r98_' + uuid.uuid4().hex[:10]
source, audit, function = prefix + '_source', prefix + '_audit', prefix + '_capture'
select_function, returning_function = prefix + '_select', prefix + '_returning'
notice_function = prefix + '_notice'
controls, failures = 0, []
last_messages = []

def query(sql):
    global last_messages
    print('TRIGGER_WHEN_BEGIN', sql, flush=True)
    last_messages = client.simple_query(sock, sql)
    result = runner.decode_wire_result(last_messages, include_types=True)
    print('TRIGGER_WHEN_RESULT', result, flush=True)
    return result

def setup(sql):
    result = query(sql)
    assert result[1] is None, (sql, result)

def check(good, label):
    global controls
    controls += 1
    print('TRIGGER_WHEN_CHECK', controls, label, 'pass=', good, flush=True)
    if not good:
        failures.append(label)

try:
    setup('CREATE TABLE ' + source + '(id INTEGER PRIMARY KEY,v INTEGER)')
    setup('CREATE TABLE ' + audit + '(v INTEGER)')
    action = 'INSERT INTO ' + audit + ' VALUES(1)'
    if reference:
        setup('CREATE FUNCTION ' + function + '() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ' +
              action + "; IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$")
    for event in ('INSERT', 'UPDATE', 'DELETE'):
        for timing in ('BEFORE', 'AFTER'):
            qualifier = 'OLD' if event == 'DELETE' else 'NEW'
            for condition, expected in [('false', 0), ('true', 2), (qualifier + '.id > 10', 1)]:
                setup('TRUNCATE TABLE ' + source)
                setup('TRUNCATE TABLE ' + audit)
                if event != 'INSERT':
                    setup('INSERT INTO ' + source + ' VALUES(5,0),(20,0)')
                trigger_action = 'EXECUTE FUNCTION ' + function + '()' if reference else action
                setup('CREATE TRIGGER r98_capture ' + timing + ' ' + event + ' ON ' + source +
                      ' FOR EACH ROW WHEN (' + condition + ') ' + trigger_action)
                statement = ('INSERT INTO ' + source + ' VALUES(5,0),(20,0)' if event == 'INSERT'
                             else 'UPDATE ' + source + ' SET v=v+1' if event == 'UPDATE'
                             else 'DELETE FROM ' + source)
                result = query(statement)
                label = timing + ' ' + event + ' WHEN ' + condition
                check(result[1] is None and result[0] == [] and result[3] == [] and result[5] == [] and
                      result[4] == ('INSERT 0 2' if event == 'INSERT' else event + ' 2'), label + ' DML')
                result = query('SELECT count(*) FROM ' + audit)
                check(result[1] is None and result[0] == [[str(expected)]] and result[5] == [20], label + ' exact-side-effects')
                result = query('SELECT count(*) FROM ' + source)
                check(result[1] is None and result[0] == [['0' if event == 'DELETE' else '2']] and
                      result[5] == [20], label + ' actual-source-rows')
                setup('DROP TRIGGER r98_capture ON ' + source)
    if reference:
        setup('CREATE FUNCTION ' + select_function + '() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM 77; RETURN NEW; END $$')
        setup('CREATE FUNCTION ' + returning_function + '() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE captured integer; BEGIN ' +
              action + ' RETURNING v INTO captured; RETURN NEW; END $$')
    for timing in ('BEFORE', 'AFTER'):
        for inner, capture, expected_effects in [('SELECT 77 AS inner_value', select_function, 0),
                                                (action + ' RETURNING v', returning_function, 2)]:
            setup('TRUNCATE TABLE ' + source)
            setup('TRUNCATE TABLE ' + audit)
            trigger_action = 'EXECUTE FUNCTION ' + capture + '()' if reference else inner
            setup('CREATE TRIGGER r98_capture ' + timing + ' INSERT ON ' + source +
                  ' FOR EACH ROW ' + trigger_action)
            label = timing + ' nested ' + inner
            result = query('INSERT INTO ' + source + ' VALUES(5,0),(20,0)')
            check(result[1] is None and result[0] == [] and result[3] == [] and result[5] == [] and
                  result[4] == 'INSERT 0 2', label + ' caller-result-not-child-result')
            result = query('SELECT count(*) FROM ' + audit)
            check(result[1] is None and result[0] == [[str(expected_effects)]] and result[5] == [20],
                  label + ' exact-side-effects')
            result = query('SELECT count(*) FROM ' + source)
            check(result[1] is None and result[0] == [['2']] and result[5] == [20], label + ' actual-source-rows')
            setup('DROP TRIGGER r98_capture ON ' + source)
    setup('TRUNCATE TABLE ' + source)
    setup('CREATE FUNCTION ' + notice_function + '() RETURNS ' + ('trigger' if reference else 'integer') +
          " LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'r98 notice'; RAISE WARNING 'r98 warning'; RETURN " +
          ('NEW' if reference else '1') + '; END $$')
    setup('CREATE TRIGGER r98_capture AFTER INSERT ON ' + source +
          ' FOR EACH ROW EXECUTE FUNCTION ' + notice_function + '()')
    result = query('INSERT INTO ' + source + ' VALUES(5,0),(20,0)')
    notices = [client.diagnostic_fields(body) for kind, body in last_messages if kind == b'N']
    print('TRIGGER_DIAGNOSTICS', notices, flush=True)
    check(result[1] is None and result[0] == [] and result[3] == [] and result[5] == [] and
          result[4] == 'INSERT 0 2', 'UDF callback preserves caller result')
    check(len(notices) == 4 and sum(fields.get(b'M') == b'r98 notice' and fields.get(b'S') == b'NOTICE'
          and fields.get(b'C') == b'00000' for fields in notices) == 2 and
          sum(fields.get(b'M') == b'r98 warning' and fields.get(b'S') == b'WARNING'
          and fields.get(b'C') == b'01000' for fields in notices) == 2, 'UDF notices and warnings retained')
    result = query('SELECT count(*) FROM ' + source)
    check(result[1] is None and result[0] == [['2']] and result[5] == [20], 'UDF caller source rows')
    result = query("SELECT 'after trigger'::text")
    check(result[1] is None and result[0] == [['after trigger']] and result[5] == [25], 'caller capture restored')
    setup('DROP TRIGGER r98_capture ON ' + source)
    assert controls == 70, controls
    print('TRIGGER_WHEN_COMPLETE', controls, 'FAILURES', len(failures), failures, flush=True)
    assert not failures, failures
finally:
    if server:
        runner.stop_ours(server)
    else:
        try:
            query('DROP TABLE IF EXISTS ' + source)
            query('DROP TABLE IF EXISTS ' + audit)
            query('DROP FUNCTION IF EXISTS ' + function + '()')
            query('DROP FUNCTION IF EXISTS ' + select_function + '()')
            query('DROP FUNCTION IF EXISTS ' + returning_function + '()')
            query('DROP FUNCTION IF EXISTS ' + notice_function + '()')
        finally:
            sock.close()
