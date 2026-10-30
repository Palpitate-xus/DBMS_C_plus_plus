"""Diagnostics preserve payloads without reinterpreting child or caller rows."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('r99_runner', root / 'tests/compat/pg_diff_runner.py')
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
prefix = 'r99_' + uuid.uuid4().hex[:10]
source = prefix + '_source'
functions, failures = [], []
controls = 0

def query(sql):
    print('DIAG_CHANNEL_BEGIN', sql, flush=True)
    messages = client.simple_query(sock, sql)
    result = runner.decode_wire_result(messages, include_types=True)
    notices = [client.diagnostic_fields(body) for kind, body in messages if kind == b'N']
    print('DIAG_CHANNEL_RESULT', result, 'NOTICES', notices, flush=True)
    return result, notices

def setup(sql):
    result, _ = query(sql)
    assert result[1] is None, (sql, result)

def check(good, label):
    global controls
    controls += 1
    print('DIAG_CHANNEL_CHECK', controls, label, good, flush=True)
    if not good:
        failures.append(label)

try:
    setup('CREATE TABLE ' + source + '(id INTEGER PRIMARY KEY)')
    for number, (level, payload) in enumerate([('NOTICE', 'first\nsecond'),
                                              ('WARNING', 'head\nNOTICE: part of one message'),
                                              ('DATA', 'NOTICE: ordinary row'),
                                              ('DATA_ERROR', 'ERROR: ordinary row'),
                                              ('NOTICE', '  leading and trailing  ')], 1):
        function = prefix + '_f' + str(number)
        literal = "'" + payload.replace("'", "''") + "'"
        if level.startswith('DATA') and not reference:
            action = 'SELECT ' + literal + ' AS payload'
        else:
            # Keep the stored body physically on one line. Runtime E-string
            # values contain real LF bytes; the independent legacy body-file
            # newline parsing issue must not mask this channel test.
            argument = "E'" + payload.replace('\\', '\\\\').replace("'", "''").replace('\n', '\\n') + "'"
            statement = 'PERFORM ' + literal if level.startswith('DATA') else "RAISE " + level + " '%', " + argument
            setup('CREATE FUNCTION ' + function + '() RETURNS ' + ('trigger' if reference else 'integer') +
                  ' LANGUAGE plpgsql AS $$ BEGIN ' + statement + '; RETURN ' + ('NEW' if reference else '7') + '; END $$')
            functions.append(function)
            action = 'EXECUTE FUNCTION ' + function + '()'
        setup('CREATE TRIGGER diag_capture AFTER INSERT ON ' + source + ' FOR EACH ROW ' + action)
        result, notices = query('INSERT INTO ' + source + ' VALUES(' + str(number) + ')')
        check(result[1] is None and result[0] == [] and result[3] == [] and result[5] == [] and
              result[4] == 'INSERT 0 1', level + ' caller result')
        expected = [] if level.startswith('DATA') else [(level.encode(), payload.encode(), b'00000' if level == 'NOTICE' else b'01000')]
        actual = [(fields.get(b'S'), fields.get(b'M'), fields.get(b'C')) for fields in notices]
        check(actual == expected, level + ' exact diagnostic payload/channel')
        result, _ = query('SELECT count(*) FROM ' + source)
        check(result[1] is None and result[0] == [[str(number)]] and result[5] == [20], level + ' actual data')
        setup('DROP TRIGGER diag_capture ON ' + source)
    for value in ('NOTICE: ordinary caller row', 'WARNING: ordinary caller row'):
        result, notices = query("SELECT '" + value + "'::text")
        check(result[1] is None and result[0] == [[value]] and result[5] == [25] and
              result[4] == 'SELECT 1' and notices == [], 'ordinary caller row stays data ' + value)
    print('DIAG_CHANNEL_COMPLETE', controls, 'FAILURES', len(failures), failures, flush=True)
    assert controls == 17 and not failures, failures
finally:
    if server:
        runner.stop_ours(server)
    else:
        try:
            query('DROP TABLE IF EXISTS ' + source)
            for function in functions:
                query('DROP FUNCTION IF EXISTS ' + function + '()')
        finally:
            sock.close()
