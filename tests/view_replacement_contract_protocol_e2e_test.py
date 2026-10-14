#!/usr/bin/env python3
"""A replacement view retains existing output names/types/modifiers and may append."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('view_contract_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    schema = 'view_contract_' + uuid.uuid4().hex[:12]
    checks, failures = 0, []

    def check(sql, rows=None, state=None, names=None, oids=None, ready=b'I'):
        nonlocal checks
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state and messages[-1] == (b'Z', ready)
        if rows is not None:
            valid = valid and result[0] == rows
        if names is not None:
            valid = valid and result[3] == names
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and not result[0] and result[4] is None
        checks += 1
        print('VIEW_REPLACEMENT', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, names, oids))
        return result

    def view(body, suffix='v', columns=''):
        return 'CREATE OR REPLACE VIEW ' + schema + '.' + suffix + columns + ' AS ' + body

    source = schema + '.rows'
    try:
        check('CREATE SCHEMA ' + schema)
        check('CREATE TABLE ' + source + '(id INT,t TEXT,i INT,v VARCHAR(10),w VARCHAR(20))')
        check("INSERT INTO " + source + " VALUES(1,'ann',9,'short','wide')")
        check(view('SELECT id,t FROM ' + source))
        for projection in ('id,i', 'id,i AS t', 'id::BIGINT,t', 'id AS renamed,t', 'id'):
            check(view('SELECT ' + projection + ' FROM ' + source), state='42P16')
            check('SELECT * FROM ' + schema + '.v', rows=[['1', 'ann']], names=['id', 't'], oids=[23, 25])
        check(view('SELECT id+1 AS id,t FROM ' + source))
        check('SELECT * FROM ' + schema + '.v', rows=[['2', 'ann']], names=['id', 't'], oids=[23, 25])
        check(view('SELECT id+1 AS id,t,i FROM ' + source))
        check('SELECT * FROM ' + schema + '.v', rows=[['2', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT id,t FROM ' + source), state='42P16')
        check('SELECT * FROM ' + schema + '.v', rows=[['2', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT id,t,i FROM ' + source, columns='(id)'))
        check('SELECT * FROM ' + schema + '.v', rows=[['1', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT id,t,i FROM ' + source, columns='(changed)'), state='42P16')
        check('SELECT * FROM ' + schema + '.v', rows=[['1', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT v FROM ' + source, suffix='width_v'))
        check(view('SELECT w AS v FROM ' + source, suffix='width_v'), state='42P16')
        check(view('SELECT CAST(v AS VARCHAR(20)) AS v FROM ' + source, suffix='width_v'), state='42P16')
        check('SELECT * FROM ' + schema + '.width_v', rows=[['short']], names=['v'], oids=[1043])
        check(view('SELECT CAST(w AS VARCHAR(10)) AS v FROM ' + source, suffix='width_v'))
        check('SELECT * FROM ' + schema + '.width_v', rows=[['wide']], names=['v'], oids=[1043])
        check(view("SELECT 'constant' AS label", suffix='constant_v'))
        check(view('SELECT 7 AS label', suffix='constant_v'), state='42P16')
        check('SELECT * FROM ' + schema + '.constant_v', rows=[['constant']], names=['label'], oids=[25])
        check(view('SELECT 1/0 AS danger', suffix='pure_v'))
        check('CREATE SEQUENCE ' + schema + '.calls')
        check(view("SELECT nextval('" + schema + ".calls') AS id", suffix='volatile_v'))
        check("SELECT nextval('" + schema + ".calls')", rows=[['1']], oids=[20])
        check(view('SELECT id,t,i FROM ' + source, suffix='quoted_v', columns='("ID")'))
        check(view('SELECT id,t,i FROM ' + source, suffix='quoted_v', columns='(id)'), state='42P16')
        check('SELECT * FROM ' + schema + '.quoted_v', rows=[['1', 'ann', '9']], names=['ID', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT id,t FROM ' + source, suffix='missing_col_v'))
        check(view('SELECT absent,t FROM ' + source, suffix='missing_col_v'), state='42703')
        check('SELECT * FROM ' + schema + '.missing_col_v', rows=[['1', 'ann']], names=['id', 't'], oids=[23, 25])
        for option in ('', 'LOCAL ', 'CASCADED '):
            check(view('SELECT id,t FROM ' + source + ' WHERE id>0 WITH ' + option + 'CHECK OPTION', suffix='check_v'))
            check('SELECT * FROM ' + schema + '.check_v', rows=[['1', 'ann']], names=['id', 't'], oids=[23, 25])
        check(view('SELECT id AS same,t AS same FROM ' + source, suffix='duplicate_v'), state='42701')
        check('CREATE TABLE ' + schema + '.array_rows(a INT[],b TEXT[])')
        check("INSERT INTO " + schema + ".array_rows VALUES(ARRAY[1,2],ARRAY['x','y'])")
        check(view('SELECT a FROM ' + schema + '.array_rows', suffix='array_v'))
        check('SELECT * FROM ' + schema + '.array_v', rows=[['{1,2}']], names=['a'], oids=[1007])
        check(view('SELECT b AS a FROM ' + schema + '.array_rows', suffix='array_v'), state='42P16')
        check('SELECT * FROM ' + schema + '.array_v', rows=[['{1,2}']], names=['a'], oids=[1007])
        check(view('SELECT ARRAY[3,4] AS a FROM ' + schema + '.array_rows', suffix='array_v'))
        check('SELECT * FROM ' + schema + '.array_v', rows=[['{3,4}']], names=['a'], oids=[1007])
        check(view('SELECT array[5,6] AS a FROM ' + schema + '.array_rows', suffix='array_v'))
        check('SELECT * FROM ' + schema + '.array_v', rows=[['{5,6}']], names=['a'], oids=[1007])
        check('BEGIN', ready=b'T')
        check('SAVEPOINT before_replace', ready=b'T')
        check(view('SELECT id+5 AS id,t,i FROM ' + source), ready=b'T')
        check('SELECT * FROM ' + schema + '.v', rows=[['6', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23], ready=b'T')
        check('ROLLBACK TO before_replace', ready=b'T')
        check('SELECT * FROM ' + schema + '.v', rows=[['1', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23], ready=b'T')
        check('RELEASE before_replace', ready=b'T')
        check('COMMIT')
        check('BEGIN', ready=b'T')
        check(view('SELECT id::BIGINT,t,i FROM ' + source), state='42P16', ready=b'E')
        check('ROLLBACK')
        check('SELECT * FROM ' + schema + '.v', rows=[['1', 'ann', '9']], names=['id', 't', 'i'], oids=[23, 25, 23])
        check(view('SELECT ID+10 AS ID FROM ' + source, suffix='UPPER_V'))
        check('SELECT * FROM ' + schema + '.upper_v', rows=[['11']], names=['id'], oids=[23])
        check(view('SELECT id FROM ' + source, suffix='"MixedView"'))
        check('SELECT * FROM ' + schema + '."MixedView"', rows=[['1']], names=['id'], oids=[23])
        for index, declared in enumerate((23, 0)):
            statement, portal = 'view_statement_' + str(index), 'view_portal_' + str(index)
            sql = 'SELECT a FROM ' + schema + '.array_v WHERE 1=$1'
            parse = client.typed(b'P', statement.encode() + b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, declared))
            sock.sendall(parse + client.typed(b'D', b'S' + statement.encode() + b'\0') + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            valid = result[1] is None and result[3] == ['a'] and result[5] == [1007] and messages[-1] == (b'Z', b'I')
            checks += 1
            print('VIEW_EXTENDED_DESCRIBE', declared, result, 'PASS', valid, flush=True)
            if not valid:
                failures.append(('describe', declared, result))
            bind = client.typed(b'B', portal.encode() + b'\0' + statement.encode() + b'\0' + struct.pack('!HHI', 0, 1, 1) + b'1' + struct.pack('!H', 0))
            sock.sendall(bind + client.typed(b'D', b'P' + portal.encode() + b'\0') + client.typed(b'E', portal.encode() + b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            valid = result[1] is None and result[0] == [['{5,6}']] and result[5] == [1007] and messages[-1] == (b'Z', b'I')
            checks += 1
            print('VIEW_EXTENDED_EXECUTE', declared, result, 'PASS', valid, flush=True)
            if not valid:
                failures.append(('execute', declared, result))
        print('VIEW_REPLACEMENT_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 80 and not failures, failures
    finally:
        try:
            client.simple_query(sock, 'DROP SCHEMA ' + schema + ' CASCADE')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
