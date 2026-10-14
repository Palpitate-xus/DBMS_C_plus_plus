#!/usr/bin/env python3
"""Schema dependency cleanup retains exact namespace identity and atomicity."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('schema_owner_runner', root / 'tests/compat/pg_diff_runner.py')
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
    prefix = 'r46_' + uuid.uuid4().hex[:16]
    schema, external = prefix + '.Owner', prefix + '_external'
    quoted = lambda value: '"' + value.replace('"', '""') + '"'
    target = quoted(schema)
    outside = quoted(external)
    survivor = 'public.' + quoted(schema + '__survivor')
    checks, failures = 0, []

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)

    def control(sql, state=None, rows=None, headers=None, oids=None):
        nonlocal checks
        result = query(sql)
        checks += 1
        valid = result[1] == state
        if state:
            valid = valid and not result[0] and result[4] is None
        if rows is not None:
            valid = valid and result[0] == rows and result[3] == headers and result[5] == oids
        print('SCHEMA_NAMESPACE_ATOMICITY', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, state, rows, headers, oids))
        return valid

    def snapshot():
        control('SELECT id,payload,note FROM ' + outside + '.rows ORDER BY id',
                rows=[['1', '7', ''], ['2', None, 'NULL']], headers=['id', 'payload', 'note'], oids=[23, 23, 25])
        control('SELECT id FROM ' + survivor, rows=[['11']], headers=['id'], oids=[23])
        control('SELECT ' + target + '.bump(4) AS value', rows=[['5']], headers=['value'], oids=[23])

    try:
        assert control('CREATE SCHEMA ' + target)
        assert control('CREATE SCHEMA ' + outside)
        assert control('CREATE DOMAIN ' + target + '.positive AS INT CHECK (VALUE > 0)')
        assert control('CREATE DOMAIN ' + outside + '.child AS ' + target + '.positive')
        assert control('CREATE TABLE ' + outside + '.rows(id INT PRIMARY KEY,payload ' + outside + '.child,note TEXT)')
        assert control("INSERT INTO " + outside + ".rows VALUES(1,7,''),(2,NULL,'NULL')")
        assert control('CREATE TABLE ' + target + '.owned(id INT PRIMARY KEY)')
        assert control('INSERT INTO ' + target + '.owned VALUES(3)')
        assert control('CREATE TABLE ' + survivor + '(id INT PRIMARY KEY)')
        assert control('INSERT INTO ' + survivor + ' VALUES(11)')
        assert control('CREATE FUNCTION ' + target + '.bump(x INT) RETURNS INT LANGUAGE SQL AS $$SELECT x+1$$')
        snapshot()
        control('DROP SCHEMA ' + target + ' RESTRICT', state='2BP01')
        snapshot()
        for savepoint in (False, True):
            assert control('BEGIN')
            if savepoint:
                assert control('SAVEPOINT schema_owner')
            assert control('DROP SCHEMA ' + target + ' CASCADE')
            control('SELECT id,note FROM ' + outside + '.rows ORDER BY id',
                    rows=[['1', ''], ['2', 'NULL']], headers=['id', 'note'], oids=[23, 25])
            control('SELECT id FROM ' + survivor, rows=[['11']], headers=['id'], oids=[23])
            if savepoint:
                assert control('ROLLBACK TO SAVEPOINT schema_owner')
                assert control('COMMIT')
            else:
                assert control('ROLLBACK')
            snapshot()
        assert control('DROP SCHEMA ' + target + ' CASCADE')
        control('SELECT id,note FROM ' + outside + '.rows ORDER BY id',
                rows=[['1', ''], ['2', 'NULL']], headers=['id', 'note'], oids=[23, 25])
        control('SELECT id FROM ' + survivor, rows=[['11']], headers=['id'], oids=[23])
        control('CREATE TABLE ' + target + '.rejected(id INT)', state='3F000')
        control('DROP SCHEMA ' + target, state='3F000')
        control('DROP SCHEMA IF EXISTS ' + target)
        assert control('CREATE SCHEMA ' + target)
        assert control('CREATE TABLE ' + target + '.recreated(id INT)')
        assert control('INSERT INTO ' + target + '.recreated VALUES(19)')
        control('SELECT id FROM ' + target + '.recreated', rows=[['19']], headers=['id'], oids=[23])
        print('SCHEMA_NAMESPACE_ATOMICITY_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks >= 40 and not failures, failures
    finally:
        try:
            query('ROLLBACK')
            query('DROP TABLE IF EXISTS ' + survivor)
            query('DROP SCHEMA IF EXISTS ' + target + ' CASCADE')
            query('DROP SCHEMA IF EXISTS ' + outside + ' CASCADE')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
