#!/usr/bin/env python3
"""A BYTEA declaration must bind builtin byte operators, not a BLOB user type."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('bytea_identity_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def check(sql, rows=None, state=None, oids=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and result[0] == [] and result[4] is None
        checks += 1
        print('BYTEA_IDENTITY', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oids))
        return valid

    try:
        assert check('CREATE TEMP TABLE bytea_identity(id INT,b BYTEA)')
        for populated in (False, True):
            if populated:
                assert check("INSERT INTO bytea_identity VALUES(1,'\\x6162'),(2,'\\xc3a9'),"
                             "(3,'\\x'),(4,'\\x00ff'),(5,NULL),(6,'\\x5f'),(7,'\\x20'),(8,'\\x4e554c4c')")
            for operator, pattern, ids in (
                    ('LIKE', '__', [1, 2, 4]), ('LIKE', '_', [6, 7]),
                    ('LIKE', '%', [1, 2, 3, 4, 6, 7, 8]),
                    ('NOT LIKE', '__', [3, 6, 7, 8])):
                for target in ('', '::bytea'):
                    check('SELECT id FROM bytea_identity WHERE b ' + operator + " '" + pattern + "'" + target + ' ORDER BY id',
                          rows=[[str(i)] for i in ids] if populated else [], oids=[23])
            for condition in ("b ILIKE '__'::bytea", "b LIKE '__'::text", "'abc'::text LIKE b"):
                check('SELECT id FROM bytea_identity WHERE ' + condition, state='42883')
            check('SELECT b FROM bytea_identity WHERE false', rows=[], oids=[17])
        assert check('ALTER TABLE bytea_identity ADD COLUMN other INT')
        check("SELECT id FROM bytea_identity WHERE b LIKE '__'::bytea ORDER BY id",
              rows=[['1'], ['2'], ['4']], oids=[23])
        print('BYTEA_IDENTITY_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 28 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
