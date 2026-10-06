#!/usr/bin/env python3
"""An already canonical UPDATE column name must not be decoded again."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('quoted_update_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        try:
            client.startup_reference(sock, user, database, password)
            version = runner.decode_wire_result(client.simple_query(sock, 'SHOW server_version_num;'))
            assert version[1] is None and version[0] == [['170002']], version
            print('Actual PG17.2 diagnostic reference', version[0], flush=True)
            assert runner.decode_wire_result(client.simple_query(sock, 'BEGIN;'))[1] is None
            namespace = 'quoted_update_ref_' + uuid.uuid4().hex
            assert runner.decode_wire_result(client.simple_query(sock, 'CREATE SCHEMA ' + namespace + ';'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'SET LOCAL search_path TO ' + namespace + ';'))[1] is None
        except BaseException:
            try:
                client.simple_query(sock, 'ROLLBACK;')
            finally:
                sock.close()
            raise
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql, state=None, rows=None, names=None, oids=None):
        if reference:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT quoted_update_statement;'))[1] is None
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        if reference:
            if result[1] is not None:
                assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO SAVEPOINT quoted_update_statement;'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE SAVEPOINT quoted_update_statement;'))[1] is None
        print('QUOTED_UPDATE', sql, result, flush=True)
        if result[1] != state or rows is not None and result[0] != rows:
            failures.append((sql, result, state, rows))
        if names is not None and result[3] != names or oids is not None and result[5] != oids:
            failures.append((sql, result, names, oids))
        return result

    try:
        # No lower-case counterpart exists: the original double-fold really
        # rejects this target. The V/v coexistence controls below alone cannot
        # reproduce that failure because the mistaken lookup still finds v.
        query('CREATE TABLE uppercase_only_rows(id INT PRIMARY KEY,"OnlyV" INT);')
        query('INSERT INTO uppercase_only_rows VALUES(1,10),(2,NULL);')
        query('UPDATE uppercase_only_rows SET "OnlyV"=11 WHERE id=1;')
        query('SELECT id,"OnlyV" FROM uppercase_only_rows ORDER BY id;', rows=[['1','11'],['2',None]])
        query('UPDATE uppercase_only_rows SET "OnlyV"="OnlyV"+1 WHERE "OnlyV"=11;')
        query('SELECT id,"OnlyV" FROM uppercase_only_rows ORDER BY id;', rows=[['1','12'],['2',None]])
        query('UPDATE uppercase_only_rows AS "Q" SET "OnlyV"=(SELECT "Q".id+40) WHERE "Q".id=2;')
        query('SELECT id,"OnlyV" FROM uppercase_only_rows ORDER BY id;', rows=[['1','12'],['2','42']])
        query('UPDATE uppercase_only_rows SET onlyv=99;', state='42703')
        query('UPDATE uppercase_only_rows SET "OnlyV"=NULL WHERE id=2;')
        query('UPDATE uppercase_only_rows SET "OnlyV"=13 WHERE id=1 RETURNING id,"OnlyV";', rows=[['1','13']], names=['id','OnlyV'], oids=[23,23])
        query("UPDATE uppercase_only_rows SET \"OnlyV\"='bad' WHERE false;", state='22P02')
        query('SELECT id,"OnlyV" FROM uppercase_only_rows ORDER BY id;', rows=[['1','13'],['2',None]])
        query('CREATE TABLE quoted_update_rows(id INT PRIMARY KEY,"V" INT,v INT);')
        query('INSERT INTO quoted_update_rows VALUES(1,10,20),(2,NULL,30);')
        query('UPDATE quoted_update_rows SET "V"=11 WHERE id=1;')
        query('SELECT id,"V",v FROM quoted_update_rows ORDER BY id;', rows=[['1','11','20'],['2',None,'30']])
        query('UPDATE quoted_update_rows SET v=21 WHERE id=1;')
        query('UPDATE quoted_update_rows SET "V"="V"+1 WHERE id=1;')
        query('SELECT id,"V",v FROM quoted_update_rows ORDER BY id;', rows=[['1','12','21'],['2',None,'30']])
        query('UPDATE quoted_update_rows AS "Q" SET "V"=(SELECT "Q".v+10) WHERE "Q".id=2;')
        query('SELECT id,"V",v FROM quoted_update_rows ORDER BY id;', rows=[['1','12','21'],['2','40','30']])
        query('UPDATE quoted_update_rows SET "V"=NULL WHERE id=2;')
        query('SELECT id,"V",v FROM quoted_update_rows ORDER BY id;', rows=[['1','12','21'],['2',None,'30']])
        query('UPDATE quoted_update_rows SET "Missing"=1;', state='42703')
        query("UPDATE quoted_update_rows SET \"V\"='bad' WHERE false;", state='22P02')
        query('SELECT id,"V",v FROM quoted_update_rows ORDER BY id;', rows=[['1','12','21'],['2',None,'30']])
        assert not failures, '%s quoted UPDATE assertions failed: %r' % (len(failures), failures)
        print('[QUOTED UPDATE TARGET PROTOCOL E2E] all controls passed')
    finally:
        if reference:
            try:
                client.simple_query(sock, 'ROLLBACK;')
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
