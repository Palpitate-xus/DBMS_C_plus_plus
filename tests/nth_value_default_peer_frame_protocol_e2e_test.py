#!/usr/bin/env python3
"""Default no-order NTH_VALUE frame is the entire peer partition."""
import argparse
import importlib.util
from pathlib import Path
import socket
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference18', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('nth_peer_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    if args.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    def ok(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)
        return result
    schema = 'nth_peer_' + uuid.uuid4().hex[:14]
    qs = '"'+schema+'"'
    failures = []
    controls = 0
    key = lambda value: (value is not None, value or '')
    try:
        if args.reference18:
            ok('BEGIN')
        ok('CREATE SCHEMA '+qs)
        for name in ('rows', 'null_rows', 'empty_rows'):
            ok('CREATE TABLE '+qs+'.'+name+'(id INT,g INT,v INT)')
        ok('INSERT INTO '+qs+'.rows VALUES(1,0,10),(2,0,10),(3,0,10)')
        ok('INSERT INTO '+qs+'.null_rows VALUES(1,0,NULL),(2,0,NULL),(3,0,NULL)')
        windows = ('', 'PARTITION BY g', 'PARTITION BY g ORDER BY id',
                   'PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW')
        for window in windows:
            for position in (1, 2, 3, 4):
                for source, suffix in (('rows',''), ('null_rows',''), ('empty_rows',''),
                                       ('rows',' WHERE FALSE'), ('rows',' LIMIT 0')):
                    sql = 'SELECT id,nth_value(v,'+str(position)+') OVER ('+window+') AS value FROM '+qs+'.'+source+suffix
                    result = query(sql)
                    expected = []
                    if source != 'empty_rows' and not suffix:
                        if source == 'null_rows':
                            expected = [None]*3
                        elif 'ORDER BY' in window or 'ROWS' in window:
                            # Prefix frames contain the nth row only from
                            # that position onward. No heap-order assumption.
                            expected = [None if i < position else '10' for i in (1,2,3)]
                        else:
                            expected = ['10' if position <= 3 else None]*3
                    good = result[1] is None and result[3] == ['id','value'] and result[5] == [23,23]
                    good = good and result[4] == 'SELECT '+str(len(expected))
                    good = good and all(len(row) == 2 for row in result[0])
                    good = good and sorted(row[0] for row in result[0]) == ([] if not expected else ['1','2','3'])
                    good = good and sorted((row[1] for row in result[0]), key=key) == sorted(expected, key=key)
                    if good and 'ORDER BY' in window and expected:
                        good = sorted(result[0]) == [[str(i), expected[i-1]] for i in (1,2,3)]
                    controls += 1
                    print('NTH_PEER_FRAME', sql, result, 'expected', expected, flush=True)
                    if not good:
                        failures.append((sql, result, expected))
        assert controls == 80
        print('[NTH DEFAULT PEER FRAME] complete controls=80 failures='+str(len(failures)), flush=True)
        assert not failures, failures
    finally:
        if args.reference18:
            try:
                query('ROLLBACK')
            finally:
                sock.close()
        else:
            try:
                query('DROP SCHEMA '+qs+' CASCADE')
            finally:
                runner.stop_ours(server)


if __name__ == '__main__':
    main()
