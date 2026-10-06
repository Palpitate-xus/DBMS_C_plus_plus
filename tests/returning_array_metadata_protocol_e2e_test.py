#!/usr/bin/env python3
"""RETURNING array descriptors retain bound types, images and SQL NULLs."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "returning_array_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql):
        result = runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)
        print("RETURNING_ARRAY", sql, result, flush=True)
        return result

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def check(sql, rows, names, types, tag):
        result = query(sql)
        actual = (result[0], result[1], result[3], result[5], result[4])
        expected = (rows, None, names, types, tag)
        if actual != expected:
            failures.append((sql, actual, expected))
            print("RETURNING_ARRAY_FAILURE", failures[-1], flush=True)

    try:
        setup('CREATE TEMP TABLE returning_array_rows(id INT PRIMARY KEY,"V" BIGINT,v INT,a INT[],b BIGINT[]);')
        check('INSERT INTO returning_array_rows VALUES(1,2147483648,3,ARRAY[1,NULL,2],ARRAY[2147483648,NULL]) '
              'RETURNING ARRAY[old."V",new."V"] AS wide,ARRAY[old.v,new.v]||ARRAY[9] AS ints,'
              'a||ARRAY[3] AS stored,b||ARRAY[4] AS stored_wide;',
              [['{NULL,2147483648}', '{NULL,3,9}', '{1,NULL,2,3}', '{2147483648,NULL,4}']],
              ['wide', 'ints', 'stored', 'stored_wide'], [1016, 1007, 1007, 1016], 'INSERT 0 1')
        check('UPDATE returning_array_rows SET id=id RETURNING new.*;',
              [['1', '2147483648', '3', '{1,NULL,2}', '{2147483648,NULL}']],
              ['id', 'V', 'v', 'a', 'b'], [23, 20, 23, 1007, 1016], 'UPDATE 1')
        check('UPDATE returning_array_rows SET "V"="V"+1,v=v+1 '
              'RETURNING WITH(OLD AS o,NEW AS "O") ARRAY[o."V","O"."V"] AS wide,'
              'ARRAY[ARRAY[o.v,"O".v],ARRAY[1,2]] AS matrix,'
              'NULL::INT[]||ARRAY["O".v] AS nullable,CAST(\'{1}\' AS TEXT)||CAST(\'{2}\' AS TEXT) AS text_concat;',
              [['{2147483648,2147483649}', '{{3,4},{1,2}}', '{4}', '{1}{2}']],
              ['wide', 'matrix', 'nullable', 'text_concat'], [1016, 1007, 1007, 25], 'UPDATE 1')
        check('UPDATE returning_array_rows SET v=NULL '
              'RETURNING ARRAY[old.v,new.v] AS nullable,ARRAY[CASE WHEN true THEN CAST(1 AS SMALLINT) ELSE CAST(9 AS BIGINT) END] '
              '||ARRAY[CAST(2147483648 AS BIGINT)] AS common;',
              [['{4,NULL}', '{1,2147483648}']], ['nullable', 'common'], [1007, 1016], 'UPDATE 1')
        check('DELETE FROM returning_array_rows RETURNING ARRAY[old."V",new."V"] AS wide,'
              'old.a||new.a AS missing_image,ARRAY[old.v,new.v] AS nullable;',
              [['{2147483649,NULL}', '{1,NULL,2}', '{NULL,NULL}']],
              ['wide', 'missing_image', 'nullable'], [1016, 1007, 1007], 'DELETE 1')
        check('UPDATE returning_array_rows SET v=1 RETURNING ARRAY[old."V",new."V"] AS wide,'
              'a||ARRAY[3] AS ints;', [], ['wide', 'ints'], [1016, 1007], 'UPDATE 0')
        assert not failures, failures
        print('[RETURNING ARRAY METADATA '+('PG18 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:
            server['sock'].close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
