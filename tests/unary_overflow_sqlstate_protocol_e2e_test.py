#!/usr/bin/env python3
"""Creation-site unary overflow keeps SQLSTATE, typed NULL and valid bounds."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('unary_state_runner',root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = '--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port),timeout=15)
        client.startup_reference(sock,user,database,password)
        runner.verify_reference_version(client,sock); server = {'sock':sock}
    else:
        server = runner.start_ours(client)
    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('UNARY_STATE',sql,result,flush=True); return result
    try:
        for type_name,minimum,safe,positive,oid in (
            ('SMALLINT','-32768','-32767','32767',21),
            ('INT','-2147483648','-2147483647','2147483647',23),
            ('BIGINT','-9223372036854775808','-9223372036854775807','9223372036854775807',20),
        ):
            sql = "SELECT -(CAST('%s' AS %s));" % (minimum,type_name)
            result = query(sql)
            assert result[1] == '22003' and result[0] == [] and result[4] is None,(sql,result)
            sql = 'SELECT -(CAST(NULL AS %s));' % type_name
            result = query(sql)
            assert result[1] is None and result[0] == [[None]] and result[5] == [oid],(sql,result)
            if safe is not None:
                sql = "SELECT -(CAST('%s' AS %s));" % (safe,type_name)
                result = query(sql)
                assert result[1] is None and result[0] == [[positive]] and result[5] == [oid],(sql,result)
        # An explicit transaction has the ordinary failed-command state.
        assert query('BEGIN;')[1] is None
        assert query('SAVEPOINT unary_keep;')[1] is None
        result = query("SELECT -(CAST('-2147483648' AS INT));")
        assert result[1] == '22003' and result[4] is None,result
        assert query('SELECT 1;')[1] == '25P02'
        assert query('ROLLBACK TO unary_keep;')[1] is None
        result = query("SELECT -(CAST('-2147483647' AS INT));")
        assert result[1] is None and result[0] == [['2147483647']],result
        assert query('ROLLBACK;')[1] is None
        print('[UNARY OVERFLOW '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__ == '__main__':main()
