#!/usr/bin/env python3
"""Canonical source occurrence conflicts precede all WITH writer effects."""
import importlib.util
import os
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('range_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('RANGE_NAMESPACE',sql,result,flush=True);return result
    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)
    def check(sql,state=None,rows=None):
        result=query(sql)
        if result[1]!=state or (rows is not None and result[0]!=rows):failures.append((sql,state,rows,result))
    try:
        if reference:runner.verify_reference_version(client,sock)
        for sql in ('CREATE TEMP TABLE range_r(id INT,v INT)','CREATE TEMP TABLE range_s(id INT,v INT)',
                    'CREATE TEMP TABLE range_sink(id INT)','CREATE TEMP SEQUENCE range_writer_seq'):setup(sql)
        prefix="WITH writer AS(INSERT INTO range_sink VALUES(nextval('range_writer_seq')) RETURNING id),unused AS("
        for command in (
            'UPDATE range_r SET v=1 FROM range_r WHERE false',
            'UPDATE range_r t SET v=1 FROM range_s t WHERE false',
            'UPDATE range_r AS "T" SET v=1 FROM range_s AS "T" WHERE false',
            'DELETE FROM range_r USING range_r WHERE false',
            'DELETE FROM range_r t USING range_s t WHERE false',
            'SELECT 1 FROM range_r t JOIN range_s t ON true',
        ):check(prefix+command+') INSERT INTO range_sink VALUES(100)',state='42712',rows=[])
        for command in ('UPDATE range_r t SET v=range_r.v FROM range_s u WHERE false',
                        'DELETE FROM range_r t USING range_s u WHERE range_r.id=u.id'):
            check(prefix+command+') INSERT INTO range_sink VALUES(100)',state='42P01',rows=[])
        check("SELECT currval('range_writer_seq')",state='55000',rows=[])
        check('SELECT id FROM range_sink',rows=[])
        check('WITH unused AS(SELECT "T".id,"t".id FROM range_r "T" JOIN range_s "t" ON true) '
              'INSERT INTO range_sink VALUES(101) RETURNING id',rows=[['101']])
        # A qualified unaliased basename is not an alias collision. These
        # isolated reference schemas live only until the final ROLLBACK.
        first='range_schema_one_'+str(os.getpid());second='range_schema_two_'+str(os.getpid())
        setup('BEGIN')
        for schema in (first,second):
            setup('CREATE SCHEMA '+schema)
            setup('CREATE TABLE '+schema+'.shared_name(id INT,v INT)')
        check('WITH unused AS(SELECT '+first+'.shared_name.id,'+second+'.shared_name.id FROM '+first+'.shared_name JOIN '+
              second+'.shared_name ON '+first+'.shared_name.id='+second+'.shared_name.id) INSERT INTO range_sink VALUES(102) RETURNING id',
              rows=[['102']])
        setup('ROLLBACK')
        assert not failures,failures
        print('[WITH DML RANGE NAMESPACE] passed')
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
