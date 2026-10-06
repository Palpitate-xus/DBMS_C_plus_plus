#!/usr/bin/env python3
"""Postgres interval output uses an explicit plus after a negative field."""
import importlib.util,socket,sys,uuid
from pathlib import Path

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('interval_format_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference='--reference18' in sys.argv[1:]
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=120);c.startup_reference(sock,u,d,pw);server={'sock':sock}
    else:server=r.start_ours(c)
    def query(sql):
        result=r.decode_wire_result(c.simple_query(server['sock'],sql),include_types=True)
        print('INTERVAL_FORMAT',sql,result,flush=True);assert result[1] is None,(sql,result);return result
    try:
        if reference:
            r.verify_reference_version(c,sock);schema='interval_format_'+uuid.uuid4().hex
            query('BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;')
        query('CREATE TEMP TABLE interval_format_rows(id INT,v INTERVAL);')
        query("INSERT INTO interval_format_rows VALUES(1,'1 month -2 days 3 hours'),(2,'-1 month 2 days -3 hours'),(3,'-1 year 1 day 3 hours'),(4,'-1 month 0 days 3 hours'),(5,'1 month -2 days 0 hours'),(6,'-1 month 2 days 0.25 seconds'),(7,'1 month 2 days 3 hours'),(8,'-1 month -2 days -3 hours'),(9,'0 days');")
        result=query('SELECT v FROM interval_format_rows ORDER BY id;')
        expected=[['1 mon -2 days +03:00:00'],['-1 mons +2 days -03:00:00'],['-1 years +1 day 03:00:00'],
            ['-1 mons +03:00:00'],['1 mon -2 days'],['-1 mons +2 days 00:00:00.25'],
            ['1 mon 2 days 03:00:00'],['-1 mons -2 days -03:00:00'],['00:00:00']]
        assert result[0]==expected and result[5]==[1186],(result,expected)
        print('[INTERVAL FORMAT '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:c.simple_query(sock,'ROLLBACK;');sock.close()
        else:r.stop_ours(server)
if __name__=='__main__':main()
