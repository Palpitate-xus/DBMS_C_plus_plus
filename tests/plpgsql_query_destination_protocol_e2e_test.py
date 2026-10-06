#!/usr/bin/env python3
"""Plain PL queries need a destination, after SPI executes with real demand."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('destination_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client(); reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); runner.verify_reference_version(client,sock); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('PL_DEST',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected: failures.append((label,actual,expected)); print('PL_DEST_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        setup('BEGIN;')
        setup('CREATE TEMP TABLE destination_rows(id INT PRIMARY KEY);')
        setup('CREATE TEMP SEQUENCE destination_sequence;')
        cases=(
            ('plain','BEGIN SELECT 1; RETURN 2; END','42601'),
            ('empty','BEGIN SELECT 1 WHERE false; RETURN 2; END','42601'),
            ('returning','BEGIN INSERT INTO destination_rows VALUES(1) RETURNING id; RETURN 2; END','42601'),
            ('bad','BEGIN SELECT missing_destination_function(1); RETURN 2; END','42883'),
            ('perform','BEGIN PERFORM 1; RETURN 2; END',None),
            ('into','DECLARE x INT; BEGIN SELECT 1 INTO x; RETURN x; END',None),
            ('next',"BEGIN SELECT nextval('destination_sequence'); RETURN 2; END",'42601'),
            ('next_empty',"BEGIN SELECT nextval('destination_sequence') WHERE false; RETURN 2; END",'42601'))
        for name,body,state in cases:
            function='destination_'+name
            setup('CREATE FUNCTION '+function+'() RETURNS INT AS $$ '+body+'; $$ LANGUAGE plpgsql;')
            setup('SAVEPOINT destination_case;')
            result=query('SELECT '+function+'();')
            check(name,result[1],state)
            if state: setup('ROLLBACK TO SAVEPOINT destination_case;')
            else: check(name+'-value',(result[0],result[5]),([['1' if name=='into' else '2']],[23]))
            setup('RELEASE SAVEPOINT destination_case;')
        check('no-insert-effects',query('SELECT id FROM destination_rows;')[0],[])
        check('spi-executed-once',query("SELECT currval('destination_sequence');")[0],[['1']])
        setup('ROLLBACK;')
        assert not failures,'%d PL destination assertions failed: %r'%(len(failures),failures)
        print('[PL QUERY DESTINATION '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
