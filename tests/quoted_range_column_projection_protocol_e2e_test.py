"""Source-bound physical ColumnRefs are fields even with quoted range names."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('quoted_range_column_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    failures=[];controls=0;schema='range_columns_'+uuid.uuid4().hex[:15];table='"'+schema+'".values_table'
    def query(sql,rows=None,oids=None,headers=None,state=None):
        nonlocal controls
        controls+=1;messages=client.simple_query(sock,sql);actual=runner.decode_wire_result(messages,include_types=True)
        print('QUOTED_RANGE_COLUMN',sql,actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows or oids is not None and actual[5]!=oids or headers is not None and actual[3]!=headers:
            failures.append((sql,actual,rows,oids,headers))
        if oids is not None and not actual[1]:
            fields=client.row_description_fields(messages)
            for field,oid in zip(fields,oids):
                if not field[1] or field[3:]!=(oid,4 if oid==23 else -1,-1,0):failures.append(('physical metadata',sql,field))
    def error(sql,state):
        if reference:query('SAVEPOINT quoted_range_error')
        query(sql,state=state)
        if reference:query('ROLLBACK TO quoted_range_error');query('RELEASE quoted_range_error')
    try:
        if reference:query('BEGIN')
        query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INT PRIMARY KEY,"q I" TEXT,"a""b" INT,"f(x)" TEXT,"x+y" INT,"01" INT)')
        for populated in (False,True):
            if populated:query('INSERT INTO '+table+" VALUES(1,'',11,'NULL',101,7),(2,NULL,NULL,NULL,NULL,NULL)")
            for alias in ['r','"R"','"r I"','"a""lias"']:
                for target,oids,headers,values in [
                    ('id',[23],['id'],[['1'],['2']]),
                    ('"q I"',[25],['q I'],[[''],[None]]),
                    ('"a""b"',[23],['a"b'],[['11'],[None]]),
                    ('"f(x)"',[25],['f(x)'],[['NULL'],[None]]),
                    ('"x+y"',[23],['x+y'],[['101'],[None]]),
                    ('"01"',[23],['01'],[['7'],[None]])]:
                    query('SELECT '+alias+'.'+target+' FROM '+table+' AS '+alias+' ORDER BY '+alias+'.id',
                          values if populated else [],oids,headers)
                query('SELECT '+alias+'.id AS "out ID",'+alias+'.id AS duplicate_id FROM '+table+' AS '+alias+' ORDER BY '+alias+'.id',
                      [['1','1'],['2','2']] if populated else [],[23,23],['out ID','duplicate_id'])
            error('SELECT values_table.id FROM '+table+' AS "R" WHERE FALSE','42P01')
            error('SELECT "wrong".id FROM '+table+' AS "R" LIMIT 0','42P01')
            error('SELECT "R".missing FROM '+table+' AS "R" WHERE FALSE','42703')
        print('QUOTED_RANGE_COLUMN_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
