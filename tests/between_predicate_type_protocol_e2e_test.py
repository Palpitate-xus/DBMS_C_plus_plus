"""Parser-owned BETWEEN is Boolean in projection and genuine DML predicates."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('between_type_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[];controls=0
    def query(sql,rows=None,types=None,tag=None,state=None):
        nonlocal controls
        controls+=1
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('BETWEEN_TYPE',sql,result,flush=True)
        expected=result[1]==state and (rows is None or result[0]==rows) and (types is None or result[5]==types) and (tag is None or result[4]==tag)
        if not expected:
            failures.append((sql,result,rows,types,tag,state))
        return result
    def error(sql,state):
        if reference:query('SAVEPOINT between_error')
        query(sql,state=state)
        if reference:
            query('ROLLBACK TO between_error');query('RELEASE between_error')
    try:
        if reference:
            query('BEGIN')
            schema='between_type_'+uuid.uuid4().hex[:18]
            query('CREATE SCHEMA "'+schema+'"')
            query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        # The original postgres_protocol_test.py SQL/values/row assertions,
        # including the failing UPDATE and DELETE, are retained verbatim.
        query("CREATE TABLE pred_t (id INT PRIMARY KEY, name TEXT)",[],[],"CREATE TABLE")
        query("INSERT INTO pred_t VALUES (1,'ann'),(2,'bob'),(3,'cat')",[],[],"INSERT 0 3")
        for sql,rows in [
            ("SELECT id FROM pred_t WHERE name NOT LIKE 'a%' ORDER BY id",[['2'],['3']]),
            ("SELECT id FROM pred_t WHERE id BETWEEN 1 AND 2 ORDER BY id",[['1'],['2']]),
            ("SELECT id FROM pred_t WHERE id NOT BETWEEN 1 AND 2",[['3']]),
            ("SELECT id FROM pred_t WHERE name BETWEEN 'b' AND 'c'",[['2']]),
            ("SELECT id FROM pred_t WHERE name NOT BETWEEN 'b' AND 'c' ORDER BY id",[['1'],['3']]),
            ("SELECT id FROM pred_t WHERE name NOT LIKE 'a%' AND id > 2",[['3']]),
            ("SELECT id FROM pred_t WHERE id BETWEEN 1 AND 2 AND name LIKE 'b%'",[['2']])]:
            query(sql,rows,[23])
        query("UPDATE pred_t SET name = 'x' WHERE name NOT LIKE 'a%'",[],[],"UPDATE 2")
        query("SELECT id FROM pred_t WHERE name = 'x' ORDER BY id",[['2'],['3']],[23])
        query("UPDATE pred_t SET name = 'y' WHERE id BETWEEN 2 AND 3",[],[],"UPDATE 2")
        query("SELECT id FROM pred_t WHERE name = 'y' ORDER BY id",[['2'],['3']],[23])
        query("DELETE FROM pred_t WHERE id NOT BETWEEN 2 AND 3",[],[],"DELETE 1")
        query("SELECT id FROM pred_t",[['2'],['3']],[23])
        for expression,expected in [
            ('2 BETWEEN 1 AND 3','t'),('2 NOT BETWEEN 1 AND 3','f'),
            ("'b' BETWEEN 'a' AND 'c'",'t'),("'b' NOT BETWEEN 'a' AND 'c'",'f'),
            ('NULL::INT BETWEEN 1 AND 3',None),('2 BETWEEN NULL AND 1','f'),
            ('2 NOT BETWEEN NULL AND 1','t'),('2 BETWEEN NULL AND 3',None),
            ('2 NOT BETWEEN 1 AND NULL',None),
            ('9007199254740993::BIGINT BETWEEN 9007199254740993 AND 9007199254740993','t')]:
            query('SELECT '+expression+' AS predicate',[[expected]],[16],'SELECT 1')
        query('CREATE TABLE empty_ranges(id INT)',[],[])
        query('CREATE SEQUENCE range_effects',[],[])
        query("UPDATE empty_ranges SET id=nextval('range_effects') WHERE id BETWEEN 1 AND 3",[],[],'UPDATE 0')
        query('DELETE FROM empty_ranges WHERE id NOT BETWEEN 1 AND 3',[],[],'DELETE 0')
        query("SELECT nextval('range_effects')",[['1']],[20])
        error('DELETE FROM empty_ranges WHERE 1','42804')
        error("UPDATE empty_ranges SET id=nextval('range_effects') WHERE 1",'42804')
        query("SELECT nextval('range_effects')",[['2']],[20])
        assert not failures,failures
        print('[BETWEEN PREDICATE TYPE PROTOCOL] complete original DML, NULL/Boolean OIDs/width, BIGINT and empty-source no-effects controls passed',flush=True)
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':
    main()
