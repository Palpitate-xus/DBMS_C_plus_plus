#!/usr/bin/env python3
"""Aggregate argument/FILTER enum operators consume actual catalog type ranks.

This gate checks argument operators, not raw custom result OIDs or the separate
fresh-comparison MIN/MAX reducer. Neither is hidden by a default TEXT result.
"""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    repo=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("enum_argument_runner",repo/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference="--reference18" in sys.argv
    collect="--collect-errors" in sys.argv;failures=[]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    def query(sql,rows=None,types=None,headers=None,state=None):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print("ENUM_AGGREGATE_ARGUMENT",sql,result,flush=True)
        try:
            assert result[1]==state,(sql,result,state)
            if rows is not None: assert result[0]==rows and result[4]=="SELECT "+str(len(rows)),(sql,result,rows)
            if types is not None: assert result[5]==types,(sql,result,types)
            if headers is not None: assert result[3]==headers,(sql,result,headers)
            if state: assert result[0]==[] and result[4] is None,(sql,result)
        except AssertionError as failure:
            if not collect: raise
            failures.append(failure.args);print("ENUM_AGGREGATE_ARGUMENT_STRONG_FAILURE",failure.args,flush=True)
        return result
    labels=["zeta","","alpha","aa","NULL","it's","longlonglong"]
    quote=lambda value:"'"+value.replace("'","''")+"'"
    operators=[("<",lambda i,j:i<j),("=",lambda i,j:i==j),("<>",lambda i,j:i!=j),
               (">",lambda i,j:i>j),("<=",lambda i,j:i<=j),(">=",lambda i,j:i>=j),("!=",lambda i,j:i!=j)]
    try:
        schema="enum_argument_"+uuid.uuid4().hex[:18]
        if reference: query("BEGIN")
        query('CREATE SCHEMA "'+schema+'"')
        query(('SET LOCAL' if reference else 'SET')+' search_path TO "'+schema+'",pg_catalog')
        query("CREATE TYPE rank_type AS ENUM ("+','.join(map(quote,labels))+")")
        query("CREATE TABLE ranks(id INT PRIMARY KEY,r rank_type)")
        query("INSERT INTO ranks VALUES "+','.join('('+str(i+1)+','+quote(label)+')' for i,label in enumerate(labels)) + ',(8,NULL)')
        query("CREATE TABLE empty_ranks(id INT,r rank_type)")
        for j,label in enumerate(labels):
            for op,predicate in operators:
                values=[predicate(i,j) for i in range(len(labels))]
                expression="r "+op+" "+quote(label)
                query("SELECT sum(CASE WHEN "+expression+" THEN 1 ELSE 0 END),bool_and("+expression+"),bool_or("+expression+") FROM ranks",
                      [[str(sum(values)),"t" if all(values) else "f","t" if any(values) else "f"]],[20,16,16],['sum','bool_and','bool_or'])
        query("SELECT SUM(CASE WHEN r<'alpha' THEN 1 ELSE 0 END),bool_and(r>'alpha') FROM ranks",[['2','f']],[20,16])
        query("SELECT bool_and(r<'alpha') FROM ranks WHERE id<=2",[['t']],[16])
        query("SELECT bool_or(r<'alpha') FROM ranks WHERE id=1",[['t']],[16])
        query("SELECT every(r<'alpha'),sum(CASE WHEN r IS DISTINCT FROM '' THEN 1 ELSE 0 END) FROM ranks WHERE id<=2",[['t','1']],[16,20])
        query("SELECT sum(CASE r WHEN 'zeta' THEN 10 WHEN '' THEN 20 WHEN NULL THEN 99 ELSE 90 END) FROM ranks",[['570']],[20])
        query("SELECT sum(CASE WHEN (CASE WHEN id=1 THEN r ELSE 'alpha' END)<'aa' THEN 1 ELSE 0 END) FROM ranks",[['8']],[20])
        query('SELECT sum(CASE WHEN "Odd Alias".r<\'alpha\' THEN 1 ELSE 0 END) AS "Total Result" FROM ranks AS "Odd Alias"', [['2']],[20],['Total Result'])
        query("SELECT sum(CASE WHEN ranks.r<'alpha' THEN 1 ELSE 0 END) FROM ranks",[['2']],[20])
        query("SELECT sum(CASE WHEN 'zeta'::rank_type<'alpha' THEN 1 ELSE 0 END) FROM ranks",[['8']],[20])
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FILTER(WHERE r<'alpha'),count(*) FILTER(WHERE r<'alpha') FROM ranks",[['2','2']],[20,20])
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FROM ranks WHERE id<=2 OR id>=2",[['2']],[20])
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FROM ranks WHERE id=1 OR id=1",[['1']],[20])
        for source in ["empty_ranks","ranks WHERE false","ranks WHERE r IS NULL"]:
            expected='0' if source.endswith('IS NULL') else None
            query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END),bool_and(r<'alpha'),count(r) FROM "+source,[[expected,None,'0']],[20,16,20])
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FROM ranks LIMIT 0",[],[20])
        query("CREATE INDEX ranks_idx ON ranks(r)")
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END),bool_and(r<'alpha') FROM ranks WHERE r<'alpha'",[['2','t']],[20,16])
        query("CREATE SEQUENCE enum_argument_seq")
        query("CREATE TABLE enum_argument_effect(seq BIGINT DEFAULT nextval('enum_argument_seq'),v INT)")
        query("CREATE FUNCTION enum_argument_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO enum_argument_effect(v) VALUES(i); RETURN i; END;$$")
        def effects(values): query("SELECT v FROM enum_argument_effect ORDER BY seq",[[str(value)] for value in values],[23],['v'])
        descriptor="SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks"
        name=b'enum_argument_descriptor'
        sock.sendall(client.typed(b'P',name+b'\0'+descriptor.encode()+b'\0\0\0')+client.typed(b'D',b'S'+name+b'\0')+client.typed(b'S'))
        messages=client.read_until_ready(sock);fields=client.row_description_fields(messages)
        print('ENUM_AGGREGATE_ARGUMENT_DESCRIBE',fields,flush=True)
        try:
            assert not any(kind in (b'E',b'D') for kind,_ in messages),messages
            assert [field[3] for field in fields]==[20] and [field[0] for field in fields]==[b'total'],fields
        except AssertionError as failure:
            if not collect: raise
            failures.append(failure.args);print('ENUM_AGGREGATE_ARGUMENT_STRONG_FAILURE',failure.args,flush=True)
        effects([])
        query(descriptor,[['3']],[20],['total']);effects([1,2])
        query("DELETE FROM enum_argument_effect")
        query("SELECT sum(enum_argument_writer(id)) FILTER(WHERE r<'alpha') FROM ranks",[['3']],[20]);effects([1,2])
        query("DELETE FROM enum_argument_effect")
        query("SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) FROM ranks WHERE id<=2 OR id>=2",[['3']],[20]);effects([1,2])
        query("DELETE FROM enum_argument_effect")
        for source in ['empty_ranks','ranks WHERE false','ranks LIMIT 0']:
            query("SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) FROM "+source,[] if source.endswith('LIMIT 0') else [[None]],[20]);effects([])
        for sql,state in [
            ("SELECT sum(CASE WHEN r<'absent' THEN enum_argument_writer(id) ELSE 0 END) FROM empty_ranks",'22P02'),
            ("SELECT sum(CASE WHEN r<'absent' THEN enum_argument_writer(id) ELSE 0 END) FROM ranks LIMIT 0",'22P02'),
            ("SELECT sum(enum_argument_writer(id)) FILTER(WHERE r<'absent') FROM empty_ranks",'22P02'),
            ("SELECT sum(CASE r WHEN 'absent' THEN 1 ELSE 0 END) FROM empty_ranks",'22P02'),
            ("SELECT sum(CASE WHEN r<'alpha'::text THEN 1 ELSE 0 END) FROM empty_ranks",'42883'),
            ("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FILTER(WHERE 1) FROM empty_ranks",'42804'),
            ("SELECT sum(sum(id)) FROM empty_ranks",'42803'),
        ]:
            if reference: query('SAVEPOINT enum_argument_error')
            query(sql,state=state)
            if reference: query('ROLLBACK TO enum_argument_error');query('RELEASE enum_argument_error')
            effects([])
        other=schema+'_other'
        query('CREATE SCHEMA "'+other+'"')
        query('CREATE TYPE "'+other+'".rank_type AS ENUM ('+','.join(map(quote,reversed(labels)))+')')
        query(('SET LOCAL' if reference else 'SET')+' search_path TO "'+other+'",pg_catalog')
        source='"'+schema+'".ranks'
        query("SELECT sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END),bool_and(r<'alpha') FROM "+source+" WHERE id<=2",[['2','t']],[20,16])
        query("SELECT sum(CASE WHEN a.r<'alpha' THEN 1 ELSE 0 END) FROM "+source+" a",[['2']],[20])
        query("SELECT sum(CASE WHEN 'zeta'::rank_type<'alpha' THEN 1 ELSE 0 END) FROM "+source,[['0']],[20])
        assert not failures,('complete enum aggregate argument matrix failed',failures)
        print('[ENUM AGGREGATE ARGUMENT BINDING] complete rank/type/NULL/empty/FILTER/index/error/once/source-identity matrix passed',flush=True)
    finally:
        if reference:
            try: client.simple_query(sock,'ROLLBACK')
            finally: sock.close()
        else: runner.stop_ours(server)


if __name__=='__main__': main()
