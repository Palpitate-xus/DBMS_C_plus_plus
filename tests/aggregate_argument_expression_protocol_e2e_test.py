#!/usr/bin/env python3
"""Direct aggregate argument ASTs, nullable overloads and exact effect demand."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    repo=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("aggregate_argument_runner",repo/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference="--reference18" in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    failures=[]
    collect_errors="--collect-errors" in sys.argv
    def query(sql,rows=None,types=None,state=None,headers=None):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print("AGGREGATE_ARGUMENT",sql,result,flush=True)
        try:
            assert result[1]==state,(sql,result,state)
            if rows is not None: assert result[0]==rows,(sql,result,rows)
            if types is not None: assert result[5]==types,(sql,result,types)
            if headers is not None: assert result[3]==headers,(sql,result,headers)
        except AssertionError as failure:
            if not collect_errors: raise
            failures.append(failure.args)
            print("AGGREGATE_ARGUMENT_STRONG_FAILURE",failure.args,flush=True)
        return result
    try:
        if reference:
            schema="aggregate_argument_"+uuid.uuid4().hex[:18]
            query("BEGIN");query('CREATE SCHEMA "'+schema+'"')
            query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query("CREATE TABLE args (id INT,n INT,t TEXT,b BOOLEAN)")
        query("INSERT INTO args VALUES (1,1,'zeta',true),(2,2,'',false),(3,NULL,NULL,NULL),(4,-1,'NULL',true)")
        query("CREATE TABLE empty_args (id INT,n INT,t TEXT,b BOOLEAN)")
        controls=[
            ("sum(n),count(t),bool_and(b)",["2","3","f"],[20,20,16]),
            ("SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END)",["2"],[20]),
            ("SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END)+0",["2"],[20]),
            # Engine default and the owned matched strict18 profile are en_US.
            # The authored C-default expectation 2 and its exact failed assert
            # remain frozen externally; explicit C below still requires 2.
            ("SUM(CASE WHEN t < 'alpha' THEN 1 ELSE 0 END)",["1"],[20]),
            ('SUM(CASE WHEN t COLLATE "C" < \'alpha\' THEN 1 ELSE 0 END)',["2"],[20]),
            ("bool_and(n > 0),bool_or(n < 0)",["f","t"],[16,16]),
            ("bool_and(t < 'alpha'),every(t <> '')",["f","f"],[16,16]),
            ('bool_and(t COLLATE "C" < \'alpha\'),every(t COLLATE "C" <> \'\')',["f","f"],[16,16]),
            ("sum(n * 2),sum(CAST(n AS BIGINT))",["4","2"],[20,1700]),
            ("count(CASE WHEN n IS NULL THEN 1 END)",["1"],[20]),
            ("sum(CASE WHEN n IS NULL THEN NULL ELSE n END)",["2"],[20]),
            ("SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END),bool_and(t < 'alpha'),count(t)",["2","f","3"],[20,16,20]),
            ("sum(DISTINCT CASE WHEN n > 0 THEN 1 ELSE 0 END)",["1"],[20]),
            ("SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END) FILTER (WHERE id <= 2)",["2"],[20]),
            ("SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END) AS \"Total Result\"",["2"],[20]),
        ]
        for expression,row,types in controls: query("SELECT "+expression+" FROM args",[row],types)
        query("SELECT pg_catalog.sum(CASE WHEN n > 0 THEN 1 ELSE 0 END) FROM args",[["2"]],[20],headers=["sum"])
        query('SELECT sum(CASE WHEN n > 0 THEN 1 ELSE 0 END) AS "Total Result" FROM args',[["2"]],[20],headers=["Total Result"])
        query("SELECT min(CASE WHEN n > 0 THEN n ELSE 0 END),max(CASE WHEN n > 0 THEN n ELSE 0 END) FROM args",[["0","2"]],[23,23])
        for source in ["empty_args","args WHERE false"]:
            query("SELECT SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END),bool_and(t < 'alpha'),count(t) FROM "+source,[[None,None,"0"]],[20,16,20])
        query("SELECT SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END),bool_and(t < 'alpha'),count(t) FROM args WHERE n IS NULL",[["0",None,"0"]],[20,16,20])
        query("CREATE INDEX args_id_idx ON args(id)")
        query("SELECT SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END),bool_and(t <> '') FROM args WHERE id = 1",[["1","t"]],[20,16])
        query("SELECT sum(n+0) FROM args WHERE id=1 OR id=2",[["3"]],[20])
        query("SELECT sum(n+0) FROM args WHERE id=1 OR id=1",[["1"]],[20])
        query("SELECT sum(n+0) FROM args WHERE false OR id=99",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE false OR id=1",[["1"]],[20])
        query("SELECT sum(n+0) FROM args WHERE NULL OR id=99",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE NULL OR id=1",[["1"]],[20])
        query("SELECT sum(n+0) FROM args WHERE true OR id=99",[["2"]],[20])
        query("SELECT sum(n+0) FROM args WHERE id=1 AND false",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE NOT false",[["2"]],[20])
        query("SELECT sum(n+0) FROM args WHERE NOT true",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE NOT NULL",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE id=1 AND (false OR id=2)",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE NOT (false OR id=1)",[["1"]],[20])
        query("SELECT sum(n+0) FROM args a WHERE false OR a.id=1",[["1"]],[20])
        query("SELECT sum(n+0) FROM empty_args WHERE true OR id=99",[[None]],[20])
        query("SELECT sum(n+0) FROM empty_args WHERE NOT false",[[None]],[20])
        query("SELECT sum(n+0) FROM args WHERE CAST(CAST(n AS NUMERIC)/3 AS NUMERIC(2,0)) > 0",[["2"]],[20])
        query("SELECT sum(n+0) FROM args WHERE n = NUMERIC '1'",[["1"]],[20])
        query("SELECT sum(n+0) FROM args WHERE b = BOOLEAN 'true'",[["0"]],[20])
        query("SELECT sum(n+0) FROM args WHERE BOOLEAN 'true'",[["2"]],[20])
        query("SELECT sum(n+0) FROM args WHERE DATE '2024-01-01' < DATE '2024-01-02'",[["2"]],[20])
        query("SELECT sum(n+0) FROM empty_args WHERE DATE '2024-01-01' < DATE '2024-01-02'",[[None]],[20])
        query("SELECT sum(n ORDER BY id),count(t ORDER BY id DESC) FROM args",[["2","3"]],[20,20])
        for sql,state in [
            ("SELECT sum(missing+1) FROM empty_args","42703"),
            ("SELECT sum(sum(n)) FROM empty_args","42803"),
            ("SELECT sum(CAST(n AS TEXT)) FROM empty_args","42883"),
            ("SELECT sum('abc') FROM empty_args","42725"),
            ("SELECT sum(CAST($1 AS INT)) FROM empty_args","42P02"),
            ("SELECT sum(n+1) FILTER (WHERE 1) FROM empty_args","42804"),
            ("SELECT sum(n+1) FILTER (WHERE 1) FROM args LIMIT 0","42804"),
        ]:
            if reference: query("SAVEPOINT argument_error")
            query(sql,state=state)
            if reference: query("ROLLBACK TO argument_error");query("RELEASE argument_error")
        query("CREATE SEQUENCE argument_seq")
        query("CREATE TABLE argument_effect (seq BIGINT DEFAULT nextval('argument_seq'),v INT)")
        query("CREATE FUNCTION argument_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO argument_effect(v) VALUES(i); RETURN i; END;$$")
        def effects(values): query("SELECT v FROM argument_effect ORDER BY seq",[[value] for value in values],[23])
        for index,(sql,types,headers) in enumerate([
            ('SELECT sum(CASE WHEN n > 0 THEN 1 ELSE 0 END) AS "Total Result" FROM empty_args',[20],["Total Result"]),
            ("SELECT bool_and(t < 'alpha'),sum(CAST(n AS BIGINT)) FROM empty_args LIMIT 0",[16,1700],["bool_and","sum"]),
            ("SELECT sum(argument_writer(n)),bool_or(argument_writer(n)>0) FROM args",[20,16],["sum","bool_or"]),
        ]):
            name=("argument_descriptor_"+str(index)).encode()
            sock.sendall(client.typed(b"P",name+b"\0"+sql.encode()+b"\0\0\0")+
                         client.typed(b"D",b"S"+name+b"\0")+client.typed(b"S"))
            messages=client.read_until_ready(sock)
            fields=client.row_description_fields(messages)
            print("AGGREGATE_ARGUMENT_DESCRIBE",sql,fields,flush=True)
            try:
                assert not any(kind in (b"E",b"D") for kind,_ in messages),(sql,messages)
                assert [field[3] for field in fields]==types,(sql,fields,types)
                assert [field[0] for field in fields]==[header.encode() for header in headers],(sql,fields,headers)
            except AssertionError as failure:
                if not collect_errors: raise
                failures.append(failure.args)
                print("AGGREGATE_ARGUMENT_STRONG_FAILURE",failure.args,flush=True)
        effects([])
        query("SELECT sum(argument_writer(n)) FILTER (WHERE id = 1) FROM args",[["1"]],[20]);effects(["1"])
        query("DELETE FROM argument_effect")
        query("SELECT sum(argument_writer(n)) FILTER (WHERE false) FROM args",[[None]],[20]);effects([])
        query("SELECT sum(argument_writer(n)) FROM empty_args",[[None]],[20]);effects([])
        query("SELECT sum(argument_writer(n)) FROM args LIMIT 0",[],[20]);effects([])
        query("SELECT SUM(CASE WHEN id = 1 THEN argument_writer(n) ELSE 0 END) FROM args",[["1"]],[20]);effects(["1"])
        query("DELETE FROM argument_effect")
        query("SELECT bool_and(argument_writer(n) < 0) FROM args",[["f"]],[16]);effects(["1","2",None,"-1"])
        query("DELETE FROM argument_effect")
        query("SELECT bool_or(argument_writer(n) > 0) FROM args",[["t"]],[16]);effects(["1","2",None,"-1"])
        query("DELETE FROM argument_effect")
        query("SELECT sum(argument_writer(n)) FROM args WHERE id <= 2 OR id >= 2",[["2"]],[20]);effects(["1","2",None,"-1"])
        query("DELETE FROM argument_effect")
        query("SELECT sum(argument_writer(n)),sum(argument_writer(n)) FROM args",[["2","2"]],[20,20]);effects(["1","1","2","2",None,None,"-1","-1"])
        owner="aggregate_owner_"+uuid.uuid4().hex[:18]
        query('CREATE SCHEMA "'+owner+'"')
        query('CREATE FUNCTION "'+owner+'".sum(i INT) RETURNS INT LANGUAGE SQL AS $$SELECT i+100$$')
        query('SELECT "'+owner+'".sum(n+0) FROM args ORDER BY id',[["101"],["102"],[None],["99"]],[23],headers=["sum"])
        assert not failures,("full original aggregate argument matrix failed",failures)
        print("[aggregate argument] full direct expression/type/NULL/empty/filter/index/error and volatile once-demand matrix passed",flush=True)
    finally:
        if reference:
            try: client.simple_query(sock,"ROLLBACK")
            finally: sock.close()
        else: runner.stop_ours(server)


if __name__=="__main__": main()
