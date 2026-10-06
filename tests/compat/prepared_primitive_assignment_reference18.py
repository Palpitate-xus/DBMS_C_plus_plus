#!/usr/bin/env python3
"""Strict PG18 analysis-stage oracle; EXPLAIN separates pure planning cases."""
import importlib.util
import socket
from pathlib import Path

CASES=[
    ("INSERT INTO assignment_rows VALUES('bad')","22P02"),
    ("INSERT INTO assignment_rows VALUES('2147483648')","22003"),
    ("INSERT INTO assignment_rows VALUES('bad'),(assignment_writer(2))","22P02"),
    ("INSERT INTO assignment_rows VALUES('bad',missing_assignment_function(1))","42883"),
    ("INSERT INTO assignment_rows VALUES('bad') RETURNING missing_assignment_function(1)","22P02"),
    ("INSERT INTO assignment_rows SELECT 'bad' WHERE false","22P02"),
    ("INSERT INTO assignment_rows SELECT 'bad' WHERE missing_assignment_function(1)=1","42883"),
    ("UPDATE assignment_rows SET id='bad' WHERE false","22P02"),
    ("UPDATE assignment_rows SET id='2147483648' WHERE false","22003"),
    ("UPDATE assignment_rows SET id='bad' WHERE missing_assignment_function(1)=1","42883"),
    ("UPDATE assignment_rows SET id='bad' RETURNING missing_assignment_function(1)","42883"),
    ("UPDATE assignment_rows SET id='bad' WHERE 1","42804"),
    ("UPDATE assignment_rows SET id=assignment_writer(2) WHERE 'bad'","22P02"),
    ("WITH ins AS(INSERT INTO assignment_rows VALUES(assignment_writer(2)) RETURNING id) UPDATE assignment_rows SET id='bad' WHERE false","22P02"),
    ("UPDATE assignment_rows SET id='2' WHERE false",None),
    ("UPDATE assignment_rows SET id=NULL WHERE false",None),
    ("INSERT INTO assignment_rows VALUES('2')",None),
    ("INSERT INTO assignment_rows SELECT '2' WHERE false",None),
]

def main():
    spec=importlib.util.spec_from_file_location("assignment_ref",Path(__file__).with_name("pg_diff_runner.py"))
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password=password); runner.verify_reference_version(client,sock)
    failures=[]
    def query(sql,state=None):
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print("ASSIGNMENT REFERENCE",sql,result,flush=True)
        if result[1]!=state:failures.append((sql,result[1],state))
        return result
    try:
        query("BEGIN;"); query("CREATE TEMP TABLE assignment_rows(id INT);")
        query("INSERT INTO assignment_rows VALUES(1);"); query("CREATE TEMP SEQUENCE assignment_effects;")
        query("CREATE FUNCTION assignment_writer(p INT) RETURNS INT LANGUAGE plpgsql VOLATILE AS "
            "$$BEGIN PERFORM nextval('assignment_effects'); RETURN p; END$$;")
        for index,(sql,state) in enumerate(CASES,1):
            query("SAVEPOINT assignment_case;"); query("EXPLAIN (FORMAT JSON) "+sql+";",state)
            query("ROLLBACK TO assignment_case;")
            result=query("SELECT nextval('assignment_effects');")
            if result[0]!=[[str(index)]]:failures.append((sql,"unexpected planning effect",result))
        assert query("SELECT id FROM assignment_rows;")[0]==[["1"]]
        print("ASSIGNMENT REFERENCE FAILURES",failures,flush=True); assert not failures,failures
    finally:
        try:client.simple_query(sock,"ROLLBACK;")
        finally:sock.close()

if __name__=="__main__":main()
