#!/usr/bin/env python3
"""Retained UNION ALL bodies stream real typed rows and preserve child demand."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("append_runner",root/"tests/compat/pg_diff_runner.py")
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==["--reference18"];assert not sys.argv[1:] or reference
    server=None
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,pw);r.verify_reference_version(c,sock)
    else: server=r.start_ours(c);sock=server["sock"]
    failures=[];counter=0

    def query(sql,state=None,rows=None,types=None,tag=None):
        result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print("TYPED APPEND",sql,result,flush=True)
        for label,actual,expected in [("state",result[1],state),("rows",result[0],rows),("types",result[5],types),("tag",result[4],tag)]:
            if expected is not None or label=="state":
                if actual!=expected:failures.append((sql,label,actual,expected))
        if state and(result[0] or result[4] is not None):failures.append((sql,"partial",result[0],result[4]))
        return result

    cases=[
        ("WITH h AS(SELECT 1) SELECT 1 UNION ALL SELECT 2147483648",None,[["1"],["2147483648"]],[20],"SELECT 2",0),
        ("WITH h AS(SELECT 1) SELECT NULL UNION ALL SELECT 'NULL'",None,[[None],["NULL"]],[25],"SELECT 2",0),
        ("WITH h AS(SELECT 1) SELECT '' UNION ALL SELECT 'a b'",None,[[""],["a b"]],[25],"SELECT 2",0),
        ("WITH h AS(SELECT 1) SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2147483648",None,[["1"],["1"],["2147483648"]],[20],"SELECT 3",0),
        ("WITH h AS(SELECT 1) SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648]",None,[["{1}"],["{2147483648}"]],[1016],"SELECT 2",0),
        ("WITH h AS(SELECT 1) SELECT id FROM app_rows UNION ALL SELECT 2147483648",None,[["1"],["2"],["2147483648"]],[20],"SELECT 3",0),
        ("WITH h AS(SELECT 1) SELECT 1 WHERE false UNION ALL SELECT 2 WHERE false",None,[],[23],"SELECT 0",0),
        ("WITH h AS(SELECT 1) SELECT CASE WHEN false THEN(SELECT 1/0) ELSE 2 END UNION ALL SELECT 3",None,[["2"],["3"]],[23],"SELECT 2",0),
        ("WITH h AS(SELECT 1) SELECT 2>ANY(SELECT app_writer(1) UNION ALL SELECT app_writer(3))",None,[["t"]],[16],"SELECT 1",1),
        ("WITH h AS(SELECT 1) SELECT 1>ALL(SELECT app_writer(2) UNION ALL SELECT app_writer(3))",None,[["f"]],[16],"SELECT 1",1),
        ("WITH h AS(SELECT 1) SELECT 1=ANY(SELECT app_writer(1) UNION ALL SELECT app_writer(2))",None,[["t"]],[16],"SELECT 1",2),
        ("WITH h AS(SELECT 1) SELECT 1=ANY(SELECT NULL::INT UNION ALL SELECT 2)",None,[[None]],[16],"SELECT 1",0),
        ("WITH h AS(SELECT 1) SELECT(SELECT app_writer(1) UNION ALL SELECT app_writer(2) UNION ALL SELECT app_writer(3))","21000",[],None,None,2),
        ("WITH h AS(SELECT 1) SELECT(SELECT app_writer(7)) UNION ALL SELECT(SELECT app_writer(7))",None,[["7"],["7"]],[23],"SELECT 2",2),
        ("WITH h AS(SELECT 1) SELECT(SELECT 1 WHERE false UNION ALL SELECT 2 WHERE false)",None,[[None]],[23],"SELECT 1",0),
        ("WITH h AS(SELECT 1) SELECT id FROM app_rows WHERE id>ANY(SELECT app_writer(0) UNION ALL SELECT app_writer(3))",None,[["1"],["2"]],[23],"SELECT 2",1),
        ("WITH h AS(SELECT 1) UPDATE app_rows SET v=app_writer(v) WHERE id=ANY(SELECT 1 UNION ALL SELECT 2) RETURNING id,v",None,[["1","10"],["2","20"]],[23,23],"UPDATE 2",2),
        ("WITH h AS(SELECT 1) UPDATE app_rows SET v=(SELECT app_writer(app_rows.v) UNION ALL SELECT 99 WHERE false) RETURNING id,v",None,[["1","10"],["2","20"]],[23,23],"UPDATE 2",2),
        ("WITH h AS(SELECT 1) DELETE FROM app_rows WHERE id>ANY(SELECT app_writer(0) UNION ALL SELECT app_bad(0)) RETURNING id",None,[["1"],["2"]],[23],"DELETE 2",1),
        ("WITH h AS(SELECT 1) DELETE FROM app_rows WHERE id=ANY(SELECT app_writer(1) UNION ALL SELECT app_bad(0)) RETURNING id","22012",[],None,None,1),
        ("WITH h AS(SELECT 1) SELECT app_writer(1) UNION ALL SELECT 1/0","22012",[],None,None,0),
    ]
    try:
        for sql in ["BEGIN","CREATE TEMP TABLE app_rows(id INT,v INT)","INSERT INTO app_rows VALUES(1,10),(2,20)","CREATE TEMP SEQUENCE app_calls","CREATE FUNCTION app_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('app_calls'); RETURN p; END$$","CREATE FUNCTION app_bad(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN RETURN 1/p; END$$"]:
            assert query(sql+";")[1] is None
        for sql,state,rows,types,tag,effects in cases:
            query("SAVEPOINT app_case;")
            query(sql+";",state,rows,types,tag)
            query("ROLLBACK TO app_case;")
            query("SELECT id,v FROM app_rows ORDER BY id;",rows=[["1","10"],["2","20"]],types=[23,23],tag="SELECT 2")
            counter+=effects+1
            query("SELECT nextval('app_calls');",rows=[[str(counter)]],types=[20],tag="SELECT 1")
        query("ROLLBACK;");print("APPEND FAILURES",failures,flush=True);assert not failures,failures
    finally:
        try:c.simple_query(sock,"ROLLBACK;")
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
