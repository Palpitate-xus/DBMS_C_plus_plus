#!/usr/bin/env python3
"""Plain/ANALYZE EXPLAIN plans the real execution root, never a dead child."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("explain_root_runner",root/"tests/compat/pg_diff_runner.py")
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==["--reference18"]
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        h,p,u,d,pw=r._reference_connection_settings()
        sock=socket.create_connection((h,p),timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,pw);r.verify_reference_version(c,sock)
    else:
        server=r.start_ours(c);sock=server["sock"]
    failures=[]

    def query(sql,state=None,rows=None):
        result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print("EXPLAIN ROOT",sql,result,flush=True)
        if result[1]!=state:failures.append((sql,"state",result[1],state))
        if rows is not None and result[0]!=rows:failures.append((sql,"rows",result[0],rows))
        if state is not None and (result[0] or result[4] is not None):
            failures.append((sql,"partial result/completion",result[0],result[4]))
        return result

    controls=[
        ("SELECT 1/0 WHERE false","22012"),
        ("SELECT 1/0 LIMIT 0","22012"),
        ("SELECT 1/0 FROM explain_root_rows WHERE false","22012"),
        ("SELECT 1/0 FROM explain_root_rows LIMIT 0","22012"),
        ("SELECT -INTERVAL '-2147483648 months' WHERE false","22008"),
        ("SELECT -INTERVAL '-2147483648 months' FROM explain_root_rows WHERE false","22008"),
        ("SELECT CAST(2147483648 AS INT) WHERE false","22003"),
        ("SELECT (SELECT 1/0) WHERE false","22012"),
        ("SELECT (SELECT 1/0) LIMIT 0","22012"),
        ("SELECT CASE WHEN id=1 THEN 1/0 ELSE 2 END FROM explain_root_rows WHERE false","22012"),
        ("SELECT CASE WHEN false THEN 1/0 ELSE 3 END",None),
        ("SELECT CASE WHEN false THEN (SELECT 1/0) ELSE 3 END",None),
        ("WITH dead AS(SELECT 1/0) SELECT 3 WHERE false",None),
        ("WITH dead AS(SELECT CAST('bad' AS INT)) SELECT 3 WHERE false","22P02"),
        ("SELECT CASE WHEN false THEN CAST('bad' AS INT) ELSE 3 END","22P02"),
        ("SELECT explain_root_writer() WHERE false",None),
        ("SELECT explain_root_writer() LIMIT 0",None),
        ("SELECT explain_root_writer(),1/0 FROM explain_root_rows","22012"),
        ("SELECT 3 FROM (SELECT 1/0 AS unused) AS q WHERE false",None),
    ]
    try:
        for sql in (
            "BEGIN","CREATE TEMP TABLE explain_root_rows(id INT)",
            "INSERT INTO explain_root_rows VALUES(1)",
            "CREATE TEMP SEQUENCE explain_root_calls",
            "CREATE FUNCTION explain_root_writer() RETURNS INT LANGUAGE plpgsql "
            "AS $$BEGIN PERFORM nextval('explain_root_calls'); RETURN 7; END$$",
        ):
            assert query(sql+";")[1] is None
        for prefix in ("EXPLAIN ","EXPLAIN ANALYZE ","EXPLAIN (FORMAT JSON) ","EXPLAIN (ANALYZE,FORMAT JSON) "):
            for sql,state in controls:
                query("SAVEPOINT explain_case;")
                query(prefix+sql+";",state)
                query("ROLLBACK TO explain_case;")
                query("SAVEPOINT effect_check;")
                query("SELECT currval('explain_root_calls');","55000",[])
                query("ROLLBACK TO effect_check;")
                query("SELECT id FROM explain_root_rows;",rows=[["1"]])
        # Positive ANALYZE actually executes the shown plan exactly once.
        query("EXPLAIN ANALYZE SELECT explain_root_writer();")
        query("SELECT currval('explain_root_calls');",rows=[["1"]])
        query("EXPLAIN SELECT explain_root_writer();")
        query("SELECT currval('explain_root_calls');",rows=[["1"]])
        query("ROLLBACK;")
        print("EXPLAIN ROOT FAILURES",failures,flush=True)
        assert not failures,failures
        print(f"[EXPLAIN ROOT] passed {len(controls)} controls in four formats plus execution guards",flush=True)
    finally:
        try:c.simple_query(sock,"ROLLBACK;")
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
