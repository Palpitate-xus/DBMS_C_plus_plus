#!/usr/bin/env python3
"""ProjectSet consumes its actual typed array and original SQL-child cursor."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("projectset_runner",root/"tests/compat/pg_diff_runner.py")
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==["--reference18"]
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,pw);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server["sock"]
    failures=[]

    def query(sql,state=None,rows=None,types=None):
        result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print("PROJECTSET",sql,result,flush=True)
        if result[1]!=state:failures.append((sql,"state",result[1],state))
        if rows is not None and result[0]!=rows:failures.append((sql,"rows",result[0],rows))
        if types is not None and result[5]!=types:failures.append((sql,"types",result[5],types))
        if state and (result[0] or result[4] is not None):failures.append((sql,"partial result/tag",result[0],result[4]))
        return result

    # SQL, state, rows, element descriptor, irreversible routine calls.
    cases=[
        ("SELECT unnest(ARRAY[1,2]) WHERE 1=ANY(SELECT 1)",None,[["1"],["2"]],[23],0),
        ("SELECT unnest(ARRAY[1,2]) WHERE 1=ANY(SELECT NULL::INT)",None,[],[23],0),
        ("SELECT unnest(ARRAY[1,2]) WHERE 1=ALL(SELECT id FROM ps_rows WHERE false)",None,[["1"],["2"]],[23],0),
        ("SELECT unnest(NULL::INT[]) WHERE 1=ANY(SELECT 1)",None,[],[23],0),
        ("SELECT unnest(ARRAY[]::INT[]) WHERE 1=ANY(SELECT 1)",None,[],[23],0),
        ("SELECT unnest(ARRAY['NULL',NULL,'',' a b ']) WHERE 1=ANY(SELECT 1)",None,[["NULL"],[None],[""],[" a b "]],[25],0),
        ("SELECT unnest(ARRAY[ps_writer(1),ps_writer(2)]) WHERE 1=ANY(SELECT 1) LIMIT 1",None,[["1"]],[23],2),
        ("SELECT unnest(ARRAY[ps_writer(1),ps_writer(2)]) WHERE 1=ANY(SELECT 0)",None,[],[23],0),
        ("SELECT unnest(ARRAY[ps_writer(1),ps_writer(2)]) WHERE 1=ANY(SELECT 1) LIMIT 0",None,[],[23],0),
        ("SELECT unnest(ARRAY[1,2]) WHERE 1=ANY(SELECT ps_writer(1))",None,[["1"],["2"]],[23],1),
        ("SELECT unnest(ARRAY[(SELECT ps_writer(3)),NULL]) WHERE 1=ANY(SELECT 1)",None,[["3"],[None]],[23],1),
        ("SELECT unnest(ARRAY[(SELECT id FROM ps_rows)]) WHERE 1=ANY(SELECT 1)","21000",[],None,0),
        ("SELECT unnest(ARRAY[-INTERVAL '-2147483648 months']) WHERE 1=ANY(SELECT 1) LIMIT 0","22008",[],None,0),
        ("SELECT unnest(ARRAY[-INTERVAL '-2147483648 months']) WHERE 1=ANY(SELECT 1)","22008",[],None,0),
        ("SELECT unnest(ARRAY[1/0]) WHERE 1=ANY(SELECT ps_writer(1)) LIMIT 0","22012",[],None,0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE 1=ANY(SELECT 1/0)","22012",[],None,0),
        ("SELECT unnest(ARRAY[1]) WHERE false AND 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)",None,[],[23],0),
        ("SELECT unnest(ARRAY[CASE WHEN false THEN(SELECT 1/0) ELSE 1 END]) WHERE 1=ANY(SELECT 1)",None,[["1"]],[23],0),
        ("WITH w AS(INSERT INTO ps_sink VALUES(ps_writer(5)) RETURNING id) SELECT unnest(ARRAY[1,2]) WHERE 5=ANY(SELECT id FROM w)",None,[["1"],["2"]],[23],1),
        ("WITH w AS(INSERT INTO ps_sink VALUES(ps_writer(5)) RETURNING id) SELECT unnest(ARRAY[ps_writer(1)]) WHERE 1=ANY(SELECT 1) LIMIT 0",None,[],[23],1),
        # Original no-FROM receiver forms retain their actual datum width.
        ("SELECT unnest(ARRAY[4,5,6])",None,[["4"],["5"],["6"]],[23],0),
        ("SELECT pg_catalog.unnest(ARRAY[4,5,6]) AS value",None,[["4"],["5"],["6"]],[23],0),
        ("SELECT unnest(ARRAY['NULL',NULL,'',' a b '])",None,[["NULL"],[None],[""],[" a b "]],[25],0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE false",None,[],[23],0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) LIMIT 0",None,[],[23],0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE missing_ps=1","42703",[],None,0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE missing_ps_function(1)=1","42883",[],None,0),
        ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE 1","42804",[],None,0),
    ]
    try:
        for sql in (
            "BEGIN","CREATE TEMP TABLE ps_rows(id INT)","INSERT INTO ps_rows VALUES(1),(2)",
            "CREATE TEMP TABLE ps_sink(id INT)","CREATE TEMP SEQUENCE ps_calls",
            "CREATE FUNCTION ps_writer(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('ps_calls'); RETURN p; END$$",
        ):assert query(sql+";")[1] is None
        query('CREATE FUNCTION "Unnest"(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 77; END$$;')
        query('SELECT "Unnest"(1);',rows=[["77"]],types=[23])
        expected=0
        for sql,state,rows,types,calls in cases:
            query("SAVEPOINT ps_case;")
            query(sql+";",state,rows,types)
            query("ROLLBACK TO ps_case;")
            query("SELECT id FROM ps_sink;",rows=[])
            expected+=calls+1
            query("SELECT nextval('ps_calls');",rows=[[str(expected)]],types=[20])
        for prefix in ("EXPLAIN ","EXPLAIN ANALYZE ","EXPLAIN (FORMAT JSON) ","EXPLAIN (ANALYZE,FORMAT JSON) "):
            for sql,state in (
                ("SELECT unnest(ARRAY[-INTERVAL '-2147483648 months']) WHERE 1=ANY(SELECT 1) LIMIT 0","22008"),
                ("SELECT unnest(ARRAY[1/0]) WHERE 1=ANY(SELECT ps_writer(1))","22012"),
                ("SELECT unnest(ARRAY[ps_writer(1)]) WHERE 1=ANY(SELECT 1) LIMIT 0",None),
                ("SELECT unnest(ARRAY[1,NULL,2]) WHERE 1=ANY(SELECT 1)",None),
                ("SELECT pg_catalog.unnest(ARRAY[4,5,6])",None),
                ("SELECT unnest(ARRAY[1/0]) WHERE false","22012"),
                ("SELECT unnest(ARRAY[-INTERVAL '-2147483648 months']) LIMIT 0","22008"),
            ):
                query("SAVEPOINT ps_explain;")
                query(prefix+sql+";",state)
                query("ROLLBACK TO ps_explain;")
                expected+=1
                query("SELECT nextval('ps_calls');",rows=[[str(expected)]],types=[20])
        query("ROLLBACK;")
        print("PROJECTSET FAILURES",failures,flush=True)
        assert not failures,failures
        print("[PROJECTSET] passed full typed-row/root/child/effect matrix",flush=True)
    finally:
        try:c.simple_query(sock,"ROLLBACK;")
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
