#!/usr/bin/env python3
"""Window input and output reverse DESC values and SQL NULL order only once."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("window_null_order_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    controls=0;failures=[]

    def query(sql,rows,oids=None,headers=None):
        nonlocal controls
        controls+=1
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print("WINDOW_NULL_ORDER",sql,actual,flush=True)
        valid=actual[1] is None and actual[0]==rows
        valid=valid and (oids is None or actual[5]==oids) and (headers is None or actual[3]==headers)
        if sql.startswith("SELECT"):valid=valid and actual[4]=="SELECT %d"%len(rows)
        if not valid:failures.append((sql,actual,rows,oids,headers))
        return actual

    try:
        if options.reference18:
            query("BEGIN",[])
            schema="window_null_order_"+uuid.uuid4().hex[:14]
            query('CREATE SCHEMA "'+schema+'"',[])
            query('SET LOCAL search_path TO "'+schema+'",pg_catalog',[])
        assert query("CREATE TABLE t(id INT,k INT,v INT)",[])[1] is None
        assert query("CREATE TABLE empty_t(id INT,k INT,v INT)",[])[1] is None
        assert query("INSERT INTO t VALUES(1,1,10),(2,NULL,20),(3,2,30),(4,3,40)",[])[1] is None
        for ascending in (True,False):
          for input_policy in (None,"LAST","FIRST"):
            def ordered(asc,policy):
                values=[1,3,4] if asc else [4,3,1]
                first=not asc if policy is None else policy=="FIRST"
                return [2]+values if first else values+[2]
            inner=ordered(ascending,input_policy)
            for bounded in (False,True):
                window="(ORDER BY k "+("ASC" if ascending else "DESC")
                if input_policy is not None:window+=" NULLS "+input_policy
                if bounded:window+=" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"
                window+=")"
                targets="id,k,row_number() OVER "+window+" AS n,rank() OVER "+window+" AS r,sum(v) OVER "+window+" AS total,count(*) OVER "+window+" AS counted"
                output_cases=[("",inner),(" ORDER BY id",[1,2,3,4])]
                for asc in (True,False):
                    for policy in (None,"LAST","FIRST"):
                        clause=" ORDER BY k "+("ASC" if asc else "DESC")
                        if policy is not None:clause+=" NULLS "+policy
                        output_cases.append((clause,ordered(asc,policy)))
                for output,order in output_cases:
                    rows=[]
                    for identifier in order:
                        rank=inner.index(identifier)+1
                        start=max(0,rank-2) if bounded else 0
                        total=sum(value*10 for value in inner[start:rank])
                        rows.append([str(identifier),None if identifier==2 else str(1 if identifier==1 else identifier-1),
                                     str(rank),str(rank),str(total),str(min(rank,2) if bounded else rank)])
                    query("SELECT "+targets+" FROM t"+output,rows,[23,23,20,20,20,20],["id","k","n","r","total","counted"])
                    query("SELECT "+targets+" FROM empty_t"+output,[],[23,23,20,20,20,20],["id","k","n","r","total","counted"])
        print("[WINDOW NULL ORDER] complete controls=%d failures=%d"%(controls,len(failures)),flush=True)
        assert not failures,failures
    finally:
        if options.reference18:client.simple_query(sock,"ROLLBACK");sock.close()
        else:runner.stop_ours(server)


if __name__=="__main__":main()
