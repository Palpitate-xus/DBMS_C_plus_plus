#!/usr/bin/env python3
"""GROUPS unbounded ends use the partition, not just the current peer group."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path

ROWS = [
    (1,"a",1,10,0,4),(2,"a",1,None,0,4),(3,"a",2,20,1,4),
    (4,"a",3,30,2,4),(5,"a",3,40,2,4),(6,"b",1,100,0,2),
    (7,"b",2,200,1,2),(8,"b",2,None,1,2),(9,None,1,7,0,2),
    (10,None,2,None,1,2),(11,"a",None,90,3,4),(12,"a",None,None,3,4)]


def expected(start,end,ascending,exclusion):
    group = lambda row: row[4] if ascending else row[5]-row[4]-1
    output = []
    for current in ROWS:
        frame = [row for row in ROWS if row[1] == current[1]
                 and (start < 0 or group(row) >= group(current)-start)
                 and (end < 0 or group(row) <= group(current)+end)
                 and not (exclusion == "CURRENT ROW" and row[0] == current[0])
                 and not (exclusion == "GROUP" and row[2] == current[2])
                 and not (exclusion == "TIES" and row[2] == current[2] and row[0] != current[0])]
        values = [row[3] for row in frame if row[3] is not None]
        output.append([str(current[0]),str(sum(values)) if values else None,
                       str(len(frame)),str(len(values)),
                       str(min(values)) if values else None,str(max(values)) if values else None])
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference18",action="store_true")
    options = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("groups_unbounded_runner",root/"tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host,port,user,database,password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    controls = 0

    def query(sql,wanted=None,oids=None,headers=None):
        nonlocal controls
        controls += 1
        actual = runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print("GROUPS_UNBOUNDED",sql,actual,flush=True)
        valid = actual[1] is None and (wanted is None or actual[0] == wanted)
        valid = valid and (oids is None or actual[5] == oids)
        valid = valid and (headers is None or actual[3] == headers)
        if wanted is not None and sql.startswith("SELECT"):
            valid = valid and actual[4] == "SELECT %d" % len(wanted)
        if not valid:
            failures.append((sql,actual,wanted,oids,headers))
        return actual

    schema = "groups_unbounded_"+uuid.uuid4().hex[:15]
    try:
        if options.reference18:
            assert query("BEGIN",[])[1] is None
            assert query('CREATE SCHEMA "'+schema+'"',[])[1] is None
            assert query('SET LOCAL search_path TO "'+schema+'",pg_catalog',[])[1] is None
        assert query("CREATE TABLE t(id INT,p TEXT,k INT,v INT)",[])[1] is None
        assert query("CREATE TABLE empty_t(id INT,p TEXT,k INT,v INT)",[])[1] is None
        literal = lambda value: "NULL" if value is None else "'"+value+"'" if isinstance(value,str) else str(value)
        assert query("INSERT INTO t VALUES "+",".join("("+",".join(map(literal,row[:4]))+")" for row in ROWS),[])[1] is None
        names = ["id","total","all_rows","nonnull","minimum","maximum"]
        for ascending in (True,False):
            for start in (-1,0,1):
                for end in (-1,0,1):
                    for exclusion in ("NO OTHERS","CURRENT ROW","GROUP","TIES"):
                        first = "UNBOUNDED PRECEDING" if start < 0 else "CURRENT ROW" if start == 0 else "1 PRECEDING"
                        last = "UNBOUNDED FOLLOWING" if end < 0 else "CURRENT ROW" if end == 0 else "1 FOLLOWING"
                        window = "(PARTITION BY p ORDER BY k "+("ASC" if ascending else "DESC")+" GROUPS BETWEEN "+first+" AND "+last+" EXCLUDE "+exclusion+")"
                        targets = ",".join(function+" OVER "+window+" AS "+name for function,name in
                            (("sum(v)","total"),("count(*)","all_rows"),("count(v)","nonnull"),("min(v)","minimum"),("max(v)","maximum")))
                        query("SELECT id,"+targets+" FROM t ORDER BY id",expected(start,end,ascending,exclusion),[23,20,20,20,23,23],names)
                        query("SELECT id,"+targets+" FROM empty_t ORDER BY id",[],[23,20,20,20,23,23],names)
        print("[WINDOW GROUPS UNBOUNDED] complete controls=%d failures=%d" % (controls,len(failures)),flush=True)
        assert not failures, failures
    finally:
        if options.reference18:
            client.simple_query(sock,"ROLLBACK")
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
