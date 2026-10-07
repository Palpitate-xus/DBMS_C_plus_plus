#!/usr/bin/env python3
"""Bit comparison identities, leading zeros, NULLs and real stored consumers."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--reference18", action="store_true")
    options = args.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("bit_comparison_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port), timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    schema = "bit_cmp_" + uuid.uuid4().hex[:12]
    created = False

    def check(sql, expected, oids):
        decoded = runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        rows, state, message, _, _, types = decoded
        assert state is None and rows==expected and types==oids, (sql,decoded,expected,oids)

    try:
        for expression, expected in [
            ("B'001'<B'01'", "t"), ("B'1'=B'01'", "f"),
            ("B'1'<>B'01'", "t"), ("B'00'>B'0'", "t"),
            ("B''<B'0'", "t"), ("B'001'<=B'01'", "t"),
            ("B'001'>=B'01'", "f"), ("B'1' IS DISTINCT FROM B'01'", "t"),
            ("B'001'::varbit<B'01'", "t"),
            ("B'1'=ANY(ARRAY[B'01'])", "f"),
            ("B'001'<ANY(ARRAY[B'01'])", "t"),
            ("B'1'=ANY(SELECT B'01')", "f"),
            ("B'001'<ALL(SELECT B'01')", "t")]:
            check("SELECT " + expression + " AS value",[[expected]],[16])
        check("SELECT NULL::bit<B'01', B'1'=NULL::varbit",[[None,None]],[16,16])
        check("SELECT CASE B'1' WHEN B'01' THEN 1 ELSE 2 END",[["2"]],[23])
        check("CREATE SCHEMA " + schema,[],[])
        created = True
        check("CREATE TABLE " + schema + ".bits (id integer, v varbit)",[],[])
        check("INSERT INTO " + schema + ".bits VALUES "
              "(1,B'1'),(2,B'01'),(3,B'001'),(4,B'0'),(5,B'00'),(6,B''),(7,NULL),(8,B'01')",[],[])
        check("SELECT id,v FROM " + schema + ".bits ORDER BY v,id",
              [["6",""],["4","0"],["5","00"],["3","001"],["2","01"],
               ["8","01"],["1","1"],["7",None]],[23,1562])
        check("SELECT id FROM " + schema + ".bits WHERE v=B'01' ORDER BY id",[["2"],["8"]],[23])
        check("SELECT id FROM " + schema + ".bits WHERE v<B'01' ORDER BY id",
              [["3"],["4"],["5"],["6"]],[23])
        check("SELECT v,count(*) FROM " + schema + ".bits GROUP BY v ORDER BY v",
              [["","1"],["0","1"],["00","1"],["001","1"],["01","2"],["1","1"],[None,"1"]],[1562,20])
        check("SELECT DISTINCT v FROM " + schema + ".bits ORDER BY v",
              [[""],["0"],["00"],["001"],["01"],["1"],[None]],[1562])
        check("CREATE TABLE " + schema + ".pairs(id int,a varbit,b varbit)",[],[])
        check("INSERT INTO " + schema + ".pairs VALUES "
              "(1,B'1',B'01'),(2,B'001',B'01'),(3,B'01',B'01'),"
              "(4,B'',B''),(5,NULL,B'01'),(6,B'01',NULL)",[],[])
        check("SELECT id FROM " + schema + ".pairs WHERE a=b ORDER BY id",[["3"],["4"]],[23])
        check("SELECT id FROM " + schema + ".pairs WHERE a<b ORDER BY id",[["2"]],[23])
        check("SELECT id FROM " + schema + ".pairs WHERE a>b ORDER BY id",[["1"]],[23])
        check("UPDATE " + schema + ".bits SET v=B'11' WHERE v=B'01' RETURNING id",
              [["2"],["8"]],[23])
        check("SELECT id,v FROM " + schema + ".bits ORDER BY id",
              [["1","1"],["2","11"],["3","001"],["4","0"],["5","00"],["6",""],
               ["7",None],["8","11"]],[23,1562])
        check("DELETE FROM " + schema + ".bits WHERE v=B'11' RETURNING id",[["2"],["8"]],[23])
        check("SELECT count(*) FROM " + schema + ".bits",[["6"]],[20])
        print("[BIT COMPARISON PROTOCOL] all 32 value/NULL/identity/storage/DML controls passed")
    finally:
        try:
            if created:
                check("DROP SCHEMA " + schema + " CASCADE",[],[])
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
