#!/usr/bin/env python3
"""Declaration-order enum operators bound to real types, not datum spellings."""
import importlib.util
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("enum_comparison_runner", repo / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def query(sql, rows=None, types=None, state=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("ENUM_COMPARISON", sql, result, flush=True)
        assert result[1] == state, (sql, result, state)
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        if types is not None:
            assert result[5] == types, (sql, result, types)
        return result

    labels = ["zeta", "", "alpha", "aa", "NULL", "it's", "longlonglong"]
    operators = [("<", lambda a,b:a<b),("=", lambda a,b:a==b),("<>", lambda a,b:a!=b),
                 (">", lambda a,b:a>b),("<=", lambda a,b:a<=b),(">=", lambda a,b:a>=b),("!=", lambda a,b:a!=b)]
    quoted = lambda text: "'" + text.replace("'", "''") + "'"
    schema = "enum_compare_" + uuid.uuid4().hex[:18]
    def check_rank_matrix():
        for j, label in enumerate(labels):
            sql = "SELECT id," + ",".join("r " + op + " " + quoted(label) for op,_ in operators) + " FROM ranks ORDER BY id"
            rows = [[str(i+1)] + ["t" if predicate(i,j) else "f" for _,predicate in operators] for i in range(len(labels))]
            rows.append(["8"] + [None] * len(operators))
            query(sql, rows, [23] + [16] * len(operators))
            query("SELECT id FROM ranks WHERE r < " + quoted(label) + " ORDER BY id", [[str(i+1)] for i in range(j)], [23])
        query("SELECT id FROM ranks ORDER BY r NULLS LAST", [[str(i)] for i in range(1,9)], [23])
        query("SELECT id,r < 'alpha' FROM ranks ORDER BY r NULLS LAST",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
        query("SELECT id,r < 'alpha' FROM ranks ORDER BY r DESC NULLS FIRST",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(8,0,-1)], [23,16])
        query("SELECT id,r < 'alpha' FROM ranks ORDER BY CASE WHEN true THEN r ELSE NULL END NULLS LAST",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
    try:
        if reference:
            query("BEGIN")
            query('CREATE SCHEMA "' + schema + '"')
            query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
        query("CREATE TYPE rank_type AS ENUM (" + ",".join(map(quoted, labels)) + ")")
        query("CREATE TABLE ranks (id INT PRIMARY KEY, r rank_type)")
        query("INSERT INTO ranks VALUES " + ",".join("(" + str(i+1) + "," + quoted(label) + ")" for i, label in enumerate(labels)) + ",(8,NULL)")
        check_rank_matrix()
        query("SELECT id,r < NULL,r = NULL,r IS DISTINCT FROM NULL,r IS NOT DISTINCT FROM NULL FROM ranks ORDER BY id",
              [[str(i),None,None,"f" if i==8 else "t","t" if i==8 else "f"] for i in range(1,9)], [23,16,16,16,16])
        query("SELECT id,r IS DISTINCT FROM '',r IS NOT DISTINCT FROM '' FROM ranks ORDER BY id",
              [[str(i),"f" if i==2 else "t","t" if i==2 else "f"] for i in range(1,9)], [23,16,16])
        query("SELECT id,CASE WHEN r < 'alpha' THEN 1 ELSE 0 END,CASE r WHEN 'zeta' THEN 10 WHEN '' THEN 20 WHEN NULL THEN 99 ELSE 90 END FROM ranks ORDER BY id",
              [[str(i),"1" if i<3 else "0","10" if i==1 else "20" if i==2 else "90"] for i in range(1,9)], [23,23,23])
        query("SELECT id,(CASE WHEN id = 1 THEN r ELSE 'alpha' END) < 'aa' FROM ranks ORDER BY id",
              [[str(i),"t"] for i in range(1,9)], [23,16])
        query("WITH q AS (SELECT id,r FROM ranks) SELECT id,r < 'alpha' FROM q ORDER BY id",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
        query("WITH q AS (SELECT id,r FROM ranks) SELECT id,r < 'alpha' FROM q ORDER BY r NULLS LAST",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
        query("WITH q AS (SELECT id,CASE WHEN id = 1 THEN r ELSE 'alpha' END AS r FROM ranks) SELECT id,r < 'aa' FROM q ORDER BY id",
              [[str(i),"t"] for i in range(1,9)], [23,16])
        query("SELECT 'zeta'::rank_type < 'alpha'::rank_type,CAST('' AS rank_type) < 'alpha',NULL::rank_type < 'alpha',CASE 'zeta'::rank_type WHEN 'zeta' THEN 7 ELSE 8 END",
              [["t","t",None,"7"]], [16,16,16,23])
        query("SELECT CASE 'zeta'::rank_type WHEN 'alpha' THEN 7 WHEN 'zeta' THEN 8 ELSE 9 END", [["8"]], [23])
        query("SELECT CASE NULL::rank_type WHEN NULL THEN 7 WHEN '' THEN 8 ELSE 9 END", [["9"]], [23])
        query("SELECT 'zeta' < 'alpha',CASE 'zeta' WHEN 'zeta' THEN 1 ELSE 0 END", [["f","1"]], [16,23])
        query('CREATE TYPE "MixedRank" AS ENUM (\'zeta\',\'\',\'alpha\')')
        query('SELECT \'zeta\'::"MixedRank" < \'alpha\',CASE \'\'::"MixedRank" WHEN \'\' THEN 1 ELSE 2 END', [["t","1"]], [16,23])
        query('SELECT (CASE WHEN true THEN \'zeta\'::"MixedRank" ELSE \'alpha\' END) < \'alpha\'', [["t"]], [16])
        other_schema=schema+"_other"
        query('CREATE SCHEMA "'+other_schema+'"')
        query('CREATE TYPE "'+other_schema+'".rank_type AS ENUM (\'alpha\',\'\',\'zeta\',\'aa\',\'NULL\',\'it\'\'s\',\'longlonglong\')')
        query(('SET LOCAL' if reference else 'SET')+' search_path TO "'+other_schema+'",pg_catalog')
        source_schema=schema if reference else "public"
        source='"'+source_schema+'".ranks'
        # The physical source OID, not today's search_path rank_type, owns
        # direct, CASE-derived and CTE-projected comparisons.
        query("SELECT id,r < 'alpha' FROM "+source+" ORDER BY id",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
        query("SELECT id,(CASE WHEN id = 1 THEN r ELSE 'alpha' END) < 'aa' FROM "+source+" ORDER BY id",
              [[str(i),"t"] for i in range(1,9)], [23,16])
        query("WITH q AS (SELECT id,CASE WHEN id = 1 THEN r ELSE 'alpha' END AS r FROM "+source+") SELECT id,r < 'aa' FROM q ORDER BY id",
              [[str(i),"t"] for i in range(1,9)], [23,16])
        query("SELECT (SELECT r FROM "+source+" WHERE id = 1) < 'alpha'", [["t"]], [16])
        query("SELECT 'zeta'::rank_type < 'alpha'::rank_type", [["f"]], [16])
        query("SELECT 'zeta'::\""+source_schema+"\".rank_type < 'alpha'::\""+source_schema+"\".rank_type", [["t"]], [16])
        query(('SET LOCAL' if reference else 'SET')+' search_path TO "'+source_schema+'",pg_catalog')
        query("CREATE INDEX ranks_cmp ON ranks(r)")
        check_rank_matrix()
        query("CREATE TYPE other_rank AS ENUM (" + ",".join(map(quoted, labels)) + ")")
        # Exact ordinary operator/input SQLSTATEs, including empty source and
        # zero execution demand. Errors never leave the reference txn aborted.
        for sql, state in [
            ("SELECT r < 'absent' FROM ranks WHERE false", "22P02"),
            ("SELECT r = 'absent' FROM ranks LIMIT 0", "22P02"),
            ("SELECT 'absent'::rank_type = 'zeta'::rank_type", "22P02"),
            ("SELECT CAST('absent' AS rank_type) < 'alpha'", "22P02"),
            ("SELECT r < 'alpha'::text FROM ranks", "42883"),
            ("SELECT 'zeta'::rank_type = 'zeta'::other_rank", "42883"),
            ("SELECT NULL::rank_type = NULL::text", "42883"),
            ("SELECT CASE r WHEN 'absent' THEN 1 ELSE 0 END FROM ranks WHERE false", "22P02"),
            ("SELECT CASE WHEN false THEN 'absent' ELSE r END FROM ranks LIMIT 0", "22P02"),
            ("SELECT CASE WHEN true THEN 'zeta'::rank_type ELSE 'zeta'::other_rank END", "42846"),
        ]:
            if reference:
                query("SAVEPOINT enum_error")
            query(sql, state=state)
            if reference:
                query("ROLLBACK TO enum_error")
                query("RELEASE enum_error")
        query("SELECT id,r < 'alpha' FROM ranks ORDER BY id",
              [[str(i),None if i==8 else "t" if i<3 else "f"] for i in range(1,9)], [23,16])
        if not reference:
            sock.close(); server["sock"]=None
            server["process"].terminate(); server["process"].wait(timeout=10)
            server["process"]=subprocess.Popen(
                [runner.DBMS_MAIN,"--data-dir",server["dir"],"--server",str(server["port"]),"--insecure"],
                cwd=server["dir"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            sock=socket.socket(); sock.settimeout(runner.wire_timeout())
            deadline=time.time()+20
            while True:
                try:
                    sock.connect(("127.0.0.1",server["port"])); break
                except OSError:
                    if time.time()>=deadline: raise
                    time.sleep(0.05)
            server["sock"]=sock; client.startup(sock,"alice","info")
            check_rank_matrix()
            query("SELECT id,CASE r WHEN 'zeta' THEN 10 WHEN '' THEN 20 ELSE 90 END FROM ranks ORDER BY id",
                  [[str(i),"10" if i==1 else "20" if i==2 else "90"] for i in range(1,9)], [23,23])
        print("[enum comparison] full seven-operator/label/NULL/CASE/CTE/no-FROM and exact error matrix passed", flush=True)
    finally:
        if reference:
            try:
                client.simple_query(sock,"ROLLBACK")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
