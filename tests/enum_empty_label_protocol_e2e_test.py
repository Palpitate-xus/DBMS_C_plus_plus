#!/usr/bin/env python3
"""Lossless counted empty enum labels, independent from custom-OID wire gaps."""
import importlib.util
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("enum_empty_runner", repo / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    server = None
    schema = "enum_empty_" + uuid.uuid4().hex[:20]
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
        print("ENUM_EMPTY", sql, result, flush=True)
        assert result[1] == state, (sql, result, state)
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        if types is not None:
            assert result[5] == types, (sql, result, types)
        return result

    def check_values():
        query("SELECT id,r IS NULL AS missing,r = '' AS blank FROM ranks ORDER BY id",
              [["1","f","f"],["2","f","t"],["3","f","f"],["4","f","f"],
               ["5","t",None],["6","f","f"]], [23,16,16])
        query("SELECT id FROM ranks WHERE r = '' ORDER BY id", [["2"]], [23])
        query("SELECT id FROM ranks WHERE r IS NULL ORDER BY id", [["5"]], [23])
        query("SELECT id FROM ranks WHERE r = 'NULL' ORDER BY id", [["4"]], [23])
        query("SELECT id FROM only_blank WHERE r = '' ORDER BY id", [["1"]], [23])
        query("SELECT id,r IS NULL FROM only_blank ORDER BY id", [["1","f"],["2","t"]], [23,16])

    try:
        if reference:
            query("BEGIN")
            query('CREATE SCHEMA "' + schema + '"')
            query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
        query("CREATE TYPE rank_type AS ENUM ('zeta','','alpha','NULL','it''s')")
        query("CREATE TABLE ranks (id INT PRIMARY KEY, r rank_type)")
        query("INSERT INTO ranks VALUES (1,'zeta'),(2,''),(3,'alpha'),(4,'NULL'),(5,NULL),(6,'it''s')")
        query("CREATE TYPE blank_type AS ENUM ('')")
        query("CREATE TABLE only_blank (id INT PRIMARY KEY, r blank_type)")
        query("INSERT INTO only_blank VALUES (1,''),(2,NULL)")
        check_values()
        query("CREATE INDEX empty_rank_idx ON ranks(r)")
        check_values()
        query("SAVEPOINT empty_label_undo" if reference else "BEGIN")
        query("UPDATE ranks SET r = '' WHERE id = 1")
        query("SELECT id FROM ranks WHERE r = '' ORDER BY id", [["1"],["2"]], [23])
        query("ROLLBACK TO empty_label_undo" if reference else "ROLLBACK")
        if reference:
            query("RELEASE empty_label_undo")
        check_values()
        if not reference:
            # Stop only this test's server without deleting its data directory.
            # Reopen the same on-disk schema and indexes with a new process.
            sock.close()
            server["sock"] = None
            server["process"].terminate()
            server["process"].wait(timeout=10)
            server["process"] = subprocess.Popen(
                [runner.DBMS_MAIN,"--data-dir",server["dir"],"--server",str(server["port"]),"--insecure"],
                cwd=server["dir"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            sock = socket.socket()
            sock.settimeout(runner.wire_timeout())
            deadline = time.time() + 20
            while True:
                try:
                    sock.connect(("127.0.0.1",server["port"]))
                    break
                except OSError:
                    if time.time() >= deadline:
                        raise
                    time.sleep(0.05)
            server["sock"] = sock
            client.startup(sock,"alice","info")
            check_values()
        scope = "strict18 values/NULL/equality/index/parent-savepoint rollback" if reference else "values/NULL/equality/index/parent rollback/cold process"
        print("[ENUM EMPTY LABEL] " + scope + " passed", flush=True)
    finally:
        if reference:
            try:
                client.simple_query(sock,"ROLLBACK")
            finally:
                sock.close()
        elif server is not None:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
