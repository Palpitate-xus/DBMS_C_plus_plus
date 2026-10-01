#!/usr/bin/env python3
"""Signed integer DML predicates retain typed RETURNING and exact row counts."""

import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "signed_dml_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    table = "signed_dml_" + uuid.uuid4().hex
    created = False

    def query(sql, rows=None, tag=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] is None, (sql, result)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)
        if tag is not None:
            assert result[4] == tag, (sql, result[4], tag)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY,v BIGINT,payload TEXT);")
        created = True
        for predicate, target in (("v=-1", "2"), ("v<-1", "1"),
                                  ("v>=+2", "4"), ("v>-1 AND v<+2", "3")):
            query(f"TRUNCATE {table};")
            query(f"INSERT INTO {table} VALUES(1,-2,'old'),(2,-1,'old'),"
                  "(3,0,'old'),(4,2,'old'),(5,NULL,NULL);")
            query(f"UPDATE {table} SET payload='changed' WHERE {predicate} RETURNING id;",
                  [[target]], "UPDATE 1")
            expected = [[str(i), "changed" if str(i) == target else "old"] for i in range(1, 5)]
            expected.append(["5", None])
            query(f"SELECT id,payload FROM {table} ORDER BY id;", expected)
            query(f"DELETE FROM {table} WHERE {predicate} RETURNING id,payload;",
                  [[target, "changed"]], "DELETE 1")
            query(f"SELECT id,payload FROM {table} ORDER BY id;",
                  [row for row in expected if row[0] != target])
            query(f"SELECT id FROM {table} WHERE v IS NULL;", [["5"]])
        query(f"DROP TABLE {table};")
        created = False
        print("[SIGNED DML PREDICATE " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK;")
                if created:
                    client.simple_query(sock, f"DROP TABLE {table};")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
