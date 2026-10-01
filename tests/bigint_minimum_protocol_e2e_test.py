#!/usr/bin/env python3
"""INT64_MIN remains a non-NULL datum through DML, indexes and COPY."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "bigint_min_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "bigint_min_" + uuid.uuid4().hex
    created = False
    minimum, maximum = "-9223372036854775808", "9223372036854775807"

    def query(sql, rows=None, ready=b"I"):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] is None, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY,v BIGINT);")
        created = True
        query(f"INSERT INTO {table} VALUES(1,{minimum}),(2,{maximum}),(3,NULL);")
        query(f"CREATE INDEX {table}_v_idx ON {table}(v);")
        query(f"SELECT id,v FROM {table} ORDER BY id;", [["1", minimum], ["2", maximum], ["3", None]])
        query(f"SELECT id FROM {table} WHERE v={minimum};", [["1"]])
        query(f"SELECT id FROM {table} WHERE v BETWEEN {minimum} AND -1;", [["1"]])
        query(f"SELECT id FROM {table} WHERE v IS NULL;", [["3"]])
        query(f"SELECT MIN(v),MAX(v) FROM {table};", [[minimum, maximum]])
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT restore;", ready=b"T")
        query(f"UPDATE {table} SET v={minimum} WHERE id=2 RETURNING id,v;", [["2", minimum]], b"T")
        query(f"SELECT id FROM {table} WHERE v={minimum} ORDER BY id;", [["1"], ["2"]], b"T")
        query(f"UPDATE {table} SET v={minimum} WHERE v={minimum} RETURNING id,v;",
              [["1", minimum], ["2", minimum]], b"T")
        query(f"UPDATE {table} SET v={minimum} WHERE id=3 RETURNING id,v;", [["3", minimum]], b"T")
        query(f"SELECT id FROM {table} WHERE v={minimum} ORDER BY id;", [["1"], ["2"], ["3"]], b"T")
        query(f"UPDATE {table} SET v=NULL WHERE id=3 RETURNING id,v;", [["3", None]], b"T")
        query(f"SELECT id FROM {table} WHERE v IS NULL;", [["3"]], b"T")
        query("ROLLBACK TO SAVEPOINT restore;", ready=b"T")
        query(f"SELECT id,v FROM {table} ORDER BY id;", [["1", minimum], ["2", maximum], ["3", None]], b"T")
        query("COMMIT;")
        query(f"DELETE FROM {table} WHERE v={minimum} RETURNING id,v;", [["1", minimum]])
        sql = f"COPY {table}(id,v) FROM STDIN;".encode()
        sock.sendall(client.typed(b"Q", sql + b"\0"))
        assert client.read_message(sock) == (b"G", b"\0" + struct.pack("!H", 2) + b"\0\0" * 2)
        sock.sendall(client.typed(b"d", ("4\t" + minimum + "\n").encode()) + client.typed(b"c", b""))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages)
        assert result[1] is None and result[4] == "COPY 1", result
        assert messages[-1] == (b"Z", b"I"), messages[-1]
        query(f"SELECT id,v FROM {table} ORDER BY id;", [["2", maximum], ["3", None], ["4", minimum]])
        query(f"SELECT id FROM {table} WHERE v={minimum};", [["4"]])
        query(f"DROP TABLE {table};")
        created = False
        print("[BIGINT MINIMUM " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
