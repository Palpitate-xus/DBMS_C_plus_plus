#!/usr/bin/env python3
"""DISCARD TEMP is transactional and preserves unrelated session state."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "discard_temp_pgdiff", root / "tests/compat/pg_diff_runner.py")
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
    table = "discard_tmp_" + uuid.uuid4().hex[:12]
    sequence = table + "_id_seq"
    oncommit = table + "_commit"

    def query(sql, extended=False, state=None, ready=b"I", rows=None,
              headers=None, oids=None, tag=None):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        for expected, actual in ((rows, result[0]), (headers, result[3]),
                                 (oids, result[5]), (tag, result[4])):
            if expected is not None:
                assert actual == expected, (sql, result, expected)

    try:
        query("DISCARD TEMP;", tag="DISCARD TEMP")
        query(f"DROP INDEX pg_temp.{table}_missing;", state="3F000")
        for extended in (False, True):
            query(f"CREATE TEMP TABLE {table}(id SERIAL,v TEXT);", extended)
            query(f"INSERT INTO {table}(v) VALUES('kept') RETURNING id;", extended,
                  rows=[["1"]], headers=["id"], oids=[23])
            query("PREPARE discard_temp_keep AS SELECT 11;", extended)
            for invalid in ("DISCARD TEMP extra;", "DISCARD 'temp';",
                            'DISCARD "temp";', "DISCARD;",
                            "DISCARD ALL extra;", "DISCARD SEQUENCES extra;"):
                query(invalid, extended, "42601")
            query(f"SELECT id FROM {table};", extended, rows=[["1"]])
            query("BEGIN;", extended, ready=b"T")
            query("DISCARD TEMPORARY;", extended, ready=b"T", tag="DISCARD TEMP")
            query(f"SELECT id FROM {table};", extended, "42P01", b"E")
            query("ROLLBACK;", extended)
            query(f"SELECT id,v FROM {table};", extended,
                  rows=[["1", "kept"]], headers=["id", "v"], oids=[23, 25])
            query(f"SELECT currval('{sequence}');", extended, rows=[["1"]])
            query(f"SELECT nextval('{sequence}');", extended, rows=[["2"]])
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT before_discard;", extended, ready=b"T")
            query("DISCARD TEMP;", extended, ready=b"T", tag="DISCARD TEMP")
            query(f"SELECT id FROM {table};", extended, "42P01", b"E")
            query("ROLLBACK TO before_discard;", extended, ready=b"T")
            query(f"SELECT id,v FROM {table};", extended, ready=b"T",
                  rows=[["1", "kept"]], headers=["id", "v"], oids=[23, 25])
            query("RELEASE before_discard;", extended, ready=b"T")
            query("COMMIT;", extended)
            query("BEGIN READ ONLY;", extended, ready=b"T")
            query("DISCARD TEMP;", extended, ready=b"T", tag="DISCARD TEMP")
            query("ROLLBACK;", extended)
            query(f"SELECT id,v FROM {table};", extended, rows=[["1", "kept"]])
            query("BEGIN;", extended, ready=b"T")
            query(f"CREATE TEMP TABLE {oncommit}(id INT) ON COMMIT DROP;", extended,
                  ready=b"T")
            query(f"INSERT INTO {oncommit} VALUES(7);", extended, ready=b"T")
            query("SAVEPOINT before_commit_discard;", extended, ready=b"T")
            query("DISCARD TEMP;", extended, ready=b"T", tag="DISCARD TEMP")
            query("ROLLBACK TO before_commit_discard;", extended, ready=b"T")
            query(f"SELECT id FROM {oncommit};", extended, ready=b"T", rows=[["7"]])
            query("COMMIT;", extended)
            query(f"SELECT id FROM {oncommit};", extended, "42P01")
            query("/* nested /* inner */ outer */ DISCARD\nTEMPORARY;", extended,
                  tag="DISCARD TEMP")
            query(f"SELECT id FROM {table};", extended, "42P01")
            query(f"SELECT nextval('{sequence}');", extended, "42P01")
            query(f"DROP INDEX pg_temp.{table}_missing;", extended, "42704")
            query("EXECUTE discard_temp_keep;", extended, rows=[["11"]])
            query("DEALLOCATE discard_temp_keep;", extended)
            query("DISCARD TEMP;", extended, tag="DISCARD TEMP")
        print("[DISCARD TEMP " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") +
              "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
