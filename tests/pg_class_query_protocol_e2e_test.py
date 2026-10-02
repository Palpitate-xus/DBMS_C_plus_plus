#!/usr/bin/env python3
"""pg_class query clauses use typed rows instead of dumping the whole catalog."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "catalog_query_runner", root / "tests/compat/pg_diff_runner.py")
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
    prefix = "cq_" + uuid.uuid4().hex[:12]
    one, two = prefix + "_a", prefix + "_b"

    def query(sql, extended=False, rows=None, headers=None, types=None,
              state=None, ready=b"I"):
        if extended:
            parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
            if headers is not None:
                sock.sendall(parse + client.typed(b"D", b"S\0") + client.typed(b"H"))
                metadata = [client.read_message(sock) for _ in range(3)]
                assert [kind for kind, _ in metadata] == [b"1", b"t", b"T"], (sql, metadata)
                described = runner.decode_wire_result(metadata, include_types=True)
                assert (described[0], described[1], described[3], described[4], described[5]) == (
                    [], None, headers, None, types), (sql, described, headers, types)
                parse = b""
            sock.sendall(parse + client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert (result[0], result[4]) == (rows, "SELECT " + str(len(rows))), (sql, result, rows)
        if headers is not None:
            assert (result[3], result[5]) == (headers, types), (sql, result, headers, types)

    try:
        query(f"CREATE TABLE {one}(id INT);")
        query(f"CREATE TABLE {two}(id INT,v TEXT);")
        for extended in (False, True):
            query(f"SELECT count(*) FROM pg_class WHERE relname='{one}';", extended,
                  [["1"]], ["count"], [20])
            query(f"SELECT count(*) FROM pg_class WHERE relname='{one}' AND FALSE;", extended,
                  [["0"]], ["count"], [20])
            query(f"SELECT count(NULL),count(relname) FROM pg_class WHERE relname='{one}';", extended,
                  [["0", "1"]], ["count", "count"], [20, 20])
            query(f"SELECT relname,relkind,relpersistence,relnatts FROM pg_class WHERE relname='{one}';", extended,
                  [[one, "r", "p", "1"]], ["relname", "relkind", "relpersistence", "relnatts"],
                  [19, 18, 18, 21])
            query(f'SELECT relname::text AS "Label Name",relkind::text,relpersistence::text '
                  f"FROM pg_catalog.pg_class WHERE relname='{one}';", extended,
                  [[one, "r", "p"]], ["Label Name", "relkind", "relpersistence"], [25, 25, 25])
            query(f"SELECT c.relname::text FROM pg_class AS c WHERE c.relname='{one}';", extended,
                  [[one]], ["relname"], [25])
            query(f"/* FROM SELECT */ SELECT relname::text FROM pg_class WHERE relname='{one}';", extended,
                  [[one]], ["relname"], [25])
            query(f"SELECT relname::text FROM pg_class WHERE relname='{one}' OR relname='{two}' "
                  "ORDER BY relname DESC LIMIT 1 OFFSET 1;", extended,
                  [[one]], ["relname"], [25])
            query(f"SELECT relname::text FROM pg_class WHERE relname='{one}' OR relname='{two}' "
                  "ORDER BY 1 DESC LIMIT 1;", extended, [[two]], ["relname"], [25])
            query(f"SELECT relname::text FROM pg_class WHERE relname='{one}' "
                  "ORDER BY 0;", extended, state="42P10")
            query(f"SELECT relname::text,nullif(relnatts,1) AS n FROM pg_class "
                  f"WHERE relname='{one}' OR relname='{two}' ORDER BY n DESC;", extended,
                  [[one, None], [two, "2"]], ["relname", "n"], [25, 21])
            query(f"SELECT relname::text,nullif(relnatts,1) AS n FROM pg_class "
                  f"WHERE relname='{one}' OR relname='{two}' ORDER BY n DESC NULLS LAST;", extended,
                  [[two, "2"], [one, None]], ["relname", "n"], [25, 21])
            query(f"SELECT relname::text FROM pg_class WHERE relname='{one}' AND NULL;", extended,
                  [], ["relname"], [25])
            query(f"SELECT NULL::text AS absent FROM pg_class WHERE relname='{one}';", extended,
                  [[None]], ["absent"], [25])
            query(f"SELECT missing_catalog_column FROM pg_class WHERE FALSE;", extended, state="42703")
            query(f"SELECT wrong.relname FROM pg_class WHERE FALSE;", extended, state="42P01")
            query(f"SELECT relname FROM pg_class WHERE relname;", extended, state="42804")
            query(f"SELECT relname FROM pg_class WHERE NULL::text;", extended, state="42804")
            query(f"SELECT relname::text FROM pg_class WHERE NULL;", extended, [], ["relname"], [25])
            query("BEGIN READ ONLY;", extended, ready=b"T")
            query(f"SELECT count(*) FROM pg_class WHERE relname='{one}';", extended,
                  [["1"]], ["count"], [20], ready=b"T")
            query("ROLLBACK;", extended)
        print("[PG CLASS QUERY " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            query("ROLLBACK;")
            query(f"DROP TABLE {one};")
            query(f"DROP TABLE {two};")
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == "__main__":
    main()
