#!/usr/bin/env python3
"""ALTER failures retain storage SQLSTATE and the original allocation stream."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("seq_alter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=15)
        client.startup_reference(sock, user, database, password=password)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def query(sql, rows=None, state=None):
        actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("SEQUENCE ALTER STATE", sql, actual, flush=True)
        assert actual[1] == state, (sql, actual, state)
        if rows is not None:
            assert actual[0] == rows, (sql, actual, rows)
        return actual

    try:
        if reference:
            query("SHOW server_version_num;", [["180006"]])
        query("CREATE TEMP SEQUENCE descending_default INCREMENT -2 NO MINVALUE NO MAXVALUE;")
        query("SELECT nextval('descending_default');", [["-1"]])
        query("SELECT nextval('descending_default');", [["-3"]])
        # Retain the original native SQL as a negative control: INCREMENT
        # alone does not replace the existing descending MAXVALUE -1.
        query("ALTER SEQUENCE descending_default INCREMENT 2 START 1 RESTART;", [], "22023")
        query("SELECT nextval('descending_default');", [["-5"]])
        query("ALTER SEQUENCE descending_default INCREMENT 2 NO MINVALUE NO MAXVALUE START 1 RESTART;")
        query("SELECT nextval('descending_default');", [["1"]])
        query("SELECT nextval('descending_default');", [["3"]])

        query("CREATE TEMP SEQUENCE alter_state_bounds START 5 MINVALUE 1 MAXVALUE 10;")
        query("SELECT nextval('alter_state_bounds');", [["5"]])
        query("BEGIN;")
        query("SAVEPOINT q;")
        query("ALTER SEQUENCE alter_state_bounds MAXVALUE 4;", [], "22023")
        query("SELECT nextval('alter_state_bounds');", [], "25P02")
        query("ROLLBACK TO q;")
        query("SELECT nextval('alter_state_bounds');", [["6"]])
        query("ROLLBACK;")
        query("SELECT nextval('alter_state_bounds');", [["7"]])
        query("ALTER SEQUENCE alter_state_bounds INCREMENT 0;", [], "22023")
        query("ALTER SEQUENCE alter_state_bounds CACHE 0;", [], "22023")
        query("SELECT nextval('alter_state_bounds');", [["8"]])
        print("[SEQUENCE ALTER SQLSTATE PROTOCOL] passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK;")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
