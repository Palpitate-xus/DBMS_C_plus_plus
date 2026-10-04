#!/usr/bin/env python3
"""Typed pg_settings subset must honor projection and predicates."""

import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "settings_query_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql, extended=False, state=None, headers=None, types=None):
        if extended:
            parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
            sock.sendall(parse +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        if headers is not None:
            assert result[3] == headers and result[5] == types, (sql, result)
        return result

    try:
        for extended in (False, True):
            result = query(
                "SELECT name, setting, unit FROM pg_catalog.pg_settings "
                "WHERE name = 'work_mem'",
                extended, headers=["name", "setting", "unit"],
                types=[25, 25, 25])
            assert len(result[0]) == 1 and result[0][0][0] == "work_mem", result
            assert result[0][0][1] and result[0][0][2] == "kB", result

            result = query(
                "SELECT unit FROM pg_settings WHERE name = 'max_connections'",
                extended, headers=["unit"], types=[25])
            assert result[0] == [[None]], result

            result = query(
                "SELECT setting FROM pg_settings WHERE name = 'absent_setting'",
                extended, headers=["setting"], types=[25])
            assert result[0] == [], result

            query("SELECT * FROM pg_settings", extended, state="0A000")
            query("SELECT category FROM pg_catalog.pg_settings", extended,
                  state="0A000")
            query("SELECT not_a_pg_settings_column FROM pg_settings", extended,
                  state="42703")
            query("SELECT name FROM pg_settings WHERE name", extended,
                  state="42804")

        for sql in (
                "CREATE TABLE pg_settings (id INTEGER)",
                "INSERT INTO pg_settings VALUES (23)"):
            result = query(sql)
            assert result[1] is None, (sql, result)
        result = query("SELECT * FROM pg_settings", headers=["id"], types=[23])
        assert result[0] == [["23"]], result

        for sql in (
                "CREATE TABLE pg_settings_view_source (id INTEGER)",
                "INSERT INTO pg_settings_view_source VALUES (29)",
                "DROP TABLE pg_settings",
                "CREATE VIEW pg_settings AS "
                "SELECT id FROM pg_settings_view_source"):
            result = query(sql)
            assert result[1] is None, (sql, result)
        result = query("SELECT * FROM pg_settings", headers=["id"], types=[23])
        assert result[0] == [["29"]], result
    finally:
        runner.stop_ours(server)

    print("[PG SETTINGS QUERY PROTOCOL E2E] typed subset passed")


if __name__ == "__main__":
    main()
