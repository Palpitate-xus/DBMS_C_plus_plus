#!/usr/bin/env python3
"""Retain the separate CREATE FUNCTION array-parameter gap without masking it."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("array_signature_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server["sock"]
    try:
        client.simple_query(sock, "BEGIN;")
        sql = 'CREATE FUNCTION "Unnest"(p INT[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 77; END$$;'
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("ARRAY SIGNATURE", sql, result, flush=True)
        assert result[1] is None, result
        result = runner.decode_wire_result(client.simple_query(sock, 'SELECT "Unnest"(ARRAY[1,2]);'), include_types=True)
        print("ARRAY SIGNATURE CALL", result, flush=True)
        assert result[1] is None and result[0] == [["77"]] and result[5] == [23], result
    finally:
        try: client.simple_query(sock, "ROLLBACK;")
        finally:
            if server: runner.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
