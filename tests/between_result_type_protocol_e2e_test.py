#!/usr/bin/env python3
"""A bound's CAST cannot determine a BETWEEN predicate's wire descriptor."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_result_type_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    schema = "owned_between_type_" + uuid.uuid4().hex[:12]; created = False; failures = []; controls = 0
    def ok(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result)
    def check(expression, value, oid, width):
        nonlocal controls
        controls += 1; sql = "SELECT " + expression + " AS value"; messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True); fields = client.row_description_fields(messages)
        if result[:2] != ([[value]], None) or result[3:] != (["value"], "SELECT 1", [oid]) or len(fields) != 1 or fields[0][3:] != (oid, width, -1, 0):
            failures.append((sql, result, fields, value, oid, width)); print("[BETWEEN RESULT TYPE FAIL] " + str(failures[-1]), flush=True)
    try:
        for kind in ("smallint", "integer", "bigint", "text", "varbit", "bit"):
            for operation in (" BETWEEN ", " NOT BETWEEN "):
                for prefix in ("NULL", "'1'"): check(prefix + operation + "NULL AND NULL::" + kind, None, 16, 1)
        check("1::smallint", "1", 21, 2); check("1::bigint", "1", 20, 8)
        check("(1 BETWEEN 0 AND 2)::text", "true", 25, -1)
        check("(1 NOT BETWEEN 0 AND 2)::integer", "0", 23, 4)
        ok("CREATE SCHEMA " + schema); created = True
        for name in ("between", "not between"):
            ok("CREATE FUNCTION " + schema + '."' + name + '"(a integer,b integer,c integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$')
            for qualifier in (schema + ".", '"' + schema + '".'):
                check(qualifier + '"' + name + '"(1,2,3)', "1", 23, 4)
        assert controls == 32, controls
        assert not failures, "%d/%d complete actual result descriptors failed" % (len(failures), controls)
        print("[BETWEEN RESULT TYPE PROTOCOL] all %d Boolean/NULL/width/OID/outer cast/real routine descriptor controls passed" % controls)
    finally:
        if created: ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
