#!/usr/bin/env python3
"""Named fixed-element array input without a typmod is not CHAR(1)/BIT(1)."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("named_array_input_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
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
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []

    def verify(sql, expected, oid, messages=None):
        nonlocal checks
        checks += 1
        if messages is None:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] is None and result[0] == [[expected]] and result[5] == [oid] and result[4] == "SELECT 1"
        print("UNCONSTRAINED_NAMED_ARRAY", checks, sql, "match", valid, "actual", result, flush=True)
        if not valid:
            failures.append((sql, expected, oid, result))

    try:
        for type_name in ["bpchar[]", "pg_catalog.bpchar[]", 'pg_catalog."bpchar"[]',
                          '"pg_catalog"."_bpchar"', "pg_catalog._bpchar"]:
            for value, expected in [("{abc,NULL,NULL}", "{abc,NULL,NULL}"),
                                    ('{"a ",NULL,"NULL",""}', '{"a ",NULL,"NULL",""}'),
                                    ('[-3:-2]={abcd,"ef "}', '[-3:-2]={abcd,"ef "}'),
                                    ("{{abcd,NULL},{efgh,ijkl}}", "{{abcd,NULL},{efgh,ijkl}}")]:
                verify("SELECT CAST('" + value + "' AS " + type_name + ") AS a", expected, 1014)
        for type_name in ["pg_catalog.bit[]", 'pg_catalog."bit"[]', '"pg_catalog"."_bit"', "pg_catalog._bit"]:
            for value, expected in [("{101,NULL,00}", "{101,NULL,00}"),
                                    ('{"",NULL,10101}', '{"",NULL,10101}'),
                                    ("[-2:-1]={101,0101}", "[-2:-1]={101,0101}"),
                                    ("{{101,NULL},{01,1111}}", "{{101,NULL},{01,1111}}")]:
                verify("SELECT CAST('" + value + "' AS " + type_name + ") AS a", expected, 1561)
        # Ordinary SQL keyword casts and explicit modifiers still supply their
        # own lengths. Named scalar input already had the unconstrained rule.
        for type_name, expected, oid in [("character[]", "{a,b}", 1014), ("char[]", "{a,b}", 1014),
                                         ("bpchar(2)[]", "{ab,bc}", 1014),
                                         ("pg_catalog.bpchar(3)[]", '{abc,"bc "}', 1014)]:
            verify("SELECT CAST('{abc,bc}' AS " + type_name + ") AS a", expected, oid)
        for type_name, expected in [("bit[]", "{1,0}"), ("bit(2)[]", "{10,01}"),
                                    ("pg_catalog.bit(3)[]", "{101,010}")]:
            verify("SELECT CAST('{101,01}' AS " + type_name + ") AS a", expected, 1561)
        verify("SELECT CAST('abcd' AS pg_catalog.bpchar) AS a", "abcd", 1042)
        verify("SELECT CAST('10101' AS pg_catalog.bit) AS a", "10101", 1560)
        for oid, values in [(1014, ["{abc,NULL}", '{"a ","NULL",""}', "[-2:-1]={abc,def}", "{{abc,def},{NULL,ghi}}", "{}", None]),
                            (1561, ["{101,NULL}", '{"",10101}', "[-2:-1]={101,01}", "{{101,01},{NULL,11}}", "{}", None])]:
            for value in values:
                parse = b"\0SELECT $1 AS a\0" + struct.pack("!HI", 1, oid)
                raw = None if value is None else value.encode()
                bind = b"\0\0" + struct.pack("!HH", 0, 1)
                bind += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"") + struct.pack("!H", 0)
                sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                             client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
                verify("wire Bind OID " + str(oid) + " " + repr(value), value, oid, client.read_until_ready(sock))
        assert checks == 57, checks
        assert not failures, ("unconstrained named array input failures", failures)
        print("[UNCONSTRAINED NAMED ARRAY INPUT] all 57 controls passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
