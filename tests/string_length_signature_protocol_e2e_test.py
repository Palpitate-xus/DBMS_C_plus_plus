#!/usr/bin/env python3
"""Length overloads bind before demand without changing text/byte/bit units."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("length_signature_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]
    schema = "length_signature_" + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False
    names = ("char_length", "character_length", "octet_length", "bit_length")

    def record(label, good, result):
        nonlocal checks
        checks += 1
        if not good:
            failures.append((label, result))
            print("STRING_LENGTH_SIGNATURE_FAILURE", label, repr(result), flush=True)
        return good

    def query(sql, rows=None, state=None, oids=None):
        messages = c.simple_query(sock, sql)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] == state and (rows is None or result[0] == rows)
        good = good and (oids is None or result[5] == oids) and messages[-1] == (b"Z", b"I")
        if state:
            good = good and not result[0] and result[4] is None
            good = good and sum(tag == b"E" for tag, _ in messages) == 1
            good = good and not any(tag in (b"T", b"D", b"C") for tag, _ in messages)
        return record(sql, good, result)

    def describe(sql, input_oid, state=None):
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, input_oid)
        sock.sendall(c.typed(b"P", parse) + c.typed(b"D", b"S\0") + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] == state and not result[0] and result[4] is None
        good = good and sum(tag == b"1" for tag, _ in messages) == (0 if state else 1)
        good = good and not any(tag in (b"D", b"C") for tag, _ in messages)
        if state:
            good = good and not any(tag == b"T" for tag, _ in messages)
        else:
            good = good and result[5] == [23]
        record("Parse/Describe " + str(input_oid) + " " + sql, good, result)

    try:
        assert query("CREATE SCHEMA " + schema)
        created = True
        assert query("CREATE TABLE " + schema + ".empty_rows(i INTEGER,t TEXT)")
        suffixes = ("", " FROM " + schema + ".empty_rows WHERE false", " FROM " + schema + ".empty_rows LIMIT 0")
        for name in names:
            invalid = ["", "'a','b'", "1", "NULL::integer", "true", "1.25::numeric", "NULL::uuid", "ARRAY[1]", "'2024-01-01'::date"]
            if name in names[:2]:
                invalid.extend(("B'101'", "'abc'::bytea", "NULL::bit varying"))
            for argument in invalid:
                expression = name + "(" + argument + ")"
                for suffix in suffixes:
                    query("SELECT " + expression + suffix, state="42883")
                    query("SELECT " + expression + " OVER()" + suffix, state="42883")
                query("SELECT CASE WHEN false THEN " + expression + " ELSE 0 END", state="42883")
                query("SELECT COALESCE(1," + expression + ")", state="42883")
            for suffix in suffixes[1:]:
                query("SELECT " + name + "(i)" + suffix, state="42883")
            query("SELECT pg_catalog." + name + "(1)", state="42883")
            query('SELECT "pg_catalog"."' + name + '"(1)', state="42883")
            query('SELECT pg_catalog."' + name.upper() + '"(\'abc\')', state="42883")
            query("SELECT " + name + "(missing => 'abc')", state="42883")
            query("SELECT " + name + "(missing) FROM " + schema + ".empty_rows LIMIT 0", state="42703")
            character = name in names[:2]
            positive = (
                ("'你好é'", "3" if character else "8" if name == "octet_length" else "64"),
                ("'é  '::text", "3" if character else "4" if name == "octet_length" else "32"),
                ("'é  '::varchar", "3" if character else "4" if name == "octet_length" else "32"),
                ("'é  '::character(3)", "1" if character else "4" if name == "octet_length" else "16"),
                ("'é  '::name", "3" if character else "4" if name == "octet_length" else "32"),
                ("''", "0"), ("NULL", None), ("NULL::text", None), ("NULL::character(4)", None))
            if not character:
                positive += (("B'101010101'", "2" if name == "octet_length" else "9"),
                             ("B''", "0"), ("'\\x610062'::bytea", "3" if name == "octet_length" else "24"),
                             ("NULL::bytea", None), ("NULL::bit varying", None))
            for argument, value in positive:
                expression = name + "(" + argument + ")"
                for suffix in suffixes:
                    query("SELECT " + expression + suffix, rows=[] if suffix else [[value]], oids=[23])
                query("SELECT " + expression + " OVER()", state="42809")
            query("SELECT pg_catalog." + name + "('你好é')", rows=[["3" if character else "8" if name == "octet_length" else "64"]], oids=[23])
            for input_oid in (0, 25, 19):
                describe("SELECT pg_catalog." + name + "($1)", input_oid)
            for input_oid in (23, 16, 1700):
                describe("SELECT pg_catalog." + name + "($1)", input_oid, "42883")
            for input_oid in (17, 1560):
                describe("SELECT pg_catalog." + name + "($1)", input_oid, "42883" if character else None)
            query("PREPARE length_bad(INTEGER) AS SELECT pg_catalog." + name + "($1)", state="42883")
            assert query("PREPARE length_good(TEXT) AS SELECT pg_catalog." + name + "($1)")
            query("EXECUTE length_good('你好é')", rows=[["3" if character else "8" if name == "octet_length" else "64"]], oids=[23])
            query("DEALLOCATE length_good")
        for name in names:
            assert query("CREATE FUNCTION " + schema + "." + name + "(i INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT i+100$$")
        assert query("SET search_path=pg_catalog," + schema)
        for name in names:
            query("SELECT " + name + "(1)", rows=[["101"]], oids=[23])
            query("SELECT " + schema + "." + name + "(1)", rows=[["101"]], oids=[23])
            query("SELECT pg_catalog." + name + "(1)", state="42883")
            query("SELECT " + name + "(NULL::integer)", rows=[[None]], oids=[23])
            query("SELECT " + name + "('abc')", rows=[["24" if name == "bit_length" else "3"]], oids=[23])
            query("SELECT " + name + "(1) OVER()", state="42809")
            source = " FROM (SELECT 1::integer AS i,'abc'::text AS t)s"
            query("SELECT " + name + "(i)" + source, rows=[["101"]], oids=[23])
            query("SELECT abs(" + name + "(i))" + source, rows=[["101"]], oids=[23])
            query("SELECT " + name + "(i)" + source + " LIMIT 0", rows=[], oids=[23])
            query("SELECT " + name + "(t)" + source, rows=[["24" if name == "bit_length" else "3"]], oids=[23])
        assert query("CREATE SEQUENCE " + schema + ".effects")
        assert query("CREATE FUNCTION " + schema + ".effect() RETURNS TEXT LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('" + schema + ".effects'); RETURN 'abc'; END;$$")
        for name in names:
            query("SELECT pg_catalog." + name + "(" + schema + ".effect()) FROM " + schema + ".empty_rows LIMIT 0", rows=[], oids=[23])
        query("SELECT nextval('" + schema + ".effects')", rows=[["1"]], oids=[20])
        print("STRING_LENGTH_SIGNATURE_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 654 and not failures, failures
    finally:
        if created:
            c.simple_query(sock, "RESET search_path")
            c.simple_query(sock, "DROP SCHEMA " + schema + " CASCADE")
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
