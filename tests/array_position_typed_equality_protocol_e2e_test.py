#!/usr/bin/env python3
"""Typed array search: equality, real input declarations and demand priority."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def array(values, kind, lower=1):
    fields = ["NULL" if value is None else '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"' for value in values]
    body = "{" + ",".join(fields) + "}"
    if lower != 1:
        body = "[" + str(lower) + ":" + str(lower + len(fields) - 1) + "]=" + body
    return literal(body) + "::" + kind + "[]"


def core_cases():
    """(expression, result or SQL NULL, SQLSTATE); no environment-owned objects."""
    cases = []
    def add(values, kind, needle, expected, lower=1, start=None):
        expression = "array_position(" + array(values, kind, lower) + "," + needle
        if start is not None:
            expression += "," + start
        cases.append((expression + ")", expected, None))
    for spelling in ("1", "1.0", "1.00", "1e0"):
        for start, expected in ((None, "1"), ("2", "3"), ("4", None)):
            add(["1.00", "2.00", "1.0"], "numeric", spelling + "::numeric", expected, start=start)
    add(["9007199254740992.00", "9007199254740993.00"], "numeric", "9007199254740993::numeric", "2")
    add(["NaN", "Infinity", "-Infinity", "0"], "numeric", "'NaN'::numeric", "1")
    add(["NaN", "Infinity", "-Infinity", "0"], "numeric", "'Infinity'::numeric", "2")
    add(["NaN", "Infinity", "-Infinity", "0"], "numeric", "'-Infinity'::numeric", "3")
    for kind in ("real", "double precision"):
        add(["-0", "1"], kind, "0::" + kind, "1")
        add(["NaN", "0"], kind, "'NaN'::" + kind, "1")
        add(["Infinity", "0"], kind, "'Infinity'::" + kind, "1")
    add(["1", "2"], "integer", "1.0::numeric", "1")
    add(["0.10000000149"], "numeric", "0.1::real", "1")
    add(["a  ", "b"], "character(3)", "'a'::character(1)", "1")
    add(["é  ", "e"], "character(3)", "'é'::character(1)", "1")
    add(["a  "], "character(3)", "'a  '::text", "1")
    add(["a  ", "a"], "text", "'a'::text", "2")
    add(["01", "1", "1.0"], "text", "'1'::text", "2")
    add(["NULL", None, ""], "text", "'NULL'::text", "1")
    add(["NULL", None, ""], "text", "NULL::text", "2")
    add(["NULL", None, ""], "text", "''::text", "3")
    add(["1 mon", "2 mons"], "interval", "'30 days'::interval", "1")
    add(["1 day", "2 days"], "interval", "'24 hours'::interval", "1")
    add(["12:00:00"], "time", "'12:00'::time", "1")
    add(["12:00:00+01"], "timetz", "'11:00:00+00'::timetz", None)
    add(["2024-01-01 00:00:00"], "timestamp", "'2024-01-01'::timestamp", "1")
    add(["2024-01-01 00:00:00+01"], "timestamptz", "'2023-12-31 23:00:00+00'::timestamptz", "1")
    add(["true", "false"], "boolean", "true", "1")
    add(["FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"], "uuid", "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid", "1")
    add(["\\x610062", "\\x61"], "bytea", "'\\x610062'::bytea", "1")
    add(["01", "1"], "bit varying", "B'1'", "2")
    add(["192.0.2.1/32"], "inet", "'192.0.2.1'::inet", "1")
    add(["192.0.2.0/24"], "cidr", "'192.0.2.0/24'::inet", "1")
    add(["08:00:2b:01:02:03"], "macaddr", "'08-00-2b-01-02-03'::macaddr", "1")
    add(["1.00", "2"], "jsonb", "'1'::jsonb", "1")
    add(["[1,2]", "[2,1]"], "jsonb", "'[1.0,2.00]'::jsonb", "1")
    add(["[1,2]", "[2,1]"], "jsonb", "'[2,1]'::jsonb", "2")
    add(['{"a":1,"b":2}'], "jsonb", "'{\"b\":2.00,\"a\":1.0}'::jsonb", "1")
    add(["[1.00,2.00)", "[2,3)"], "numrange", "'[1,2)'::numrange", "1")
    add(["[1,3]", "[4,8)"], "int4range", "'[1,4)'::int4range", "1")
    add(["a:1", "a:2"], "tsvector", "'a:2'::tsvector", "2")
    for lower in (-3, 0, 7):
        add(["1.00", None, "1.0"], "numeric", "1::numeric", str(lower), lower)
        add(["1.00", None, "1.0"], "numeric", "1::numeric", str(lower + 2), lower, str(lower + 1))
        add(["1.00", None, "1.0"], "numeric", "NULL::numeric", str(lower + 1), lower)
    cases += [("array_position(NULL::numeric[],1::numeric)", None, None),
              ("array_position('{}'::numeric[],NULL::numeric)", None, None),
              ("array_position('{}'::integer[],1,NULL::integer)", None, None),
              ("array_position('{1,2}'::integer[],NULL,NULL::integer)", None, None),
              ("array_position('{1,NULL}'::integer[],NULL,NULL::integer)", None, "22004"),
              ("array_position(ARRAY[ARRAY[1,2]],1)", None, "0A000"),
              ("array_position(NULL,NULL)", None, None),
              ("array_position('{}',NULL)", None, None),
              ("array_position('{1.00}'::numeric[],'1')", "1", None)]
    for kind, value in (("point", "(0,0)"), ("line", "{1,2,3}"), ("lseg", "[(0,0),(1,1)]"),
                        ("box", "(0,0),(1,1)"), ("path", "[(0,0),(1,1)]"),
                        ("polygon", "((0,0),(1,1),(0,2))"), ("circle", "<(0,0),1>"), ("json", "1")):
        source = array([value], kind)
        needle = literal(value) + "::" + kind
        for start in ("", ",2"):
            cases.append(("array_position(" + source + "," + needle + start + ")", None, "42883"))
        cases.append(("array_position(" + source + ",NULL::" + kind + ")", None, None))
        cases.append(("array_position(" + source + "," + needle + ",NULL::integer)", None, "22004"))
        cases.append(("array_position(" + source + ",NULL::" + kind + ",NULL::integer)", None, None))
        cases.append(("array_position(" + array([None], kind) + ",NULL::" + kind + ")", None, "42883"))
        cases.append(("array_position('{}'::" + kind + "[]," + needle + ")", None, None))
    return cases


def invalid_signatures():
    return ("array_position()", "array_position(ARRAY[1])", "array_position(ARRAY[1],1,1,1)",
            "array_position(1,1)", "array_position('{}'::text,1)",
            "array_position('{1}'::integer[],'1'::text)",
            "array_position('{1}'::integer[],1,'1'::text)",
            "array_position('{1}'::integer[],1,1::bigint)",
            "array_position('{1}'::integer[],1,1.0::numeric)",
            "array_position('{true}'::boolean[],'true'::text)")


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("array_position_typed_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r); c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h, p, u, d, pw = r._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=r.wire_timeout())
        c.startup_reference(sock, u, d, pw); r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server["sock"]
    schema = "array_eq_" + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(label, good, observed):
        nonlocal checks
        checks += 1
        if not good:
            failures.append((label, observed))
            print("ARRAY_POSITION_TYPED_FAILURE", label, repr(observed), flush=True)
        return good

    def query(sql, rows=None, state=None, oids=None):
        messages = c.simple_query(sock, sql)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] == state and (rows is None or result[0] == rows)
        good = good and (oids is None or result[5] == oids) and messages[-1] == (b"Z", b"I")
        if state:
            good = good and result[0] == [] and result[4] is None
            good = good and sum(tag == b"E" for tag, _ in messages) == 1
            good = good and not any(tag in (b"T", b"D", b"C") for tag, _ in messages)
        return check(sql, good, result)

    def parse(sql, input_oids, state=None, output_oid=23):
        packet = b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(input_oids))
        packet += b"".join(struct.pack("!I", oid) for oid in input_oids)
        sock.sendall(c.typed(b"P", packet) + c.typed(b"D", b"S\0") + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        result = r.decode_wire_result(messages, include_types=True)
        parsed = sum(tag == b"1" for tag, _ in messages)
        good = result[1] == state and not result[0] and result[4] is None and parsed == (0 if state else 1)
        good = good and (not any(tag == b"T" for tag, _ in messages) if state else result[5] == [output_oid])
        check("Parse/Describe " + str(input_oids) + " " + sql, good, (result, parsed, [tag for tag, _ in messages]))

    try:
        assert query("CREATE SCHEMA " + schema)
        created = True
        assert query("CREATE TABLE " + schema + ".empty_rows(i INTEGER)")
        suffixes = ("", " FROM " + schema + ".empty_rows WHERE false", " FROM " + schema + ".empty_rows LIMIT 0")
        for expression, expected, state in core_cases():
            query("SELECT " + expression + " AS pos", rows=None if state else [[expected]], state=state, oids=None if state else [23])
        for expression in invalid_signatures():
            for suffix in suffixes:
                query("SELECT " + expression + suffix, state="42883")
                query("SELECT " + expression + " OVER()" + suffix, state="42883")
            query("SELECT COALESCE(7," + expression + ")", state="42883")
        for expression in ("array_position('{1}'::integer[],'bad')", "array_position(NULL::integer[],'bad')",
                           "array_position('{}'::integer[],1,'bad')"):
            for suffix in suffixes:
                query("SELECT " + expression + suffix, state="22P02")
            query("SELECT COALESCE(7," + expression + ")", state="22P02")
        for kind, value in (("point", "(0,0)"), ("json", "1")):
            expression = "array_position(" + array([value], kind) + "," + literal(value) + "::" + kind + ")"
            for suffix in suffixes[1:]:
                query("SELECT " + expression + suffix, state="42883")
            query("SELECT COALESCE(7," + expression + ")", rows=[["7"]], oids=[23])
        for source, needle, state in ((1007, 23, None), (1007, 25, "42883"), (1231, 1700, None),
                                      (1009, 25, None), (1007, 16, "42883")):
            parse("SELECT pg_catalog.array_position($1,$2)", [source, needle], state)
        parse("SELECT array_position(ARRAY[1],1,$1)", [25], "42883")
        parse("SELECT array_position(ARRAY[1],1,$1)", [23])
        assert query("PREPARE array_eq_good(NUMERIC[],NUMERIC) AS SELECT array_position($1,$2)")
        query("EXECUTE array_eq_good('{1.00,2}',1)", rows=[["1"]], oids=[23])
        query("EXECUTE array_eq_good(NULL,1)", rows=[[None]], oids=[23])
        query("DEALLOCATE array_eq_good")
        query("PREPARE array_eq_bad(INTEGER[],TEXT) AS SELECT array_position($1,$2)", state="42883")
        assert query("CREATE TABLE " + schema + ".data_rows(id INTEGER PRIMARY KEY,a NUMERIC[],n NUMERIC)")
        assert query("INSERT INTO " + schema + ".data_rows VALUES(1,'{1.00,2.00}',1),(2,'[-2:-1]={1.00,2.00}',1),(3,NULL,1),(4,'{}',NULL)")
        query("SELECT id,array_position(a,n) FROM " + schema + ".data_rows ORDER BY id", rows=[["1", "1"], ["2", "-2"], ["3", None], ["4", None]], oids=[23, 23])
        query("UPDATE " + schema + ".data_rows SET a='{0.0100,0.01000}' WHERE id=2 RETURNING array_position(a,0.01::numeric)", rows=[["1"]], oids=[23])
        assert query("CREATE SEQUENCE " + schema + ".effects")
        assert query("CREATE FUNCTION " + schema + ".effect() RETURNS NUMERIC LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('" + schema + ".effects'); RETURN 1; END;$$")
        query("SELECT array_position('{1.00}'::numeric[]," + schema + ".effect()) FROM " + schema + ".empty_rows LIMIT 0", rows=[], oids=[23])
        query("SELECT COALESCE(7,array_position('{1.00}'::numeric[]," + schema + ".effect()))", rows=[["7"]], oids=[23])
        query("SELECT nextval('" + schema + ".effects')", rows=[["1"]], oids=[20])
        query("SELECT array_position('{1.00}'::numeric[]," + schema + ".effect())", rows=[["1"]], oids=[23])
        query("SELECT currval('" + schema + ".effects')", rows=[["2"]], oids=[20])
        # A concrete user declaration owns its exact call, not the builtin's
        # INTEGER result or planning behavior. Keep UNKNOWN ambiguity too.
        assert query("CREATE TABLE " + schema + ".owned_rows(a INTEGER[],n INTEGER)")
        assert query("INSERT INTO " + schema + ".owned_rows VALUES('{1}',1),(NULL,1)")
        assert query("CREATE FUNCTION " + schema + ".array_position(p INTEGER[],q INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$BEGIN RETURN 'user-owned';END;$$")
        assert query("SET search_path TO " + schema + ",pg_catalog")
        query("SELECT array_position('{1}'::integer[],1)", rows=[["user-owned"]], oids=[25])
        query("SELECT pg_catalog.array_position('{1}'::integer[],1)", rows=[["1"]], oids=[23])
        query("SELECT array_position('{1.00}'::numeric[],1::numeric)", rows=[["1"]], oids=[23])
        query("SELECT array_position('{1}',1)", rows=[["user-owned"]], oids=[25])
        query("SELECT array_position(NULL,NULL)", state="42725")
        query("SELECT array_position(NULL::integer[],1)", rows=[["user-owned"]], oids=[25])
        query("SELECT array_position(a,n),pg_catalog.array_position(a,n) FROM owned_rows ORDER BY n", rows=[["user-owned", "1"], ["user-owned", None]], oids=[25, 23])
        query("SELECT array_position(a,n) FROM owned_rows WHERE false", rows=[], oids=[25])
        query("SELECT COALESCE('ok',array_position('{1}'::integer[],1))", rows=[["ok"]], oids=[25])
        query("SELECT array_position('{1}'::integer[],1) OVER()", state="42809")
        for suffix in suffixes[1:]:
            query("SELECT array_position(NULL,NULL)" + suffix, state="42725")
        parse("SELECT array_position($1,$2)", [1007, 23], output_oid=25)
        parse("SELECT array_position($1,$2)", [0, 0], "42725")
        query("SET search_path TO public")
        print("ARRAY_POSITION_TYPED_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 258 and not failures, failures
    finally:
        if created:
            c.simple_query(sock, "DROP SCHEMA " + schema + " CASCADE")
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
