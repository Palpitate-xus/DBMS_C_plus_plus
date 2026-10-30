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
    """All-match equality and NULL/empty/capability behavior; actual PG18 gold."""
    cases = []
    def add(values, kind, needle, expected, lower=1):
        cases.append(("array_positions(" + array(values, kind, lower) + "," + needle + ")", expected, None))
    for spelling in ("1", "1.0", "1.00", "1e0"):
        add(["1.00", "2.00", "1.0"], "numeric", spelling + "::numeric", "{1,3}")
    add(["9007199254740992.00", "9007199254740993.00", "9007199254740993.0"], "numeric", "9007199254740993::numeric", "{2,3}")
    for special in ("NaN", "Infinity", "-Infinity"):
        add([special, "0", special], "numeric", literal(special) + "::numeric", "{1,3}")
    for kind in ("real", "double precision"):
        add(["-0", "0", "1"], kind, "0::" + kind, "{1,2}")
        for special in ("NaN", "Infinity"):
            add([special, "0", special], kind, literal(special) + "::" + kind, "{1,3}")
    add(["1", "2", "1"], "integer", "1.0::numeric", "{1,3}")
    add(["0.10000000149", "0", "0.10000000149"], "numeric", "0.1::real", "{1,3}")
    add(["a  ", "b", "a"], "character(3)", "'a'::character(1)", "{1,3}")
    add(["é  ", "e", "é"], "character(3)", "'é'::character(1)", "{1,3}")
    add(["a  ", "a", "a  "], "text", "'a  '::text", "{1,3}")
    add(["01", "1", "1.0", "1"], "text", "'1'::text", "{2,4}")
    for needle, expected in (("'NULL'::text", "{1}"), ("NULL::text", "{2}"), ("''::text", "{3}")):
        add(["NULL", None, ""], "text", needle, expected)
    add(["1 mon", "30 days", "2 mons"], "interval", "'30 days'::interval", "{1,2}")
    add(["12:00:00", "13:00:00", "12:00"], "time", "'12:00'::time", "{1,3}")
    add(["12:00:00+01", "11:00:00+00", "12:00:00+01"], "timetz", "'11:00:00+00'::timetz", "{2}")
    add(["2024-01-01 00:00:00", "2024-02-01", "2024-01-01"], "timestamp", "'2024-01-01'::timestamp", "{1,3}")
    add(["2024-01-01 00:00:00+01", "2023-12-31 23:00:00+00"], "timestamptz", "'2023-12-31 23:00:00+00'::timestamptz", "{1,2}")
    add(["true", "false", "true"], "boolean", "true", "{1,3}")
    add(["FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", "ffffffff-ffff-ffff-ffff-ffffffffffff"], "uuid", "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid", "{1,2}")
    add(["\\x610062", "\\x61", "\\x610062"], "bytea", "'\\x610062'::bytea", "{1,3}")
    add(["01", "1", "1"], "bit varying", "B'1'", "{2,3}")
    add(["192.0.2.1/32", "192.0.2.2", "192.0.2.1"], "inet", "'192.0.2.1'::inet", "{1,3}")
    add(["192.0.2.0/24", "198.51.100.0/24", "192.0.2.0/24"], "cidr", "'192.0.2.0/24'::inet", "{1,3}")
    add(["08:00:2b:01:02:03", "08-00-2b-01-02-03"], "macaddr", "'08:00:2b:01:02:03'::macaddr", "{1,2}")
    add(["1.00", "2", "1"], "jsonb", "'1.0'::jsonb", "{1,3}")
    add(["[1,2]", "[2,1]", "[1.00,2.0]"], "jsonb", "'[1,2]'::jsonb", "{1,3}")
    add(['{"a":1,"b":2}', '{"a":2}', '{"b":2.00,"a":1.0}'], "jsonb", literal('{"a":1,"b":2}') + "::jsonb", "{1,3}")
    add(["true", "false", "true"], "jsonb", "'true'::jsonb", "{1,3}")
    add(["null", '"null"', "null"], "jsonb", "'null'::jsonb", "{1,3}")
    add(["[1.00,2.00)", "[2,3)", "[1,2)"], "numrange", "'[1,2)'::numrange", "{1,3}")
    add(["[1,3]", "[1,4)", "[5,6)"], "int4range", "'[1,4)'::int4range", "{1,2}")
    add(["empty", "[1,2)", "empty"], "numrange", "'empty'::numrange", "{1,3}")
    add(["a:1", "a:2", "a:1"], "tsvector", "'a:1'::tsvector", "{1,3}")
    for lower in (-3, 0, 7):
        add(["1.00", None, "1.0", "2", "1"], "numeric", "1::numeric", "{" + ",".join(str(lower + i) for i in (0, 2, 4)) + "}", lower)
        add(["1.00", None, "1.0"], "numeric", "NULL::numeric", "{" + str(lower + 1) + "}", lower)
        add(["1.00", None, "1.0"], "numeric", "99::numeric", "{}", lower)
    cases += [("array_positions(NULL::numeric[],1::numeric)", None, None),
              ("array_positions('{}'::numeric[],NULL::numeric)", "{}", None),
              ("array_positions('{1,2}'::integer[],NULL)", "{}", None),
              ("array_positions('{1,2}'::integer[],99)", "{}", None),
              ("array_positions(ARRAY[ARRAY[1,2]],1)", None, "0A000"),
              ("array_positions(NULL,NULL)", None, None),
              ("array_positions('{}',NULL)", "{}", None),
              ("array_positions('{1.00,1.0}'::numeric[],'1')", "{1,2}", None)]
    for kind, value in (("point", "(0,0)"), ("line", "{1,2,3}"), ("lseg", "[(0,0),(1,1)]"),
                        ("box", "(0,0),(1,1)"), ("path", "[(0,0),(1,1)]"),
                        ("polygon", "((0,0),(1,1),(0,2))"), ("circle", "<(0,0),1>"), ("json", "1")):
        source = array([value], kind)
        needle = literal(value) + "::" + kind
        cases.append(("array_positions(" + source + "," + needle + ")", None, "42883"))
        cases.append(("array_positions(" + source + ",NULL::" + kind + ")", "{}", None))
        cases.append(("array_positions(" + array([None], kind) + ",NULL::" + kind + ")", None, "42883"))
        cases.append(("array_positions('{}'::" + kind + "[]," + needle + ")", "{}", None))
        cases.append(("array_positions(NULL::" + kind + "[]," + needle + ")", None, None))
    return cases


def invalid_signatures():
    return ("array_positions()", "array_positions(ARRAY[1])", "array_positions(ARRAY[1],1,1)",
            "array_positions(1,1)", "array_positions('{}'::text,1)",
            "array_positions('{1}'::integer[],'1'::text)",
            "array_positions('{true}'::boolean[],'true'::text)",
            "array_positions('{a}'::text[],1)", "array_positions(ARRAY[1],ARRAY[1])")


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("array_positions_typed_runner", root / "tests/compat/pg_diff_runner.py")
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
    schema = "array_positions_" + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(label, good, observed):
        nonlocal checks
        checks += 1
        if not good:
            failures.append((label, observed))
            print("ARRAY_POSITIONS_TYPED_FAILURE", label, repr(observed), flush=True)
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

    def parse(sql, input_oids, state=None, output_oid=1007):
        packet = b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(input_oids))
        packet += b"".join(struct.pack("!I", oid) for oid in input_oids)
        sock.sendall(c.typed(b"P", packet) + c.typed(b"D", b"S\0") + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        result = r.decode_wire_result(messages, include_types=True)
        parsed = sum(tag == b"1" for tag, _ in messages)
        good = result[1] == state and not result[0] and result[4] is None and parsed == (0 if state else 1)
        good = good and (not any(tag == b"T" for tag, _ in messages) if state else result[5] == [output_oid])
        check("Parse/Describe " + str(input_oids) + " " + sql, good, (result, parsed, [tag for tag, _ in messages]))

    def bind_execute(source, needle, expected):
        # Actual typed statement parameters, not substituted SQL or the SQL
        # PREPARE command. Text array data retains its declared lower bound.
        packet = b"\0\0" + struct.pack("!HH", 0, 2)
        for value in (source, needle):
            if value is None:
                packet += struct.pack("!i", -1)
            else:
                raw = value.encode()
                packet += struct.pack("!i", len(raw)) + raw
        packet += struct.pack("!H", 0)
        sock.sendall(c.typed(b"B", packet) + c.typed(b"D", b"P\0") +
                     c.typed(b"E", b"\0" + struct.pack("!I", 0)) + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] is None and result[0] == [[expected]] and result[5] == [1007]
        good = good and result[4] == "SELECT 1" and sum(tag == b"2" for tag, _ in messages) == 1
        good = good and messages[-1] == (b"Z", b"I")
        check("Bind/Execute " + repr((source, needle)), good, (result, [tag for tag, _ in messages]))

    try:
        assert query("CREATE SCHEMA " + schema)
        created = True
        assert query("CREATE TABLE " + schema + ".empty_rows(i INTEGER)")
        suffixes = ("", " FROM " + schema + ".empty_rows WHERE false", " FROM " + schema + ".empty_rows LIMIT 0")
        for expression, expected, state in core_cases():
            query("SELECT " + expression + " AS pos", rows=None if state else [[expected]], state=state, oids=None if state else [1007])
        for expression in invalid_signatures():
            for suffix in suffixes:
                query("SELECT " + expression + suffix, state="42883")
                query("SELECT " + expression + " OVER()" + suffix, state="42883")
            query("SELECT COALESCE('{7}'::integer[]," + expression + ")", state="42883")
        for expression in ("array_positions(ARRAY[1],1)", "array_positions(NULL::integer[],NULL)",
                           "array_positions('{1}'::integer[],'bad')",
                           "array_positions(" + array(["(0,0)"], "point") + ",'(0,0)'::point)"):
            for suffix in suffixes:
                query("SELECT " + expression + " OVER()" + suffix, state="42809")
        for expression in ("array_positions('{1}'::integer[],'bad')", "array_positions(NULL::integer[],'bad')",
                           "array_positions('{}'::integer[],'bad')"):
            for suffix in suffixes:
                query("SELECT " + expression + suffix, state="22P02")
            query("SELECT COALESCE('{7}'::integer[]," + expression + ")", state="22P02")
        for kind, value in (("point", "(0,0)"), ("json", "1")):
            expression = "array_positions(" + array([value], kind) + "," + literal(value) + "::" + kind + ")"
            for suffix in suffixes[1:]:
                query("SELECT " + expression + suffix, state="42883")
            query("SELECT COALESCE('{7}'::integer[]," + expression + ")", rows=[["{7}"]], oids=[1007])
        for source, needle, state in ((1007, 23, None), (1007, 25, "42883"), (1231, 1700, None),
                                      (1009, 25, None), (1007, 16, "42883")):
            parse("SELECT pg_catalog.array_positions($1,$2)", [source, needle], state)
        parse("SELECT array_positions($1,1)", [25], "42883")
        parse("SELECT array_positions($1,1)", [1007])
        parse("SELECT pg_catalog.array_positions($1,$2)", [1017, 600])
        parse("SELECT pg_catalog.array_positions($1,$2)", [0, 0])
        parse("SELECT pg_catalog.array_positions($1,$2) AS positions", [1007, 23])
        for source, needle, expected in (("[-2:1]={1,NULL,1,2}", "1", "{-2,0}"),
                                         ("[-2:1]={1,NULL,1,2}", None, "{-1}"),
                                         ("[-2:1]={1,NULL,1,2}", "99", "{}"),
                                         ("{}", "1", "{}"), (None, "1", None)):
            bind_execute(source, needle, expected)
        assert query("PREPARE array_positions_good(NUMERIC[],NUMERIC) AS SELECT array_positions($1,$2)")
        query("EXECUTE array_positions_good('{1.00,2}',1)", rows=[["{1}"]], oids=[1007])
        query("EXECUTE array_positions_good(NULL,1)", rows=[[None]], oids=[1007])
        query("DEALLOCATE array_positions_good")
        query("PREPARE array_positions_bad(INTEGER[],TEXT) AS SELECT array_positions($1,$2)", state="42883")
        assert query("CREATE TABLE " + schema + ".data_rows(id INTEGER PRIMARY KEY,a NUMERIC[],n NUMERIC)")
        assert query("INSERT INTO " + schema + ".data_rows VALUES(1,'{1.00,2.00,1.0}',1),(2,'[-2:0]={1.00,NULL,1.0}',1),(3,NULL,1),(4,'{}',NULL)")
        query("SELECT id,array_positions(a,n) FROM " + schema + ".data_rows ORDER BY id", rows=[["1", "{1,3}"], ["2", "{-2,0}"], ["3", None], ["4", "{}"]], oids=[23, 1007])
        query("UPDATE " + schema + ".data_rows SET a='{0.0100,0.01000}' WHERE id=2 RETURNING array_positions(a,0.01::numeric)", rows=[["{1,2}"]], oids=[1007])
        assert query("CREATE SEQUENCE " + schema + ".effects")
        assert query("CREATE FUNCTION " + schema + ".effect() RETURNS NUMERIC LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('" + schema + ".effects'); RETURN 1; END;$$")
        query("SELECT array_positions('{1.00}'::numeric[]," + schema + ".effect()) FROM " + schema + ".empty_rows LIMIT 0", rows=[], oids=[1007])
        query("SELECT COALESCE('{7}'::integer[],array_positions('{1.00}'::numeric[]," + schema + ".effect()))", rows=[["{7}"]], oids=[1007])
        query("SELECT nextval('" + schema + ".effects')", rows=[["1"]], oids=[20])
        query("SELECT array_positions('{1.00,2,1.0}'::numeric[]," + schema + ".effect())", rows=[["{1,3}"]], oids=[1007])
        query("SELECT currval('" + schema + ".effects')", rows=[["2"]], oids=[20])
        query("SELECT id,array_positions(a," + schema + ".effect()) FROM " + schema + ".data_rows ORDER BY id", rows=[["1", "{1,3}"], ["2", "{}"], ["3", None], ["4", "{}"]], oids=[23, 1007])
        query("SELECT currval('" + schema + ".effects')", rows=[["6"]], oids=[20])
        # A concrete user declaration owns its exact call, not the builtin's
        # INTEGER result or planning behavior. Keep UNKNOWN ambiguity too.
        assert query("CREATE TABLE " + schema + ".owned_rows(a INTEGER[],n INTEGER)")
        assert query("INSERT INTO " + schema + ".owned_rows VALUES('{1}',1),(NULL,1)")
        assert query("CREATE FUNCTION " + schema + ".array_positions(p INTEGER[],q INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$BEGIN RETURN 'user-owned';END;$$")
        assert query("SET search_path TO " + schema + ",pg_catalog")
        query("SELECT array_positions('{1}'::integer[],1)", rows=[["user-owned"]], oids=[25])
        query("SELECT pg_catalog.array_positions('{1}'::integer[],1)", rows=[["{1}"]], oids=[1007])
        query("SELECT array_positions('{1.00}'::numeric[],1::numeric)", rows=[["{1}"]], oids=[1007])
        query("SELECT array_positions('{1}',1)", rows=[["user-owned"]], oids=[25])
        query("SELECT array_positions(NULL,NULL)", state="42725")
        query("SELECT array_positions(NULL::integer[],1)", rows=[["user-owned"]], oids=[25])
        query("SELECT array_positions(a,n),pg_catalog.array_positions(a,n) FROM owned_rows ORDER BY n", rows=[["user-owned", "{1}"], ["user-owned", None]], oids=[25, 1007])
        query("SELECT array_positions(a,n) FROM owned_rows WHERE false", rows=[], oids=[25])
        query("SELECT COALESCE('ok',array_positions('{1}'::integer[],1))", rows=[["ok"]], oids=[25])
        query("SELECT array_positions('{1}'::integer[],1) OVER()", state="42809")
        for suffix in suffixes[1:]:
            query("SELECT array_positions(NULL,NULL)" + suffix, state="42725")
        parse("SELECT array_positions($1,$2)", [1007, 23], output_oid=25)
        parse("SELECT array_positions($1,$2)", [0, 0], "42725")
        query("SET search_path TO public")
        print("ARRAY_POSITIONS_TYPED_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 248 and not failures, failures
    finally:
        if created:
            c.simple_query(sock, "DROP SCHEMA " + schema + " CASCADE")
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
