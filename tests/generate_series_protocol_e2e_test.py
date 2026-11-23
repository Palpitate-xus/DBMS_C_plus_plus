"""Typed function sources, generate_series overloads and lazy query demand."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("series_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference="--reference18" in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    checked=0;failures=[]

    def check(sql, rows=None, oids=None, state=None, describe=False, *,
              names=None, kinds=None, execute=False, arguments=()):
        nonlocal checked
        checked += 1
        if describe or execute:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(arguments))
            parse += b"".join(struct.pack("!I", 23) for _ in arguments)
            packets = [client.typed(b"P", parse)]
            if execute:
                bind = b"\0" * 4 + struct.pack("!H", len(arguments))
                bind += b"".join(struct.pack("!I", len(value)) + value for value in arguments)
                packets.extend((client.typed(b"B", bind + b"\0\0"),
                                client.typed(b"D", b"P\0"),
                                client.typed(b"E", b"\0" * 5)))
            else:
                packets.append(client.typed(b"D", b"S\0"))
            packets.append(client.typed(b"S"))
            sock.sendall(b"".join(packets))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = (result[1] == state
                 and (rows is None or result[0] == rows)
                 and (oids is None or result[5] == oids)
                 and (names is None or result[3] == names))
        if kinds is not None:
            valid = valid and b"".join(kind for kind, _ in messages) == kinds and messages[-1] == (b"Z", b"I")
            if state is None and not describe and rows is not None:
                valid = valid and result[4] == f"SELECT {len(rows)}"
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print("GENERATE_SERIES", sql, "describe=", describe, result, "pass=", valid, flush=True)
        if not valid:
            failures.append((sql, describe, execute, result, rows, oids, state))

    try:
        cases=[
            ("SELECT lower('ABC'),length('a'),abs(2),generate_series(1,2)",[["abc","1","2","1"],["abc","1","2","2"]],[25,23,23,23]),
            ("SELECT generate_series(1,3)",[["1"],["2"],["3"]],[23]),
            ("SELECT pg_catalog.generate_series(3,1,-1)",[["3"],["2"],["1"]],[23]),
            ("SELECT generate_series(1::SMALLINT,2::INT)",[["1"],["2"]],[23]),
            ("SELECT generate_series(2147483646,2147483647)",[["2147483646"],["2147483647"]],[23]),
            ("SELECT generate_series(-2147483647-1,-2147483647-1,-1)",[["-2147483648"]],[23]),
            ("SELECT generate_series(9223372036854775806,9223372036854775807)",[["9223372036854775806"],["9223372036854775807"]],[20]),
            ("SELECT generate_series(-9223372036854775807-1,-9223372036854775807-1,-1::BIGINT)",[["-9223372036854775808"]],[20]),
            ("SELECT generate_series(1.0::NUMERIC,2.0::NUMERIC,0.25::NUMERIC)",[["1.0"],["1.25"],["1.50"],["1.75"],["2.00"]],[1700]),
            ("SELECT generate_series(2::NUMERIC,1::NUMERIC,-0.5::NUMERIC)",[["2"],["1.5"],["1.0"]],[1700]),
            ("SELECT generate_series(1,0)",[],[23]),
            ("SELECT generate_series(0,1,-1)",[],[23]),
            ("SELECT generate_series(NULL::INT,1,0)",[],[23]),
            ("SELECT generate_series(1,NULL::INT,0)",[],[23]),
            ("SELECT generate_series(1,2,NULL::INT)",[],[23]),
            ("SELECT generate_series(1,2),generate_series(10,12)",[["1","10"],["2","11"],[None,"12"]],[23,23]),
            ("SELECT unnest(ARRAY['a','b']),generate_series(1,3)",[["a","1"],["b","2"],[None,"3"]],[25,23]),
            ("SELECT generate_series(1,9223372036854775807) LIMIT 1",[["1"]],[20]),
            ("SELECT generate_series(1,2,0) WHERE false",[],[23]),
            ("SELECT generate_series(1,2,0) LIMIT 0",[],[23]),
            ("SELECT generate_series(1,2,0) WHERE NULL::BOOL",[],[23]),
            ("SELECT generate_series(TIMESTAMP '2024-01-31',TIMESTAMP '2024-03-31',INTERVAL '1 month')",[["2024-01-31 00:00:00"],["2024-02-29 00:00:00"],["2024-03-29 00:00:00"]],[1114]),
            ("SELECT generate_series(TIMESTAMP '2024-01-01 00:00:00.001',TIMESTAMP '2024-01-01 00:00:00.003',INTERVAL '0.001 second')",[["2024-01-01 00:00:00.001"],["2024-01-01 00:00:00.002"],["2024-01-01 00:00:00.003"]],[1114]),
            ("SELECT generate_series(TIMESTAMPTZ '2024-03-09 17:00:00+00',TIMESTAMPTZ '2024-03-11 16:00:00+00',INTERVAL '1 day','America/New_York')",[["2024-03-09 17:00:00+00"],["2024-03-10 16:00:00+00"],["2024-03-11 16:00:00+00"]],[1184]),
            ("SELECT generate_series(DATE '2024-01-01',DATE '2024-01-02',INTERVAL '1 day')",[["2024-01-01 00:00:00+00"],["2024-01-02 00:00:00+00"]],[1184]),
        ]
        for sql,rows,oids in cases:
            check(sql,rows,oids)
            check(sql,[],oids,describe=True)
        from_cases = [
            ("SELECT * FROM generate_series(1,0)", [], ["generate_series"], [23]),
            ("SELECT * FROM generate_series(NULL::integer,3,0)", [], ["generate_series"], [23]),
            ("SELECT * FROM generate_series(1+4,1,-2)",
             [["5"], ["3"], ["1"]], ["generate_series"], [23]),
            ("SELECT * FROM generate_series(9223372036854775806,9223372036854775807)",
             [["9223372036854775806"], ["9223372036854775807"]], ["generate_series"], [20]),
            ("SELECT * FROM generate_series(1.0::numeric,2.0::numeric,0.5::numeric)",
             [["1.0"], ["1.5"], ["2.0"]], ["generate_series"], [1700]),
            ("SELECT * FROM \"pg_catalog\".\"generate_series\" /* comment */ (1,2) AS \"Pick\"(\"Value\")",
             [["1"], ["2"]], ["Value"], [23]),
            ("SELECT g,g.i FROM generate_series(1,2) AS g(i)",
             [["1", "1"], ["2", "2"]], ["g", "i"], [23, 23]),
            ("SELECT * FROM generate_series(5,1,-2) WITH ORDINALITY AS g(i,n) WHERE i<5 ORDER BY i",
             [["1", "3"], ["3", "2"]], ["i", "n"], [23, 20]),
            ("SELECT a.i,b.j FROM generate_series(1,2) a(i) CROSS JOIN generate_series(3,4) b(j) ORDER BY a.i,b.j",
             [["1", "3"], ["1", "4"], ["2", "3"], ["2", "4"]], ["i", "j"], [23, 23]),
            ("WITH s AS (SELECT * FROM generate_series(1,3) AS n) SELECT n FROM s LIMIT 2",
             [["1"], ["2"]], ["n"], [23]),
            ("SELECT count(*) FROM generate_series(1,0) g(i)", [["0"]], ["count"], [20]),
            ("SELECT FROM generate_series(1,2)", [[], []], [], []),
            ("SELECT * FROM generate_series(1,2,0) LIMIT 0", [], ["generate_series"], [23]),
            ("SELECT * FROM generate_series(1,2,0) WHERE false", [], ["generate_series"], [23]),
            ("SELECT * FROM generate_series(1,2) WITH ORDINALITY AS r",
             [["1", "1"], ["2", "2"]], ["r", "ordinality"], [23, 20]),
            ("SELECT * FROM ROWS FROM(generate_series(1,3), generate_series(10,11))",
             [["1", "10"], ["2", "11"], ["3", None]], ["generate_series", "generate_series"], [23, 23]),
            ("SELECT * FROM ROWS FROM(unnest(ARRAY[1,NULL,3]),generate_series(10,11)) "
             "WITH ORDINALITY AS r(a,b,n)",
             [["1", "10", "1"], [None, "11", "2"], ["3", None, "3"]], ["a", "b", "n"], [23, 23, 20]),
            ("SELECT * FROM ROWS FROM(generate_series(1,0),generate_series(NULL::integer,4)) "
             "WITH ORDINALITY AS r(a,b,n)", [], ["a", "b", "n"], [23, 23, 20]),
            ("SELECT * FROM ROWS FROM(unnest(ARRAY['a','b']),generate_series(1,0),upper('hi')) AS r(word,n,tag)",
             [["a", None, "HI"], ["b", None, None]], ["word", "n", "tag"], [25, 23, 25]),
            ("SELECT * FROM ROWS FROM(upper('a'),abs(-2)) AS r(a,b)", [["A", "2"]], ["a", "b"], [25, 23]),
            ("SELECT * FROM ROWS FROM(unnest(ARRAY[1,2],ARRAY[3::bigint])) AS t(a,b)",
             [["1", "3"], ["2", None]], ["a", "b"], [23, 20]),
            ("SELECT r.* FROM ROWS FROM(generate_series(1,2),generate_series(10,12)) AS r(a)",
             [["1", "10"], ["2", "11"], [None, "12"]], ["a", "generate_series"], [23, 23]),
            ("SELECT r FROM ROWS FROM(generate_series(1,2)) AS r(a)", [["1"], ["2"]], ["r"], [23]),
            ("SELECT * FROM ROWS FROM(generate_series(1,2)) WITH ORDINALITY AS r",
             [["1", "1"], ["2", "2"]], ["r", "ordinality"], [23, 20]),
            ("SELECT FROM ROWS FROM(generate_series(1,2),generate_series(1,3))", [[], [], []], [], []),
            ("WITH q AS (SELECT * FROM ROWS FROM(generate_series(1,2),generate_series(10,12)) t(a,b)) "
             "SELECT count(a),count(b) FROM q", [["2", "3"]], ["count", "count"], [20, 20]),
            ("SELECT x.i,t.b FROM generate_series(1,3) x(i) "
             "LEFT JOIN ROWS FROM(generate_series(1,2),generate_series(10,10)) t(a,b) ON t.a=x.i ORDER BY x.i",
             [["1", "10"], ["2", None], ["3", None]], ["i", "b"], [23, 23]),
            ("SELECT * FROM ROWS FROM(generate_series(1,2,0),generate_series(10,12)) LIMIT 0",
             [], ["generate_series", "generate_series"], [23, 23]),
            ("SELECT n,v,n AS again FROM ROWS FROM(unnest(ARRAY['NULL',NULL,'中文']),"
             "generate_series(10,11)) WITH ORDINALITY AS g(v,x,n) WHERE x IS NULL OR n=1",
             [["1", "NULL", "1"], ["3", "中文", "3"]], ["n", "v", "again"], [20, 25, 20]),
            ("SELECT DISTINCT v FROM ROWS FROM(unnest(ARRAY['text','text']),generate_series(1,0)) AS g(v,x)",
             [["text"]], ["v"], [25]),
            ("SELECT b,a,b AS again FROM ROWS FROM(generate_series(1::bigint,3),"
             "unnest(ARRAY[true,NULL])) AS g(a,b)",
             [["t", "1", "t"], [None, "2", None], [None, "3", None]], ["b", "a", "again"], [16, 20, 16]),
            ("SELECT v, p FROM ROWS FROM(unnest(ARRAY['leading space','',NULL]),generate_series(6,7)) "
             "WITH ORDINALITY AS g(v,x,p) OFFSET 1 LIMIT 2",
             [["", "2"], [None, "3"]], ["v", "p"], [25, 20]),
            ("SELECT * FROM unnest(ARRAY['NULL','',NULL,'中文']) WITH ORDINALITY AS u(v,n)",
             [["NULL", "1"], ["", "2"], [None, "3"], ["中文", "4"]], ["v", "n"], [25, 20]),
            ("SELECT v,n FROM pg_catalog.unnest(ARRAY[9,7,8]) WITH ORDINALITY AS u(v,n) WHERE v<9 ORDER BY v",
             [["7", "2"], ["8", "3"]], ["v", "n"], [23, 20]),
            ("SELECT * FROM unnest(ARRAY[1,2,3],ARRAY['A',NULL]) WITH ORDINALITY AS u(i,v,n)",
             [["1", "A", "1"], ["2", None, "2"], ["3", None, "3"]], ["i", "v", "n"], [23, 25, 20]),
            ("SELECT * FROM UNNEST(ARRAY[1,2],ARRAY[2::bigint]) AS zipped",
             [["1", "2"], ["2", None]], ["unnest", "unnest"], [23, 20]),
            ('SELECT * FROM "pg_catalog"."unnest"(ARRAY[ARRAY[1,2],ARRAY[3,4]]) AS "X"',
             [["1"], ["2"], ["3"], ["4"]], ["X"], [23]),
            ("SELECT * FROM unnest(NULL::integer[],ARRAY[4::bigint]) WITH ORDINALITY",
             [[None, "4", "1"]], ["unnest", "unnest", "ordinality"], [23, 20, 20]),
            ("SELECT * FROM unnest('{}'::int[]) WITH ORDINALITY", [], ["unnest", "ordinality"], [23, 20]),
            ("SELECT u,u.v FROM unnest(ARRAY['x','y']) AS u(v)", [["x", "x"], ["y", "y"]], ["u", "v"], [25, 25]),
            ("SELECT * FROM unnest(ARRAY[1,2]) u(v) LEFT JOIN unnest(ARRAY[2,3]) z(w) ON v=w ORDER BY v",
             [["1", None], ["2", "2"]], ["v", "w"], [23, 23]),
            ("SELECT n FROM (SELECT n FROM unnest(ARRAY['a','b']) WITH ORDINALITY u(v,n)) q",
             [["1"], ["2"]], ["n"], [20]),
            ("SELECT * FROM unnest(string_to_array('c,d,e', ',')) AS u", [["c"], ["d"], ["e"]], ["u"], [25]),
            ("SELECT FROM unnest(ARRAY[1,2])", [[], []], [], []),
        ]
        for sql, rows, names, oids in from_cases:
            check(sql, rows, oids, names=names, kinds=b"T"+b"D"*len(rows)+b"CZ")
            check(sql, [], oids, names=names, describe=True, kinds=b"1tTZ")
            check(sql, rows, oids, names=names, execute=True, kinds=b"12T"+b"D"*len(rows)+b"CZ")
        check("SELECT * FROM generate_series($1::integer,$2::integer) WITH ORDINALITY AS g(i,n)",
              [["8", "1"], ["9", "2"]], [23, 20], names=["i", "n"], execute=True,
              kinds=b"12TDDCZ", arguments=(b"8", b"9"))
        check("SELECT * FROM unnest(ARRAY[$1::integer,NULL]) WITH ORDINALITY AS u(v,n) OFFSET 1",
              [[None, "2"]], [23, 20], names=["v", "n"], execute=True,
              kinds=b"12TDCZ", arguments=(b"23",))
        for sql, state in (
            ("SELECT * FROM pg_catalog.unnest(ARRAY[1],ARRAY[2])", "42883"),
            ("SELECT * FROM unnest(1)", "42883"),
            ("SELECT * FROM unnest(ARRAY[1,2]) AS u(v,w)", "42P10"),
            ("SELECT * FROM unnest(ARRAY[unnest(ARRAY[1])])", "0A000"),
            ("SELECT * FROM unnest(ARRAY[sum(1)])", "42803"),
            ("SELECT * FROM unnest(ARRAY[ROW(1,2)])", "42601"),
            ("SELECT * FROM unnest(ARRAY[1]) WITH", "42601"),
            ("SELECT * FROM unnest(ARRAY[1]) AS u(v,)", "42601"),
        ):
            check(sql, [], state=state, kinds=b"EZ")
            check(sql, [], state=state, describe=True, kinds=b"EZ")
            check(sql, [], state=state, execute=True, kinds=b"EZ")
        check("CREATE SEQUENCE series_argument_seq")
        once = "SELECT v FROM generate_series(nextval('series_argument_seq'),4::bigint) AS s(v) LIMIT 2"
        check(once, [], [20], names=["v"], describe=True, kinds=b"1tTZ")
        check(once, [["1"], ["2"]], [20], names=["v"], execute=True, kinds=b"12TDDCZ")
        check("SELECT nextval('series_argument_seq')", [["2"]], [20], kinds=b"TDCZ")
        check("DROP SEQUENCE series_argument_seq")
        check("CREATE SEQUENCE unnest_argument_seq START WITH 30")
        unnest_once = ("SELECT * FROM unnest(ARRAY[nextval('unnest_argument_seq'),"
                       "nextval('unnest_argument_seq')]) WITH ORDINALITY AS u(v,n)")
        check(unnest_once, [], [20, 20], names=["v", "n"], describe=True, kinds=b"1tTZ")
        check(unnest_once + " LIMIT 0", [], [20, 20], names=["v", "n"], kinds=b"TCZ")
        check(unnest_once, [["30", "1"], ["31", "2"]], [20, 20], names=["v", "n"], execute=True, kinds=b"12TDDCZ")
        check("SELECT nextval('unnest_argument_seq')", [["32"]], [20], kinds=b"TDCZ")
        check("DROP SEQUENCE unnest_argument_seq")
        check("CREATE SEQUENCE rows_from_argument_seq START WITH 10")
        zip_once = ("SELECT * FROM ROWS FROM(nextval('rows_from_argument_seq'), "
                    "generate_series(nextval('rows_from_argument_seq'),15::bigint)) "
                    "WITH ORDINALITY AS r(n,i,k) LIMIT 2")
        check(zip_once, [], [20, 20, 20], names=["n", "i", "k"], describe=True, kinds=b"1tTZ")
        check(zip_once, [["10", "11", "1"], [None, "12", "2"]], [20, 20, 20],
              names=["n", "i", "k"], execute=True, kinds=b"12TDDCZ")
        check("SELECT nextval('rows_from_argument_seq')", [["12"]], [20], kinds=b"TDCZ")
        check("DROP SEQUENCE rows_from_argument_seq")
        if not reference:
            check("SET statement_timeout TO '2s'")
            check("SELECT i FROM generate_series(1,9223372036854775807) AS g(i) LIMIT 1",
                  [["1"]], [20], names=["i"], kinds=b"TDCZ")
            check("SELECT * FROM ROWS FROM(generate_series(1,9223372036854775807),unnest(ARRAY[1,2])) "
                  "AS g(i,v) LIMIT 3", [["1", "1"], ["2", "2"], ["3", None]],
                  [20, 23], names=["i", "v"], kinds=b"TDDDCZ")
            check("RESET statement_timeout")
        check("SELECT * FROM generate_series(1,3,0)", state="22023", kinds=b"TEZ")
        check("SELECT * FROM generate_series(1,2) g(i,j)", state="42P10", kinds=b"EZ")
        check("SELECT * FROM generate_series(1,(2)", state="42601", kinds=b"EZ")
        check("SELECT * FROM ROWS FROM()", state="42601", kinds=b"EZ")
        check("SELECT * FROM ROWS FROM(generate_series(1,2),)", state="42601", kinds=b"EZ")
        check("SELECT generate_series FROM ROWS FROM(generate_series(1,2),generate_series(10,12))",
              state="42702", kinds=b"EZ")
        check("SELECT * FROM ROWS FROM(generate_series(1,2),generate_series(10,12,0)) LIMIT 1",
              state="22023", kinds=b"TEZ")
        check("SELECT * FROM ROWS FROM(no_such_function(1))", state="42883", kinds=b"EZ")
        for sql in ("SELECT * FROM ROWS FROM(sum(1))", "SELECT * FROM ROWS FROM(generate_series(sum(1),3))"):
            check(sql, state="42803", kinds=b"EZ")
            check(sql, state="42803", execute=True, kinds=b"EZ")
        check("SELECT * FROM ROWS FROM(sum('text'::text))", state="42883", kinds=b"EZ")
        errors=[
            ("SELECT generate_series(1::SMALLINT,2::SMALLINT)","42725"),
            ("SELECT generate_series(1,2,0)","22023"),
            ("SELECT generate_series(1::BIGINT,2::BIGINT,0::BIGINT)","22023"),
            ("SELECT generate_series(1::NUMERIC,2::NUMERIC,0::NUMERIC)","22023"),
            ("SELECT generate_series(1)","42883"),
            ("SELECT generate_series(1,2,3,4) WHERE false","42883"),
            ("SELECT generate_series('1','2') WHERE false","42725"),
            ("SELECT generate_series('1'::TEXT,2) WHERE false","42883"),
            ("SELECT generate_series(NULL::TEXT,2) WHERE false","42883"),
            ("SELECT generate_series('NaN'::NUMERIC,2::NUMERIC)","22023"),
            ("SELECT generate_series(1::NUMERIC,'Infinity'::NUMERIC)","22023"),
            ("SELECT generate_series(TIMESTAMP '2024-01-01',TIMESTAMP '2024-01-02',INTERVAL '0 seconds')","22023"),
            ("SELECT generate_series(TIMESTAMPTZ '2024-01-01',TIMESTAMPTZ '2024-01-02',INTERVAL '1 day','not_a_timezone')","22023"),
        ]
        for sql,state in errors:check(sql,state=state)
        check("CREATE FUNCTION public.generate_series(a INTEGER,b INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$SELECT 99$$")
        check("SELECT generate_series(1,2)",[["1"],["2"]],[23])
        check("SELECT public.generate_series(1,2)",[["99"]],[23])
        check("SET search_path TO public,pg_catalog")
        check("SELECT generate_series(1,2)",[["99"]],[23])
        check("SELECT pg_catalog.generate_series(1,2)",[["1"],["2"]],[23])
        check("SET search_path TO pg_catalog,public")
        check("SELECT generate_series(1,2)",[["1"],["2"]],[23])
        # This fixture creates exactly one public overload. A separate DDL
        # follow-up owns typed DROP signatures; keep cleanup unambiguous.
        check("DROP FUNCTION public.generate_series")
        check("CREATE FUNCTION public.generate_series(a TEXT,b TEXT) RETURNS TEXT LANGUAGE SQL AS $$SELECT 'stored text'::TEXT$$")
        check("SELECT generate_series('a'::TEXT,'b'::TEXT)",[["stored text"]],[25])
        check("SELECT generate_series('a','b')",[["stored text"]],[25])
        check("SELECT pg_catalog.generate_series('a'::TEXT,'b'::TEXT) WHERE false",state="42883")
        check("DROP FUNCTION public.generate_series")
        check("CREATE FUNCTION public.unnest(a INTEGER) RETURNS TEXT LANGUAGE SQL AS $$SELECT 'stored'::TEXT$$")
        stored_unnest = "SELECT * FROM public.unnest(1) WITH ORDINALITY AS u(v,n)"
        check(stored_unnest, [["stored", "1"]], [25, 20], names=["v", "n"], kinds=b"TDCZ")
        check(stored_unnest, [], [25, 20], names=["v", "n"], describe=True, kinds=b"1tTZ")
        check(stored_unnest, [["stored", "1"]], [25, 20], names=["v", "n"], execute=True, kinds=b"12TDCZ")
        check("SELECT * FROM unnest(1)", [["stored"]], [25], names=["unnest"], kinds=b"TDCZ")
        check("SELECT * FROM unnest(ARRAY[1,2],ARRAY['b']) AS r(a,b)",
              [["1", "b"], ["2", None]], [23, 25], names=["a", "b"], kinds=b"TDDCZ")
        check("DROP FUNCTION public.unnest")
        check("SELECT 1",[["1"]],[23])
        print("GENERATE_SERIES_COMPLETE",checked,"FAILED",len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=="__main__":main()
