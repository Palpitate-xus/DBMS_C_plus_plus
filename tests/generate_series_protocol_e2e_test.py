"""Real generate_series overloads, typed descriptors and lazy query demand."""
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

    def check(sql,rows=None,oids=None,state=None,describe=False):
        nonlocal checked
        checked+=1
        if describe:
            parse=b"\0"+sql.encode()+b"\0"+struct.pack("!H",0)
            sock.sendall(client.typed(b"P",parse)+client.typed(b"D",b"S\0")+client.typed(b"S"))
            messages=client.read_until_ready(sock)
        else:messages=client.simple_query(sock,sql)
        result=runner.decode_wire_result(messages,include_types=True)
        valid=result[1]==state
        if rows is not None:valid=valid and result[0]==rows
        if oids is not None:valid=valid and result[5]==oids
        if state:valid=valid and result[0]==[] and result[4] is None
        print("GENERATE_SERIES",sql,"describe=",describe,result,"pass=",valid,flush=True)
        if not valid:failures.append((sql,describe,result,rows,oids,state))

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
        check("SELECT 1",[["1"]],[23])
        print("GENERATE_SERIES_COMPLETE",checked,"FAILED",len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=="__main__":main()
