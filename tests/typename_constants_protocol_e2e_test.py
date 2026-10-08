#!/usr/bin/env python3
import importlib.util
import json
import socket
import sys
from pathlib import Path
repo=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('typename_probe_runner',repo/'tests/compat/pg_diff_runner.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
client=runner.load_protocol_client()
reference='--reference18' in sys.argv
if reference:
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
else:
    server=runner.start_ours(client);sock=server['sock']
failures=0;checked=0
queries=[
    "VARBIT 'b01'", "BIT VARYING 'b01'", "pg_catalog.varbit 'b01'", '"varbit" \'b01\'',
    "VARBIT(2) 'b111'", "BIT(2) 'b111'", "BIT 'b01'", "BIT VARYING(2) 'b111'",
    "VARBIT(4) 'b01'", "BIT(4) 'b01'", "VARBIT ''", "BIT ''",
    "pg_catalog.varbit(2) 'b111'", '"pg_catalog"."varbit"(2) \'b111\'',
    "VARBIT(+2) 'b111'", "VARBIT(0) 'b01'", "VARBIT(-2) 'b01'", "VARBIT(2,3) 'b01'",
    "VARBIT 'b02'", "VARBIT 'xg'", "VARBIT 'X0aF'", "VARBIT 'x'",
    "CAST('b111' AS BIT VARYING(2))", "'b111'::BIT VARYING(2)",
    "CAST('b01' AS pg_catalog.varbit)", "'b01'::pg_catalog.varbit",
    "CAST('12:23:34' AS TIME(2) WITHOUT TIME ZONE)",
    "CAST('2020-01-01 12:23:34' AS TIMESTAMP(2) WITHOUT TIME ZONE)",
    "CAST('{01,111}' AS BIT VARYING(2)[])", "'{01,111}'::BIT VARYING(2)[]",
    "CAST('b01' AS BIT VARYING) = B'01'", "VARBIT 'b01' = B'01'",
    "CASE WHEN true THEN VARBIT 'b01' ELSE NULL END", "ARRAY[VARBIT 'b01',NULL]",
    "VARBIT 'b01' IN(B'01')", "VARBIT 'b01' BETWEEN B'00' AND B'11'",
    "TEXT 'x'", "INTEGER '12'", "NUMERIC(4,2) '1.2'", "UUID '12345678-1234-1234-1234-123456789012'",
    "not_a_type 'x'", "not_a_namespace.varbit 'b01'", '"VARBIT" \'b01\'',
    "CAST('b01' AS not_a_type)", "'b01'::not_a_type"
]
try:
    cases=json.loads((repo/'tests/compat/typename_constants_pg18_expected.json').read_text())
    assert len(cases)==45 and [case['sql'] for case in cases]==['SELECT '+expression+' AS value' for expression in queries]
    for case in cases:
        sql=case['sql'];expected=case['reference']
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        good=expected[1]==actual[1] and (expected[1] is not None or
            (expected[0]==actual[0] and expected[3:]==list(actual[3:])))
        checked+=1;failures+=not good
        print(json.dumps(dict(sql=sql,reference=expected,actual=actual,pass_=good)),flush=True)
finally:
    if reference:sock.close()
    else:runner.stop_ours(server)
print('TYPENAME_CONSTANTS_CHECKED',checked,'FAILED',failures,flush=True)
raise SystemExit(bool(failures))
