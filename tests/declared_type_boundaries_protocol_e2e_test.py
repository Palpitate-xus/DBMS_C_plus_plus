"""Full declared-type boundary corpus; expected records are actual PG18.6."""
import importlib.util
import json
import socket
import sys
from pathlib import Path

repo=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('declared_type_boundary_runner',repo/'tests/compat/pg_diff_runner.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
client=runner.load_protocol_client()
expressions=[]
for declaration in ('FLOAT','FLOAT(1)','FLOAT(24)','FLOAT(25)','FLOAT(53)',
                    'FLOAT(0)','FLOAT(54)','FLOAT(-1)','FLOAT(+1)','FLOAT(24,25)'):
    expressions += [declaration+" '1.25'","CAST('1.25' AS "+declaration+')',"'1.25'::"+declaration]
for declaration,value in (
    ('INTEGER(2)','12'),('int4(2)','12'),('TEXT(3)','abcdef'),
    ('VARCHAR(0)','a'),('VARCHAR(-1)','a'),('VARCHAR(2,3)','abc'),
    ('NUMERIC(0)','1'),('NUMERIC(2,2000)','1'),('CHAR','abc'),
    ('bpchar','abc'),('pg_catalog.bpchar','abc'),('"char"','abc'),
    ('pg_catalog."char"','abc'),('BIT','01'),('pg_catalog.bit','01'),
    ('"integer"','12'),('pg_catalog.integer','12'),('"int4"','12'),
    ('float4','1.25'),('float8','1.25')):
    expressions += [declaration+" '"+value+"'","CAST('"+value+"' AS "+declaration+')']
expressions += ["TRUE 'x'","NULL 'x'","ARRAY 'x'","SELECT 'x'",
                "CASE 'x' WHEN 'x' THEN 1 ELSE 2 END"]
cases=json.loads((repo/'tests/compat/declared_type_boundaries_pg18_expected.json').read_text())
assert len(cases)==75 and [case['sql'] for case in cases]==['SELECT '+expression+' AS value' for expression in expressions]
reference='--reference18' in sys.argv
if reference:
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
else:
    server=runner.start_ours(client);sock=server['sock']
checked=failures=0
try:
    for case in cases:
        expected=case['reference']
        actual=runner.decode_wire_result(client.simple_query(sock,case['sql']),include_types=True)
        good=expected[1]==actual[1] and (expected[1] is not None or
            (expected[0]==actual[0] and expected[3:]==list(actual[3:])))
        checked+=1;failures+=not good
        print(json.dumps(dict(sql=case['sql'],reference=expected,actual=actual,pass_=good)),flush=True)
finally:
    if reference:sock.close()
    else:runner.stop_ours(server)
print('DECLARED_TYPE_BOUNDARIES_CHECKED',checked,'FAILED',failures,flush=True)
raise SystemExit(bool(failures))
