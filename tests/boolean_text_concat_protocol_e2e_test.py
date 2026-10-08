"""Text concatenation uses boolean text output, not boolean wire output."""
import importlib.util
import socket
import sys
from pathlib import Path

repo=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('boolean_concat_runner',repo/'tests/compat/pg_diff_runner.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
client=runner.load_protocol_client()
cases=(
    ("TEXT 'x' || TRUE","xtrue"),("TRUE || TEXT 'x'","truex"),
    ("TEXT 'x' || FALSE","xfalse"),("FALSE || TEXT 'x'","falsex"),
    ("TEXT 'x' || CAST(true AS boolean)","xtrue"),
    ("TEXT 'x' || CAST(false AS boolean)","xfalse"),
    ("TEXT 'x' || NULL::boolean",None),("NULL::boolean || TEXT 'x'",None),
    ("'x' || TRUE","xtrue"),("TRUE || 'x'","truex"),
    ("TEXT 'x' || 1","x1"),("1 || TEXT 'x'","1x"),
    ("CAST('a' AS CHAR(3)) || TRUE","atrue"),
    ("FALSE || CAST('a' AS CHAR(3))","falsea"),
)
reference='--reference18' in sys.argv
if reference:
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
else:
    server=runner.start_ours(client);sock=server['sock']
checked=failures=0
try:
    for expression,value in cases:
        sql='SELECT '+expression+' AS value'
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        good=actual[0]==[[value]] and actual[1] is None and actual[3:]==(['value'],'SELECT 1',[25])
        checked+=1;failures+=not good
        print('BOOLEAN_TEXT_CONCAT_PROTOCOL',sql,actual,'pass=',good,flush=True)
finally:
    if reference:sock.close()
    else:runner.stop_ours(server)
print('BOOLEAN_TEXT_CONCAT_PROTOCOL_CHECKED',checked,'FAILED',failures,flush=True)
raise SystemExit(0 if checked==14 and not failures else 1)
