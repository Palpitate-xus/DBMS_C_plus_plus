import importlib.util
import socket
import sys
from pathlib import Path

repo=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('char_comparison_runner',repo/'tests/compat/pg_diff_runner.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
client=runner.load_protocol_client()
reference='--reference18' in sys.argv
if reference:
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
else:
    server=runner.start_ours(client);sock=server['sock']
checked=failures=0
try:
    for a in (0,1,65,127,128,255):
        for b in (0,1,65,127,128,255):
            for op in ('=','<>','<','>','<=','>='):
                truth={'=':a==b,'<>':a!=b,'<':a<b,'>':a>b,'<=':a<=b,'>=':a>=b}[op]
                sql="SELECT \"char\" '\\%03o' %s \"char\" '\\%03o' AS value"%(a,op,b)
                actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
                good=actual[0]==[['t' if truth else 'f']] and actual[1] is None and actual[3:]==(['value'],'SELECT 1',[16])
                checked+=1;failures+=not good
                print('CHAR_BYTE_COMPARISON',sql,actual,'pass=',good,flush=True)
finally:
    if reference:sock.close()
    else:runner.stop_ours(server)
print('CHAR_BYTE_COMPARISON_CHECKED',checked,'FAILED',failures,flush=True)
raise SystemExit(0 if checked==216 and not failures else 1)
