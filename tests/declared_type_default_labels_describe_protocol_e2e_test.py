import importlib.util
import json
import socket
import struct
import sys
from pathlib import Path

repo=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('type_label_describe_runner',repo/'tests/compat/pg_diff_runner.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
client=runner.load_protocol_client()
cases=json.loads((repo/'tests/compat/declared_type_default_labels_pg18_expected.json').read_text())
assert len(cases)==78
reference='--reference18' in sys.argv
if reference:
    host,port,user,database,password=runner._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
    client.startup_reference(sock,user,database,password);runner.verify_reference_version(client,sock)
else:
    server=runner.start_ours(client);sock=server['sock']
checked=failures=0
def require(good,role,actual):
    global checked,failures
    checked+=1;failures+=not good
    print('TYPE_LABEL_DESCRIBE',role,actual,'pass=',good,flush=True)
try:
    begin=runner.decode_wire_result(client.simple_query(sock,'BEGIN'),include_types=True)
    assert begin[1] is None,begin
    for index,case in enumerate(cases):
        sql=case['sql'];expected=case['reference']
        name=('type_label_'+str(index)).encode();portal=name+b'_p'
        sock.sendall(client.typed(b'P',name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
            client.typed(b'D',b'S'+name+b'\0')+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        actual=runner.decode_wire_result(messages,include_types=True)
        fields=client.row_description_fields(messages) if any(kind==b'T' for kind,_ in messages) else []
        require(actual[1] is None and actual[3]==expected[3] and actual[5]==expected[5] and
            any(kind==b'1' for kind,_ in messages) and len(fields)==1 and fields[0][6]==0,
            'Parse/DescribeS '+sql,actual)
        if actual[1] is not None:continue
        sock.sendall(client.typed(b'B',portal+b'\0'+name+b'\0'+struct.pack('!HHH',0,0,0))+
            client.typed(b'D',b'P'+portal+b'\0')+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        actual=runner.decode_wire_result(messages,include_types=True)
        require(actual[1] is None and actual[3]==expected[3] and actual[5]==expected[5] and
            any(kind==b'2' for kind,_ in messages),'Bind/DescribeP '+sql,actual)
        if actual[1] is not None:continue
        sock.sendall(client.typed(b'E',portal+b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        actual=runner.decode_wire_result(messages,include_types=True)
        require(actual[1] is None and actual[0]==expected[0] and actual[4]==expected[4],'Execute '+sql,actual)
        sock.sendall(client.typed(b'C',b'P'+portal+b'\0')+client.typed(b'C',b'S'+name+b'\0')+client.typed(b'S'))
        messages=client.read_until_ready(sock)
        actual=runner.decode_wire_result(messages,include_types=True)
        require(actual[1] is None and sum(kind==b'3' for kind,_ in messages)==2,'Close '+sql,actual)
    commit=runner.decode_wire_result(client.simple_query(sock,'COMMIT'),include_types=True)
    assert commit[1] is None,commit
finally:
    if reference:sock.close()
    else:runner.stop_ours(server)
print('TYPE_LABEL_DESCRIBE_CHECKED',checked,'FAILED',failures,flush=True)
raise SystemExit(0 if checked==312 and not failures else 1)

