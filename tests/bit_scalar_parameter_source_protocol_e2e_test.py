"""Declared BIT scalar parameter type is analyzed before any Bind value."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('bit_scalar_param_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = '--reference18' in sys.argv; server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    schema = 'bit_scalar_param_' + uuid.uuid4().hex[:12]; table = '"' + schema + '".rows'
    failures = []; controls = 0

    def check(ok, label, actual):
        nonlocal controls
        controls += 1; print('BIT_SCALAR_PARAM_SOURCE', label, actual, flush=True)
        if not ok: failures.append((label, actual))

    def simple(sql, rows=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        check(result[1] is None and (rows is None or result[0] == rows), sql, result)

    def prepare(sql, oid, expected_state=None):
        name = ('bit_scalar_param_' + uuid.uuid4().hex[:12]).encode()
        sock.sendall(client.typed(b'P',name+b'\0'+sql.encode()+b'\0'+struct.pack('!HI',1,oid))+
                     client.typed(b'D',b'S'+name+b'\0')+client.typed(b'S'))
        messages = client.read_until_ready(sock); result = runner.decode_wire_result(messages, include_types=True)
        check(result[1] == expected_state and not any(kind == b'D' for kind,_ in messages), ('Parse/DescribeS',sql,oid), result)
        if result[1] is not None: return None
        descriptions = [payload for kind,payload in messages if kind == b't']
        check(descriptions == [struct.pack('!HI',1,oid)], ('actual declared ParameterDescription',sql,oid), descriptions)
        return name

    def close(name, portal=None):
        if name is None: return
        messages = client.typed(b'C',b'S'+name+b'\0')
        if portal is not None: messages = client.typed(b'C',b'P'+portal+b'\0')+messages
        sock.sendall(messages+client.typed(b'S'))
        closed = client.read_until_ready(sock); check(not any(kind == b'E' for kind,_ in closed), 'Close', closed)

    def execute(name, raw, rows):
        if name is None: return
        portal = name+b'_p'; value = struct.pack('!i',-1) if raw is None else struct.pack('!i',len(raw.encode()))+raw.encode()
        bind = portal+b'\0'+name+b'\0'+struct.pack('!HH',0,1)+value+struct.pack('!H',0)
        sock.sendall(client.typed(b'B',bind)+client.typed(b'D',b'P'+portal+b'\0')+
                     client.typed(b'E',portal+b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        result = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
        check(result[1] is None and result[0] == rows and result[5] == [23], ('Bind/DescribeP/Execute',raw), result)
        close(name,portal)

    try:
        simple('CREATE SCHEMA "'+schema+'"')
        simple('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,v VARBIT,p TEXT)')
        simple('CREATE INDEX bit_scalar_param_v ON '+table+'(v)')
        for populated in (False,True):
            if populated: simple('INSERT INTO '+table+" VALUES(1,B'01','b01'),(2,B'',NULL),(3,NULL,'b01')")
            for op in ('=','<>','!=','<','>','<=','>='):
                for direction in ('v'+op+'$1','$1'+op+'v'):
                    sql = 'SELECT id FROM '+table+' WHERE '+direction+' ORDER BY id'
                    for oid in (25,23): close(prepare(sql,oid,'42883'))
                    for oid in (1560,1562): close(prepare(sql,oid))
            for oid in (1560,1562):
                for raw,rows in [('b01',[['1']]),('x',[['2']]),(None,[])]:
                    execute(prepare('SELECT id FROM '+table+' WHERE v=$1 ORDER BY id',oid),raw,rows if populated else [])
            execute(prepare('SELECT id FROM '+table+' WHERE p=$1 ORDER BY id',25),'b01',[['1'],['3']] if populated else [])
        print('BIT_SCALAR_PARAM_SOURCE_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures, failures
    finally:
        try: client.simple_query(sock,'DROP SCHEMA "'+schema+'" CASCADE')
        finally:
            if server: runner.stop_ours(server)
            else: sock.close()


if __name__ == '__main__': main()
