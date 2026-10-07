#!/usr/bin/env python3
"""Real text-format BIT parameters use the complete prefixed input codec."""
import argparse
import importlib.util
import socket
import struct
import uuid
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args();root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_text_parameter_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server["sock"]
    inputs=[("",""),("0","0"),("01","01"),("001","001"),
            ("b",""),("B",""),("b01","01"),("B001","001"),
            ("x",""),("X",""),("x1","0001"),("X0aF","000010101111"),
            ("x0","0000"),("Xff","11111111"),
            ("b02",None),("xg",None),("b 01",None),("x 1",None),("B'01'",None),("x-1",None),
            (None,None)]
    failures=[];controls=0
    try:
        for oid in (1560,1562):
            for source,bits in inputs:
                controls+=1;statement=("owned_bit_input_"+uuid.uuid4().hex[:12]).encode()
                portal=statement+b"_portal";sql=b"SELECT $1=B'01' AS value"
                parse=statement+b"\0"+sql+b"\0"+struct.pack("!HI",1,oid)
                raw=None if source is None else source.encode()
                parameter=struct.pack("!i",-1) if raw is None else struct.pack("!i",len(raw))+raw
                bind=portal+b"\0"+statement+b"\0"+struct.pack("!HH",0,1)+parameter+struct.pack("!H",0)
                sock.sendall(client.typed(b"P",parse)+client.typed(b"B",bind)+
                    client.typed(b"D",b"P"+portal+b"\0")+
                    client.typed(b"E",portal+b"\0"+struct.pack("!I",0))+client.typed(b"S"))
                messages=client.read_until_ready(sock);result=runner.decode_wire_result(messages,include_types=True)
                state="22P02" if source is not None and bits is None else None
                expected=None if source is None else "t" if bits=="01" else "f"
                rows=[] if state else [[expected]]
                difference=result[0]!=rows or result[1]!=state
                if state is None and result[1] is None:
                    difference=difference or result[3:]!=(["value"],"SELECT 1",[16])
                    fields=client.row_description_fields(messages)
                    difference=difference or len(fields)!=1 or fields[0][3:]!=(16,1,-1,0)
                elif any(kind==b"D" for kind,_ in messages):difference=True
                if difference:
                    error=(oid,source,result,rows,state);failures.append(str(error))
                    print("[BIT TEXT PARAMETER FAIL] "+str(error),flush=True)
                close=client.typed(b"C",b"S"+statement+b"\0")
                if result[1] is None:close=client.typed(b"C",b"P"+portal+b"\0")+close
                sock.sendall(close+client.typed(b"S"));closed=client.read_until_ready(sock)
                assert not any(kind==b"E" for kind,_ in closed),closed
        assert not failures,"%d/%d complete real parameter controls failed"%(len(failures),controls)
        print("[BIT TEXT PARAMETER PROTOCOL] all %d complete real Parse/Bind text prefix/hex/empty/NULL/error/OID controls passed"%controls)
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=="__main__":main()
