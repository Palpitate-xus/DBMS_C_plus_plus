#!/usr/bin/env python3
"""INET/CIDR input notation, exact errors, persisted values and wire bytes."""
import importlib.util
import ipaddress
from pathlib import Path
import socket
import struct
import sys
import uuid

# Canonical values verified using server_version_num=180006, not libc inet_pton.
CASES = (
    ("0", None, "0.0.0.0/8"),
    ("1", None, "1.0.0.0/8"),
    ("10", None, "10.0.0.0/8"),
    ("127", None, "127.0.0.0/8"),
    ("128", None, "128.0.0.0/16"),
    ("223", None, "223.0.0.0/24"),
    ("224", None, "224.0.0.0/4"),
    ("225", None, "225.0.0.0/8"),
    ("239", None, "239.0.0.0/8"),
    ("240", None, "240.0.0.0/32"),
    ("255", None, "255.0.0.0/32"),
    ("10/8", "10.0.0.0/8", "10.0.0.0/8"),
    ("10.1", None, "10.1.0.0/16"),
    ("10.1/16", "10.1.0.0/16", "10.1.0.0/16"),
    ("10.1.2", None, "10.1.2.0/24"),
    ("10.1.2/24", "10.1.2.0/24", "10.1.2.0/24"),
    ("10.1.2.3", "10.1.2.3", "10.1.2.3/32"),
    ("10.1.2.3/24", "10.1.2.3/24", None),
    ("10/9", "10.0.0.0/9", "10.0.0.0/9"),
    ("10.1/8", "10.1.0.0/8", None),
    ("10.1/17", "10.1.0.0/17", "10.1.0.0/17"),
    ("10.00.1.0/24", "10.0.1.0/24", "10.0.1.0/24"),
    ("010.1.2.3", "10.1.2.3", "10.1.2.3/32"),
    ("10.1.2.3/032", "10.1.2.3", "10.1.2.3/32"),
    ("192.168.1", None, "192.168.1.0/24"),
    ("192.168.1/24", "192.168.1.0/24", "192.168.1.0/24"),
    ("192.168.1.0/255.255.255.0", None, None),
    ("0/0", "0.0.0.0/0", "0.0.0.0/0"),
    ("::1", "::1", "::1/128"),
    ("::1/64", "::1/64", None),
    ("::ffff:192.168.1.1/128", "::ffff:192.168.1.1", "::ffff:192.168.1.1/128"),
    ("2001:db8::/32", "2001:db8::/32", "2001:db8::/32"),
    ("1.2.3.4/33", None, None),
    ("1.2.3.4/0", "1.2.3.4/0", None),
    ("256.1.2.3", None, None),
    ("10.1.2.3 ", None, None),
    ("10.1.2.3\t", None, None),
    ("10..1", None, None),
    ("10/", None, None),
    ("", None, None),
    ("::ffff:010.1.2.3", None, None),
    ("10/15", "10.0.0.0/15", "10.0.0.0/15"),
    ("10/16", None, "10.0.0.0/16"),
    ("10.1/23", "10.1.0.0/23", "10.1.0.0/23"),
    ("10.1/24", None, "10.1.0.0/24"),
    ("192.168", None, "192.168.0.0/24"),
    ("224.1", None, "224.1.0.0/16"),
    ("240.1", None, "240.1.0.0/32"),
    ("255.255.255.255", "255.255.255.255", "255.255.255.255/32"),
    ("0xa", None, "160.0.0.0/16"),
    ("0x0a", None, "10.0.0.0/8"),
    ("0xC0A8", None, "192.168.0.0/24"),
    ("0xC0A801", None, "192.168.1.0/24"),
    ("0xC0A80100/24", None, "192.168.1.0/24"),
    ("0xc0000200/24", None, "192.0.2.0/24"),
    ("0xc0000201/24", None, None),
    ("0x0", None, "0.0.0.0/8"),
    ("0Xc00002/24", None, "192.0.2.0/24"),
    ("0x", None, None),
    ("0x12g", None, None),
    ("0x123456789", None, None),
    ("1.2.3.4.5", None, None),
    ("1.2.3.", None, None),
    ("1.2.3.4/00032", "1.2.3.4", "1.2.3.4/32"),
    ("1.2.3.4/+24", None, None),
    ("1.2.3.4/-1", None, None),
    ("::/00", None, None),
    ("::1/0128", None, None),
    ("::/0", "::/0", "::/0"),
    ("::1/001", None, None),
    ("::/129", None, None),
    ("::192.0.2.1", "::192.0.2.1", "::192.0.2.1/128"),
    ("0:0:0:0:0:192.0.2.1", None, None),
    ("2001:0db8::/032", None, None),
    ("::ffff:192.168.1.1/0128", None, None),
    ("10.", None, None),
    ("10./8", "10.0.0.0/8", None),
    ("10./9", "10.0.0.0/9", None),
    ("10./16", None, None),
    ("10.1.", None, None),
    ("10.1./16", "10.1.0.0/16", None),
    ("10.1.2./24", "10.1.2.0/24", None),
    ("10.1.2.3.", "10.1.2.3", None),
    ("10.1.2.3./24", "10.1.2.3/24", None),
    ("10.1.2.3..", None, None),
    ("10..1/8", None, None),
    ("10.1.2.3./32", "10.1.2.3", None),
    ("10.1.2.3./33", None, None),
    ("0xc0000200.", None, None),
    ("0xc0000200./24", None, None),
)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("inet_input_runner",root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv[1:]
    if reference:
        host,port,user,database,password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        runner.verify_reference_version(client,sock)
        server = {"sock":sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks,failures = 0,[]

    def check(label,ok,observed):
        nonlocal checks
        checks += 1
        if not ok:
            failures.append((label,observed))
            print("INET_INPUT_FAILURE",label,repr(observed),flush=True)

    def literal(value):
        return "'" + value.replace("'","''") + "'"

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)

    def expect(label,sql,state=None,rows=None,oids=None):
        result = query(sql)
        ok = result[1] == state
        if state:
            ok = ok and result[0] == [] and result[4] is None
        else:
            if rows is not None: ok = ok and result[0] == rows
            if oids is not None: ok = ok and result[5] == oids
        check(label,ok,result)

    def parameter(kind,oid,value,canonical,binary=False):
        raw = value.encode()
        parse = b"\0" + ("SELECT $1::" + kind).encode() + b"\0" + struct.pack("!HI",1,oid)
        bind = (b"\0\0" + struct.pack("!HH",0,1) + struct.pack("!i",len(raw)) + raw +
                struct.pack("!HH",1,1 if binary else 0))
        sock.sendall(client.typed(b"P",parse) + client.typed(b"B",bind) +
                     client.typed(b"D",b"P\0") + client.typed(b"E",b"\0" + struct.pack("!I",0)) +
                     client.typed(b"S"))
        messages = client.read_until_ready(sock)
        if binary:
            interface = ipaddress.ip_interface(canonical)
            address = interface.ip.packed
            wire = bytes([2 if interface.version == 4 else 3,interface.network.prefixlen,
                          1 if kind == "cidr" else 0,len(address)]) + address
            errors = [client.diagnostic_fields(body) for tag,body in messages if tag == b"E"]
            rows = [body for tag,body in messages if tag == b"D"]
            fields = client.row_description_fields(messages) if any(tag==b"T" for tag,_ in messages) else []
            check(kind + " binary result " + value,not errors and
                  rows == [struct.pack("!Hi",1,len(wire))+wire] and
                  len(fields)==1 and fields[0][3]==oid and fields[0][6]==1,(errors,rows,fields))
        else:
            result = runner.decode_wire_result(messages,include_types=True)
            check(kind + " text parameter " + value,
                  result[1] == ("22P02" if canonical is None else None) and
                  (result[0] == [] if canonical is None else
                   result[0] == [[canonical]] and result[5] == [oid]),result)

    try:
        suffix = uuid.uuid4().hex[:10]
        for index,kind,oid in ((1,"inet",869),(2,"cidr",650)):
            table = "inet_input_" + suffix + "_" + kind
            setup = query("CREATE TEMP TABLE " + table + "(id INTEGER PRIMARY KEY,v " + kind + ")")
            assert setup[1] is None,setup
            stored = []
            for ordinal,item in enumerate(CASES):
                value,canonical = item[0],item[index]
                state = "22P02" if canonical is None else None
                expect(kind + " cast " + value,"SELECT CAST(" + literal(value) + " AS " + kind + ")",
                       state,None if state else [[canonical]],None if state else [oid])
                parameter(kind,oid,value,canonical)
                expect(kind + " insert " + value,
                       "INSERT INTO " + table + " VALUES (" + str(ordinal) + "," + literal(value) + ")",
                       state,[])
                if canonical is not None:
                    stored.append([str(ordinal),canonical])
            expect(kind + " persisted complete rows","SELECT id,v FROM " + table + " ORDER BY id",
                   rows=stored,oids=[23,oid])
            initial = "192.0.2.1" if kind=="inet" else "192.0.2.0/24"
            updated = "10/8" if kind=="inet" else "0xc0000200/24"
            canonical = "10.0.0.0/8" if kind=="inet" else "192.0.2.0/24"
            invalid = "10/16" if kind=="inet" else "192.0.2.1/24"
            expect(kind+" update setup","INSERT INTO "+table+" VALUES (1000,"+literal(initial)+")",rows=[])
            expect(kind+" legal update","UPDATE "+table+" SET v="+literal(updated)+" WHERE id=1000",rows=[])
            expect(kind+" update value","SELECT v FROM "+table+" WHERE id=1000",rows=[[canonical]],oids=[oid])
            expect(kind+" rejected update","UPDATE "+table+" SET v="+literal(invalid)+" WHERE id=1000","22P02",[])
            expect(kind+" failed update retains value","SELECT v FROM "+table+" WHERE id=1000",rows=[[canonical]],oids=[oid])
            for value in ("10/8","010.1.2.3","2001:db8::/32"):
                canonical = next(item[index] for item in CASES if item[0]==value)
                parameter(kind,oid,value,canonical,binary=True)
            for source in ("integer","bigint","numeric","boolean","bytea","macaddr","macaddr8",'"char"'):
                expression = "NULL::"+source+"::"+kind
                expect(kind+" absent cast "+source,"SELECT "+expression,"42846",[])
                expect(kind+" absent cast zero rows "+source,"SELECT "+expression+" WHERE false","42846",[])
                expect(kind+" absent cast discarded branch "+source,
                       "SELECT CASE WHEN false THEN "+expression+" ELSE NULL::"+kind+" END","42846",[])
            for source in ("text","varchar","bpchar","name","inet","cidr"):
                expect(kind+" legal NULL source "+source,"SELECT NULL::"+source+"::"+kind,
                       rows=[[None]],oids=[oid])
            expect(kind+" integer value not address text","SELECT 10::"+kind,"42846",[])
            for source in ("text","varchar","bpchar","name"):
                expect(kind+" explicit string input "+source,
                       "SELECT '10.0.0.0/8'::"+source+"::"+kind,rows=[["10.0.0.0/8"]],oids=[oid])
            for ordinal,source in enumerate(("integer","bigint","numeric","boolean","bytea","macaddr",
                                             "macaddr8",'"char"',"text","varchar","bpchar","name")):
                expect(kind+" typed assignment NULL "+source,
                       "INSERT INTO "+table+" VALUES ("+str(2000+ordinal)+",NULL::"+source+")","42804",[])
            expect(kind+" typed integer assignment","INSERT INTO "+table+" VALUES (2100,10)","42804",[])
            expect(kind+" typed integer zero-row assignment",
                   "INSERT INTO "+table+" SELECT 2101,10 WHERE false","42804",[])
            expect(kind+" typed text zero-row update",
                   "UPDATE "+table+" SET v='10.0.0.0/8'::text WHERE false","42804",[])
            for source in ("inet","cidr"):
                expect(kind+" legal typed NULL assignment "+source,
                       "INSERT INTO "+table+" VALUES ("+str(2200+(source=="cidr"))+",NULL::"+source+")",rows=[])
            expect(kind+" assignment row count","SELECT count(*) FROM "+table,
                   rows=[[str(len(stored)+3)]],oids=[20])
            expect(kind+" invalid unknown zero rows","SELECT 'invalid'::"+kind+" WHERE false","22P02",[])
            expect(kind+" invalid unknown discarded branch",
                   "SELECT CASE WHEN false THEN 'invalid'::"+kind+" ELSE NULL::"+kind+" END","22P02",[])
            expect(kind+" legal unknown discarded branch",
                   "SELECT CASE WHEN false THEN '10/8'::"+kind+" ELSE NULL::"+kind+" END",rows=[[None]],oids=[oid])
            expect(kind+" qualified input","SELECT '10/8'::pg_catalog."+kind,rows=[["10.0.0.0/8"]],oids=[oid])
        print("INET_INPUT_COMPLETE",checks,"FAILURES",len(failures),flush=True)
        assert not failures,failures
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
