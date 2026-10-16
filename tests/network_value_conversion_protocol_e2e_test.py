#!/usr/bin/env python3
"""Typed INET/CIDR conversions, common values, assignments and wire bytes."""
import importlib.util
import ipaddress
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("network_conversion_runner",root / "tests/compat/pg_diff_runner.py")
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
            print("NETWORK_CONVERSION_FAILURE",label,repr(observed),flush=True)

    def expect(label,sql,rows=None,oids=None,state=None):
        result = runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        ok = result[1] == state
        if state:
            ok = ok and result[0] == [] and result[4] is None
        else:
            if rows is not None: ok = ok and result[0] == rows
            if oids is not None: ok = ok and result[5] == oids
        check(label,ok,result)

    def parameter(raw,canonical,input_binary,output_binary):
        source = ipaddress.ip_interface(raw) if raw is not None else None
        if source is None:
            payload = b""
        elif input_binary:
            address = source.ip.packed
            payload = bytes([2 if source.version==4 else 3,source.network.prefixlen,0,len(address)]) + address
        else:
            payload = raw.encode()
        parse = b"\0SELECT $1::cidr\0" + struct.pack("!HI",1,869)
        bind = (b"\0\0" + struct.pack("!HHH",1,int(input_binary),1) +
                struct.pack("!i",-1 if source is None else len(payload)) + payload +
                struct.pack("!HH",1,int(output_binary)))
        sock.sendall(client.typed(b"P",parse) + client.typed(b"B",bind) +
                     client.typed(b"D",b"P\0") + client.typed(b"E",b"\0"+struct.pack("!I",0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        label = "INET parameter to CIDR " + repr(raw) + " formats " + str((input_binary,output_binary))
        if output_binary:
            if source is None:
                expected = struct.pack("!Hi",1,-1)
            else:
                network = source.network
                address = network.network_address.packed
                wire = bytes([2 if source.version==4 else 3,network.prefixlen,1,len(address)]) + address
                expected = struct.pack("!Hi",1,len(wire)) + wire
            errors = [client.diagnostic_fields(body) for tag,body in messages if tag==b"E"]
            rows = [body for tag,body in messages if tag==b"D"]
            fields = client.row_description_fields(messages) if any(tag==b"T" for tag,_ in messages) else []
            check(label,not errors and rows==[expected] and len(fields)==1 and
                  fields[0][3]==650 and fields[0][6]==1,(errors,rows,fields))
        else:
            result = runner.decode_wire_result(messages,include_types=True)
            check(label,result[1] is None and result[0]==[[canonical]] and result[5]==[650],result)

    try:
        # Every valid prefix, including non-byte masks and zero/full width.
        # These exact values were independently checked against actual 180006.
        for address in ("203.0.113.129","2001:db8:abcd:1234:fe80:dead:beef:1235"):
            maximum = ipaddress.ip_address(address).max_prefixlen
            for prefix in range(maximum+1):
                value = address+"/"+str(prefix)
                network = ipaddress.ip_network(value,strict=False)
                cidr = str(network)
                inet = str(network.network_address) if prefix==maximum else cidr
                expect("INET to CIDR "+value,"SELECT '"+value+"'::inet::cidr",[[cidr]],[650])
                expect("COALESCE CIDR to INET "+value,
                       "SELECT COALESCE(NULL::inet,'"+cidr+"'::cidr)",[[inet]],[869])
                expect("VALUES common INET "+value,
                       "VALUES('"+cidr+"'::cidr),(NULL::inet)",[[inet],[None]],[869])
        expect("NULL INET to CIDR","SELECT NULL::inet::cidr",[[None]],[650])
        expect("NULL CIDR to INET","SELECT NULL::cidr::inet",[[None]],[869])
        expect("CASE existing positive",
               "SELECT CASE WHEN true THEN '192.0.2.129/32'::cidr ELSE NULL::inet END",[["192.0.2.129"]],[869])
        expect("COALESCE first INET positive",
               "SELECT COALESCE('192.0.2.129/24'::inet,'192.0.2.0/24'::cidr)",[["192.0.2.129/24"]],[869])
        expect("COALESCE all NULL common INET","SELECT COALESCE(NULL::cidr,NULL::inet)",[[None]],[869])
        expect("COALESCE all NULL CIDR","SELECT COALESCE(NULL::cidr,NULL::cidr)",[[None]],[650])
        expect("COALESCE CIDR first IPv4",
               "SELECT COALESCE('192.0.2.129/32'::cidr,NULL::inet)",[["192.0.2.129"]],[869])
        expect("COALESCE CIDR first IPv6",
               "SELECT COALESCE('2001:db8::1234/128'::cidr,NULL::inet)",[["2001:db8::1234"]],[869])
        expect("COALESCE UNKNOWN INET","SELECT COALESCE(NULL::inet,'192.0.2.129')",[["192.0.2.129"]],[869])
        expect("COALESCE UNKNOWN CIDR","SELECT COALESCE(NULL::cidr,'192.0.2.0/24')",[["192.0.2.0/24"]],[650])
        for expression in ("COALESCE('192.0.2.129'::inet,'invalid')",
                           "COALESCE(NULL::inet,'invalid')"):
            expect("COALESCE bad UNKNOWN","SELECT "+expression,[],state="22P02")
        expect("COALESCE bad UNKNOWN zero rows",
               "SELECT COALESCE('192.0.2.129'::inet,'invalid') WHERE false",[],state="22P02")
        expect("COALESCE bad UNKNOWN discarded branch",
               "SELECT CASE WHEN false THEN COALESCE(NULL::inet,'invalid') ELSE NULL::inet END",[],state="22P02")
        expect("COALESCE NULL reverse order","SELECT COALESCE(NULL::inet,NULL::cidr)",[[None]],[869])
        for kind in ("inet","cidr"):
            for source in ("text","name","integer","macaddr","boolean"):
                expression = "COALESCE(NULL::"+kind+",NULL::"+source+")"
                expect("COALESCE absent implicit input "+kind+"/"+source,"SELECT "+expression,[],state="42804")
                expect("COALESCE absent input zero rows "+kind+"/"+source,
                       "SELECT "+expression+" WHERE false",[],state="42804")
                expect("COALESCE absent input discarded branch "+kind+"/"+source,
                       "SELECT CASE WHEN false THEN "+expression+" ELSE NULL::"+kind+" END",[],state="42804")
        for raw,canonical in (("192.0.2.129/24","192.0.2.0/24"),
                              ("192.0.2.129/25","192.0.2.128/25"),
                              ("255.255.255.255/0","0.0.0.0/0"),
                              ("2001:db8::1235/127","2001:db8::1234/127"),
                              ("2001:db8::1234/0","::/0"),
                              ("::ffff:192.0.2.129/120","::ffff:192.0.2.0/120")):
            for input_binary in (False,True):
                for output_binary in (False,True):
                    parameter(raw,canonical,input_binary,output_binary)
        for input_binary in (False,True):
            parameter(None,None,input_binary,True)
        cases = (("inet","cidr","192.0.2.129/24","192.0.2.0/24"),
                 ("inet","cidr","2001:db8::1234/64","2001:db8::/64"),
                 ("cidr","inet","192.0.2.129/32","192.0.2.129"),
                 ("cidr","inet","2001:db8::1234/128","2001:db8::1234"))
        for source,target,raw,canonical in cases:
            suffix = uuid.uuid4().hex[:10]
            table,inputs = "network_value_t_"+suffix,"network_value_s_"+suffix
            oid = 650 if target=="cidr" else 869
            value = "'"+raw+"'::"+source
            expect("target setup",f"CREATE TEMP TABLE {table}(id INTEGER PRIMARY KEY,v {target})",[])
            expect("source setup",f"CREATE TEMP TABLE {inputs}(id INTEGER PRIMARY KEY,v {source})",[])
            expect("source row",f"INSERT INTO {inputs} VALUES(1,{value})",[])
            expect("VALUES assignment",f"INSERT INTO {table} VALUES(1,{value}) RETURNING v",[[canonical]],[oid])
            expect("direct SELECT assignment",f"INSERT INTO {table} SELECT 2,{value} RETURNING v",[[canonical]],[oid])
            expect("column SELECT assignment",f"INSERT INTO {table} SELECT id+100,v FROM {inputs} RETURNING v",
                   [[canonical]],[oid])
            expect("update seed",f"INSERT INTO {table} VALUES(3,'10.0.0.0/8'::{target})",[])
            expect("UPDATE assignment",f"UPDATE {table} SET v={value} WHERE id=3 RETURNING v",[[canonical]],[oid])
            expect("conflict assignment",
                   f"INSERT INTO {table} VALUES(3,'10.0.0.0/8'::{target}) ON CONFLICT(id) DO UPDATE SET v={value} RETURNING v",
                   [[canonical]],[oid])
            expect("update reset",f"UPDATE {table} SET v='10.0.0.0/8'::{target} WHERE id=3",[])
            expect("UPDATE FROM assignment",
                   f"UPDATE {table} SET v=s.v FROM {inputs} s WHERE {table}.id=3 AND s.id=1 RETURNING {table}.v",
                   [[canonical]],[oid])
            expect("TEXT NULL not assignment",f"INSERT INTO {table} VALUES(2000,NULL::text)",[],state="42804")
            expect("TEXT zero-row not assignment",f"INSERT INTO {table} SELECT 2001,NULL::text WHERE false",[],state="42804")
            expect("TEXT zero-row update not assignment",f"UPDATE {table} SET v=NULL::text WHERE false",[],state="42804")
            expect("invalid later row atomic input",
                   f"INSERT INTO {table} VALUES(2001,{value}),(2002,'invalid'::{source})",[],state="22P02")
            expect("complete persisted values",f"SELECT id,v FROM {table} ORDER BY id",
                   [[str(key),canonical] for key in (1,2,3,101)],[23,oid])
        for initial,incoming,canonical in (("192.0.2.129/24","192.0.2.130/25","192.0.2.128/25"),
                                           ("2001:db8::1234/64","2001:db8::1235/127","2001:db8::1234/127")):
            table = "network_excluded_"+uuid.uuid4().hex[:10]
            expect("EXCLUDED target setup",f"CREATE TEMP TABLE {table}(id INTEGER PRIMARY KEY,h INET,v CIDR)",[])
            expect("EXCLUDED seed",f"INSERT INTO {table} VALUES(1,'{initial}','10.0.0.0/8')",[])
            expect("EXCLUDED actual typed INET assignment",
                   f"INSERT INTO {table} VALUES(1,'{incoming}','10.0.0.0/8') ON CONFLICT(id) DO UPDATE SET v=excluded.h RETURNING v,h",
                   [[canonical,initial]],[650,869])
            expect("EXCLUDED typed NULL assignment",
                   f"INSERT INTO {table} VALUES(1,NULL,'10.0.0.0/8') ON CONFLICT(id) DO UPDATE SET v=excluded.h RETURNING v,h",
                   [[None,initial]],[650,869])
            expect("EXCLUDED untaken assignment",
                   f"INSERT INTO {table} VALUES(2,'{incoming}','{canonical}') ON CONFLICT(id) DO UPDATE SET v=excluded.h RETURNING v,h",
                   [[canonical,incoming]],[650,869])
            for source in ("text","name","integer"):
                for key in (1,3):
                    expect("conflict typed input rejected before taken/untaken path "+source+"/"+str(key),
                           f"INSERT INTO {table} VALUES({key},'{initial}','10.0.0.0/8') ON CONFLICT(id) DO UPDATE SET v=NULL::{source}",
                           [],state="42804")
            for key in (1,3):
                expect("conflict bad UNKNOWN rejected before taken/untaken path "+str(key),
                       f"INSERT INTO {table} VALUES({key},'{initial}','10.0.0.0/8') ON CONFLICT(id) DO UPDATE SET v='192.0.2.129/24'",
                       [],state="22P02")
            expect("EXCLUDED complete stored rows",f"SELECT id,v,h FROM {table} ORDER BY id",
                   [["1",None,initial],["2",canonical,incoming]],[23,650,869])
        print("NETWORK_CONVERSION_COMPLETE",checks,"FAILURES",len(failures),flush=True)
        assert not failures,failures
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__ == "__main__":
    main()
