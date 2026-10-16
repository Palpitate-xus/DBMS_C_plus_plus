#!/usr/bin/env python3
"""MAC input grammar, EUI widths, exact error codes and genuine wire formats."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid

MAC6 = "08:00:2b:01:02:03"
MAC8 = "08:00:2b:01:02:03:04:05"
EXPANDED = "08:00:2b:ff:fe:01:02:03"
FORM6 = (MAC6, "08-00-2b-01-02-03", "08002b:010203", "08002b-010203",
         "0800.2b01.0203", "0800-2b01-0203", "08002b010203")
VALID6 = FORM6 + ("8:0:2b:1:2:3", " 08:00:2b:01:02:03 \t",
                  "08:00:2b: 01:02:03", "+8:0:2b:1:2:3", "0x08:0:2b:1:2:3")
VALID8 = (MAC8, "08-00-2b-01-02-03-04-05", "08002b:0102030405",
          "08002b-0102030405", "0800.2b01.0203.0405", "0800-2b01-0203-0405",
          "08002b01:02030405", "08002b0102030405", " " + MAC8 + " \t")
BAD6 = ("08:00-2b:01:02:03", "::08002b010203", "0:8002b010203",
        "08:00:2b:01:02:03:", "08::00:2b:01:02:03", "08:00:2b:01:02",
        "08:00:2b:01:02:03:04", "08002b010203Z")
BAD8 = ("08:00-2b:01:02:03:04:05", "::08002b0102030405", "0:8002b0102030405",
        "08::00:2b:01:02:03:04:05", "08:00:2b: 01:02:03:04:05", "8:0:2b:1:2:3",
        "08:00:2b:01:02:03:04", "08:00:2b:01:02:03:04:05:06")


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("mac_input_runner",root / "tests/compat/pg_diff_runner.py")
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
    checks, failures = 0, []

    def check(label,ok,observed):
        nonlocal checks
        checks += 1
        if not ok:
            failures.append((label,observed))
            print("MAC_INPUT_FAILURE",label,repr(observed),flush=True)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)

    def literal(value):
        return "'" + value.replace("'","''") + "'"

    def expect(label,sql,state=None,rows=None,oid=None):
        result = query(sql)
        ok = result[1] == state
        if state:
            ok = ok and result[0] == [] and result[4] is None
        else:
            if rows is not None: ok = ok and result[0] == rows
            if oid is not None: ok = ok and result[5] == [oid]
        check(label,ok,result)

    def parameter(kind,oid,value,state,canonical,input_oid=None):
        declared = oid if input_oid is None else input_oid
        parse = b"\0" + ("SELECT $1::" + kind).encode() + b"\0" + struct.pack("!HI",1,declared)
        raw = value.encode()
        bind = b"\0\0" + struct.pack("!HH",0,1) + struct.pack("!i",len(raw)) + raw + struct.pack("!H",0)
        sock.sendall(client.typed(b"P",parse) + client.typed(b"B",bind) +
                     client.typed(b"E",b"\0" + struct.pack("!I",0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages,include_types=True)
        check(kind + " parameter oid=" + str(declared) + " " + value,
              result[1] == state and (result[0] == [] if state else result[0] == [[canonical]]),result)

    def binary_parameter(label,raw,expected):
        parse = b"\0SELECT $1::macaddr8\0" + struct.pack("!HI",1,774)
        bind = (b"\0\0" + struct.pack("!HHH",1,1,1) + struct.pack("!i",len(raw)) + raw +
                struct.pack("!HH",1,1))
        sock.sendall(client.typed(b"P",parse) + client.typed(b"B",bind) +
                     client.typed(b"E",b"\0" + struct.pack("!I",0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for tag,body in messages if tag == b"E"]
        rows = [body for tag,body in messages if tag == b"D"]
        check(label,not errors and rows == [struct.pack("!Hi",1,len(expected)) + expected],(errors,rows))

    try:
        suffix = uuid.uuid4().hex[:10]
        for kind,oid,canonical,valid,bad in (
                ("macaddr",829,MAC6,VALID6,BAD6), ("macaddr8",774,MAC8,VALID8,BAD8)):
            table = "mac_input_" + suffix + "_" + kind
            result = query("CREATE TEMP TABLE " + table + "(id INTEGER,v " + kind + ")")
            assert result[1] is None, result
            samples = [(value,None,canonical) for value in valid]
            samples += [(value,"22P02",None) for value in bad]
            samples += [(value,"22003" if kind == "macaddr" else "22P02",None)
                        for value in ("100:00:2b:01:02:03","-1:00:2b:01:02:03")]
            if kind == "macaddr8":
                samples += [(value,None,EXPANDED) for value in FORM6]
                samples += [("08:00:2b:01:02:03:",None,EXPANDED),
                            ("08002b010203Z",None,EXPANDED),
                            ("08002b0102030405Z",None,MAC8)]
            expected_count = 0
            for index,(value,state,expected) in enumerate(samples):
                cast = "CAST(" + literal(value) + " AS " + kind + ")"
                label = kind + " " + repr(value)
                expect(label + " cast","SELECT " + cast,state,None if state else [[expected]],oid)
                expect(label + " unreachable unknown input",
                       "SELECT CASE WHEN true THEN NULL::" + kind + " ELSE " + cast + " END WHERE false",
                       state,[],oid)
                parameter(kind,oid,value,state,expected)
                expect(label + " insert","INSERT INTO " + table + " VALUES(" + str(index) + "," + literal(value) + ")",state)
                if state is None: expected_count += 1
                expect(label + " committed row count","SELECT count(*) FROM " + table,rows=[[str(expected_count)]],oid=20)
            expect(kind + " initial old value","SELECT v FROM " + table + " WHERE id=0",rows=[[canonical]],oid=oid)
            expect(kind + " rejected update","UPDATE " + table + " SET v=" + literal(bad[0]) + " WHERE id=0","22P02")
            expect(kind + " update old value retained","SELECT v FROM " + table + " WHERE id=0",rows=[[canonical]],oid=oid)
            expect(kind + " qualified builtin","SELECT CAST(" + literal(valid[0]) + " AS pg_catalog.\"" + kind + "\")",rows=[[canonical]],oid=oid)
            for source in ("1","NULL::integer"):
                expect(kind + " absent integer cast " + source,"SELECT CAST(" + source + " AS " + kind + ") WHERE false","42846")
            parameter(kind,oid,valid[0],None,canonical,25)
            parameter(kind,oid,bad[0],"22P02",None,25)
            parameter(kind,oid,"100:00:2b:01:02:03","22003" if kind=="macaddr" else "22P02",None,25)
            expect(kind + " typed NULL","SELECT CAST(NULL AS " + kind + ")",rows=[[None]],oid=oid)
            name_value = MAC6 if kind=="macaddr" else EXPANDED
            name_cast = "CAST(" + literal(MAC6) + "::name AS " + kind + ")"
            expect(kind+" explicit NAME value","SELECT "+name_cast,rows=[[name_value]],oid=oid)
            expect(kind+" explicit NAME NULL","SELECT NULL::name::"+kind,rows=[[None]],oid=oid)
            expect(kind+" qualified NAME source","SELECT "+literal(MAC6)+"::pg_catalog.name::"+kind,
                   rows=[[name_value]],oid=oid)
            expect(kind+" quoted qualified NAME source","SELECT "+literal(MAC6)+'::"pg_catalog"."name"::'+kind,
                   rows=[[name_value]],oid=oid)
            expect(kind+" quoted qualified NAME NULL",'SELECT NULL::"pg_catalog"."name"::'+kind,
                   rows=[[None]],oid=oid)
            expect(kind+" invalid qualified NAME content",'SELECT \'invalid\'::"pg_catalog"."name"::'+kind,"22P02")
            expect(kind+" qualified NAME cast target",
                   "SELECT CAST("+literal(MAC6)+"::name AS pg_catalog.\""+kind+"\")",rows=[[name_value]],oid=oid)
            expect(kind+" NAME zero rows","SELECT "+name_cast+" WHERE false",rows=[],oid=oid)
            expect(kind+" discarded NAME branch",
                   "SELECT CASE WHEN false THEN "+name_cast+" ELSE NULL::"+kind+" END",rows=[[None]],oid=oid)
            expect(kind+" invalid NAME content","SELECT 'invalid'::name::"+kind,"22P02")
            parameter(kind,oid,MAC6,None,name_value,19)
            parameter(kind,oid,"invalid","22P02",None,19)
            parameter(kind,oid,"100:00:2b:01:02:03","22003" if kind=="macaddr" else "22P02",None,19)
            expect(kind+" NAME not implicit assignment",
                   "INSERT INTO "+table+" VALUES(10000,"+literal(MAC6)+"::name)","42804")
            expect(kind+" NAME NULL not implicit assignment",
                   "INSERT INTO "+table+" VALUES(10001,NULL::name)","42804")
            expect(kind+" explicit NAME assignment","INSERT INTO "+table+" VALUES(10002,"+name_cast+")")
            expect(kind+" stored NAME bytes","SELECT v FROM "+table+" WHERE id=10002",rows=[[name_value]],oid=oid)
            expect(kind+" failed NAME assignments absent",
                   "SELECT count(*) FROM "+table+" WHERE id IN(10000,10001)",rows=[["0"]],oid=20)
            expect(kind+" absent BYTEA cast retained","SELECT NULL::bytea::"+kind+" WHERE false","42846")
            for operation,expected in (("=","t"),("<>","f"),("<=","t"),(">=","t"),("<","f"),(">","f")):
                value = literal(canonical) + "::" + kind
                expect(kind + " same-type operator " + operation,"SELECT " + value + operation + value,rows=[[expected]],oid=16)
            if kind == "macaddr":
                expect("implicit EUI64 assignment","INSERT INTO " + table + " VALUES(900," + literal(EXPANDED) + "::macaddr8)")
                expect("implicit EUI64 assignment bytes","SELECT v FROM " + table + " WHERE id=900",rows=[[MAC6]],oid=829)
                expect("implicit EUI64 assignment range","INSERT INTO " + table + " VALUES(901," + literal(MAC8) + "::macaddr8)","22003")
                expect("implicit assignment failure absent","SELECT count(*) FROM " + table + " WHERE id=901",rows=[["0"]],oid=20)
            else:
                expect("implicit MAC48 assignment","INSERT INTO " + table + " VALUES(900," + literal(MAC6) + "::macaddr)")
                expect("implicit MAC48 assignment bytes","SELECT v FROM " + table + " WHERE id=900",rows=[[EXPANDED]],oid=774)
            source_kind = "macaddr8" if kind=="macaddr" else "macaddr"
            source_value = EXPANDED if kind=="macaddr" else MAC6
            assigned_value = MAC6 if kind=="macaddr" else EXPANDED
            source = literal(source_value) + "::" + source_kind
            expect(kind + " INSERT SELECT constant conversion","INSERT INTO " + table + " SELECT 902," + source)
            expect(kind + " INSERT SELECT constant bytes","SELECT v FROM " + table + " WHERE id=902",rows=[[assigned_value]],oid=oid)
            source_table = "mac_source_" + suffix + "_" + kind
            setup = query("CREATE TEMP TABLE " + source_table + "(v " + source_kind + ")")
            assert setup[1] is None, setup
            setup = query("INSERT INTO " + source_table + " VALUES(" + literal(source_value) + ")")
            assert setup[1] is None, setup
            expect(kind + " INSERT SELECT physical column conversion","INSERT INTO " + table + " SELECT 904,v FROM " + source_table)
            expect(kind + " INSERT SELECT physical column bytes","SELECT v FROM " + table + " WHERE id=904",rows=[[assigned_value]],oid=oid)
            expect(kind + " UPDATE width conversion","UPDATE " + table + " SET v=" + source + " WHERE id=900")
            expect(kind + " UPDATE converted bytes","SELECT v FROM " + table + " WHERE id=900",rows=[[assigned_value]],oid=oid)
            if kind=="macaddr":
                expect("UPDATE non-EUI48 range","UPDATE " + table + " SET v=" + literal(MAC8) + "::macaddr8 WHERE id=900","22003")
                expect("UPDATE range failure old bytes","SELECT v FROM " + table + " WHERE id=900",rows=[[MAC6]],oid=829)

        expect("real MAC48 to EUI64","SELECT CAST(" + literal(MAC6) + "::macaddr AS macaddr8)",rows=[[EXPANDED]],oid=774)
        expect("real EUI64 to MAC48","SELECT CAST(" + literal(EXPANDED) + "::macaddr8 AS macaddr)",rows=[[MAC6]],oid=829)
        expect("non-EUI48 conversion range","SELECT CAST(" + literal(MAC8) + "::macaddr8 AS macaddr)","22003")
        expect("CASE chooses actual MAC64 common type",
               "SELECT CASE WHEN true THEN " + literal(MAC6) + "::macaddr ELSE " + literal(EXPANDED) + "::macaddr8 END",
               rows=[[EXPANDED]],oid=774)
        expect("CASE chooses actual MAC48 common type",
               "SELECT CASE WHEN true THEN " + literal(EXPANDED) + "::macaddr8 ELSE " + literal(MAC6) + "::macaddr END",
               rows=[[MAC6]],oid=829)
        expect("COALESCE width conversion","SELECT COALESCE(NULL::macaddr," + literal(EXPANDED) + "::macaddr8)",rows=[[MAC6]],oid=829)
        expect("VALUES width conversion","VALUES(" + literal(MAC6) + "::macaddr),(" + literal(EXPANDED) + "::macaddr8)",rows=[[MAC6],[MAC6]],oid=829)
        for left,right in (("macaddr","macaddr8"),("macaddr8","macaddr")):
            for operation in ("=","<>","<",">","<=",">="):
                a = literal(MAC6 if left == "macaddr" else EXPANDED) + "::" + left
                b = literal(MAC6 if right == "macaddr" else EXPANDED) + "::" + right
                expect("ambiguous " + left + operation + right,"SELECT " + a + operation + b + " WHERE false","42725")
        mac48 = bytes.fromhex("08002b010203")
        mac64 = bytes.fromhex("08002bfffe010203")
        binary_parameter("binary EUI48 accepted as EUI64",mac48,mac64)
        binary_parameter("binary EUI64 unchanged",mac64,mac64)
        print("MAC_INPUT_COMPLETE",checks,"FAILURES",len(failures),flush=True)
        assert not failures, failures
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__ == "__main__":
    main()
