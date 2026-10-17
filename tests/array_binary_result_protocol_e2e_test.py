#!/usr/bin/env python3
"""Array binary result envelopes use real element OIDs and element send codecs."""
import importlib.util
import ipaddress
import socket
import struct
import sys
import uuid
from pathlib import Path


def binary_array(element_oid, elements, dimensions=None):
    if dimensions is None:
        dimensions = [(len(elements), 1)] if elements else []
    result = struct.pack("!iiI", len(dimensions), int(None in elements), element_oid)
    for length, lower in dimensions:
        result += struct.pack("!ii", length, lower)
    for value in elements:
        result += struct.pack("!i", -1 if value is None else len(value)) + (value or b"")
    return result


def numeric(value):
    special = {"NaN": 0xC000, "Infinity": 0xD000, "-Infinity": 0xF000}
    if value in special:
        return struct.pack("!hhHh", 0, 0, special[value], 0)
    negative = value.startswith("-")
    value = value.lstrip("+-")
    integer, _, fraction = value.partition(".")
    integer = integer.lstrip("0") or "0"
    integer = "0" * (-len(integer) % 4) + integer
    digits = integer + fraction + "0" * (-len(fraction) % 4)
    groups = [int(digits[i:i + 4]) for i in range(0, len(digits), 4)]
    weight = len(integer) // 4 - 1
    while groups and groups[0] == 0:
        groups.pop(0)
        weight -= 1
    while groups and groups[-1] == 0:
        groups.pop()
    if not groups:
        weight, negative = 0, False
    return struct.pack("!hhHh", len(groups), weight, 0x4000 if negative else 0, len(fraction)) + b"".join(
        struct.pack("!H", group) for group in groups)


def network(value, cidr=False):
    address, _, prefix = value.partition("/")
    address = ipaddress.ip_address(address)
    width = 32 if address.version == 4 else 128
    return bytes((2 if width == 32 else 3, int(prefix or width), int(cidr), len(address.packed))) + address.packed


def examples():
    pack = struct.pack
    return [
        (1000, 16, "{t,NULL,f}", [b"\x01", None, b"\x00"]),
        (1005, 21, "{-32768,NULL,32767}", [pack("!h", -32768), None, pack("!h", 32767)]),
        (1007, 23, "{1,NULL,-2}", [pack("!i", 1), None, pack("!i", -2)]),
        (1016, 20, "{-9223372036854775808,NULL,9223372036854775807}",
         [pack("!q", -(2**63)), None, pack("!q", 2**63 - 1)]),
        (1231, 1700, "{1.00,NULL,2,1.0}", [numeric("1.00"), None, numeric("2"), numeric("1.0")]),
        (1021, 700, "{1.25,NULL,-0,Infinity,-Infinity}",
         [pack("!f", 1.25), None, pack("!f", -0.0), pack("!f", float("inf")), pack("!f", -float("inf"))]),
        (1022, 701, "{1.25,NULL,-0,Infinity,-Infinity}",
         [pack("!d", 1.25), None, pack("!d", -0.0), pack("!d", float("inf")), pack("!d", -float("inf"))]),
        (1009, 25, r'{NULL,"NULL","","中文","a,b","a\\b","a\"b","{nested}"}',
         [None, b"NULL", b"", "中文".encode(), b"a,b", b"a\\b", b'a"b', b"{nested}"]),
        (1015, 1043, '{abc,NULL,"NULL"}', [b"abc", None, b"NULL"]),
        (1014, 1042, '{abc,NULL,"NULL"}', [b"abc", None, b"NULL"]),
        (1001, 17, r'{"\\x0001ff",NULL,"\\x"}', [bytes.fromhex("0001ff"), None, b""]),
        (2951, 2950, "{00000000-0000-0000-0000-000000000001,NULL}", [bytes.fromhex("00" * 15 + "01"), None]),
        (1182, 1082, "{2000-01-01,NULL,1999-12-31}", [pack("!i", 0), None, pack("!i", -1)]),
        (1183, 1083, "{12:34:56,NULL,00:00:00}", [pack("!q", 45296 * 1000000), None, pack("!q", 0)]),
        (1115, 1114, '{"2000-01-01 00:00:01",NULL,infinity,-infinity}',
         [pack("!q", 1000000), None, pack("!q", 2**63 - 1), pack("!q", -(2**63))]),
        (1185, 1184, '{"2000-01-01 00:00:01+00",NULL,infinity,-infinity}',
         [pack("!q", 1000000), None, pack("!q", 2**63 - 1), pack("!q", -(2**63))]),
        (1561, 1560, "{1,NULL,0}", [pack("!i", 1) + b"\x80", None, pack("!i", 1) + b"\x00"]),
        (1563, 1562, '{10101,NULL,""}', [pack("!i", 5) + b"\xa8", None, pack("!i", 0)]),
        (1041, 869, "{192.0.2.1/24,NULL,2001:db8::1/64}", [network("192.0.2.1/24"), None, network("2001:db8::1/64")]),
        (651, 650, "{192.0.2.0/24,NULL,2001:db8::/64}", [network("192.0.2.0/24", True), None, network("2001:db8::/64", True)]),
        (1040, 829, "{08:00:2b:01:02:03,NULL}", [bytes.fromhex("08002b010203"), None]),
        (775, 774, "{08:00:2b:ff:fe:01:02:03,NULL}", [bytes.fromhex("08002bfffe010203"), None]),
        (791, 790, "{1.23,NULL,-2.50}", [pack("!q", 123), None, pack("!q", -250)]),
        (1017, 600, '{"(1,2)",NULL,"(-3,4)"}', [pack("!dd", 1, 2), None, pack("!dd", -3, 4)]),
        (1018, 601, '{"[(1,2),(3,4)]",NULL}', [pack("!dddd", 1, 2, 3, 4), None]),
        (1019, 602, '{"[(1,2),(3,4)]",NULL}', [b"\x00" + pack("!i", 2) + pack("!dddd", 1, 2, 3, 4), None]),
        # A single box has no inter-element delimiter; multi-box text delimiter
        # compatibility is a separate pre-existing input issue, not this fix.
        (1020, 603, '{"(1,2),(3,4)"}', [pack("!dddd", 3, 4, 1, 2)]),
        (1027, 604, '{"((1,2),(3,4),(0,0))",NULL}', [pack("!i", 3) + pack("!dddddd", 1, 2, 3, 4, 0, 0), None]),
        (629, 628, '{"{1,2,3}",NULL}', [pack("!ddd", 1, 2, 3), None]),
        (719, 718, '{"<(1,2),3>",NULL}', [pack("!ddd", 1, 2, 3), None]),
    ]


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("array_binary_result_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []

    def simple(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def run(sql, oids, values, result_oids, expected, tag="SELECT 1", formats=None):
        nonlocal checks
        checks += 1
        if formats is None:
            formats = [1] * len(result_oids)
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids))
        parse += b"".join(struct.pack("!I", oid) for oid in oids)
        bind = b"\0\0" + struct.pack("!HH", 0, len(values))
        for value in values:
            raw = None if value is None else value.encode()
            bind += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        bind += struct.pack("!H", len(formats)) + b"".join(struct.pack("!H", code) for code in formats)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                     client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        valid = (not errors and rows == expected and [field[3] for field in fields] == result_oids and
                 [field[6] for field in fields] == formats and tags == [tag] and
                 sum(kind == b"1" for kind, _ in messages) == 1 and
                 sum(kind == b"2" for kind, _ in messages) == 1 and messages[-1][0] == b"Z")
        print("ARRAY_BINARY_RESULT", checks, sql, oids, "match", valid, "errors", errors, flush=True)
        if not valid:
            failures.append((sql, oids, rows, expected, fields, tags, errors))

    try:
        if reference:
            simple("BEGIN")
            schema = "array_binary_" + uuid.uuid4().hex[:12]
            simple("CREATE SCHEMA " + schema)
            simple("SET search_path TO " + schema)
        simple("SET lc_monetary TO 'C'")
        for array_oid, element_oid, value, elements in examples():
            run("SELECT $1 AS a", [array_oid], [value], [array_oid], [[binary_array(element_oid, elements)]])
            run("SELECT $1 AS a", [array_oid], ["{}"], [array_oid], [[binary_array(element_oid, [])]])
            run("SELECT $1 AS a", [array_oid], [None], [array_oid], [[None]])
        shapes = [
            (1007, 23, "[-2:1]={1,NULL,1,2}", [struct.pack("!i", 1), None, struct.pack("!i", 1), struct.pack("!i", 2)], [(4, -2)]),
            (1007, 23, "[0:1][-2:-1]={{1,NULL},{3,4}}", [struct.pack("!i", 1), None, struct.pack("!i", 3), struct.pack("!i", 4)], [(2, 0), (2, -2)]),
            (1231, 1700, "{{1.00,NULL},{-0.0020,100000000000000000001.00}}",
             [numeric("1.00"), None, numeric("-0.0020"), numeric("100000000000000000001.00")], [(2, 1), (2, 1)]),
            (1009, 25, '{{{"a,b","NULL"}},{{NULL,""}}}', [b"a,b", b"NULL", None, b""], [(2, 1), (1, 1), (2, 1)]),
            (1007, 23, "{{{{{{1}}}}}}", [struct.pack("!i", 1)], [(1, 1)] * 6),
            (1007, 23, "{NULL,NULL}", [None, None], [(2, 1)]),
            (1007, 23, "{{NULL,NULL},{NULL,NULL}}", [None] * 4, [(2, 1), (2, 1)]),
            (1007, 23, "[2147483645:2147483646]={1,2}", [struct.pack("!i", 1), struct.pack("!i", 2)], [(2, 2147483645)]),
            (1007, 23, "[-2147483648:-2147483647]={1,2}", [struct.pack("!i", 1), struct.pack("!i", 2)], [(2, -2147483648)]),
        ]
        for array_oid, element_oid, value, elements, dimensions in shapes:
            run("SELECT $1 AS a", [array_oid], [value], [array_oid], [[binary_array(element_oid, elements, dimensions)]])
        integer = binary_array(23, [struct.pack("!i", 1), None])
        text = binary_array(25, [b"NULL", None, b""])
        run("SELECT $1 AS a,$2 AS t,$1 AS again", [1007, 1009], ["{1,NULL}", '{"NULL",NULL,""}'],
            [1007, 1009, 1007], [[integer, b'{"NULL",NULL,""}', integer]], formats=[1, 0, 1])
        run("SELECT $1 AS a,$2 AS t", [1007, 1009], ["{1,NULL}", '{"NULL",NULL,""}'], [1007, 1009], [[integer, text]])
        run("SELECT $1 AS a WHERE false", [1007], ["{1,NULL}"], [1007], [], "SELECT 0")
        simple("CREATE TABLE array_binary_rows(id INTEGER PRIMARY KEY,a INTEGER[],t TEXT[])")
        run("INSERT INTO array_binary_rows VALUES(1,$1,$2) RETURNING a,t", [1007, 1009],
            ["{1,NULL}", '{"NULL",NULL,""}'], [1007, 1009], [[integer, text]], "INSERT 0 1")
        run("SELECT a AS t,t AS a FROM array_binary_rows WHERE id=1", [], [], [1007, 1009], [[integer, text]])
        bounded = binary_array(23, [struct.pack("!i", 7), None], [(2, -3)])
        run("UPDATE array_binary_rows SET a=$1 WHERE id=1 RETURNING a", [1007], ["[-3:-2]={7,NULL}"], [1007], [[bounded]], "UPDATE 1")
        run("SELECT a FROM array_binary_rows WHERE id=1", [], [], [1007], [[bounded]])
        for oid, value, expected in [(23, "-7", struct.pack("!i", -7)), (1700, "1.00", numeric("1.00")),
                                     (25, "NULL", b"NULL"), (16, "t", b"\x01")]:
            run("SELECT $1 AS scalar", [oid], [value], [oid], [[expected]])
        assert checks == 110, checks
        assert not failures, ("array binary result failures", failures)
        print("[ARRAY BINARY RESULT] all 110 controls passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
