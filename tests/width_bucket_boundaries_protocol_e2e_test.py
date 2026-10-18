#!/usr/bin/env python3
"""Four-argument width_bucket: directional edges, exact decimals and int4 I/O."""
from fractions import Fraction
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def cases():
    output = []
    for kind in ("integer", "numeric", "double precision"):
        for count in (1, 5, 2147483647):
            for low, high in ((0, 10), (10, 0)):
                for value in (-1, 0, 1, 2, 4, 5, 6, 8, 9, 10, 11):
                    before = value < low if low < high else value > low
                    after = value >= high if low < high else value <= high
                    state = None
                    if before:
                        bucket = 0
                    elif after:
                        bucket = count + 1
                        if bucket > 2147483647:
                            state = "22003"
                    elif kind == "numeric":
                        bucket = int(Fraction(value-low, high-low) * count) + 1
                    else:
                        ratio = ((float(value)-low)/(high-low) if low < high else
                                 (low-float(value))/(low-high))
                        bucket = min(int(count * ratio), count-1) + 1
                    arguments = [str(v)+"::"+kind for v in (value, low, high)] + [str(count)]
                    output.append((arguments, None if state else str(bucket), state))
    for kind in ("numeric", "double precision"):
        cast = lambda value: "'"+value+"'::"+kind
        for arguments, result, state in (
            (("0.3", "0", "1", "10"), "4", None),
            (("9", "10", "0", "10"), "2", None),
            (("Infinity", "10", "0", "5"), "0", None),
            (("-Infinity", "10", "0", "5"), "6", None),
            (("Infinity", "0", "10", "5"), "6", None),
            (("-Infinity", "0", "10", "5"), "0", None),
            (("NaN", "0", "10", "5"), None, "2201G"),
            (("1", "NaN", "10", "5"), None, "2201G"),
            (("1", "0", "NaN", "5"), None, "2201G"),
            (("1", "-Infinity", "10", "5"), None, "2201G"),
            (("1", "0", "Infinity", "5"), None, "2201G"),
            (("1", "1", "1", "5"), None, "2201G"),
            (("1", "0", "10", "0"), None, "2201G"),
            (("1", "0", "10", "-2147483648"), None, "2201G"),
        ):
            output.append(([cast(v) for v in arguments[:3]]+[arguments[3]], result, state))
        for index in range(4):
            arguments = [cast(v) for v in ("1", "0", "10")]+["5"]
            arguments[index] = "NULL::"+("integer" if index == 3 else kind)
            output.append((arguments, None, None))
    for values, result in (
        (("9007199254740993", "9007199254740992", "9007199254740994", "2"), "2"),
        (("9007199254740993", "9007199254740994", "9007199254740992", "2"), "2"),
        (("1e-400", "0", "2e-400", "2"), "2"),
        (("1e400", "0", "2e400", "2"), "2"),
        (("-1e400", "-2e400", "0", "2"), "2"),
        (("0", "-1e999", "1e999", "10"), "6"),
        (("0", "1e999", "-1e999", "10"), "6"),
        (("0", "-1e-999", "1e999", "10"), "1"),
        (("1e-999", "0", "2e-999", "2"), "2"),
        (("0.9999999999999999999999999999999999999999", "0", "1", "10"), "10"),
    ):
        output.append(([v+"::numeric" for v in values[:3]]+[values[3]], result, None))
    for values, result in (
        (("0", "-1e308", "1e308", "10"), "6"),
        (("0", "1e308", "-1e308", "10"), "6"),
        (("4e-324", "0", "1e-323", "2"), "2"),
        (("0.9999999999999999", "0", "1", "2147483647"), "2147483647"),
        (("-0", "0", "10", "5"), "1"),
    ):
        output.append((["'"+v+"'::double precision" for v in values[:3]]+[values[3]], result, None))
    return output


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("width_bucket_runner", root/"tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    controls, failures = 0, []
    ready = b"I"

    def record(label, good, detail):
        nonlocal controls
        controls += 1
        print("WIDTH_BUCKET_PROTOCOL", controls, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((controls, label, detail))

    def execute(mode, sql, parameters=None, binary_input=False):
        if mode == "simple" and parameters is None:
            return client.simple_query(sock, sql)
        parameters = [] if parameters is None else parameters
        parse = b"\0"+sql.encode()+b"\0"+struct.pack("!H", len(parameters))
        parse += b"".join(struct.pack("!I", oid) for oid, _ in parameters)
        bind = b"\0\0"+struct.pack("!HHH", 1, int(binary_input), len(parameters))
        for _, value in parameters:
            bind += struct.pack("!i", -1 if value is None else len(value))+(value or b"")
        bind += struct.pack("!HH", 1, int(mode == "binary"))
        sock.sendall(client.typed(b"P", parse)+client.typed(b"D", b"S\0")+
                     client.typed(b"B", bind)+client.typed(b"D", b"P\0")+
                     client.typed(b"E", b"\0"+struct.pack("!I", 0))+client.typed(b"S"))
        return client.read_until_ready(sock)

    def query(mode, sql, values, state=None, parameters=None, binary_input=False):
        messages = execute(mode, sql, parameters, binary_input)
        errors = [client.diagnostic_fields(body).get(b"C", b"").decode()
                  for kind, body in messages if kind == b"E"]
        rows = client.data_row_values(messages)
        tags = [body[:-1] for kind, body in messages if kind == b"C"]
        descriptions = [client.row_description_fields([message])
                        for message in messages if message[0] == b"T"]
        if state:
            good = errors == [state] and not rows and not tags
        else:
            expected = [[None if value is None else
                         struct.pack("!i", int(value)) if mode == "binary" else str(value).encode()]
                        for value in values]
            wanted_descriptions = 1 if mode == "simple" and parameters is None else 2
            good = not errors and rows == expected and tags == [("SELECT "+str(len(values))).encode()]
            good = good and len(descriptions) == wanted_descriptions
            for index, fields in enumerate(descriptions):
                fmt = int(mode == "binary" and index == 1)
                good = good and [(f[0], f[3], f[4], f[5], f[6]) for f in fields] == [
                    (b"v", 23, 4, -1, fmt)]
        good = good and messages[-1] == (b"Z", ready)
        record((mode, sql, parameters, binary_input), good,
               (errors, rows, tags, [[(f[0], f[3], f[4], f[5], f[6]) for f in fs] for fs in descriptions], messages[-1]))

    def command(sql, state=None):
        messages = client.simple_query(sock, sql)
        errors = [client.diagnostic_fields(body).get(b"C", b"").decode()
                  for kind, body in messages if kind == b"E"]
        record(sql, errors == ([state] if state else []) and messages[-1] == (b"Z", ready),
               (errors, messages[-1]))

    try:
        corpus = cases()
        assert len(corpus) == 249, len(corpus)
        for mode in ("simple", "extended", "binary"):
            for arguments, result, state in corpus:
                query(mode, "SELECT width_bucket("+",".join(arguments)+") AS v", [result], state)
            for expression, state in (
                ("width_bucket(1,0,10)", "42883"),
                ("width_bucket(1,0,10,5,6)", "42883"),
                ("width_bucket(true,0,10,5)", "42883"),
                ("width_bucket('1'::text,0,10,5)", "42883"),
                ("width_bucket(1,0,10,5::bigint)", "42883"),
                ("width_bucket('bad',0,10,5)", "22P02"),
            ):
                query(mode, "SELECT "+expression+" AS v", [], state)
            query(mode, "SELECT width_bucket(NULL::numeric,0,10,5) AS v WHERE false", [])
            query(mode, "SELECT width_bucket(9,10,0,5) AS v LIMIT 0", [])
        for oid, values, wanted in (
            (1700, ("9007199254740993", "9007199254740992", "9007199254740994", "2"), "2"),
            (1700, ("1e-400", "0", "2e-400", "2"), "2"),
            (1700, ("0", "1e999", "-1e999", "10"), "6"),
            (701, ("0.3", "0", "1", "10"), "4"),
            (701, ("9", "10", "0", "10"), "2"),
            (701, (None, "10", "0", "10"), None),
        ):
            parameters = [(oid, None if value is None else value.encode()) for value in values[:3]]
            parameters += [(23, values[3].encode())]
            for mode in ("extended", "binary"):
                query(mode, "SELECT width_bucket($1,$2,$3,$4) AS v", [wanted], parameters=parameters)
        for mode in ("extended", "binary"):
            parameters = [(701, struct.pack("!d", value)) for value in (9.0, 10.0, 0.0)]
            parameters += [(23, struct.pack("!i", 10))]
            query(mode, "SELECT width_bucket($1,$2,$3,$4) AS v", ["2"], parameters=parameters, binary_input=True)
        command("CREATE TEMP TABLE width_bucket_rows(id INT,x NUMERIC,lo NUMERIC,hi NUMERIC,n INT)")
        command("INSERT INTO width_bucket_rows VALUES(1,8,10,0,5),(2,9007199254740993,9007199254740992,9007199254740994,2),(3,1e-400,0,2e-400,2),(4,NULL,10,0,5)")
        for mode in ("simple", "extended", "binary"):
            query(mode, "SELECT width_bucket(x,lo,hi,n) AS v FROM width_bucket_rows ORDER BY id", ["2", "2", "2", None])
            query(mode, "SELECT width_bucket(x,lo,hi,n) AS v FROM width_bucket_rows WHERE false", [])
        ready = b"T"
        command("BEGIN")
        command("SAVEPOINT bucket_state")
        ready = b"E"
        command("UPDATE width_bucket_rows SET n=width_bucket(hi,lo,hi,2147483647) WHERE id=2", state="22003")
        ready = b"T"
        command("ROLLBACK TO bucket_state")
        query("binary", "SELECT width_bucket(x,lo,hi,n) AS v FROM width_bucket_rows WHERE id=2", ["2"])
        ready = b"E"
        query("extended", "SELECT width_bucket(10,0,10,2147483647) AS v", [], state="22003")
        ready = b"T"
        command("ROLLBACK TO bucket_state")
        query("binary", "SELECT width_bucket(x,lo,hi,n) AS v FROM width_bucket_rows WHERE id=2", ["2"])
        command("RELEASE bucket_state")
        ready = b"I"
        command("COMMIT")
        query("simple", "SELECT width_bucket(9,10,0,10) AS v", ["2"])
        print("WIDTH_BUCKET_PROTOCOL_COMPLETE", controls, "FAILURES", len(failures), flush=True)
        assert controls == 804 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
