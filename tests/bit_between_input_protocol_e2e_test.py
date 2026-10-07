#!/usr/bin/env python3
"""BETWEEN transforms each BIT comparison's actual UNKNOWN input independently."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("bit_between_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server["sock"]
    lefts = [("B'01'", "bit", "01"), ("B'01'::varbit", "bit", "01"),
             ("NULL::bit", "bit", None), ("NULL::varbit", "bit", None),
             ("'01'", "unknown", "01"), ("NULL", "unknown", None),
             ("'b01'", "unknown", "b01"), ("'2'", "unknown", "2")]
    bounds = [("B'01'", "bit", "01"), ("B'1'", "bit", "1"),
              ("'01'", "unknown", "01"), ("'b01'", "unknown", "b01"),
              ("'x1'", "unknown", "x1"), ("''", "unknown", ""),
              ("'102'", "unknown", "102"), ("NULL", "unknown", None),
              ("1", "integer", 1)]
    failures = []; controls = 0; schema = "owned_bit_between_" + uuid.uuid4().hex[:12]; created = False

    def bit_input(value):
        if value is None: return None
        if value[:1].lower() == "x":
            digits = value[1:]
            if any(c not in "0123456789abcdefABCDEF" for c in digits): raise ValueError("22P02")
            return "".join(format(int(c, 16), "04b") for c in digits)
        bits = value[1:] if value[:1].lower() == "b" else value
        if any(c not in "01" for c in bits): raise ValueError("22P02")
        return bits

    def comparison(left, right, lower):
        _, lhs, a = left; _, rhs, b = right
        if lhs == "bit" or rhs == "bit":
            if lhs == "integer" or rhs == "integer": raise ValueError("42883")
            if lhs == "unknown": a = bit_input(a)
            if rhs == "unknown": b = bit_input(b)
        elif rhs == "integer":
            if a is not None:
                try: a = int(a)
                except ValueError: raise ValueError("22P02")
        if a is None or b is None: return None
        return a >= b if lower else a <= b

    def expected(left, lower, upper, negate):
        try:
            a = comparison(left, lower, True); b = comparison(left, upper, False)
        except ValueError as error: return None, str(error)
        result = False if a is False or b is False else None if a is None or b is None else True
        if result is not None and negate: result = not result
        return result, None

    def check(sql, rows, oid, state=None):
        nonlocal controls
        controls += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        difference = result[0] != rows or result[1] != state
        if state is None: difference = difference or result[3:] != (["value"], "SELECT " + str(len(rows)), [oid])
        if difference:
            failure = (sql, result, rows, state, oid); failures.append(str(failure))
            print("[BIT BETWEEN INPUT FAIL] " + str(failure), flush=True)

    def ok(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result)

    truth = lambda value: None if value is None else "t" if value else "f"
    try:
        for left in lefts:
            for lower in bounds:
                for upper in bounds:
                    if not any(item[1] == "bit" for item in (left, lower, upper)): continue
                    for negate in (False, True):
                        value, state = expected(left, lower, upper, negate)
                        expression = left[0] + (" NOT BETWEEN " if negate else " BETWEEN ") + lower[0] + " AND " + upper[0]
                        check("SELECT " + expression + " AS value", [] if state else [[truth(value)]], 16, state)
        ok("CREATE SCHEMA " + schema); created = True
        for kind in ("bit(2)", "varbit"):
            for population in ("populated", "empty", "nulls"):
                table = schema + "." + ("fixed_" if kind == "bit(2)" else "varying_") + population
                ok("CREATE TABLE " + table + "(id integer PRIMARY KEY,b " + kind + ")")
                if population != "empty": ok("INSERT INTO " + table + " VALUES(1," + ("NULL" if population == "nulls" else "B'01'") + ")")
                left = ("b", "bit", None if population == "nulls" else "01")
                for lower_index, upper_index in ((3, 4), (5, 3), (0, 6), (6, 0), (0, 8), (8, 0)):
                    lower = bounds[lower_index]; upper = bounds[upper_index]
                    for negate in (False, True):
                        value, state = expected(left, lower, upper, negate)
                        expression = "b" + (" NOT BETWEEN " if negate else " BETWEEN ") + lower[0] + " AND " + upper[0]
                        for path in range(4):
                            if path == 0:
                                sql = "SELECT " + expression + " AS value FROM " + table + " ORDER BY id"
                                rows = [] if population == "empty" or state else [[truth(value)]]; oid = 16
                            else:
                                condition = "(" + expression + ")"
                                if path == 2: condition = "id=1 AND " + condition
                                if path == 3: condition = "id=1 OR " + condition
                                sql = "SELECT id AS value FROM " + table + " WHERE " + condition + " ORDER BY id"
                                rows = [["1"]] if not state and population != "empty" and (value is True or path == 3) else []; oid = 23
                            check(sql, rows, oid, state)
        assert controls == 1192, controls
        assert not failures, "%d/%d complete BIT BETWEEN input controls failed" % (len(failures), controls)
        print("[BIT BETWEEN INPUT PROTOCOL] all %d scalar/pair-context/NULL/input/error/empty/index/OR/OID controls passed" % controls)
    finally:
        if created: ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
