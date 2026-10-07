#!/usr/bin/env python3
"""Actual volatile routines/SQL children retain BETWEEN's two demanded sites."""
import argparse
import importlib.util
import socket
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("between_demand_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    schema = "owned_between_demand_" + uuid.uuid4().hex[:12]; created = False
    failures = []; controls = 0

    def capture(sql): return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    def ok(sql):
        result = capture(sql); assert result[1] is None, (sql, result); return result
    def counter(): return int(ok("SELECT currval('" + schema + ".effects') AS calls")[0][0][0])
    def check(expression, value, calls, state=None, oid=16):
        nonlocal controls
        controls += 1; sql = "SELECT " + expression + " AS value"; before = counter()
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True); actual_calls = counter() - before
        rows = [] if state else [[value]]
        mismatch = actual_calls != calls or result[:2] != (rows, state)
        if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT 1", [oid])
        else: mismatch = mismatch or any(kind in (b"T", b"D") for kind, _ in messages)
        if mismatch:
            failures.append((sql, result, actual_calls, rows, calls, state, oid))
            print("[BETWEEN DEMAND FAIL] " + str(failures[-1]), flush=True)

    # (SQL, actual decoded BIT datum, constant-NULL role, body effects).
    def literal(value):
        return ("NULL::varbit" if value is None else "B'" + value + "'", value, value is None, 0)
    def writer(value):
        source = literal(value); return (schema + ".writer(" + source[0] + ")", value, False, 1)
    def child(value):
        source = writer(value); return ("(SELECT " + source[0] + ")", value, False, 1)
    def bound(sql):
        values = {"B'01'": "01", "B'11'": "11", "NULL": None, "'b01'": "01", "'x1'": "0001", "'102'": "INVALID"}
        return (sql, values[sql], sql == "NULL", 0)
    def pair(left, right, lower):
        if left[2] or right[2]: return None, 0
        result = None if left[1] is None or right[1] is None else left[1] >= right[1] if lower else left[1] <= right[1]
        return result, left[3] + right[3]
    def range_case(left, lower, upper, negate):
        expression = left[0] + (" NOT BETWEEN " if negate else " BETWEEN ") + lower[0] + " AND " + upper[0]
        if lower[1] == "INVALID" or upper[1] == "INVALID": check(expression, None, 0, "22P02"); return
        a, calls = pair(left, lower, True)
        if a is False: check(expression, "t" if negate else "f", calls); return
        b, more = pair(left, upper, False); calls += more
        value = False if b is False else None if a is None or b is None else True
        if value is not None and negate: value = not value
        check(expression, None if value is None else "t" if value else "f", calls)

    try:
        ok("CREATE SCHEMA " + schema); created = True; ok("CREATE SEQUENCE " + schema + ".effects")
        ok("CREATE FUNCTION " + schema + ".writer(p varbit) RETURNS varbit LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('" + schema + ".effects'); RETURN p; END; $$")
        for name in ("between", "not between"):
            ok("CREATE FUNCTION " + schema + '."' + name + '"(a varbit,b varbit,c varbit) RETURNS varbit LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval(\'' + schema + ".effects'); RETURN a; END; $$")
        ok("SELECT nextval('" + schema + ".effects')")
        values = ("00", "01", "11", None)
        pairs = (("B'01'", "B'11'"), ("NULL", "B'11'"), ("B'01'", "NULL"), ("NULL", "NULL"),
                 ("B'11'", "B'01'"), ("'b01'", "'x1'"), ("'102'", "B'11'"), ("B'01'", "'102'"))
        for value in values:
            for lower, upper in pairs:
                for negate in (False, True): range_case(writer(value), bound(lower), bound(upper), negate)
        for value in values:
            for lower in ("B'01'", "NULL"):
                for upper in ("11", None):
                    for negate in (False, True): range_case(literal(value), bound(lower), writer(upper), negate)
        for value in values:
            for lower in ("01", None):
                for upper in ("11", None):
                    for negate in (False, True): range_case(writer(value), writer(lower), writer(upper), negate)
        assert controls == 128, controls
        ok("SET search_path TO " + schema + ",pg_catalog")
        for name in ("between", "not between"):
            for qualifier in ("", schema + ".", '"' + schema + '".'):
                routine = qualifier + '"' + name + '"'
                check(routine + "(B'01',B'11',B'00')", "01", 1, oid=1562)
                check(routine + "(NULL::varbit,B'11',B'00')", None, 1, oid=1562)
                check(routine + "(B'01',B'11',B'00') BETWEEN B'01' AND B'01'", "t", 2)
                check(routine + "(B'00',B'11',B'00') NOT BETWEEN B'01' AND B'11'", "t", 1)
        ok("SET search_path TO public,pg_catalog")
        assert controls == 152, controls
        for left, lower, upper, negate in (
            (child("01"), bound("B'01'"), bound("B'01'"), False),
            (child("00"), bound("B'01'"), bound("B'11'"), False),
            (child(None), bound("B'01'"), bound("B'11'"), False),
            (literal("00"), bound("B'01'"), child("11"), False),
            (literal("01"), bound("B'01'"), child("11"), False),
            (literal("00"), bound("B'01'"), child("11"), True),
            (child("01"), ("'01'", "01", False, 0), ("'01'", "01", False, 0), False),
            (child("00"), ("'01'", "01", False, 0), ("'11'", "11", False, 0), False),
            (child(None), ("'01'", "01", False, 0), ("'11'", "11", False, 0), False),
            (child("01"), bound("'102'"), bound("B'11'"), False),
            (child("01"), ("B'00'", "00", False, 0), bound("'102'"), False),
            (literal("01"), child("00"), child("11"), False)):
            range_case(left, lower, upper, negate)
        assert controls == 164, controls
        for expression, state, value in (
            ("CAST('2' AS bit) BETWEEN NULL AND NULL", "22P02", None),
            ("CAST('2' AS varbit) BETWEEN NULL AND NULL", "22P02", None),
            ("CAST('xg' AS varbit) BETWEEN NULL AND B'11'", "22P02", None),
            ("NULL::integer BETWEEN 1/0 AND 1", "22012", None),
            ("NULL::integer BETWEEN 1 AND 1/0", "22012", None),
            ("1 BETWEEN 0 AND 'x'::integer", "22P02", None),
            ("0 BETWEEN 1 AND 'x'::integer", "22P02", None),
            ("0 BETWEEN 1 AND 1/0", None, "f"),
            ("B'00' BETWEEN B'01' AND '2'::varbit", "22P02", None),
            ("NULL::bit BETWEEN 1 AND NULL", "42883", None),
            ("NULL::bit NOT BETWEEN NULL AND 1", "42883", None),
            ("NULL::bit BETWEEN '102' AND NULL", "22P02", None),
            ("NULL::bit BETWEEN NULL AND '102'", "22P02", None)):
            check(expression, value, 0, state)
        assert controls == 177, controls
        writer_name = schema + ".writer"
        zero = "(1/0)::bit"
        for expression, state, value in (
            (writer_name + "(B'00') BETWEEN B'01' AND " + zero, "22012", None),
            (writer_name + "(B'01') BETWEEN NULL AND " + zero, "22012", None),
            (writer_name + "(B'01') BETWEEN B'01' AND " + zero, "22012", None),
            ("B'00' BETWEEN B'01' AND " + zero, None, "f"),
            ("NULL::bit BETWEEN B'01' AND " + zero, "22012", None),
            ("NULL::bit BETWEEN " + zero + " AND B'11'", "22012", None),
            ("B'00' BETWEEN B'01' AND " + writer_name + "(" + zero + ")", None, "f"),
            (writer_name + "(B'00') BETWEEN B'01' AND " + writer_name + "(" + zero + ")", "22012", None),
            (writer_name + "(" + zero + ") BETWEEN NULL AND NULL", "22012", None),
            (writer_name + "(B'00') BETWEEN B'01' AND (SELECT " + zero + ")", "22012", None),
            ("B'00' BETWEEN B'01' AND (SELECT " + zero + ")", None, "f"),
            (writer_name + "(B'00') NOT BETWEEN B'01' AND " + zero, "22012", None),
            ("B'00' NOT BETWEEN B'01' AND " + zero, None, "t")):
            check(expression, value, 0, state)
        assert controls == 190, controls
        assert not failures, "%d/%d complete demand/role/child/input controls failed" % (len(failures), controls)
        print("[BETWEEN DEMAND PROTOCOL] all %d full 128 demand/24 actual routine roles/12 children/13 input-error/13 planning-demand controls passed" % controls)
    finally:
        if created:
            ok("SET search_path TO public,pg_catalog"); ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
