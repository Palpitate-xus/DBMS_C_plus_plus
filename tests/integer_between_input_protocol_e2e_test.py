#!/usr/bin/env python3
"""Every INTEGER BETWEEN comparison owns its real UNKNOWN literal input."""
import argparse
import importlib.util
import re
import socket
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--reference18", action="store_true")
    options = parser.parse_args(); root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("integer_between_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner); client = runner.load_protocol_client()
    server = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else: server = runner.start_ours(client); sock = server["sock"]
    controls = 0; failures = []; schema = "owned_integer_between_" + uuid.uuid4().hex[:12]; created = False
    limits = {"smallint": (-32768, 32767), "integer": (-2147483648, 2147483647), "bigint": (-9223372036854775808, 9223372036854775807)}
    def integer(value, kind):
        if value is None: return None
        value = str(value).strip()
        if not re.fullmatch(r"[+-]?[0-9]+", value): raise ValueError("22P02")
        result = int(value)
        if not limits[kind][0] <= result <= limits[kind][1]: raise ValueError("22003")
        return result
    def comparison(left, right, lower):
        _, a_type, a = left; _, b_type, b = right
        if a_type == "unknown" and b_type == "unknown": a_type = b_type = "text"
        elif a_type == "unknown": a_type = b_type; a = integer(a, a_type) if a_type in limits else a
        elif b_type == "unknown": b_type = a_type; b = integer(b, b_type) if b_type in limits else b
        if (a_type in limits) != (b_type in limits): raise ValueError("42883")
        if a is None or b is None: return None
        return a >= b if lower else a <= b
    def expected(left, lower, upper, negate):
        try: a = comparison(left, lower, True); b = comparison(left, upper, False)
        except ValueError as error: return None, str(error)
        value = False if a is False or b is False else None if a is None or b is None else True
        if value is not None and negate: value = not value
        return None if value is None else "t" if value else "f", None
    def check(sql, rows, oid, state=None):
        nonlocal controls
        controls += 1; messages = client.simple_query(sock, sql); result = runner.decode_wire_result(messages, include_types=True)
        mismatch = result[:2] != (rows, state)
        if state is None: mismatch = mismatch or result[3:] != (["value"], "SELECT " + str(len(rows)), [oid])
        else: mismatch = mismatch or any(kind in (b"T", b"D") for kind, _ in messages)
        if mismatch:
            failures.append((sql, result, rows, oid, state)); print("[INTEGER BETWEEN FAIL] " + str(failures[-1]), flush=True)
    def ok(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result); return result
    def expression(left, lower, upper, negate):
        return left[0] + (" NOT BETWEEN " if negate else " BETWEEN ") + lower[0] + " AND " + upper[0]
    def bounds(kind):
        return [("0::" + kind, kind, 0), ("2::" + kind, kind, 2), ("NULL::" + kind, kind, None),
                ("'1'", "unknown", "1"), ("'b01'", "unknown", "b01"), ("''", "unknown", ""),
                ("NULL", "unknown", None), ("'1'::text", "text", "1")]
    try:
        for kind in limits:
            lefts = [("'1'", "unknown", "1"), ("NULL", "unknown", None), ("'b01'", "unknown", "b01"),
                     ("''", "unknown", ""), ("'32768'", "unknown", "32768"), ("'2147483648'", "unknown", "2147483648"),
                     ("'9223372036854775808'", "unknown", "9223372036854775808"), ("' +01 '", "unknown", " +01 "),
                     ("1::" + kind, kind, 1), ("NULL::" + kind, kind, None),
                     ("'1'::text", "text", "1"), ("NULL::text", "text", None)]
            for left in lefts:
                for lower in bounds(kind):
                    for upper in bounds(kind):
                        for negate in (False, True):
                            value, state = expected(left, lower, upper, negate)
                            check("SELECT " + expression(left, lower, upper, negate) + " AS value", [] if state else [[value]], 16, state)
        assert controls == 4608, controls
        ok("CREATE SCHEMA " + schema); created = True
        for kind in limits:
            for population in ("populated", "empty", "nulls"):
                table = schema + "." + kind + "_" + population
                ok("CREATE TABLE " + table + "(id integer PRIMARY KEY,i " + kind + ")")
                if population != "empty": ok("INSERT INTO " + table + " VALUES(1," + ("NULL" if population == "nulls" else "1") + ")")
                members = bounds(kind); left = ("i", kind, None if population == "nulls" else 1)
                for lower_index, upper_index in ((3, 1), (0, 4), (4, 1), (6, 4), (4, 6), (5, 6), (6, 5), (7, 1)):
                    lower, upper = members[lower_index], members[upper_index]
                    for negate in (False, True):
                        value, state = expected(left, lower, upper, negate); condition = expression(left, lower, upper, negate)
                        for path in range(4):
                            if path == 0:
                                sql = "SELECT " + condition + " AS value FROM " + table + " ORDER BY id"
                                rows = [] if state or population == "empty" else [[value]]; oid = 16
                            else:
                                where = "(" + condition + ")"
                                if path == 2: where = "id=1 AND " + where
                                if path == 3: where = "id=1 OR " + where
                                sql = "SELECT id AS value FROM " + table + " WHERE " + where + " ORDER BY id"
                                rows = [["1"]] if not state and population != "empty" and (value == "t" or path == 3) else []; oid = 23
                            check(sql, rows, oid, state)
        assert controls == 5184, controls
        ok("CREATE SEQUENCE " + schema + ".effects")
        ok("CREATE FUNCTION " + schema + ".writer(p integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('" + schema + ".effects'); RETURN p; END; $$")
        ok("SELECT nextval('" + schema + ".effects')")
        def counter(): return int(ok("SELECT currval('" + schema + ".effects') AS calls")[0][0][0])
        for argument in ("1", "NULL::integer"):
            for range_sql in ("'b01' AND 2", "0 AND 'b01'", "NULL AND 'b01'", "'b01' AND NULL", "'' AND 2", "0 AND ''"):
                for operation in (" BETWEEN ", " NOT BETWEEN "):
                    before = counter()
                    check("SELECT " + schema + ".writer(" + argument + ")" + operation + range_sql + " AS value", [], 16, "22P02")
                    after = counter()
                    if after != before:
                        failures.append(("pure input admission executed volatile writer", argument, range_sql, operation, before, after))
                        print("[INTEGER BETWEEN FAIL] " + str(failures[-1]), flush=True)
        assert controls == 5208, controls
        assert not failures, "%d/%d complete INTEGER BETWEEN controls failed" % (len(failures), controls)
        print("[INTEGER BETWEEN PROTOCOL] all %d scalar/input/NULL/width/overflow/table/empty/index/OR/volatile-admission controls passed" % controls)
    finally:
        if created: ok("DROP SCHEMA " + schema + " CASCADE")
        if server: runner.stop_ours(server)
        else: sock.close()


if __name__ == "__main__": main()
