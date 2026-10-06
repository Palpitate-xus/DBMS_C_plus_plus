#!/usr/bin/env python3
"""AT TIME ZONE binds and evaluates a typed, nullable value expression."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("timezone_operand_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    def function(name, kind, declaration, expression, direct=False):
        action = f"RETURN {expression};" if direct else f"SELECT {expression} INTO n; RETURN n;"
        body = f"DECLARE {declaration} n {kind}; BEGIN {action} END;"
        sql = f"CREATE FUNCTION {name}() RETURNS {kind} LANGUAGE plpgsql AS $body$ {body} $body$;"
        result = query(sql)
        assert result[1] is None, (sql, result)

    try:
        stamp = "TIMESTAMP '2026-10-06 00:00:00'"
        # Ordinary SQL controls remain registered alongside stored queries.
        expect(f"SELECT {stamp} AT TIME ZONE 'UTC';", [["2026-10-06 00:00:00+00"]], [1184])
        function("zone_variable", "TIMESTAMPTZ", "tz TEXT := 'UTC';", stamp + " AT TIME ZONE tz")
        expect("SELECT zone_variable();", [["2026-10-06 00:00:00+00"]], [1184])
        expect(f"SELECT {stamp} AT TIME ZONE ('U'||'TC');", [["2026-10-06 00:00:00+00"]], [1184])
        for name, expression in (("zone_lower", "lower(tz)"), ("zone_case", "CASE WHEN TRUE THEN tz ELSE 'bad' END"),
                                 ("zone_coalesce", "coalesce(tz,'UTC')"), ("zone_parentheses", "(tz||'')")):
            function(name, "TIMESTAMPTZ", "tz TEXT := 'UTC';", stamp + " AT TIME ZONE " + expression)
            expect(f"SELECT {name}();", [["2026-10-06 00:00:00+00"]], [1184])
        function("zone_quoted", "TIMESTAMPTZ", '\"TZ\" TEXT := \'America/New_York\'; tz TEXT := \'UTC\';', stamp + ' AT TIME ZONE "TZ"')
        expect("SELECT zone_quoted();", [["2026-10-06 04:00:00+00"]], [1184])
        function("zone_direct", "TIMESTAMPTZ", '\"TZ\" TEXT := \'UTC\';', stamp + ' AT TIME ZONE "TZ"', direct=True)
        expect("SELECT zone_direct();", [["2026-10-06 00:00:00+00"]], [1184])
        function("zone_operator_names", "TIMESTAMPTZ", '\"time\" TEXT := \'bad\'; \"zone\" TEXT := \'bad\'; tz TEXT := \'UTC\';', stamp + ' AT /* grammar */ TIME ZONE tz')
        expect("SELECT zone_operator_names();", [["2026-10-06 00:00:00+00"]], [1184])
        function("zone_null", "TIMESTAMPTZ", "tz TEXT;", stamp + " AT TIME ZONE tz")
        expect("SELECT zone_null();", [[None]], [1184])
        function("zone_input_null", "TIMESTAMPTZ", "tz TEXT := 'UTC';", "CAST(NULL AS TIMESTAMP) AT TIME ZONE tz")
        expect("SELECT zone_input_null();", [[None]], [1184])
        function("zone_chain", "TIMESTAMP", "tz TEXT := 'UTC';", stamp + " AT TIME ZONE tz AT TIME ZONE 'America/New_York'")
        expect("SELECT zone_chain();", [["2026-10-05 20:00:00"]], [1114])
        function("zone_interval", "TIMESTAMPTZ", "tz INTERVAL := '02:00';", stamp + " AT TIME ZONE tz")
        expect("SELECT zone_interval();", [["2026-10-05 22:00:00+00"]], [1184])
        function("zone_unknown", "TIMESTAMPTZ", '"NoSuchZone" TEXT := \'NoSuchZone\';', stamp + ' AT TIME ZONE "NoSuchZone"')
        function("zone_wrong_type", "TIMESTAMPTZ", "tz INT := 7;", stamp + " AT TIME ZONE tz")
        function("zone_null_wrong_type", "TIMESTAMPTZ", "tz INT;", stamp + " AT TIME ZONE tz")
        for name, state in (("zone_unknown", "22023"), ("zone_wrong_type", "42883"), ("zone_null_wrong_type", "42883")):
            result = query(f"SELECT {name}();")
            assert result[1] == state and result[0] == [], (name, result)
        expect("SELECT zone_variable(),zone_null();", [["2026-10-06 00:00:00+00", None]], [1184, 1184])
        print("[PLPGSQL TIMEZONE OPERAND PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
