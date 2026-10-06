#!/usr/bin/env python3
"""INTO demand limits execution, not just the stored first-row result.

Expectations were observed on PostgreSQL 17.2 with no extra too_many_rows
warnings/errors; PG18 pl_exec.c retains the same 1/2/0 executor contract.
"""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("into_demand_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def function(name, body, arguments=""):
        result = query(f"CREATE FUNCTION {name}({arguments}) RETURNS INT LANGUAGE plpgsql AS $body$ {body} $body$;")
        assert result[1] is None, (name, result)

    # The projection is volatile because counted() allocates a sequence value.
    # Sorting on id must not demand the third projection; sorting on the
    # projection itself must evaluate all three keys. The sequence is probed
    # after rollback, so function atomicity cannot hide excessive execution.
    cases = [
        ("count_plain", "SELECT counted(id) FROM demand_rows", False, [["1"]], None, 1),
        ("count_plain_strict", "SELECT counted(id) FROM demand_rows", True, [], "P0003", 2),
        ("count_order_id", "SELECT counted(id) FROM demand_rows ORDER BY id", False, [["1"]], None, 1),
        ("count_order_id_strict", "SELECT counted(id) FROM demand_rows ORDER BY id", True, [], "P0003", 2),
        ("count_order_projected", "SELECT counted(id) AS projected FROM demand_rows ORDER BY projected", False, [["1"]], None, 3),
        ("count_order_projected_strict", "SELECT counted(id) AS projected FROM demand_rows ORDER BY projected", True, [], "P0003", 3),
        ("cast_plain", "SELECT CAST(payload AS INT) FROM demand_rows", False, [["11"]], None, 0),
        ("cast_plain_strict", "SELECT CAST(payload AS INT) FROM demand_rows", True, [], "P0003", 0),
        # A non-volatile projection may run below Sort. These are controls,
        # not bugs: PG also evaluates the invalid third cast before sorting.
        ("cast_order_id", "SELECT CAST(payload AS INT) FROM demand_rows ORDER BY id", False, [], "22P02", 0),
        ("cast_order_id_strict", "SELECT CAST(payload AS INT) FROM demand_rows ORDER BY id", True, [], "22P02", 0),
        ("cast_order_projected", "SELECT CAST(payload AS INT) AS projected FROM demand_rows ORDER BY projected", False, [], "22P02", 0),
        ("cast_order_projected_strict", "SELECT CAST(payload AS INT) AS projected FROM demand_rows ORDER BY projected", True, [], "22P02", 0),
        ("div_second", "SELECT div_second(id) FROM demand_rows ORDER BY id", False, [["-12"]], None, 0),
        ("div_second_strict", "SELECT div_second(id) FROM demand_rows ORDER BY id", True, [], "22012", 0),
        ("div_third", "SELECT div_third(id) FROM demand_rows ORDER BY id", False, [["-6"]], None, 0),
        ("div_third_strict", "SELECT div_third(id) FROM demand_rows ORDER BY id", True, [], "P0003", 0),
        ("write_cte", "WITH changed AS(INSERT INTO demand_side SELECT id FROM demand_rows RETURNING id) SELECT counted(id) FROM demand_rows ORDER BY id", False, [["1"]], None, 1),
        ("write_cte_strict", "WITH changed AS(INSERT INTO demand_side SELECT id FROM demand_rows RETURNING id) SELECT counted(id) FROM demand_rows ORDER BY id", True, [], "P0003", 2),
    ]
    controls = [
        ("ordinary_counted", "SELECT counted(id) FROM demand_rows ORDER BY id", [["1"], ["2"], ["3"]], None, 3),
        ("ordinary_cast", "SELECT CAST(payload AS INT) FROM demand_rows ORDER BY id", [], "22P02", 0),
        ("ordinary_div_second", "SELECT div_second(id) FROM demand_rows ORDER BY id", [], "22012", 0),
        ("ordinary_div_third", "SELECT div_third(id) FROM demand_rows ORDER BY id", [], "22012", 0),
    ]
    # Independent PG 17.2 probes also cover demand through qualification,
    # offsets, compound expressions, mixed sort projections and derived input.
    extra = [
        ("offset", "SELECT counted(id) FROM demand_rows OFFSET 2", False, [["3"]], None, 3, "n"),
        ("sorted_offset", "SELECT counted(id) FROM demand_rows ORDER BY id OFFSET 2", False, [["3"]], None, 3, "n"),
        ("limit_zero", "SELECT counted(id) FROM demand_rows ORDER BY id LIMIT 0", False, [[None]], None, 0, "n"),
        ("volatile_where", "SELECT counted(id) FROM demand_rows WHERE counted(id)>0", False, [["1"]], None, 2, "n"),
        ("volatile_where_strict", "SELECT counted(id) FROM demand_rows WHERE counted(id)>0", True, [], "P0003", 4, "n"),
        ("or_overlap", "SELECT counted(id) FROM demand_rows WHERE id=1 OR id>=1", False, [["1"]], None, 1, "n"),
        ("or_overlap_strict", "SELECT counted(id) FROM demand_rows WHERE id=1 OR id>=1", True, [], "P0003", 2, "n"),
        ("compound", "SELECT coalesce(counted(id),0) FROM demand_rows ORDER BY id", False, [["1"]], None, 1, "n"),
        ("mixed_sort", "SELECT counted(id),CAST(payload AS INT) FROM demand_rows ORDER BY id", False, [], "22P02", 0, "n,m"),
        ("mixed_plain", "SELECT counted(id),CAST(payload AS INT) FROM demand_rows", False, [["1"]], None, 1, "n,m"),
        ("column_offset", "SELECT id FROM demand_rows OFFSET 2", False, [["3"]], None, 0, "n"),
        ("sorted_column_offset", "SELECT id FROM demand_rows ORDER BY id DESC OFFSET 2", False, [["1"]], None, 0, "n"),
        ("derived_projection", "SELECT v FROM(SELECT counted(id) AS v FROM demand_rows)r", False, [["1"]], None, 1, "n"),
        ("derived_input", "SELECT counted(id) FROM(SELECT id FROM demand_rows)r", False, [["1"]], None, 1, "n"),
        ("interleaved_offset", "SELECT counted(id) FROM demand_rows WHERE nextval('demand_private_seq')<=2 OFFSET 1", False, [[None]], None, 4, "n"),
        ("sort_alias_collision", "SELECT counted(id) AS keyval FROM demand_alias_rows ORDER BY keyval", False, [["1"]], None, 3, "n"),
        ("plain_sort_alias_collision", "SELECT id AS keyval FROM demand_alias_rows ORDER BY keyval", False, [["1"]], None, 0, "n"),
        ("sorted_cast_input_error", "SELECT CAST(payload AS INT) FROM demand_error_order_rows ORDER BY id DESC", False, [], "22003", 0, "n"),
        ("physical_alias_nonkey", "SELECT id AS keyval,counted(id) FROM demand_alias_rows ORDER BY keyval", False, [["1"]], None, 1, "n,m"),
        ("physical_ordinal_nonkey", "SELECT id,counted(id) FROM demand_alias_rows ORDER BY 1", False, [["1"]], None, 1, "n,m"),
    ]
    try:
        for sql in ("CREATE TEMP TABLE demand_rows(id INT,payload TEXT);",
                    "INSERT INTO demand_rows VALUES(1,'11'),(2,'22'),(3,'bad');",
                    "CREATE TEMP TABLE demand_side(id INT);",
                    "CREATE TEMP TABLE demand_alias_rows(id INT,keyval INT);",
                    "INSERT INTO demand_alias_rows VALUES(1,3),(2,2),(3,1);",
                    "CREATE TEMP TABLE demand_error_order_rows(id INT,payload TEXT);",
                    "INSERT INTO demand_error_order_rows VALUES(1,'2147483648'),(2,'22'),(3,'bad');",
                    "CREATE SEQUENCE demand_private_seq START 1;"):
            assert query(sql)[1] is None, sql
        function("counted", "DECLARE q BIGINT; BEGIN SELECT nextval('demand_private_seq') INTO q; RETURN arg; END;", "arg INT")
        function("div_second", "BEGIN RETURN 12/(arg-2); END;", "arg INT")
        function("div_third", "BEGIN RETURN 12/(arg-3); END;", "arg INT")
        for name, sql, strict, *_ in cases:
            function("demand_" + name, f"DECLARE n INT; BEGIN {sql} INTO {'STRICT ' if strict else ''}n; RETURN n; END;")
        for name, sql, strict, *_, targets in extra:
            function("demand_" + name, f"DECLARE n INT; m INT; BEGIN {sql} INTO {'STRICT ' if strict else ''}{targets}; RETURN n; END;")
        assert query("BEGIN;")[1] is None
        for name, sql, rows, state, calls in [
            (name, f"SELECT demand_{name}();", rows, state, calls)
            for name, _, _, rows, state, calls in cases
        ] + controls + [
            (name, f"SELECT demand_{name}();", rows, state, calls)
            for name, _, _, rows, state, calls, _ in extra
        ]:
            assert query("SELECT setval('demand_private_seq',1,false);")[1] is None
            assert query("SAVEPOINT demand_case;")[1] is None
            result = query(sql)
            if (result[0], result[1]) != (rows, state):
                failures.append((name, "result", (rows, state), result))
            if name == "write_cte" and result[1] is None:
                side = query("SELECT id FROM demand_side ORDER BY id;")
                if side[0] != [["1"], ["2"], ["3"]] or side[1] is not None:
                    failures.append((name, "complete auxiliary write", side))
            assert query("ROLLBACK TO SAVEPOINT demand_case;")[1] is None
            probe = query("SELECT nextval('demand_private_seq')-1;")
            if probe[0] != [[str(calls)]] or probe[1] is not None:
                failures.append((name, "calls after rollback", calls, probe))
            assert query("RELEASE SAVEPOINT demand_case;")[1] is None
        assert query("ROLLBACK;")[1] is None
        assert not failures, failures
        print("[PLPGSQL INTO EXECUTION DEMAND PROTOCOL E2E] passed 18 INTO + 4 ordinary + 20 boundary controls")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
