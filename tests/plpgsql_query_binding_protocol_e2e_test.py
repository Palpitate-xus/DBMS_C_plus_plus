#!/usr/bin/env python3
"""Reached SQL binding precedes CTE writes and nontransactional nextval.

Expected SQLSTATEs/results are PostgreSQL 17.2 variable_conflict=error.
This is a namespace/preparation test, not a whole PL/pgSQL compatibility claim.
"""
import importlib.util
import sys
from pathlib import Path


CASES = [
    ("projection", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;", "", "42702", []),
    ("predicate", "", "DECLARE id INT:=99;n INT; BEGIN SELECT binding_rows.id INTO n FROM binding_rows WHERE id=1; RETURN n; END;", "", "42702", []),
    ("false_rows", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_rows WHERE FALSE; RETURN n; END;", "", "42702", []),
    ("parameter", "id INT", "DECLARE n INT; BEGIN SELECT id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;", "99", "42702", []),
    ("qualified_parameter", "id INT", "DECLARE n INT; BEGIN SELECT binding_qualified_parameter.id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;", "99", None, [["99"]]),
    ("quoted_collision", "", 'DECLARE "ID" INT:=99;n INT; BEGIN SELECT "ID" INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;', "", "42702", []),
    ("writing_cte", "", "DECLARE id INT:=99;n INT; BEGIN WITH ins AS(INSERT INTO binding_side VALUES(nextval('binding_private_seq')) RETURNING binding_side.id) SELECT id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;", "", "42702", []),
    ("qualified_column", "", "DECLARE id INT:=99;n INT; BEGIN SELECT binding_rows.id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;", "", None, [["1"]]),
    ("unique_local", "", "DECLARE wanted INT:=1;n INT; BEGIN SELECT binding_rows.id INTO n FROM binding_rows WHERE binding_rows.id=wanted; RETURN n; END;", "", None, [["1"]]),
    ("quoted_distinct", "", 'DECLARE "ID" INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;', "", None, [["1"]]),
    ("quoted_unique", "", 'DECLARE "Id" INT:=99;n INT; BEGIN SELECT "Id" INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;', "", None, [["99"]]),
    ("quoted_qualified", "", 'DECLARE "ID" INT:=99;n INT; BEGIN SELECT binding_rows."ID" INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END;', "", None, [["7"]]),
    ("empty_table", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_empty; RETURN n; END;", "", "42702", []),
    ("cte_collision", "", "DECLARE id INT:=99;n INT; BEGIN WITH source AS(SELECT binding_rows.id FROM binding_rows WHERE FALSE) SELECT id INTO n FROM source; RETURN n; END;", "", "42702", []),
    ("derived_collision", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM(SELECT binding_rows.id FROM binding_rows WHERE FALSE)source; RETURN n; END;", "", "42702", []),
    ("cte_parameter", "", "DECLARE wanted INT:=31;n INT; BEGIN WITH source AS(SELECT wanted AS value) SELECT source.value INTO n FROM source; RETURN n; END;", "", None, [["31"]]),
    ("derived_parameter", "", "DECLARE wanted INT:=32;n INT; BEGIN SELECT source.value INTO n FROM(SELECT wanted AS value)source; RETURN n; END;", "", None, [["32"]]),
    ("missing_qualifier", "id INT", "DECLARE n INT; BEGIN SELECT missing_label.id INTO n FROM binding_rows WHERE FALSE; RETURN n; END;", "99", "42P01", []),
    ("block_parameter", "", "<<scope_name>> DECLARE id INT:=41;n INT; BEGIN SELECT scope_name.id INTO n FROM binding_rows WHERE binding_rows.id=1; RETURN n; END scope_name;", "", None, [["41"]]),
    ("shadow_parameter", "id INT", "DECLARE id INT:=7;n INT; BEGIN SELECT binding_shadow_parameter.id INTO n; RETURN n; END;", "99", None, [["99"]]),
    ("nested_block", "", "<<outer_scope>> DECLARE id INT:=42;n INT; BEGIN <<inner_scope>> DECLARE id INT:=7; BEGIN SELECT outer_scope.id INTO n; END inner_scope; RETURN n; END outer_scope;", "", None, [["42"]]),
    ("unreached", "", "DECLARE id INT:=99;n INT; BEGIN IF FALSE THEN SELECT id INTO n FROM binding_rows; END IF; RETURN 7; END;", "", None, [["7"]]),
    ("typed_null", "value INT", "DECLARE n INT; BEGIN SELECT binding_typed_null.value INTO n; RETURN n; END;", "NULL", None, [[None]]),
    ("positional", "z INT,a INT", "DECLARE n INT; BEGIN SELECT $2 INTO n; RETURN n; END;", "17,21", None, [["21"]]),
    ("positional_after_local", "z INT,a INT", "DECLARE local_value INT:=99;n INT;m INT; BEGIN SELECT local_value,$2 INTO n,m; RETURN m; END;", "17,21", None, [["21"]]),
    ("positional_after_local_first", "z INT,a INT", "DECLARE local_value INT:=99;n INT;m INT; BEGIN SELECT local_value,$1 INTO n,m; RETURN m; END;", "17,21", None, [["17"]]),
    ("positional_before_local", "z INT,a INT", "DECLARE local_value INT:=99;n INT;m INT; BEGIN SELECT $1,local_value INTO n,m; RETURN n; END;", "17,21", None, [["17"]]),
    ("cte_unaliased_parameter", "", "DECLARE wanted INT:=33;n INT; BEGIN WITH source AS(SELECT wanted) SELECT source.wanted INTO n FROM source; RETURN n; END;", "", None, [["33"]]),
    ("derived_unaliased_parameter", "", "DECLARE wanted INT:=34;n INT; BEGIN SELECT source.wanted INTO n FROM(SELECT(wanted))source; RETURN n; END;", "", None, [["34"]]),
    ("scalar_cte", "", "DECLARE wanted INT:=35;n INT; BEGIN WITH source AS(SELECT wanted AS value) SELECT(SELECT source.value FROM source) INTO n; RETURN n; END;", "", None, [["35"]]),
    ("scalar_collision", "", "DECLARE id INT:=99;n INT; BEGIN SELECT(SELECT id FROM binding_empty) INTO n; RETURN n; END;", "", "42702", []),
    ("quoted_block", "", '<<"scope.dot">> DECLARE id INT:=43;n INT; BEGIN SELECT "scope.dot".id INTO n; RETURN n; END "scope.dot";', "", None, [["43"]]),
    ("split_block", "", '<<"scope.dot">> DECLARE id INT:=43;n INT; BEGIN SELECT scope.dot.id INTO n; RETURN n; END "scope.dot";', "", "42P01", []),
    ("join_using", "", "DECLARE n INT; BEGIN SELECT id INTO n FROM binding_rows a JOIN binding_empty b USING(id); RETURN n; END;", "", None, [[None]]),
    ("join_using_collision", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_rows a JOIN binding_empty b USING(id); RETURN n; END;", "", "42702", []),
    ("join_qualified_using", "", "DECLARE id INT:=99;n INT; BEGIN SELECT a.id INTO n FROM binding_rows a JOIN binding_rows b USING(id) WHERE a.id=1; RETURN n; END;", "", None, [["1"]]),
    ("catalog_collision", "", "DECLARE relname TEXT:='wrong';n TEXT; BEGIN SELECT relname INTO n FROM pg_catalog.pg_class WHERE FALSE; RETURN 1; END;", "", "42702", []),
    ("view_collision", "", "DECLARE id INT:=99;n INT; BEGIN SELECT id INTO n FROM binding_view WHERE FALSE; RETURN n; END;", "", "42702", []),
    ("sql_keyword", "", 'DECLARE "current_date" INT:=99;n DATE;v INT; BEGIN SELECT CURRENT_DATE INTO n; SELECT "current_date" INTO v; IF n IS NOT NULL THEN RETURN v; END IF; RETURN 0; END;', "", None, [["99"]]),
    ("missing_positional", "", "DECLARE n INT; BEGIN SELECT $1 INTO n; RETURN n; END;", "", "42P02", []),
    ("insert_select", "", "DECLARE wanted INT:=44;n INT; BEGIN INSERT INTO binding_side SELECT wanted; SELECT binding_side.id INTO n FROM binding_side WHERE binding_side.id=wanted; DELETE FROM binding_side WHERE binding_side.id=wanted; RETURN n; END;", "", None, [["44"]]),
    ("qualified_extract", "", "DECLARE field TEXT:='year';n INT; BEGIN SELECT pg_catalog.extract(field,DATE '2026-10-06') INTO n; RETURN n; END;", "", None, [["2026"]]),
    ("writing_qualified_extract", "", "DECLARE n INT; BEGIN WITH ins AS(INSERT INTO binding_side VALUES(nextval('binding_private_seq')) RETURNING binding_side.id) SELECT pg_catalog.extract(missing_field,DATE '2026-10-06') INTO n; RETURN n; END;", "", "42703", []),
]

# Independently reproduced broader routine-DDL namespace gap. Keep this
# diagnostic visible; default binder regression does not claim it fixed.
# Run with --include-open-namespace to retain the real CREATE 42601 vs PG17.2.
OPEN_NAMESPACE_CASES = [
    ("schema_parameter", "id INT", "DECLARE n INT; BEGIN SELECT binding_schema_parameter.id INTO n; RETURN n; END;", "99", None, [["99"]]),
]


def main(include_open_namespace=False):
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("query_binding_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        cases = CASES + (OPEN_NAMESPACE_CASES if include_open_namespace else [])
        for sql in ('CREATE TEMP TABLE binding_rows(id INT,"ID" INT);',
                    'INSERT INTO binding_rows VALUES(1,7),(2,8);',
                    'CREATE TEMP TABLE binding_empty(id INT);',
                    'CREATE TEMP TABLE binding_side(id INT);',
                    'CREATE VIEW binding_view AS SELECT id FROM binding_rows;',
                    'CREATE SEQUENCE binding_private_seq;'):
            result = query(sql)
            assert result[1] is None, (sql, result)
        if include_open_namespace:
            assert query('CREATE SCHEMA binding_private_schema;')[1] is None
        for name, arguments, body, values, state, rows in cases:
            function = ('binding_private_schema.' if name == 'schema_parameter' else '') + f'binding_{name}'
            result = query(f'CREATE FUNCTION {function}({arguments}) RETURNS INT LANGUAGE plpgsql AS $body$ {body} $body$;')
            if result[1] is not None:
                print('BINDING_CASE', name, 'CREATE_FAIL', result, flush=True)
                failures.append((name, 'CREATE', result))
                continue
            result = query(f'SELECT {function}({values});')
            passed = result[1] == state and result[0] == rows
            print('BINDING_CASE', name, 'PASS' if passed else 'FAIL', result, flush=True)
            if not passed: failures.append((name, state, rows, result))
            if name == 'writing_cte':
                # Sequence state does not roll back: this is a true pre-effect
                # proof, not merely an empty table after transaction rollback.
                side = query('SELECT id FROM binding_side;')
                counter = query("SELECT currval('binding_private_seq');")
                initial = query("SELECT nextval('binding_private_seq');")
                passed = side[1] is None and side[0] == [] and counter[1] == '55000' and initial[0] == [['1']]
                print('BINDING_PRE_EFFECT', 'PASS' if passed else 'FAIL', side, counter, initial, flush=True)
                if not passed: failures.append(('pre_effect', side, counter, initial))
            if name == 'writing_qualified_extract':
                side = query('SELECT id FROM binding_side;')
                counter = query("SELECT currval('binding_private_seq');")
                following = query("SELECT nextval('binding_private_seq');")
                passed = side[1] is None and side[0] == [] and counter[0] == [['1']] and following[0] == [['2']]
                print('BINDING_QUALIFIED_EXTRACT_PRE_EFFECT', 'PASS' if passed else 'FAIL', side, counter, following, flush=True)
                if not passed: failures.append(('qualified_extract_pre_effect', side, counter, following))
        assert not failures, failures
        print('[PLPGSQL QUERY BINDING PROTOCOL E2E] passed', len(cases), '+ two sequence pre-effect controls')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main(include_open_namespace='--include-open-namespace' in sys.argv)
