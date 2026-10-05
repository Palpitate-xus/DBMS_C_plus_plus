#!/usr/bin/env python3
"""JOIN projections retain both relation schemas in RowDescription."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "join_type_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE join_type_left (id INT, name TEXT);",
            "CREATE TABLE join_type_right (id INT, val NUMERIC);",
            "INSERT INTO join_type_left VALUES (1, 'one'), (2, 'two');",
            "INSERT INTO join_type_right VALUES (1, 1.5), (3, 3.5);",
            "CREATE TABLE join_exact_left (id INT, txt TEXT);",
            "CREATE TABLE join_exact_right (id INT, txt TEXT);",
            ("INSERT INTO join_exact_left VALUES "
             "(1, ''), (2, NULL), (3, 'NULL'), (4, 'a b'), "
             "(5, 'left only');"),
            ("INSERT INTO join_exact_right VALUES "
             "(1, 'r one'), (2, ''), (3, NULL), (4, 'NULL'), "
             "(6, 'right only');"),
            "CREATE TABLE join_collision_left (id INT, txt TEXT);",
            "CREATE TABLE join_collision_right (id INT, txt TEXT);",
            "INSERT INTO join_collision_left VALUES (1, 'NULL');",
            ("INSERT INTO join_collision_right VALUES "
             "(2, 'b c'), (1, 'b c');"),
            "CREATE TABLE join_pred_left (id INT, txt TEXT);",
            "CREATE TABLE join_pred_right (id INT);",
            ("INSERT INTO join_pred_left VALUES "
             "(1, 'a,b'), (2, 'x'), (3, NULL), "
             "(4, 'l.id'), (5, 'r.id');"),
            "INSERT INTO join_pred_right VALUES (1), (2), (3), (4), (5);",
            "CREATE TABLE join_orientation_left (lkey INT, ltxt TEXT);",
            "CREATE TABLE join_orientation_right (rkey INT, rtxt TEXT);",
            ("INSERT INTO join_orientation_left VALUES "
             "(1, 'left one'), (2, 'left two');"),
            ("INSERT INTO join_orientation_right VALUES "
             "(1, 'right one'), (3, 'right three');"),
            "CREATE TABLE join_outer_chain_a (id INT);",
            "CREATE TABLE join_outer_chain_b (id INT);",
            "CREATE TABLE join_outer_chain_c (id INT);",
            "CREATE TABLE join_outer_chain_text_a (id INT, label TEXT);",
            "CREATE TABLE join_outer_chain_text_b "
            "(id INT, a_id INT, label TEXT);",
            "CREATE TABLE join_outer_chain_text_c "
            "(id INT, b_id INT, label TEXT);",
            "CREATE TABLE join_conj_chain_a (id INT);",
            "CREATE TABLE join_conj_chain_b "
            "(id INT, a_id INT, val INT);",
            "CREATE TABLE join_conj_chain_c (id INT, b_id INT);",
            "CREATE TABLE join_natural_empty_a (id INT);",
            "CREATE TABLE join_natural_empty_b (value INT);",
            "CREATE TABLE join_natural_no_rows (value INT);",
            "CREATE TABLE join_using_composite_a (id INT, code INT);",
            "CREATE TABLE join_using_composite_b "
            "(id INT, code INT, bval INT);",
            "CREATE TABLE join_using_composite_c "
            "(id INT, code INT, cval INT);",
            "CREATE TABLE join_numeric_scale_a (id NUMERIC);",
            "CREATE TABLE join_numeric_scale_b (id NUMERIC);",
            "INSERT INTO join_outer_chain_a VALUES (1), (2);",
            "INSERT INTO join_outer_chain_b VALUES (1);",
            "INSERT INTO join_outer_chain_c VALUES (1), (3);",
            "INSERT INTO join_outer_chain_text_a VALUES "
            "(1, 'alpha one'), (2, '');",
            "INSERT INTO join_outer_chain_text_b VALUES "
            "(1, 1, 'literal NULL'), (2, 2, NULL);",
            "INSERT INTO join_outer_chain_text_c VALUES "
            "(1, 1, 'gamma value');",
            "INSERT INTO join_conj_chain_a VALUES (1), (2);",
            "INSERT INTO join_conj_chain_b VALUES "
            "(1, 1, 0), (2, 1, 1), (3, 2, 1);",
            "INSERT INTO join_conj_chain_c VALUES (2, 2), (4, 3);",
            "INSERT INTO join_natural_empty_a VALUES (1), (2);",
            "INSERT INTO join_natural_empty_b VALUES (10), (20);",
            "INSERT INTO join_using_composite_a VALUES "
            "(1, 10), (1, 20), (2, 20);",
            "INSERT INTO join_using_composite_b VALUES "
            "(1, 10, 100), (1, 20, 200), (2, 10, 300);",
            "INSERT INTO join_using_composite_c VALUES "
            "(1, 20, 1000), (2, 10, 2000), (1, 10, 3000);",
            "INSERT INTO join_numeric_scale_a VALUES (1.0);",
            "INSERT INTO join_numeric_scale_b VALUES (1.00);",
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
        for index in range(14):
            relation = "join_many_%02d" % index
            for sql in [
                "CREATE TABLE %s (id INT);" % relation,
                "INSERT INTO %s VALUES (7);" % relation,
            ]:
                _, state, message, _ = runner.ours_query(
                    client, server["sock"], sql)
                assert state is None, (sql, state, message)

        cases = [
            (("SELECT * FROM join_type_left INNER JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_left.id;"),
             [23, 25, 23, 1700]),
            (("SELECT * FROM join_type_left LEFT JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_left.id;"),
             [23, 25, 23, 1700]),
            (("SELECT * FROM join_type_left RIGHT JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_right.id;"),
             [23, 25, 23, 1700]),
            (("SELECT name, val FROM join_type_left l JOIN join_type_right r "
              "ON l.id = r.id ORDER BY name;"),
             [25, 1700]),
            ("SELECT count(*) FROM join_type_left CROSS JOIN join_type_right;",
             [20]),
        ]
        for sql, expected_types in cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows, (sql, rows)
            assert len(headers) == len(expected_types), (sql, headers)
            assert type_oids == expected_types, (sql, type_oids, expected_types)
            assert command_tag == "SELECT %d" % len(rows), (sql, command_tag)

        orientation_cases = [
            (("SELECT l.lkey, r.rtxt FROM join_orientation_left l "
              "JOIN join_orientation_right r ON r.rkey = l.lkey "
              "ORDER BY l.lkey;"),
             [["1", "right one"]]),
            (("SELECT lkey, rtxt FROM join_orientation_left "
              "JOIN join_orientation_right ON rkey = lkey "
              "ORDER BY lkey;"),
             [["1", "right one"]]),
            (("SELECT l.lkey, r.rtxt FROM join_orientation_left l "
              "LEFT JOIN join_orientation_right r ON r.rkey = l.lkey "
              "ORDER BY l.lkey;"),
             [["1", "right one"], ["2", None]]),
        ]
        for sql, expected_rows in orientation_cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert headers == ["lkey", "rtxt"], (sql, headers)
            assert type_oids == [23, 25], (sql, type_oids)
            assert command_tag == "SELECT %d" % len(expected_rows), (
                sql, command_tag)

        outer_chain_cases = [
            (("SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
              "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
              "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
              "ORDER BY a.id;"),
             [["1", "1", "1"], ["2", None, None]]),
            (("SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
              "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
              "RIGHT JOIN join_outer_chain_c c ON b.id = c.id "
              "ORDER BY c.id;"),
             [["1", "1", "1"], [None, None, "3"]]),
            (("SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
              "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
              "FULL OUTER JOIN join_outer_chain_c c ON b.id = c.id "
              "ORDER BY a.id NULLS LAST;"),
             [["1", "1", "1"], ["2", None, None], [None, None, "3"]]),
        ]
        for outer_chain_sql, expected_rows in outer_chain_cases:
            rows, state, message, headers, command_tag, type_oids = (
                runner.decode_wire_result(
                    client.simple_query(server["sock"], outer_chain_sql),
                    include_types=True))
            assert state is None, (outer_chain_sql, state, message)
            assert rows == expected_rows, (outer_chain_sql, rows, expected_rows)
            assert headers == ["id", "id", "id"], headers
            assert type_oids == [23, 23, 23], type_oids
            assert command_tag == "SELECT %d" % len(expected_rows), command_tag

        outer_chain_where_sql = (
            "SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
            "WHERE a.id = 2;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], outer_chain_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", None, None]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        outer_chain_null_filter_sql = (
            "SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
            "WHERE b.id IS NULL;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    outer_chain_null_filter_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", None, None]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        projected_chain_sql = (
            "SELECT a.id FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], projected_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1"], ["2"]], rows
        assert headers == ["id"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        ordered_limited_chain_sql = (
            "SELECT a.id FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
            "ORDER BY a.id DESC LIMIT 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    ordered_limited_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert headers == ["id"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        nulls_first_chain_sql = (
            "SELECT a.id FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
            "ORDER BY c.id NULLS FIRST;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], nulls_first_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2"], ["1"]], rows
        assert headers == ["id"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        ordinal_offset_chain_sql = (
            "SELECT a.id AS row_no FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id "
            "ORDER BY 1 DESC OFFSET 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], ordinal_offset_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        assert headers == ["row_no"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        reordered_projection_sql = (
            "SELECT c.id AS third, a.id AS first "
            "FROM join_outer_chain_a a "
            "LEFT JOIN join_outer_chain_b b ON a.id = b.id "
            "LEFT JOIN join_outer_chain_c c ON b.id = c.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], reordered_projection_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "1"], [None, "2"]], rows
        assert headers == ["third", "first"], headers
        assert type_oids == [23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        cross_chain_sql = (
            "SELECT a.id, b.id, c.id FROM join_outer_chain_a a "
            "CROSS JOIN join_outer_chain_b b "
            "CROSS JOIN join_outer_chain_c c;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], cross_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert sorted(rows) == sorted([
            ["1", "1", "1"], ["1", "1", "3"],
            ["2", "1", "1"], ["2", "1", "3"]]), rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        many_relation_refs = [
            "join_many_%02d t%d" % (index, index) for index in range(14)]
        many_join_sql = "SELECT t13.id FROM " + many_relation_refs[0]
        for index in range(1, 14):
            many_join_sql += (
                " JOIN " + many_relation_refs[index] +
                " ON t%d.id = t%d.id" % (index - 1, index))
        many_join_sql += ";"
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], many_join_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["7"]], rows
        assert headers == ["id"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        join_conjunction_chain_sql = (
            "SELECT a.id, b.id, c.id FROM join_conj_chain_a a "
            "JOIN join_conj_chain_b b "
            "ON a.id = b.a_id AND b.val = 1 "
            "JOIN join_conj_chain_c c "
            "ON b.id = c.b_id AND c.id > 2;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    join_conjunction_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", "3", "4"]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        outer_join_conjunction_chain_sql = (
            "SELECT a.id, b.id, c.id FROM join_conj_chain_a a "
            "LEFT JOIN join_conj_chain_b b "
            "ON a.id = b.a_id AND b.val = 1 "
            "LEFT JOIN join_conj_chain_c c "
            "ON b.id = c.b_id AND c.id > 2 ORDER BY a.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    outer_join_conjunction_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "2", None], ["2", "3", "4"]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        right_join_conjunction_chain_sql = (
            "SELECT a.id, b.id, c.id FROM join_conj_chain_a a "
            "RIGHT JOIN join_conj_chain_b b "
            "ON a.id = b.a_id AND b.val = 1 "
            "RIGHT JOIN join_conj_chain_c c "
            "ON b.id = c.b_id AND c.id > 2 ORDER BY c.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    right_join_conjunction_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [[None, None, "2"], ["2", "3", "4"]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        full_join_conjunction_chain_sql = (
            "SELECT a.id, b.id, c.id FROM join_conj_chain_a a "
            "FULL OUTER JOIN join_conj_chain_b b "
            "ON a.id = b.a_id AND b.val = 1 "
            "FULL OUTER JOIN join_conj_chain_c c "
            "ON b.id = c.b_id AND c.id > 2 "
            "ORDER BY c.id NULLS FIRST, a.id NULLS LAST;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    full_join_conjunction_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "2", None], [None, "1", None],
            [None, None, "2"], ["2", "3", "4"],
        ], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        deferred_inner_on_sql = (
            "SELECT a.id, b.id, c.id FROM join_conj_chain_a a "
            "JOIN join_conj_chain_b b ON a.id = b.a_id "
            "JOIN join_conj_chain_c c "
            "ON b.id = c.b_id AND a.id > 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], deferred_inner_on_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", "3", "4"]], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        single_using_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "JOIN join_conj_chain_b b USING (id) ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], single_using_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "1", "0"], ["2", "1", "1"]], rows
        assert headers == ["id", "a_id", "val"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        merged_and_qualified_using_sql = (
            "SELECT id, a.id, b.id FROM join_conj_chain_a a "
            "FULL OUTER JOIN join_conj_chain_b b USING (id) "
            "ORDER BY 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    merged_and_qualified_using_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "1", "1"], ["2", "2", "2"], ["3", None, "3"],
        ], rows
        assert headers == ["id", "id", "id"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 3", command_tag

        single_natural_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "NATURAL JOIN join_conj_chain_b b ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], single_natural_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "1", "0"], ["2", "1", "1"]], rows
        assert headers == ["id", "a_id", "val"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        using_chain_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "JOIN join_conj_chain_b b USING (id) "
            "JOIN join_conj_chain_c c USING (id) WHERE id = 2;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], using_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", "1", "1", "2"]], rows
        assert headers == ["id", "a_id", "val", "b_id"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        composite_using_chain_sql = (
            "SELECT * FROM join_using_composite_a a "
            "JOIN join_using_composite_b b USING (id, code) "
            "JOIN join_using_composite_c c USING (code, id) "
            "ORDER BY 1, 2;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    composite_using_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["10", "1", "100", "3000"],
            ["20", "1", "200", "1000"],
        ], rows
        assert headers == ["code", "id", "bval", "cval"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        numeric_scale_using_sql = (
            "SELECT * FROM join_numeric_scale_a a "
            "JOIN join_numeric_scale_b b USING (id);")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], numeric_scale_using_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1.0"]], rows
        assert headers == ["id"], headers
        assert type_oids == [1700], type_oids
        assert command_tag == "SELECT 1", command_tag

        for numeric_join_sql, expected_key in (
                ("SELECT * FROM join_numeric_scale_a a "
                 "LEFT JOIN join_numeric_scale_b b USING (id);", "1.0"),
                ("SELECT * FROM join_numeric_scale_a a "
                 "RIGHT JOIN join_numeric_scale_b b USING (id);", "1.00"),
                ("SELECT * FROM join_numeric_scale_a a "
                 "FULL OUTER JOIN join_numeric_scale_b b USING (id);", "1.0")):
            rows, state, message, headers, command_tag, type_oids = (
                runner.decode_wire_result(
                    client.simple_query(server["sock"], numeric_join_sql),
                    include_types=True))
            assert state is None, (numeric_join_sql, state, message)
            assert rows == [[expected_key]], (numeric_join_sql, rows)
            assert headers == ["id"], (numeric_join_sql, headers)
            assert type_oids == [1700], (numeric_join_sql, type_oids)
            assert command_tag == "SELECT 1", (numeric_join_sql, command_tag)

        numeric_scale_on_sql = (
            "SELECT * FROM join_numeric_scale_a a "
            "JOIN join_numeric_scale_b b ON a.id = b.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], numeric_scale_on_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1.0", "1.00"]], rows
        assert headers == ["id", "id"], headers
        assert type_oids == [1700, 1700], type_oids
        assert command_tag == "SELECT 1", command_tag

        natural_chain_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "NATURAL JOIN join_conj_chain_b b "
            "NATURAL JOIN join_conj_chain_c c WHERE id = 2;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], natural_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", "1", "1", "2"]], rows
        assert headers == ["id", "a_id", "val", "b_id"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 1", command_tag

        natural_left_cartesian_sql = (
            "SELECT * FROM join_natural_empty_a a "
            "NATURAL LEFT JOIN join_natural_empty_b b "
            "ORDER BY id, value;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    natural_left_cartesian_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "10"], ["1", "20"], ["2", "10"], ["2", "20"],
        ], rows
        assert headers == ["id", "value"], headers
        assert type_oids == [23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        natural_right_cartesian_sql = (
            "SELECT * FROM join_natural_empty_a a "
            "NATURAL RIGHT JOIN join_natural_empty_b b "
            "ORDER BY id, value;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"],
                                    natural_right_cartesian_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "10"], ["1", "20"], ["2", "10"], ["2", "20"],
        ], rows
        assert headers == ["id", "value"], headers
        assert type_oids == [23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        natural_full_empty_sql = (
            "SELECT * FROM join_natural_empty_a a "
            "NATURAL FULL OUTER JOIN join_natural_no_rows b "
            "ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], natural_full_empty_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", None], ["2", None]], rows
        assert headers == ["id", "value"], headers
        assert type_oids == [23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        left_using_chain_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "LEFT JOIN join_conj_chain_b b USING (id) "
            "LEFT JOIN join_conj_chain_c c USING (id) ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], left_using_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "1", "0", None], ["2", "1", "1", "2"],
        ], rows
        assert headers == ["id", "a_id", "val", "b_id"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        right_using_chain_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "RIGHT JOIN join_conj_chain_b b USING (id) "
            "RIGHT JOIN join_conj_chain_c c USING (id) ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], right_using_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["2", "1", "1", "2"], ["4", None, None, "3"],
        ], rows
        assert headers == ["id", "a_id", "val", "b_id"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        full_using_chain_sql = (
            "SELECT * FROM join_conj_chain_a a "
            "FULL OUTER JOIN join_conj_chain_b b USING (id) "
            "FULL OUTER JOIN join_conj_chain_c c USING (id) ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], full_using_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "1", "0", None], ["2", "1", "1", "2"],
            ["3", "2", "1", None], ["4", None, None, "3"],
        ], rows
        assert headers == ["id", "a_id", "val", "b_id"], headers
        assert type_oids == [23, 23, 23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        structured_chain_sql = (
            "SELECT * FROM join_outer_chain_text_a a "
            "LEFT JOIN join_outer_chain_text_b b ON a.id = b.a_id "
            "LEFT JOIN join_outer_chain_text_c c ON b.id = c.b_id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], structured_chain_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "alpha one", "1", "1", "literal NULL", "1", "1",
             "gamma value"],
            ["2", "", "2", "2", None, None, None, None],
        ], rows
        assert headers == [
            "id", "label", "id", "a_id", "label", "id", "b_id", "label",
        ], headers
        assert type_oids == [23, 25, 23, 23, 25, 23, 23, 25], type_oids
        assert command_tag == "SELECT 2", command_tag

        structured_where_sql = (
            "SELECT a.label, b.label, c.label "
            "FROM join_outer_chain_text_a a "
            "LEFT JOIN join_outer_chain_text_b b ON a.id = b.a_id "
            "LEFT JOIN join_outer_chain_text_c c ON b.id = c.b_id "
            "WHERE b.label IS NULL;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], structured_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["", None, None]], rows
        assert headers == ["label", "label", "label"], headers
        assert type_oids == [25, 25, 25], type_oids
        assert command_tag == "SELECT 1", command_tag

        exact_sql = (
            "SELECT l.id, l.txt, r.txt FROM join_exact_left l "
            "JOIN join_exact_right r ON l.id = r.id ORDER BY l.id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], exact_sql), include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "r one"],
            ["2", None, ""],
            ["3", "NULL", None],
            ["4", "a b", "NULL"],
        ], rows
        assert headers == ["id", "txt", "txt"], headers
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        predicate_cases = [
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE r.txt LIKE 'r%' ORDER BY l.id;"),
             [["1"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE r.txt ILIKE 'R%' ORDER BY l.id;"),
             [["1"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.id IN (1, 4) ORDER BY l.id;"),
             [["1"], ["4"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.txt IN ('', 'a b', 'NULL') ORDER BY l.id;"),
             [["1"], ["3"], ["4"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.txt IN ('a b') ORDER BY l.id;"),
             [["4"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.txt IN ('') ORDER BY l.id;"),
             [["1"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.id NOT IN (1, NULL) ORDER BY l.id;"),
             []),
            (("SELECT l.id FROM join_pred_left l "
              "JOIN join_pred_right r ON l.id = r.id "
              "WHERE l.txt IN ('a,b') ORDER BY l.id;"),
             [["1"]]),
            (("SELECT l.id FROM join_pred_left l "
              "JOIN join_pred_right r ON l.id = r.id "
              "WHERE l.txt = 'l.id' ORDER BY l.id;"),
             [["4"]]),
            (("SELECT l.id FROM join_pred_left l "
              "JOIN join_pred_right r ON l.id = r.id "
              "WHERE l.txt = 'r.id' ORDER BY l.id;"),
             [["5"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.id BETWEEN 2 AND 3 ORDER BY l.id;"),
             [["2"], ["3"]]),
            (("SELECT l.id FROM join_exact_left l "
              "JOIN join_exact_right r ON l.id = r.id "
              "WHERE l.id < r.id ORDER BY l.id;"),
             []),
        ]
        for predicate_sql, expected_rows in predicate_cases:
            rows, state, message, headers, command_tag, type_oids = (
                runner.decode_wire_result(
                    client.simple_query(server["sock"], predicate_sql),
                    include_types=True))
            assert state is None, (predicate_sql, state, message)
            assert rows == expected_rows, (predicate_sql, rows)
            assert type_oids == [23], (predicate_sql, type_oids)
            assert command_tag == "SELECT %d" % len(rows), (
                predicate_sql, command_tag)

        outer_predicate_sql = (
            "SELECT l.id, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id "
            "WHERE r.txt NOT LIKE 'r%' ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], outer_predicate_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["2", ""], ["4", "NULL"]], rows
        assert command_tag == "SELECT 2", command_tag

        cross_column_predicate_sql = (
            "SELECT l.id, r.id FROM join_exact_left l "
            "CROSS JOIN join_exact_right r WHERE l.id = r.id ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], cross_column_predicate_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"]
        ], rows
        assert type_oids == [23, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        ordered_sql = (
            "SELECT l.id, l.txt FROM join_exact_left l "
            "JOIN join_exact_right r ON l.id = r.id "
            "ORDER BY r.txt DESC NULLS LAST, l.id DESC LIMIT 2 OFFSET 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], ordered_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["4", "a b"], ["2", None]], rows
        assert headers == ["id", "txt"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 2", command_tag

        alias_order_sql = (
            "SELECT l.id, r.txt AS value FROM join_exact_left l "
            "JOIN join_exact_right r ON l.id = r.id "
            "ORDER BY value ASC NULLS FIRST;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], alias_order_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["3", None], ["2", ""], ["4", "NULL"], ["1", "r one"]
        ], rows
        assert headers == ["id", "value"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        ordinal_order_sql = (
            "SELECT l.id, l.txt FROM join_exact_left l "
            "JOIN join_exact_right r ON l.id = r.id ORDER BY 1 DESC;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], ordinal_order_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["4", "a b"], ["3", "NULL"], ["2", None], ["1", ""]
        ], rows
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        left_sql = (
            "SELECT l.id, l.txt, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], left_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "r one"], ["2", None, ""],
            ["3", "NULL", None], ["4", "a b", "NULL"],
            ["5", "left only", None],
        ], rows
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 5", command_tag

        expression_sql = (
            "SELECT l.id, l.txt || r.txt AS joined, "
            "COALESCE(l.txt, 'nil') AS left_value "
            "FROM join_exact_left l LEFT JOIN join_exact_right r "
            "ON l.id = r.id ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], expression_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "r one", ""], ["2", None, "nil"],
            ["3", None, "NULL"], ["4", "a bNULL", "a b"],
            ["5", None, "left only"],
        ], rows
        assert headers == ["id", "joined", "left_value"], headers
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 5", command_tag

        for bad_expression, sqlstate in [
            ("COALESCE(txt, 'nil')", "42702"),
            ("COALESCE(missing.txt, 'nil')", "42P01"),
            ("COALESCE(l.missing_txt, 'nil')", "42703"),
        ]:
            invalid_sql = (
                "SELECT " + bad_expression + " FROM join_exact_left l "
                "JOIN join_exact_right r ON l.id = r.id;")
            _, state, message, _, _, _ = runner.decode_wire_result(
                client.simple_query(server["sock"], invalid_sql),
                include_types=True)
            assert state == sqlstate, (invalid_sql, state, message)

        for bad_column, sqlstate in [
            ("txt", "42702"),
            ("missing.txt", "42P01"),
            ("l.missing_txt", "42703"),
        ]:
            invalid_sql = (
                "SELECT " + bad_column + " FROM join_exact_left l "
                "JOIN join_exact_right r ON l.id = r.id;")
            _, state, message, _, _, _ = runner.decode_wire_result(
                client.simple_query(server["sock"], invalid_sql),
                include_types=True)
            assert state == sqlstate, (invalid_sql, state, message)

        left_where_sql = (
            "SELECT l.id, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id "
            "WHERE r.txt = 'r one';")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], left_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "r one"]], rows
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 1", command_tag

        explicit_as_where_sql = (
            "SELECT l.id, r.txt FROM join_exact_left AS l "
            "LEFT JOIN join_exact_right AS r ON l.id = r.id "
            "WHERE r.txt = 'r one';")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], explicit_as_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "r one"]], rows
        assert headers == ["id", "txt"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 1", command_tag

        preserved_where_sql = (
            "SELECT l.id, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id "
            "WHERE l.txt = 'left only';")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], preserved_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["5", None]], rows
        assert command_tag == "SELECT 1", command_tag

        null_extended_where_sql = (
            "SELECT l.id, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id "
            "WHERE r.txt IS NULL ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], null_extended_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["3", None], ["5", None]], rows
        assert command_tag == "SELECT 2", command_tag

        not_null_where_sql = (
            "SELECT l.id, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id "
            "WHERE r.txt IS NOT NULL ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], not_null_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["1", "r one"], ["2", ""], ["4", "NULL"]], rows
        assert command_tag == "SELECT 3", command_tag

        right_sql = (
            "SELECT l.txt, r.id, r.txt FROM join_exact_left l "
            "RIGHT JOIN join_exact_right r ON l.id = r.id ORDER BY r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], right_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["", "1", "r one"], [None, "2", ""],
            ["NULL", "3", None], ["a b", "4", "NULL"],
            [None, "6", "right only"],
        ], rows
        assert type_oids == [25, 23, 25], type_oids
        assert command_tag == "SELECT 5", command_tag

        right_where_sql = (
            "SELECT l.txt, r.id FROM join_exact_left l "
            "RIGHT JOIN join_exact_right r ON l.id = r.id "
            "WHERE l.txt = 'a b';")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], right_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["a b", "4"]], rows
        assert command_tag == "SELECT 1", command_tag

        full_sql = (
            "SELECT l.id, l.txt, r.id, r.txt FROM join_exact_left l "
            "FULL OUTER JOIN join_exact_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], full_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "1", "r one"], ["2", None, "2", ""],
            ["3", "NULL", "3", None], ["4", "a b", "4", "NULL"],
            ["5", "left only", None, None],
            [None, None, "6", "right only"],
        ], rows
        assert type_oids == [23, 25, 23, 25], type_oids
        assert command_tag == "SELECT 6", command_tag

        full_where_sql = (
            "SELECT l.id, l.txt, r.id FROM join_exact_left l "
            "FULL OUTER JOIN join_exact_right r ON l.id = r.id "
            "WHERE l.id = 5;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], full_where_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["5", "left only", None]], rows
        assert command_tag == "SELECT 1", command_tag

        collision_sql = (
            "SELECT l.txt, r.txt FROM join_collision_left l "
            "FULL OUTER JOIN join_collision_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], collision_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["NULL", "b c"], [None, "b c"]], rows
        assert type_oids == [25, 25], type_oids
        assert command_tag == "SELECT 2", command_tag

        distinct_sql = (
            "SELECT DISTINCT l.txt FROM join_exact_left l "
            "CROSS JOIN join_exact_right r;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], distinct_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [[""], [None], ["NULL"], ["a b"], ["left only"]], rows
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 5", command_tag

        cross_limit_sql = (
            "SELECT l.txt AS left_text, r.txt AS right_text "
            "FROM join_exact_left l CROSS JOIN join_exact_right r "
            "LIMIT 3 OFFSET 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], cross_limit_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["", ""], ["", None], ["", "NULL"]], rows
        assert headers == ["left_text", "right_text"], headers
        assert type_oids == [25, 25], type_oids
        assert command_tag == "SELECT 3", command_tag

        aggregate_sql = (
            "SELECT count(l.txt) AS left_count, "
            "count(r.txt) AS right_count, min(l.txt) AS minimum, "
            "max(r.txt) AS maximum "
            "FROM join_exact_left l JOIN join_exact_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], aggregate_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["3", "3", "", "r one"]], rows
        assert headers == [
            "left_count", "right_count", "minimum", "maximum"
        ], headers
        assert type_oids == [20, 20, 25, 25], type_oids
        assert command_tag == "SELECT 1", command_tag
        print("[JOIN TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
