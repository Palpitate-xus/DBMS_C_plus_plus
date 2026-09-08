#!/usr/bin/env python3
"""Plain table SELECT preserves cells and SQL NULL without text framing."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "table_structured_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE exact_table_rows (id INT, v TEXT);",
            ("INSERT INTO exact_table_rows VALUES "
             "(1, 'hello world'), (2, ''), (3, NULL), (4, 'NULL'), "
             "(5, 'line1\nline2'), (6, 'say \"hi\"'), (7, ' lead ');"),
            "CREATE TABLE exact_null_order (id INT, v INT);",
            "INSERT INTO exact_null_order VALUES (1, NULL), (2, 1), (3, 2);",
            "CREATE TABLE exact_scalar_text_order (v TEXT);",
            "INSERT INTO exact_scalar_text_order VALUES ('2'), ('10'), (NULL);",
            "CREATE TABLE exact_scalar_or_rows (id INT, v TEXT);",
            ("INSERT INTO exact_scalar_or_rows VALUES "
             "(1, 'same'), (2, 'same'), (3, 'NULL'), "
             "(4, 'line\nbreak'), (5, NULL);"),
            "CREATE TABLE exact_distinct_on (id INT, v TEXT);",
            ("INSERT INTO exact_distinct_on VALUES "
             "(1, ''), (2, NULL), (3, ''), (4, NULL), "
             "(5, 'NULL'), (6, 'NULL');"),
            "CREATE TABLE exact_aggregate_rows (g INT, v TEXT);",
            ("INSERT INTO exact_aggregate_rows VALUES "
             "(1, 'hello world'), (1, 'line\nbreak'), "
             "(2, 'NULL'), (3, NULL), (4, '');"),
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        sql = "SELECT id, v FROM exact_table_rows ORDER BY id;"
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        expected = [
            ["1", "hello world"],
            ["2", ""],
            ["3", None],
            ["4", "NULL"],
            ["5", "line1\nline2"],
            ["6", 'say "hi"'],
            ["7", " lead "],
        ]
        assert state is None, (state, message)
        assert rows == expected, (rows, expected)
        assert headers == ["id", "v"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 7", command_tag

        aggregate_sql = (
            "SELECT count(*), string_agg(v, '|') AS joined "
            "FROM exact_aggregate_rows;")
        aggregate_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], aggregate_sql),
            include_types=True)
        aggregate_rows, aggregate_state, aggregate_message, \
            aggregate_headers, aggregate_tag, aggregate_types = \
            aggregate_decoded
        assert aggregate_state is None, (
            aggregate_state, aggregate_message)
        assert aggregate_rows == [
            ["5", "hello world|line\nbreak|NULL|"]
        ], aggregate_rows
        assert aggregate_headers == ["count", "joined"], aggregate_headers
        assert aggregate_types == [20, 25], aggregate_types
        assert aggregate_tag == "SELECT 1", aggregate_tag

        grouped_aggregate_sql = (
            "SELECT g, string_agg(v, '|') AS joined "
            "FROM exact_aggregate_rows GROUP BY g ORDER BY g DESC;")
        grouped_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], grouped_aggregate_sql),
            include_types=True)
        grouped_rows, grouped_state, grouped_message, grouped_headers, \
            grouped_tag, grouped_types = grouped_decoded
        assert grouped_state is None, (grouped_state, grouped_message)
        assert grouped_rows == [
            ["4", ""],
            ["3", None],
            ["2", "NULL"],
            ["1", "hello world|line\nbreak"],
        ], grouped_rows
        assert grouped_headers == ["g", "joined"], grouped_headers
        assert grouped_types == [23, 25], grouped_types
        assert grouped_tag == "SELECT 4", grouped_tag

        reordered_group_sql = (
            "SELECT string_agg(v, '|') AS joined, g "
            "FROM exact_aggregate_rows GROUP BY g ORDER BY g;")
        reordered_group_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], reordered_group_sql),
            include_types=True)
        reordered_group_rows, reordered_group_state, \
            reordered_group_message, reordered_group_headers, \
            reordered_group_tag, reordered_group_types = \
            reordered_group_decoded
        assert reordered_group_state is None, (
            reordered_group_state, reordered_group_message)
        assert reordered_group_rows == [
            ["hello world|line\nbreak", "1"],
            ["NULL", "2"],
            [None, "3"],
            ["", "4"],
        ], reordered_group_rows
        assert reordered_group_headers == ["joined", "g"], \
            reordered_group_headers
        assert reordered_group_types == [25, 23], reordered_group_types
        assert reordered_group_tag == "SELECT 4", reordered_group_tag

        omitted_group_key_sql = (
            "SELECT count(*) AS total FROM exact_aggregate_rows "
            "GROUP BY g ORDER BY total DESC LIMIT 1;")
        omitted_group_key_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], omitted_group_key_sql),
            include_types=True)
        omitted_group_key_rows, omitted_group_key_state, \
            omitted_group_key_message, omitted_group_key_headers, \
            omitted_group_key_tag, omitted_group_key_types = \
            omitted_group_key_decoded
        assert omitted_group_key_state is None, (
            omitted_group_key_state, omitted_group_key_message)
        assert omitted_group_key_rows == [["2"]], omitted_group_key_rows
        assert omitted_group_key_headers == ["total"], \
            omitted_group_key_headers
        assert omitted_group_key_types == [20], omitted_group_key_types
        assert omitted_group_key_tag == "SELECT 1", omitted_group_key_tag

        repeated_group_key_sql = (
            "SELECT g, count(*), g AS again FROM exact_aggregate_rows "
            "GROUP BY g ORDER BY g;")
        repeated_group_key_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], repeated_group_key_sql),
            include_types=True)
        repeated_group_key_rows, repeated_group_key_state, \
            repeated_group_key_message, repeated_group_key_headers, \
            repeated_group_key_tag, repeated_group_key_types = \
            repeated_group_key_decoded
        assert repeated_group_key_state is None, (
            repeated_group_key_state, repeated_group_key_message)
        assert repeated_group_key_rows == [
            ["1", "2", "1"], ["2", "1", "2"],
            ["3", "1", "3"], ["4", "1", "4"],
        ], repeated_group_key_rows
        assert repeated_group_key_headers == ["g", "count", "again"], \
            repeated_group_key_headers
        assert repeated_group_key_types == [23, 20, 23], \
            repeated_group_key_types
        assert repeated_group_key_tag == "SELECT 4", repeated_group_key_tag

        distinct_aggregate_sql = (
            "SELECT DISTINCT g, count(*) FROM exact_aggregate_rows "
            "GROUP BY GROUPING SETS ((g), (g)) ORDER BY g;")
        distinct_aggregate_decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], distinct_aggregate_sql),
            include_types=True)
        distinct_aggregate_rows, distinct_aggregate_state, \
            distinct_aggregate_message, distinct_aggregate_headers, \
            distinct_aggregate_tag, distinct_aggregate_types = \
            distinct_aggregate_decoded
        assert distinct_aggregate_state is None, (
            distinct_aggregate_state, distinct_aggregate_message)
        assert distinct_aggregate_rows == [
            ["1", "2"], ["2", "1"], ["3", "1"], ["4", "1"],
        ], distinct_aggregate_rows
        assert distinct_aggregate_headers == ["g", "count"], \
            distinct_aggregate_headers
        assert distinct_aggregate_types == [23, 20], \
            distinct_aggregate_types
        assert distinct_aggregate_tag == "SELECT 4", distinct_aggregate_tag

        reversed_sql = "SELECT v, id FROM exact_table_rows ORDER BY id;"
        reversed_rows, reversed_state, reversed_message, reversed_headers = (
            runner.ours_query(client, server["sock"], reversed_sql))
        assert reversed_state is None, (reversed_state, reversed_message)
        assert reversed_rows == [[row[1], row[0]] for row in expected], \
            reversed_rows
        assert reversed_headers == ["v", "id"], reversed_headers

        one_column_sql = "SELECT v FROM exact_table_rows ORDER BY id;"
        one_column_rows, one_column_state, one_column_message, _ = (
            runner.ours_query(client, server["sock"], one_column_sql))
        assert one_column_state is None, (
            one_column_state, one_column_message)
        assert one_column_rows == [[row[1]] for row in expected], \
            one_column_rows

        filtered_plain_sql = (
            "SELECT v, id FROM exact_table_rows "
            "WHERE id >= 2 AND id <= 7 ORDER BY id;")
        filtered_plain_rows, filtered_plain_state, filtered_plain_message, _ = (
            runner.ours_query(client, server["sock"], filtered_plain_sql))
        assert filtered_plain_state is None, (
            filtered_plain_state, filtered_plain_message)
        assert filtered_plain_rows == [
            [row[1], row[0]] for row in expected[1:]
        ], filtered_plain_rows

        plain_or_sql = (
            "SELECT v FROM exact_scalar_or_rows "
            "WHERE id <= 4 OR id >= 2;")
        plain_or_rows, plain_or_state, plain_or_message, _ = (
            runner.ours_query(client, server["sock"], plain_or_sql))
        assert plain_or_state is None, (
            plain_or_state, plain_or_message)
        assert plain_or_rows == [
            ["same"], ["same"], ["NULL"], ["line\nbreak"], [None],
        ], plain_or_rows

        ordered_plain_or_sql = (
            "SELECT v FROM exact_scalar_or_rows "
            "WHERE id <= 4 OR id >= 2 ORDER BY id DESC;")
        ordered_plain_or_rows, ordered_plain_or_state, \
            ordered_plain_or_message, _ = runner.ours_query(
                client, server["sock"], ordered_plain_or_sql)
        assert ordered_plain_or_state is None, (
            ordered_plain_or_state, ordered_plain_or_message)
        assert ordered_plain_or_rows == [
            [None], ["line\nbreak"], ["NULL"], ["same"], ["same"],
        ], ordered_plain_or_rows

        plain_alias_sql = (
            "SELECT v AS rendered, id FROM exact_table_rows "
            "ORDER BY rendered;")
        plain_alias_rows, plain_alias_state, plain_alias_message, _ = (
            runner.ours_query(client, server["sock"], plain_alias_sql))
        assert plain_alias_state is None, (
            plain_alias_state, plain_alias_message)
        plain_alias_expected = [
            [row[1], row[0]] for row in sorted(
                (row for row in expected if row[1] is not None),
                key=lambda row: row[1])
        ] + [[None, "3"]]
        assert plain_alias_rows == plain_alias_expected, (
            plain_alias_rows, plain_alias_expected)

        expression_order_sql = (
            "SELECT id FROM exact_table_rows ORDER BY lower(v);")
        expression_order_rows, expression_order_state, \
            expression_order_message, _ = runner.ours_query(
                client, server["sock"], expression_order_sql)
        assert expression_order_state is None, (
            expression_order_state, expression_order_message)
        assert expression_order_rows == [
            ["2"], ["7"], ["1"], ["5"], ["4"], ["6"], ["3"],
        ], expression_order_rows

        plain_distinct_sql = (
            "SELECT DISTINCT v FROM exact_scalar_or_rows ORDER BY v;")
        plain_distinct_rows, plain_distinct_state, plain_distinct_message, _ = (
            runner.ours_query(client, server["sock"], plain_distinct_sql))
        assert plain_distinct_state is None, (
            plain_distinct_state, plain_distinct_message)
        assert plain_distinct_rows == [
            ["NULL"], ["line\nbreak"], ["same"], [None],
        ], plain_distinct_rows

        distinct_on_sql = (
            "SELECT DISTINCT ON (v) v, id FROM exact_distinct_on "
            "ORDER BY v, id;")
        distinct_on_rows, distinct_on_state, distinct_on_message, _ = (
            runner.ours_query(client, server["sock"], distinct_on_sql))
        assert distinct_on_state is None, (
            distinct_on_state, distinct_on_message)
        assert distinct_on_rows == [
            ["", "1"], ["NULL", "5"], [None, "2"],
        ], distinct_on_rows

        plain_slice_sql = (
            "SELECT v FROM exact_table_rows "
            "ORDER BY id LIMIT 3 OFFSET 2;")
        plain_slice_rows, plain_slice_state, plain_slice_message, _ = (
            runner.ours_query(client, server["sock"], plain_slice_sql))
        assert plain_slice_state is None, (
            plain_slice_state, plain_slice_message)
        assert plain_slice_rows == [
            [None], ["NULL"], ["line1\nline2"],
        ], plain_slice_rows

        scalar_cases = [
            (
                "SELECT upper(v) AS rendered FROM exact_table_rows "
                "ORDER BY id;",
                [[None if row[1] is None else row[1].upper()]
                 for row in expected],
            ),
            (
                "SELECT coalesce(v, 'fallback') AS rendered "
                "FROM exact_table_rows ORDER BY id;",
                [["fallback" if row[1] is None else row[1]]
                 for row in expected],
            ),
            (
                "SELECT v || '!' AS rendered FROM exact_table_rows "
                "ORDER BY id;",
                [[None if row[1] is None else row[1] + "!"]
                 for row in expected],
            ),
        ]
        for scalar_sql, scalar_expected in scalar_cases:
            scalar_rows, scalar_state, scalar_message, scalar_headers = (
                runner.ours_query(client, server["sock"], scalar_sql))
            assert scalar_state is None, (
                scalar_sql, scalar_state, scalar_message)
            assert scalar_rows == scalar_expected, (
                scalar_sql, scalar_rows, scalar_expected)
            assert scalar_headers == ["rendered"], scalar_headers

        predicate_sql = (
            "SELECT upper(v) AS rendered FROM exact_table_rows "
            "WHERE id >= 2 AND id <= 7 ORDER BY id;")
        predicate_rows, predicate_state, predicate_message, _ = (
            runner.ours_query(client, server["sock"], predicate_sql))
        assert predicate_state is None, (
            predicate_state, predicate_message)
        assert predicate_rows == [
            [None if row[1] is None else row[1].upper()]
            for row in expected[1:]
        ], predicate_rows

        or_sql = (
            "SELECT upper(v) AS rendered FROM exact_scalar_or_rows "
            "WHERE id <= 4 OR id >= 2;")
        or_rows, or_state, or_message, _ = runner.ours_query(
            client, server["sock"], or_sql)
        assert or_state is None, (or_state, or_message)
        assert len(or_rows) == 5, or_rows
        assert or_rows.count(["SAME"]) == 2, or_rows
        assert ["NULL"] in or_rows, or_rows
        assert ["LINE\nBREAK"] in or_rows, or_rows
        assert [None] in or_rows, or_rows

        ordered_or_sql = (
            "SELECT upper(v) AS rendered FROM exact_scalar_or_rows "
            "WHERE id <= 4 OR id >= 2 ORDER BY id DESC;")
        ordered_or_rows, ordered_or_state, ordered_or_message, _ = (
            runner.ours_query(client, server["sock"], ordered_or_sql))
        assert ordered_or_state is None, (
            ordered_or_state, ordered_or_message)
        assert ordered_or_rows == [
            [None], ["LINE\nBREAK"], ["NULL"], ["SAME"], ["SAME"],
        ], ordered_or_rows

        distinct_sql = (
            "SELECT DISTINCT upper(v) AS rendered "
            "FROM exact_scalar_or_rows ORDER BY rendered;")
        distinct_rows, distinct_state, distinct_message, _ = (
            runner.ours_query(client, server["sock"], distinct_sql))
        assert distinct_state is None, (distinct_state, distinct_message)
        assert distinct_rows == [
            ["LINE\nBREAK"], ["NULL"], ["SAME"], [None],
        ], distinct_rows

        slice_sql = (
            "SELECT upper(v) AS rendered FROM exact_table_rows "
            "ORDER BY id LIMIT 3 OFFSET 2;")
        slice_rows, slice_state, slice_message, _ = runner.ours_query(
            client, server["sock"], slice_sql)
        assert slice_state is None, (slice_state, slice_message)
        assert slice_rows == [[None], ["NULL"], ["LINE1\nLINE2"]], \
            slice_rows

        alias_values = [
            None if row[1] is None else row[1].upper()
            for row in expected
        ]
        alias_expected = [[value] for value in sorted(
            (value for value in alias_values if value is not None),
            key=str.casefold)] + [[None]]
        alias_sql = (
            "SELECT upper(v) AS rendered FROM exact_table_rows "
            "ORDER BY rendered;")
        alias_rows, alias_state, alias_message, _ = runner.ours_query(
            client, server["sock"], alias_sql)
        assert alias_state is None, (alias_state, alias_message)
        assert alias_rows == alias_expected, (alias_rows, alias_expected)

        ordinal_values = [
            None if row[1] is None else row[1] + "!"
            for row in expected
        ]
        ordinal_expected = [[value] for value in sorted(
            (value for value in ordinal_values if value is not None),
            key=str.casefold, reverse=True)] + [[None]]
        ordinal_sql = (
            "SELECT v || '!' AS rendered FROM exact_table_rows "
            "ORDER BY 1 DESC NULLS LAST;")
        ordinal_rows, ordinal_state, ordinal_message, _ = runner.ours_query(
            client, server["sock"], ordinal_sql)
        assert ordinal_state is None, (ordinal_state, ordinal_message)
        assert ordinal_rows == ordinal_expected, (
            ordinal_rows, ordinal_expected)

        typed_order_sql = (
            "SELECT coalesce(v, 'z') AS rendered "
            "FROM exact_scalar_text_order ORDER BY rendered;")
        typed_order_rows, typed_order_state, typed_order_message, _ = (
            runner.ours_query(client, server["sock"], typed_order_sql))
        assert typed_order_state is None, (
            typed_order_state, typed_order_message)
        assert typed_order_rows == [["10"], ["2"], ["z"]], \
            typed_order_rows

        order_cases = [
            ("SELECT id FROM exact_null_order ORDER BY v;",
             [["2"], ["3"], ["1"]]),
            ("SELECT id FROM exact_null_order ORDER BY v DESC;",
             [["1"], ["3"], ["2"]]),
            ("SELECT id FROM exact_null_order ORDER BY v DESC NULLS LAST;",
             [["3"], ["2"], ["1"]]),
            ("SELECT id FROM exact_null_order ORDER BY v ASC NULLS FIRST;",
             [["1"], ["2"], ["3"]]),
        ]
        for order_sql, expected_rows in order_cases:
            order_rows, order_state, order_message, _ = runner.ours_query(
                client, server["sock"], order_sql)
            assert order_state is None, (order_sql, order_state, order_message)
            assert order_rows == expected_rows, (
                order_sql, order_rows, expected_rows)
        print("[TABLE STRUCTURED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
