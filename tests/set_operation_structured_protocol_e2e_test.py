#!/usr/bin/env python3
"""Set operations retain exact cells, NULL metadata, and common types."""

import importlib.util
from pathlib import Path
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "setop_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE setop_edges (id INT, v TEXT);",
            ("INSERT INTO setop_edges VALUES "
             "(1, 'a b'), (2, ''), (3, 'NULL'), (4, NULL), "
             "(5, 'line1\nline2'), (6, '  edge  ');"),
            ("CREATE TABLE fetch_ties_plain "
             "(id INT, score NUMERIC, label TEXT);"),
            ("INSERT INTO fetch_ties_plain VALUES "
            "(1, 10, 'z'), (2, 20, 'b'), (3, 20.00, 'a'), "
            "(4, 20.0, 'a'), (5, 30, 'z'), "
            "(6, NULL, 'null-a'), (6, NULL, 'null-a');"),
            "CREATE TABLE fetch_ties_char (id INT, code CHAR(3));",
            ("INSERT INTO fetch_ties_char VALUES "
             "(1, 'a'), (2, 'a '), (3, 'b');")
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        plain_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT id, score FROM fetch_ties_plain ORDER BY score "
                 "FETCH FIRST 2 ROWS WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = plain_ties
        assert state is None, (state, message)
        assert rows == [["1", "10"], ["2", "20"],
                        ["3", "20.00"], ["4", "20.0"]], rows
        assert headers == ["id", "score"], headers
        assert type_oids == [23, 1700], type_oids
        assert command_tag == "SELECT 4", command_tag

        plain_alias_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT score AS amount, id FROM fetch_ties_plain "
                 "ORDER BY amount FETCH FIRST 2 ROWS WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            plain_alias_ties
        assert state is None, (state, message)
        assert rows == [["10", "1"], ["20", "2"],
                        ["20.00", "3"], ["20.0", "4"]], rows
        assert headers == ["amount", "id"], headers
        assert type_oids == [1700, 23], type_oids
        assert command_tag == "SELECT 4", command_tag

        plain_offset_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT id, score FROM fetch_ties_plain ORDER BY score "
                 "OFFSET 1 ROW FETCH NEXT 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            plain_offset_ties
        assert state is None, (state, message)
        assert rows == [["2", "20"], ["3", "20.00"], ["4", "20.0"]], rows
        assert type_oids == [23, 1700], type_oids
        assert command_tag == "SELECT 3", command_tag

        plain_multi_key_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT label, id, score FROM fetch_ties_plain "
                 "ORDER BY score DESC NULLS LAST, label ASC "
                 "FETCH FIRST 2 ROWS WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            plain_multi_key_ties
        assert state is None, (state, message)
        assert rows == [["z", "5", "30"], ["a", "3", "20.00"],
                        ["a", "4", "20.0"]], rows
        assert headers == ["label", "id", "score"], headers
        assert type_oids == [25, 23, 1700], type_oids
        assert command_tag == "SELECT 3", command_tag

        plain_null_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT id, score FROM fetch_ties_plain "
                 "ORDER BY score NULLS FIRST "
                 "FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = plain_null_ties
        assert state is None, (state, message)
        assert rows == [["6", None], ["6", None]], rows
        assert type_oids == [23, 1700], type_oids
        assert command_tag == "SELECT 2", command_tag

        char_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT id, code FROM fetch_ties_char ORDER BY code "
                 "FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, _, command_tag, _ = char_ties
        assert state is None, (state, message)
        assert [row[0] for row in rows] == ["1", "2"], rows
        assert command_tag == "SELECT 2", command_tag

        hidden_order_key = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT id FROM fetch_ties_plain ORDER BY score "
                 "FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        _, state, message, _, _, _ = hidden_order_key
        assert state == "0A000", (state, message)

        sql = ("SELECT upper(v) AS value FROM setop_edges "
               "UNION ALL SELECT 'tail'::text;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["A B"], [""], ["NULL"], [None], ["LINE1\nLINE2"],
            ["  EDGE  "], ["tail"]
        ], rows
        assert headers == ["value"], headers
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 7", command_tag

        distinct = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT v FROM setop_edges UNION SELECT v FROM setop_edges;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = distinct
        assert state is None, (state, message)
        assert rows == [
            ["a b"], [""], ["NULL"], [None], ["line1\nline2"],
            ["  edge  "]
        ], rows
        assert headers == ["v"], headers
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 6", command_tag

        promoted = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 AS n UNION SELECT count(*) FROM setop_edges;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = promoted
        assert state is None, (state, message)
        assert rows == [["1"], ["6"]], rows
        assert headers == ["n"], headers
        assert type_oids == [20], type_oids
        assert command_tag == "SELECT 2", command_tag

        ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 10 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = ordered
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        ordered_by_multiple_keys = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1 AS grp, 'b' AS label UNION ALL "
                 "SELECT 1, 'a' UNION ALL SELECT 0, 'z' UNION ALL "
                 "SELECT 1, NULL::text "
                 "ORDER BY grp DESC, label ASC NULLS FIRST;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            ordered_by_multiple_keys
        assert state is None, (state, message)
        assert rows == [
            ["1", None], ["1", "a"], ["1", "b"], ["0", "z"]
        ], rows
        assert headers == ["grp", "label"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        fetch_ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n "
                 "FETCH NEXT 1 ROWS ONLY;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = fetch_ordered
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        offset_fetch_ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n "
                 "OFFSET 1 ROWS FETCH FIRST ROW ONLY;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            offset_fetch_ordered
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        with_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 "
                 "ORDER BY n FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = with_ties
        assert state is None, (state, message)
        assert rows == [["1"], ["1"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        parenthesized_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("((SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2)) "
                 "ORDER BY n FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            parenthesized_ties
        assert state is None, (state, message)
        assert rows == [["1"], ["1"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        offset_with_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 "
                 "UNION ALL SELECT 2 ORDER BY n OFFSET 1 ROWS "
                 "FETCH NEXT 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = \
            offset_with_ties
        assert state is None, (state, message)
        assert rows == [["1"], ["1"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        numeric_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT 1::numeric AS n UNION ALL SELECT 1.00::numeric "
                 "UNION ALL SELECT 2::numeric ORDER BY n "
                 "FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = numeric_ties
        assert state is None, (state, message)
        assert rows == [["1"], ["1.00"]], rows
        assert headers == ["n"], headers
        assert type_oids == [1700], type_oids
        assert command_tag == "SELECT 2", command_tag

        null_ties = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT NULL::text AS v UNION ALL SELECT NULL::text "
                 "UNION ALL SELECT ''::text ORDER BY v NULLS FIRST "
                 "FETCH FIRST 1 ROW WITH TIES;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = null_ties
        assert state is None, (state, message)
        assert rows == [[None], [None]], rows
        assert headers == ["v"], headers
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 2", command_tag

        null_ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT NULL::text AS v UNION ALL SELECT ''::text "
                 "ORDER BY v NULLS FIRST;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = null_ordered
        assert state is None, (state, message)
        assert rows == [[None], [""]], rows
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 2", command_tag

        precedence = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = precedence
        assert state is None, (state, message)
        assert rows == [["1"], ["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        left_associative = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 EXCEPT SELECT 1 UNION SELECT 2;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = left_associative
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        parenthesized = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("(SELECT 1 UNION SELECT 2) INTERSECT "
                 "(SELECT 2 UNION SELECT 3);")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = parenthesized
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        parenthesized_leaf = runner.decode_wire_result(
            client.simple_query(server["sock"], "(SELECT 1);"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = parenthesized_leaf
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        commented_operand = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "(SELECT 1 UNION SELECT 2) /* left operand */ INTERSECT SELECT 2;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = commented_operand
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        parenthesized_order = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "((SELECT 2 AS n UNION ALL SELECT 1)) ORDER BY n LIMIT 1;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = parenthesized_order
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        duplicate_all = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = duplicate_all
        assert state is None, (state, message)
        assert rows == [["1"], ["1"], ["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 3", command_tag

        branch_limit = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("(SELECT 2 AS n UNION ALL SELECT 1 ORDER BY n LIMIT 1) "
                 "UNION ALL SELECT 3 ORDER BY n;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = branch_limit
        assert state is None, (state, message)
        assert rows == [["1"], ["3"]], rows
        assert headers == ["n"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        parse = (b"setop_stmt\0(SELECT 1 UNION SELECT 2) INTERSECT "
                 b"(SELECT 2 UNION SELECT 3)\0" + struct.pack("!H", 0))
        bind = (b"setop_portal\0setop_stmt\0" + struct.pack("!H", 0) +
                struct.pack("!H", 0) + struct.pack("!H", 0))
        extended = client.typed(b"P", parse) + \
            client.typed(b"B", bind) + \
            client.typed(b"E", b"setop_portal\0" + struct.pack("!I", 0)) + \
            client.typed(b"S")
        server["sock"].sendall(extended)
        messages = client.read_until_ready(server["sock"])
        assert not any(kind == b"E" for kind, _ in messages), messages
        assert client.data_row_values(messages) == [[b"2"]], messages
        assert any(kind == b"1" for kind, _ in messages), messages
        assert any(kind == b"2" for kind, _ in messages), messages
        assert any(kind == b"C" and body == b"SELECT 1\0"
                   for kind, body in messages), messages
        assert messages[-1] == (b"Z", b"I"), messages

        parse = (b"plain_ties_stmt\0SELECT id, score FROM fetch_ties_plain "
                 b"ORDER BY score FETCH FIRST 2 ROWS WITH TIES\0" +
                 struct.pack("!H", 0))
        bind = (b"plain_ties_portal\0plain_ties_stmt\0" +
                struct.pack("!H", 0) + struct.pack("!H", 0) +
                struct.pack("!H", 0))
        extended = client.typed(b"P", parse) + \
            client.typed(b"B", bind) + \
            client.typed(b"E", b"plain_ties_portal\0" +
                         struct.pack("!I", 0)) + client.typed(b"S")
        server["sock"].sendall(extended)
        messages = client.read_until_ready(server["sock"])
        assert not any(kind == b"E" for kind, _ in messages), messages
        assert client.data_row_values(messages) == [
            [b"1", b"10"], [b"2", b"20"],
            [b"3", b"20.00"], [b"4", b"20.0"]
        ], messages
        assert any(kind == b"C" and body == b"SELECT 4\0"
                   for kind, body in messages), messages
        assert messages[-1] == (b"Z", b"I"), messages

        error_cases = [
            ("SELECT 1 UNION SELECT 1, 2;", "42601"),
            ("SELECT 1 UNION SELECT DATE '2024-01-01';", "42804"),
            ("SELECT 1 AS n UNION SELECT 2 ORDER BY missing;", "42P10"),
            ("SELECT 1 AS x, 2 AS x UNION ALL SELECT 3, 4 ORDER BY x;",
             "42702"),
            ("SELECT 1 UNION ALL SELECT 1 FETCH FIRST 1 ROW WITH TIES;",
             "42601"),
            ("SELECT id FROM fetch_ties_plain "
             "FETCH FIRST 1 ROW WITH TIES;", "42601"),
        ]
        for sql, expected_state in error_cases:
            _, state, _, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state == expected_state, (sql, state)
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], "SELECT 9;")
            assert state is None and rows == [["9"]], (
                sql, state, message, rows)

        print("[STRUCTURED SET OPERATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
