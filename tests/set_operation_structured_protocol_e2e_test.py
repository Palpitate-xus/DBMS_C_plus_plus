#!/usr/bin/env python3
"""Set operations retain exact cells, NULL metadata, and common types."""

import importlib.util
from pathlib import Path
import struct


def check_set_operands(client, sock):
    sequence = "setop_array_branch_calls"
    created = client.simple_query(sock, f"CREATE SEQUENCE {sequence}")
    assert created == [(b"C", b"CREATE SEQUENCE\0"), (b"Z", b"I")], created
    created = client.simple_query(sock, "CREATE TABLE ordinary_set_operands (n integer)")
    assert created == [(b"C", b"CREATE TABLE\0"), (b"Z", b"I")], created
    inserted = client.simple_query(sock, "INSERT INTO ordinary_set_operands VALUES (3),(3),(8),(NULL)")
    assert inserted == [(b"C", b"INSERT 0 4\0"), (b"Z", b"I")], inserted
    cases = [
        ("(SELECT v AS \"Number\" FROM unnest(ARRAY[7,8]) AS u(v) LIMIT 1) "
         "UNION ALL SELECT 10000000000 ORDER BY \"Number\"", "Number", 20, 8,
         [[b"7"], [b"10000000000"]]),
        ("SELECT 9 AS v UNION ALL SELECT v FROM unnest(ARRAY[4,5]) AS u(v) "
         "ORDER BY v LIMIT 2", "v", 23, 4, [[b"4"], [b"5"]]),
        ("SELECT v FROM unnest(ARRAY[2,2,1]) AS u(v) UNION SELECT 6 ORDER BY v",
         "v", 23, 4, [[b"1"], [b"2"], [b"6"]]),
        ("SELECT v FROM unnest(ARRAY[2,2,1]) AS u(v) EXCEPT ALL SELECT 2 ORDER BY v",
         "v", 23, 4, [[b"1"], [b"2"]]),
        ("SELECT v FROM unnest(ARRAY[ARRAY[2,2],ARRAY[1,9]]) AS u(v) INTERSECT ALL "
         "SELECT v FROM unnest(ARRAY[1,2,2,7]) AS u(v) ORDER BY v",
         "v", 23, 4, [[b"1"], [b"2"], [b"2"]]),
        ("SELECT v FROM unnest(ARRAY[7,NULL,8]) AS u(v) UNION SELECT NULL::integer "
         "ORDER BY v NULLS FIRST", "v", 23, 4, [[None], [b"7"], [b"8"]]),
        ("SELECT v FROM (SELECT v FROM ROWS FROM(unnest(ARRAY['',NULL,'NULL',''])) AS u(v) "
         "UNION SELECT 'last') AS combined ORDER BY v COLLATE \"C\" NULLS FIRST",
         "v", 25, -1, [[None], [b""], [b"NULL"], [b"last"]]),
        ("SELECT v FROM unnest(ARRAY /* retain the constructor */ [8,9]) AS u(v) "
         "UNION ALL SELECT 1 ORDER BY v LIMIT 0", "v", 23, 4, []),
        ("SELECT v FROM unnest(ARRAY[7,8]) AS u(v) "
         f"UNION ALL SELECT nextval('{sequence}') LIMIT 1", "v", 20, 8, [[b"7"]]),
        ("SELECT v FROM unnest(ARRAY[7,NULL,8,NULL]) AS u(v) UNION SELECT NULL "
         "ORDER BY v NULLS FIRST", "v", 23, 4, [[None], [b"7"], [b"8"]]),
        ("SELECT '8' AS v UNION SELECT v FROM unnest(ARRAY[7,8]) AS u(v) ORDER BY v",
         "v", 23, 4, [[b"7"], [b"8"]]),
        ("SELECT v FROM unnest(ARRAY[1,2,2,NULL,NULL]) AS u(v) INTERSECT ALL SELECT '2' "
         "ORDER BY v", "v", 23, 4, [[b"2"]]),
        ("SELECT v FROM unnest(ARRAY[1,2,2,NULL,NULL]) AS u(v) EXCEPT ALL SELECT NULL "
         "ORDER BY v NULLS FIRST", "v", 23, 4, [[None], [b"1"], [b"2"], [b"2"]]),
        ("SELECT 'true' AS v UNION SELECT v FROM unnest(ARRAY[true,false]) AS u(v) ORDER BY v",
         "v", 16, 1, [[b"f"], [b"t"]]),
        ("SELECT count(*) AS n FROM (SELECT v FROM unnest(ARRAY[1.0,1.00,2.50]) AS u(v) "
         "UNION SELECT '2.5') AS set_rows", "n", 20, 8, [[b"2"]]),
        ("SELECT count(*) AS n FROM (SELECT v FROM unnest(ARRAY[-0.0::float8,0.0::float8,'NaN'::float8]) "
         "AS u(v) UNION SELECT 'NaN') AS set_rows", "n", 20, 8, [[b"2"]]),
        ("SELECT v FROM unnest(ARRAY[7,8]) AS u(v) UNION SELECT '7' ORDER BY v LIMIT 0",
         "v", 23, 4, []),
        ("WITH a AS (SELECT v FROM unnest(ARRAY[7,8]) AS u(v) UNION SELECT '10') "
         "SELECT v FROM a ORDER BY v", "v", 23, 4, [[b"7"], [b"8"], [b"10"]]),
        ("VALUES (7) UNION SELECT NULL ORDER BY 1", "column1", 23, 4, [[b"7"], [None]]),
        ("SELECT 7 AS v UNION SELECT NULL ORDER BY v NULLS FIRST", "v", 23, 4, [[None], [b"7"]]),
        ("SELECT '8' AS v UNION SELECT 7 ORDER BY v", "v", 23, 4, [[b"7"], [b"8"]]),
        ("SELECT NULL AS v UNION (SELECT NULL UNION SELECT 7) ORDER BY v NULLS FIRST",
         "v", 23, 4, [[None], [b"7"]]),
        ("SELECT n FROM ordinary_set_operands INTERSECT ALL VALUES (3),(3),(3),(NULL) "
         "ORDER BY n NULLS FIRST", "n", 23, 4, [[None], [b"3"], [b"3"]]),
        ("SELECT n FROM ordinary_set_operands EXCEPT ALL SELECT '3' ORDER BY n NULLS FIRST",
         "n", 23, 4, [[None], [b"3"], [b"8"]]),
        ("WITH a AS (SELECT 7 AS v UNION SELECT NULL) SELECT v FROM a ORDER BY v NULLS FIRST",
         "v", 23, 4, [[None], [b"7"]]),
        ("SELECT v FROM (SELECT 7 AS v UNION SELECT NULL) AS mixed ORDER BY v NULLS FIRST",
         "v", 23, 4, [[None], [b"7"]]),
        ("SELECT UNION SELECT", None, 0, 0, [[]]),
        ("SELECT UNION ALL SELECT", None, 0, 0, [[], []]),
        ("SELECT INTERSECT ALL SELECT", None, 0, 0, [[]]),
        ("SELECT EXCEPT SELECT", None, 0, 0, []),
    ]
    for sql, name, oid, size, rows in cases:
        parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
        sync = client.typed(b"S", b"")
        bind = client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        describe_statement = client.typed(b"D", b"S\0")
        describe_portal = client.typed(b"D", b"P\0")
        execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
        results = [client.simple_query(sock, sql)]
        for request in (parse + describe_statement + sync,
                        parse + bind + describe_portal + execute + sync):
            sock.sendall(request)
            results.append(client.read_until_ready(sock))
        execution_kinds = b"T" + b"D" * len(rows) + b"CZ"
        for messages, kinds in zip(results, (execution_kinds, b"1tTZ", b"12" + execution_kinds)):
            assert b"".join(kind for kind, _ in messages) == kinds, (sql, messages)
            fields = [] if name is None else [(name.encode(), 0, 0, oid, size, -1, 0)]
            assert client.row_description_fields(messages) == fields, (sql, messages)
            assert messages[-1] == (b"Z", b"I"), (sql, messages)
            if kinds.endswith(b"CZ"):
                assert client.data_row_values(messages) == rows, (sql, messages)
                assert messages[-2] == (b"C", f"SELECT {len(rows)}".encode() + b"\0"), (sql, messages)
    # The first unconsumed branch must still be untouched after both executions.
    next_value = client.simple_query(sock, f"SELECT nextval('{sequence}')")
    assert client.data_row_values(next_value) == [[b"1"]], next_value

    # Input conversion is validated before the read begins, even at LIMIT 0.
    errors = [
        (f"SELECT v FROM unnest(ARRAY[nextval('{sequence}')]) AS u(v) "
         "EXCEPT SELECT 'not a number' LIMIT 0", b"22P02"),
        (f"SELECT nextval('{sequence}') UNION SELECT 'not a number' LIMIT 0", b"22P02"),
        ("SELECT NULL UNION SELECT NULL UNION SELECT 7", b"42804"),
        ("SELECT 1 AS n UNION SELECT 2 ORDER BY missing", b"42703"),
    ]
    for sql, state in errors:
        parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
        simple_error = client.simple_query(sock, sql)
        sock.sendall(parse + client.typed(b"S", b""))
        parse_error = client.read_until_ready(sock)
        for messages in (simple_error, parse_error):
            assert [kind for kind, _ in messages] == [b"E", b"Z"], messages
            assert b"C" + state + b"\0" in messages[0][1], messages
            assert messages[1] == (b"Z", b"I"), messages
    next_value = client.simple_query(sock, f"SELECT nextval('{sequence}')")
    assert client.data_row_values(next_value) == [[b"2"]], next_value


def check_set_source_permissions(client, sock):
    def command(sql):
        messages = client.simple_query(sock, sql)
        assert messages[0][0] == b"C" and messages[-1] == (b"Z", b"I"), (sql, messages)
        flags = {"SET ROLE ordinary_set_reader": b"off", "RESET ROLE": b"on"}
        notice = [(b"S", b"is_superuser\0" + flags[sql] + b"\0")] if sql in flags else []
        assert messages[1:-1] in ([], notice), (sql, messages)

    command("CREATE TABLE ordinary_set_acl(id integer, note text)")
    command("INSERT INTO ordinary_set_acl VALUES (1,'visible'),(2,'hidden')")
    command("CREATE ROLE ordinary_set_reader")
    command("GRANT SELECT(id) ON ordinary_set_acl TO ordinary_set_reader")
    command("SET ROLE ordinary_set_reader")
    messages = client.simple_query(sock, "SELECT id FROM ordinary_set_acl UNION SELECT '3' ORDER BY id")
    assert client.data_row_values(messages) == [[b"1"], [b"2"], [b"3"]], messages
    messages = client.simple_query(sock, "SELECT note FROM ordinary_set_acl UNION SELECT NULL")
    assert [kind for kind, _ in messages] == [b"E", b"Z"], messages
    assert b"C42501\0" in messages[0][1], messages
    command("RESET ROLE")

    command("GRANT SELECT ON ordinary_set_acl TO ordinary_set_reader")
    command("SET ROLE ordinary_set_reader")
    messages = client.simple_query(sock, "SELECT id FROM ordinary_set_acl EXCEPT SELECT '2' ORDER BY id")
    assert client.data_row_values(messages) == [[b"1"]], messages
    command("RESET ROLE")
    command("CREATE POLICY ordinary_set_visible ON ordinary_set_acl FOR SELECT TO ordinary_set_reader USING (id=1)")
    command("ALTER TABLE ordinary_set_acl ENABLE ROW LEVEL SECURITY")
    command("SET ROLE ordinary_set_reader")
    messages = client.simple_query(sock, "SELECT id FROM ordinary_set_acl UNION SELECT NULL ORDER BY id NULLS FIRST")
    assert client.data_row_values(messages) == [[None], [b"1"]], messages
    command("RESET ROLE")


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
        rows, state, message, headers, command_tag, type_oids = hidden_order_key
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        assert headers == ["id"], headers
        assert command_tag == "SELECT 1", command_tag
        assert type_oids == [23], type_oids

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
            ("SELECT 1 AS n UNION SELECT 2 ORDER BY missing;", "42703"),
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

        check_set_operands(client, server["sock"])
        check_set_source_permissions(client, server["sock"])
        print("[STRUCTURED SET OPERATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
