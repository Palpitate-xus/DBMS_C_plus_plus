#!/usr/bin/env python3
"""Compare ordinary view lookups and verify real heap demand and snapshot fallback."""

from contextlib import closing
import json
import socket
import struct
import sys
import uuid

from session_setting_functions_protocol_e2e_test import Wire, runner


def plan_nodes(node):
    yield node
    for child in node.get("children", []):
        yield from plan_nodes(child)


def explain_plan(wire, sql, ready=b"I", analyze=True):
    options = "ANALYZE, " if analyze else ""
    frames = wire.send_statements(f"EXPLAIN ({options}FORMAT JSON) " + sql)
    rows, state, message, names, tag = runner.decode_wire_result(frames)
    assert (state, names, tag, frames[-1]) == (None, ["QUERY PLAN"], "EXPLAIN", (b"Z", ready)), (sql, message, frames)
    return json.loads(rows[0][0])


def exercise(wire, peer, schema, reference):
    base, inner, outer = (f"{schema}.{name}" for name in ("base", "inner_v", "outer_v"))
    limited, computed = (f"{schema}.{name}" for name in ("limited_v", "computed_v"))
    wire.command(f"CREATE TABLE {base}(id int PRIMARY KEY, qty int, items int[])", "CREATE TABLE")
    wire.command(f"INSERT INTO {base} VALUES "
                 "(-3,9,ARRAY[1,NULL,2]),(1,10,NULL),(2,-4,NULL),(4,0,NULL),(5,NULL,NULL),"
                 "(2147483647,8,ARRAY[7])", "INSERT 0 6")
    wire.command(f"CREATE VIEW {inner}(qty, k, items) AS SELECT qty, id, items FROM {base} "
                 "WHERE qty > 0 OR qty IS NULL", "CREATE VIEW")
    wire.command(f"CREATE VIEW {outer} AS SELECT i.k AS number, i.qty, i.items FROM {inner} i", "CREATE VIEW")
    wire.command(f"CREATE VIEW {limited} AS SELECT * FROM {outer} ORDER BY number LIMIT 1", "CREATE VIEW")
    wire.command(f"CREATE VIEW {computed} AS SELECT id AS number, 10/qty AS quotient FROM {base}", "CREATE VIEW")

    def quantities(source, condition, values, connection=wire, ready=b"I"):
        connection.query(f"SELECT qty FROM {source} WHERE {condition}",
                         rows=[(value,) for value in values], tag=f"SELECT {len(values)}",
                         fields=[("qty", 23)], ready=ready)

    for condition, values in (
        ("number = 1", ("10",)), ("-3 = number", ("9",)), ("number = 2", ()),
        ("number = 5", (None,)), ("number = 2147483647", ("8",)),
        ("number = 2147483648::bigint", ()), ("number = -9223372036854775808", ()),
        ("number = NULL", ()), ("qty = 9 AND number = -3", ("9",)),
        ("qty = 10 AND number = -3", ()), ("number = -3 AND number = 1", ()),
    ):
        quantities(outer, condition, values)
    quantities(outer + " AS r", "r.number = 1", ("10",))
    quantities(limited, "number = 1", ())
    quantities(limited, "number = -3", ("9",))
    wire.query(f"SELECT qty FROM {outer} WHERE number = 1 OR number = -3 ORDER BY number",
               rows=[("9",), ("10",)], tag="SELECT 2", fields=[("qty", 23)])
    wire.query(f"SELECT quotient FROM {computed} WHERE number = 4", state="22012", tag=None)

    def explain(sql, ready=b"I", analyze=True):
        return explain_plan(wire, sql, ready, analyze)

    direct = f"SELECT qty FROM {outer} WHERE number = 1"
    if reference:
        # Match the reference result and prove it searched the actual base
        # key. Plans have different node names on the two servers.
        probe = explain(direct)[0]["Plan"]
        assert probe["Node Type"] == "Index Scan" and probe["Relation Name"] == "base", probe
        assert probe["Index Cond"] == "(id = 1)", probe
    else:
        for condition, fetched in (("number = 1", 1), ("qty = 9 AND number = -3", 1),
                                   ("number = 2", 1), ("number = -421", 0)):
            query = f"SELECT qty FROM {outer} WHERE {condition}"
            nodes = list(plan_nodes(explain(query)["plan"]))
            pages = [node for node in nodes if node["nodeType"] == "IntegerPrimaryKeyPage"]
            assert len(pages) == 1 and pages[0]["usedPrimaryKeyPage"] == "true", nodes
            assert int(pages[0]["heapRowsFetched"]) == fetched, nodes
            assert pages[0]["indexKeyRange"] == "true" and int(pages[0]["indexNodesVisited"]) > 0, nodes
            assert sum(node["nodeType"] == "View Scan" for node in nodes) == 2, nodes
            assert all(node.get("actualRows", 0) == 0 for node in nodes if node["nodeType"] == "TableScan"), nodes
        for query in (f"SELECT qty FROM {limited} WHERE number = 1",
                      f"SELECT quotient FROM {computed} WHERE number = 4",
                      f"SELECT qty FROM {outer} WHERE number = 1 OR qty = 9"):
            assert not any(node["nodeType"] in {"IntegerPrimaryKeyPage", "IntegerPrimaryKeyRange"}
                           for node in plan_nodes(explain(query, analyze=False)["plan"])), query

    name = b"view_lookup"
    sql = f"SELECT number,qty,items FROM {outer} WHERE number=$1"
    sync = wire.packet(b"S")
    frames = wire.exchange(wire.parse(sql, name, (23,)) + sync)
    assert frames == [(b"1", b""), (b"Z", b"I")], frames
    for value, expected in ((-3, [[struct.pack("!i", -3), struct.pack("!i", 9),
                                  struct.pack("!10i", 1, 1, 23, 3, 1, 4, 1, -1, 4, 2)]]),
                            (5, [[struct.pack("!i", 5), None, None]]), (2, []), (None, [])):
        data = b"\0" + name + b"\0" + struct.pack("!HHHi", 1, 1, 1, 4 if value is not None else -1)
        if value is not None:
            data += struct.pack("!i", value)
        frames = wire.exchange(wire.packet(b"B", data + struct.pack("!HH", 1, 1)) +
                               wire.describe_portal() + wire.execute() + sync)
        assert [kind for kind, _ in frames] == [b"2", b"T"] + [b"D"] * len(expected) + [b"C", b"Z"], frames
        assert [(field[0], field[3], field[-1]) for field in wire.client.row_description_fields(frames)] == [
            (b"number", 23, 1), (b"qty", 23, 1), (b"items", 1007, 1)], frames
        assert wire.client.data_row_values(frames) == expected, frames
        assert frames[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")], frames

    if not reference:
        sql = f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT qty FROM {outer} WHERE number=$1"
        binary = b"\0\0" + struct.pack("!HHHiiH", 1, 1, 1, 4, -3, 0)
        for bind in (wire.bind(arguments=(b"-3",)), wire.packet(b"B", binary)):
            frames = wire.exchange(wire.parse(sql, oids=(23,)) + bind + wire.execute() + sync)
            rows, state, message, _, tag = runner.decode_wire_result(frames)
            assert (state, tag, frames[-1]) == (None, "EXPLAIN", (b"Z", b"I")), (message, frames)
            nodes = list(plan_nodes(json.loads(rows[0][0])["plan"]))
            assert any(node.get("usedPrimaryKeyPage") == "true" and node.get("heapRowsFetched") == "1"
                       for node in nodes), nodes

    wire.command(f"ALTER TABLE {base} RENAME COLUMN id TO physical_id", "ALTER TABLE")
    wire.command(f"ALTER VIEW {outer} RENAME COLUMN number TO order_number", "ALTER VIEW")
    quantities(outer, "order_number = 1", ("10",))
    wire.command("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T")
    quantities(outer, "order_number = 1", ("10",), ready=b"T")
    peer.command(f"UPDATE {base} SET physical_id=9,qty=11 WHERE physical_id=1", "UPDATE 1")
    quantities(outer, "order_number = 9", (), ready=b"T")
    quantities(outer, "order_number = 1", ("10",), ready=b"T")
    wire.command("COMMIT", "COMMIT")
    quantities(outer, "order_number = 1", ())
    quantities(outer, "order_number = 9", ("11",))

    for type_name, keys in (("smallint", (-32768, 32767)),
                            ("bigint", (-9223372036854775808, 9223372036854775807))):
        source, view = (f"{schema}.{type_name}_{suffix}" for suffix in ("base", "view"))
        wire.command(f"CREATE TABLE {source}(id {type_name} PRIMARY KEY, qty int)", "CREATE TABLE")
        wire.command(f"INSERT INTO {source} VALUES ({keys[0]},11),({keys[1]},12)", "INSERT 0 2")
        wire.command(f"CREATE VIEW {view} AS SELECT qty,id AS number FROM {source}", "CREATE VIEW")
        for key, value in zip(keys, ("11", "12")):
            quantities(view, f"number = {key}", (value,))
        if not reference:
            query = f"SELECT qty FROM {view} WHERE number = {keys[1]}"
            nodes = list(plan_nodes(explain(query)["plan"]))
            assert any(node.get("usedPrimaryKeyPage") == "true" for node in nodes), nodes

    wire.command("BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN", b"T")
    quantities(outer, "order_number = 9", ("11",), ready=b"T")
    if not reference:
        query = f"SELECT qty FROM {outer} WHERE order_number = 9"
        nodes = list(plan_nodes(explain(query, b"T")["plan"]))
        assert any(node.get("usedPrimaryKeyPage") == "false" for node in nodes), nodes
        assert any(node["nodeType"] == "TableScan" and node.get("actualRows") == 6 for node in nodes), nodes
    wire.command(f"UPDATE {base} SET qty=12 WHERE physical_id=9", "UPDATE 1", b"T")
    quantities(outer, "order_number = 9", ("12",), ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")
    quantities(outer, "order_number = 9", ("11",))

    if not reference:
        # The same SQL resolves to different views as search_path changes.
        # Plain EXPLAIN must show the relation actually bound for this query.
        other = schema + "_other"
        wire.command("CREATE SCHEMA " + other, "CREATE SCHEMA")
        wire.command(f"CREATE TABLE {other}.base(id int, qty int)", "CREATE TABLE")
        wire.command(f"INSERT INTO {other}.base VALUES (9,22)", "INSERT 0 1")
        wire.command(f"CREATE VIEW {other}.outer_v AS SELECT id AS order_number, qty FROM {other}.base", "CREATE VIEW")
        for namespace, value in ((schema, "11"), (other, "22"), (schema, "11")):
            wire.command(f"SET search_path TO {namespace}, public", "SET")
            quantities("outer_v", "order_number = 9", (value,))
            nodes = list(plan_nodes(explain("SELECT qty FROM outer_v WHERE order_number = 9", analyze=False)["plan"]))
            views = [node["relation"] for node in nodes if node["nodeType"] == "View Scan"]
            assert views and views[0] == namespace + ".outer_v", (namespace, views)
            assert any(node["nodeType"] == "IntegerPrimaryKeyPage" for node in nodes) == (namespace == schema), nodes


def exercise_ranges(wire, peer, schema, reference):
    base, inner, middle, outer = (f"{schema}.range_{name}" for name in ("base", "inner", "middle", "outer"))
    wire.command(f"CREATE TABLE {base}(id int PRIMARY KEY, qty int, note text)", "CREATE TABLE")
    # Early rows are deliberately out of key order. A complete index range
    # must preserve the heap order used when the unordered LIMIT falls back.
    first = [7, 4, 5, 9, 8, 3, 6, 2, 1]
    other = list(range(10, 3073))
    ids = first + other
    wire.command("BEGIN", "BEGIN", b"T")
    for start in range(0, len(ids), 128):
        batch = ids[start:start+128]
        values = ",".join(f"({key},{key if key%2 else -key},'row-{key}')" for key in batch)
        wire.command(f"INSERT INTO {base} VALUES {values}", f"INSERT 0 {len(batch)}", b"T")
    wire.command("COMMIT", "COMMIT")
    wire.command(f"CREATE VIEW {inner} AS SELECT qty,id AS item,note FROM {base} WHERE id >= 3 AND qty > 0", "CREATE VIEW")
    wire.command(f"CREATE VIEW {middle} AS SELECT item AS number,qty,note FROM {inner} WHERE qty IS NOT NULL", "CREATE VIEW")
    wire.command(f"CREATE VIEW {outer} AS SELECT * FROM {middle}", "CREATE VIEW")
    sql = f"SELECT number FROM {outer} WHERE "

    def check(condition, expected, order="ORDER BY number", ready=b"I"):
        query = f"{sql}{condition} {order}".rstrip()
        wire.query(query, rows=[(str(n),) for n in expected], tag=f"SELECT {len(expected)}",
                   fields=[("number", 23)], ready=ready)
        return query

    def verify_range_plan(query, fetched, intervals=None, ready=b"I"):
        if reference:
            return
        nodes = list(plan_nodes(explain_plan(wire, query, ready)["plan"]))
        ranges = [node for node in nodes if node["nodeType"] == "IntegerPrimaryKeyRange"]
        assert len(ranges) == 1, [(n["nodeType"], n.get("relation")) for n in nodes]
        plan = ranges[0]
        assert (plan["usedPrimaryKeyRange"] == "true") == (fetched is not None), (query, plan)
        if fetched is not None:
            assert int(plan["heapRowsFetched"]) == fetched and int(plan["indexNodesVisited"]) > 0, (query, plan)
            assert plan.get("indexRanges") == intervals, (query, plan)
            assert all(node.get("actualRows", 0) == 0 for node in nodes if node["nodeType"] == "TableScan"), (query, plan)

    cases = (
        ("7 >= number", [3, 5, 7], 5, None),
        ("number BETWEEN 2 AND 7", [3, 5, 7], 5, None),
        ("number BETWEEN SYMMETRIC 7 AND 2", [3, 5, 7], 5, None),
        ("number BETWEEN 3 AND 8 AND number NOT BETWEEN 4 AND 6", [3, 7], 3, "2"),
        ("number BETWEEN 3 AND 8 AND number <> 4", [3, 5, 7], 5, "2"),
        ("(number BETWEEN 3 AND 7 AND qty > 0) OR (number BETWEEN 521 AND 524 AND qty >= 0)",
         [3, 5, 7, 521, 523], 9, "2"),
        ("number BETWEEN 200 AND 456", list(range(201, 457, 2)), 257, None),
        ("number BETWEEN 3 AND 260 OR number BETWEEN 1301 AND 1566",
         [*range(3, 261, 2), *range(1301, 1567, 2)], 524, "2"),
        ("number BETWEEN 3 AND 3072", list(range(3, 3073, 2)), None, None),
    )
    for condition, expected, fetched, intervals in cases:
        verify_range_plan(check(condition, expected), fetched, intervals)
    if not reference:
        query = f"SELECT number FROM {outer} WHERE number + 0 BETWEEN 3 AND 1026 ORDER BY number"
        nodes = list(plan_nodes(explain_plan(wire, query)["plan"]))
        inputs = [node for node in nodes if "retainedInputRows" in node]
        assert sum(node["nodeType"] == "View Scan" for node in inputs) == 3, inputs
        assert all(node["retainedInputRows"] == "0" for node in inputs), inputs
        assert any(node["nodeType"] == "TableScan" and node.get("actualRows") == len(ids)
                   for node in nodes), query
    check("number < 3", [])
    check("number > 2147483647::bigint", [])
    check("number BETWEEN NULL AND 7", [])
    check("number < 3 OR qty = 11", [11])
    page = check("number BETWEEN 2 AND 7", [5, 3], "ORDER BY number DESC LIMIT 2 OFFSET 1")
    verify_range_plan(page, 5)
    wide_condition = "number BETWEEN 3 AND 1026"
    wide_rows = list(range(3, 1027, 2))
    wide = check(wide_condition, wide_rows)
    verify_range_plan(wide, 1024)
    wire.command("SET work_mem='64kB'", "SET")
    check(wide_condition, wide_rows)
    verify_range_plan(wide, None)
    wire.command("RESET work_mem", "RESET")
    if not reference:
        unordered = check("number BETWEEN 2 AND 7", [7, 5], "LIMIT 2")
        # This expression keeps the original full scan and proves fallback
        # and index access agree on the incumbent order in this fixture.
        check("number + 0 BETWEEN 2 AND 7", [7, 5], "LIMIT 2")
        verify_range_plan(unordered, 5)
        across_chunks = check(wide_condition, [7, 5, 9, 3], "LIMIT 4")
        check("number + 0 BETWEEN 3 AND 1026", [7, 5, 9, 3], "LIMIT 4")
        verify_range_plan(across_chunks, 1024)

    # The keys alone fit in 64 kB. Resolving the stored text exceeds that
    # budget, so the original source must return the complete result instead.
    wire.command(f"UPDATE {base} SET note=repeat('x',8192) WHERE id BETWEEN 100 AND 111", "UPDATE 12")
    payload_query = f"SELECT length(note) AS n FROM {outer} WHERE number BETWEEN 100 AND 111 ORDER BY number"
    expected = [("8192",)] * 6
    wire.query(payload_query, rows=expected, tag="SELECT 6", fields=[("n", 23)])
    verify_range_plan(payload_query, 12)
    wire.command("SET work_mem='64kB'", "SET")
    wire.query(payload_query, rows=expected, tag="SELECT 6", fields=[("n", 23)])
    verify_range_plan(payload_query, None)
    wire.command("RESET work_mem", "RESET")

    prepared = f"SELECT number FROM {outer} WHERE number BETWEEN $1 AND $2 ORDER BY number"
    name = b"view_range"
    sync = wire.packet(b"S")
    assert wire.exchange(wire.parse(prepared, name, (23, 23)) + sync) == [(b"1", b""), (b"Z", b"I")]
    for lower, upper, expected in ((2,7,[3,5,7]), (9,3,[]), (521,524,[521,523]), (None,7,[])):
        values = b"".join(struct.pack("!i", -1) if value is None else struct.pack("!ii", 4, value)
                          for value in (lower, upper))
        bind = b"\0" + name + b"\0" + struct.pack("!HHH", 1, 1, 2) + values + struct.pack("!HH", 1, 1)
        frames = wire.exchange(wire.packet(b"B", bind) + wire.describe_portal() + wire.execute() + sync)
        assert [k for k, _ in frames] == [b"2", b"T"] + [b"D"] * len(expected) + [b"C", b"Z"], frames
        assert [(f[0], f[3], f[-1]) for f in wire.client.row_description_fields(frames)] == [(b"number", 23, 1)], frames
        assert wire.client.data_row_values(frames) == [[struct.pack("!i", value)] for value in expected], frames
        assert frames[-2:] == [(b"C", f"SELECT {len(expected)}\0".encode()), (b"Z", b"I")], frames

    wire.command("BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", b"T")
    check("number BETWEEN 29 AND 35", [29, 31, 33, 35], ready=b"T")
    check(wide_condition, wide_rows, ready=b"T")
    peer.command(f"UPDATE {base} SET id=4100 WHERE id=31", "UPDATE 1")
    check("number BETWEEN 29 AND 35", [29, 31, 33, 35], ready=b"T")
    verify_range_plan(check(wide_condition, wide_rows, ready=b"T"), None, ready=b"T")
    check("number BETWEEN 4099 AND 4101", [], ready=b"T")
    wire.command("COMMIT", "COMMIT")
    check("number BETWEEN 29 AND 35", [29, 33, 35])
    verify_range_plan(check(wide_condition, [n for n in wide_rows if n != 31]), 1023)
    check("number BETWEEN 4099 AND 4101", [4100])

    wire.command("BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN", b"T")
    serial = check("number BETWEEN 29 AND 35", [29, 33, 35], ready=b"T")
    verify_range_plan(serial, None, ready=b"T")
    wire.command("ROLLBACK", "ROLLBACK")


def exercise_reusable_view_inputs(wire, schema):
    view = f"{schema}.range_outer"
    labels = f"{schema}.labels"
    wire.command(f"CREATE TABLE {labels}(id int, tag text)", "CREATE TABLE")
    wire.command(f"INSERT INTO {labels} VALUES (3,'a'),(3,'b'),(4,'c'),(5,'d')", "INSERT 0 4")
    for reader in (wire, Wire(wire.client, wire.sock, extended=True)):
        reader.query(f"SELECT m.tag,v.number FROM {labels} m LEFT JOIN {view} v ON m.id=v.number ORDER BY m.tag",
                     rows=[("a", "3"), ("b", "3"), ("c", None), ("d", "5")], tag="SELECT 4",
                     fields=[("tag", 25), ("number", 23)])
        reader.query(f"WITH pick AS MATERIALIZED (SELECT number FROM {view} WHERE number<=7) "
                     "SELECT a.number AS a,b.number AS b FROM pick a CROSS JOIN pick b WHERE a.number<b.number ORDER BY a,b",
                     rows=[("3", "5"), ("3", "7"), ("5", "7")], tag="SELECT 3", fields=[("a", 23), ("b", 23)])

    sequence, counted = f"{schema}.view_calls", f"{schema}.counted_v"
    wire.command(f"CREATE SEQUENCE {sequence} START WITH 100", "CREATE SEQUENCE")
    wire.command(f"CREATE VIEW {counted} AS SELECT nextval('{sequence}') AS n FROM {labels}", "CREATE VIEW")
    wire.query(f"SELECT nextval('{sequence}') AS n", rows=[("100",)], fields=[("n", 20)])
    wire.query(f"SELECT n FROM {counted} LIMIT 0", tag="SELECT 0", fields=[("n", 20)])
    wire.query(f"SELECT currval('{sequence}') AS n", rows=[("100",)], fields=[("n", 20)])
    wire.query(f"SELECT n FROM {counted} LIMIT 2", rows=[("101",), ("102",)], tag="SELECT 2", fields=[("n", 20)])
    wire.query(f"SELECT currval('{sequence}') AS n", rows=[("102",)], fields=[("n", 20)])
    oid, = map(int, wire.client.data_row_values(
        wire.send_statements(f"SELECT '{counted}'::regclass::oid"))[0])
    sampled = (f"WITH sampled AS MATERIALIZED (SELECT n FROM {counted} LIMIT 2) "
               "SELECT a.n AS a,b.n AS b FROM sampled a CROSS JOIN sampled b ORDER BY a,b")
    name = b"sampled_cte"
    sync = wire.packet(b"S")
    statement = wire.exchange(wire.parse(sampled, name) + wire.packet(b"D", b"S" + name + b"\0") + sync)
    assert [kind for kind, _ in statement] == [b"1", b"t", b"T", b"Z"], statement
    columns = [(key, oid, 1, 20, 8, -1, 0) for key in (b"a", b"b")]
    assert wire.client.row_description_fields(statement) == columns
    wire.query(f"SELECT currval('{sequence}') AS n", rows=[("102",)], fields=[("n", 20)])
    reply = wire.exchange(wire.bind(name) + wire.describe_portal() + wire.execute() + sync)
    assert wire.client.row_description_fields(reply) == columns
    wire.check(sampled, reply, rows=[("103", "103"), ("103", "104"), ("104", "103"), ("104", "104")],
               tag="SELECT 4", fields=[("a", 20), ("b", 20)])
    wire.query(f"SELECT currval('{sequence}') AS n", rows=[("104",)], fields=[("n", 20)])


def exercise_view_column_descriptions(wire, schema):
    view = f"{schema}.described_v"
    wire.command(f"CREATE VIEW {view} AS SELECT number,note::varchar(12) AS label "
                 f"FROM {schema}.range_outer WHERE number<=7", "CREATE VIEW")
    oid, = map(int, wire.client.data_row_values(
        wire.send_statements(f"SELECT '{view}'::regclass::oid"))[0])
    number = (oid, 1, 23, 4, -1)
    label = (oid, 2, 1043, -1, 16)
    base = f"{schema}.column_order"
    ordered_view = f"{schema}.column_order_v"
    wire.command(f"CREATE TABLE {base}(z int PRIMARY KEY, discarded bigint, a varchar(8))", "CREATE TABLE")
    wire.command(f"INSERT INTO {base} VALUES (4, 99, 'word'), (5, 98, NULL)", "INSERT 0 2")
    wire.command(f"ALTER TABLE {base} DROP COLUMN discarded", "ALTER TABLE")
    wire.command(f"CREATE VIEW {ordered_view} AS SELECT * FROM {base}", "CREATE VIEW")
    ordered_oid, = map(int, wire.client.data_row_values(
        wire.send_statements(f"SELECT '{ordered_view}'::regclass::oid"))[0])
    sync = wire.packet(b"S")
    describe_statement = wire.packet(b"D", b"S\0")
    cases = (
        (f"WITH pick AS MATERIALIZED (SELECT * FROM {view}) "
         "SELECT a.number AS a,b.number AS b FROM pick a CROSS JOIN pick b WHERE a.number<b.number ORDER BY a,b",
         [(b"a", *number), (b"b", *number)], [(3, 5), (3, 7), (5, 7)]),
        (f"SELECT d.label,d.number+1 AS computed FROM (SELECT * FROM {view}) AS d ORDER BY d.number",
         [(b"label", *label), (b"computed", 0, 0, 23, 4, -1)], [(b"row-3", 4), (b"row-5", 6), (b"row-7", 8)]),
        (f"WITH first_read AS (SELECT * FROM {view}), second_read(n,l) AS (SELECT * FROM first_read) "
         "SELECT l,n FROM second_read WHERE false",
         [(b"l", *label), (b"n", *number)], []),
        (f"WITH first_read AS (SELECT * FROM {ordered_view}) SELECT * FROM first_read ORDER BY z",
         [(b"z", ordered_oid, 1, 23, 4, -1), (b"a", ordered_oid, 2, 1043, -1, 12)],
         [(4, b"word"), (5, None)]),
    )
    for sql, fields, values in cases:
        statement = wire.exchange(wire.parse(sql) + describe_statement + sync)
        assert [kind for kind, _ in statement] == [b"1", b"t", b"T", b"Z"], (sql, statement)
        assert wire.client.row_description_fields(statement) == [(*field, 0) for field in fields]
        portal = wire.exchange(wire.bind(result_format=1) + wire.describe_portal() + wire.execute() + sync)
        assert [kind for kind, _ in portal] == [b"2", b"T"] + [b"D"] * len(values) + [b"C", b"Z"], (sql, portal)
        assert wire.client.row_description_fields(portal) == [(*field, 1) for field in fields]
        expected = [[struct.pack("!i", cell) if isinstance(cell, int) else cell
                     for cell in row] for row in values]
        assert wire.client.data_row_values(portal) == expected, (sql, portal)
        assert portal[-2:] == [(b"C", f"SELECT {len(values)}\0".encode()), (b"Z", b"I")], (sql, portal)


def main():
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        startup = client.startup_reference
    else:
        server = runner.start_ours(client)
        host, port, user, database, password = "127.0.0.1", server["port"], "alice", "info", "secret"
        startup = client.startup

    schema = "view_lookup_" + uuid.uuid4().hex[:12]
    main_socket = server["sock"] if server else socket.create_connection((host, port), timeout=runner.wire_timeout())
    try:
        if reference:
            startup(main_socket, user, database, password)
            runner.verify_reference_version(client, main_socket)
        wire = Wire(client, main_socket)
        wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        with closing(socket.create_connection((host, port), timeout=runner.wire_timeout())) as peer:
            startup(peer, user, database, password)
            exercise(wire, Wire(client, peer), schema, reference)
            exercise_ranges(wire, Wire(client, peer), schema, reference)
            exercise_reusable_view_inputs(wire, schema)
            exercise_view_column_descriptions(wire, schema)
        print("[INDEXED VIEW READS] passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(main_socket, "ROLLBACK")
                client.simple_query(main_socket, "DROP SCHEMA " + schema + " CASCADE")
            finally:
                main_socket.close()


if __name__ == "__main__":
    main()
