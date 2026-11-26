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
        options = "ANALYZE, " if analyze else ""
        frames = wire.send_statements(f"EXPLAIN ({options}FORMAT JSON) " + sql)
        rows, state, message, names, tag = runner.decode_wire_result(frames)
        assert (state, names, tag, frames[-1]) == (None, ["QUERY PLAN"], "EXPLAIN", (b"Z", ready)), (sql, message, frames)
        return json.loads(rows[0][0])

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
                      f"SELECT qty FROM {outer} WHERE number = 1 OR number = -3"):
            assert not any(node["nodeType"] == "IntegerPrimaryKeyPage"
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
