#!/usr/bin/env python3
"""Check typed, reusable row count parameters in ordinary SELECT portals."""

import json
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("BOUND_ROW_COUNT")
    client, sock = review.client, review.sock
    packet = client.typed
    sync = packet(b"S")
    cases = 0

    def request(*messages):
        sock.sendall(b"".join(messages) + sync)
        return client.read_until_ready(sock)

    def prepare(sql, declared, inferred, origin, state=None):
        nonlocal cases
        cases += 1
        name = f"page_{cases}".encode() + b"\0"
        parsed = name + sql.encode() + b"\0" + struct.pack("!H", len(declared))
        parsed += b"".join(struct.pack("!I", oid) for oid in declared)
        frames = request(packet(b"P", parsed), packet(b"D", b"S" + name))
        if state:
            valid = b"".join(kind for kind, _ in frames) == b"EZ"
            valid = valid and client.diagnostic_fields(frames[0][1]).get(b"C") == state.encode()
        else:
            types = struct.pack("!H", len(inferred)) + b"".join(struct.pack("!I", oid) for oid in inferred)
            valid = (b"".join(kind for kind, _ in frames) == b"1tTZ"
                     and frames[1] == (b"t", types) and client.row_description_fields(frames) == [origin])
        review.record(("Parse", sql, declared), valid, frames, state or (inferred, origin))
        return name

    def execute(name, inputs, expected, origin, *, binary=False, state=None, bind_error=False, plan_rows=None, ready=b"I"):
        fmt = int(binary)
        bind = b"\0" + name + struct.pack("!HHH", 1, fmt, len(inputs))
        for value in inputs:
            bind += struct.pack("!i", -1 if value is None else len(value)) + (value or b"")
        bind += struct.pack("!HH", 1, fmt)
        frames = request(packet(b"B", bind), packet(b"D", b"P\0"), packet(b"E", b"\0" + struct.pack("!I", 0)))
        kinds = b"".join(kind for kind, _ in frames)
        field = client.row_description_fields(frames) if b'T' in kinds else []
        rows = client.data_row_values(frames)
        valid = field == [(*origin[:-1], fmt)] and frames[-1] == (b"Z", ready)
        if bind_error:
            valid = kinds == b"EZ" and frames[-1] == (b"Z", ready)
            valid = valid and client.diagnostic_fields(frames[0][1]).get(b"C") == state.encode()
        elif state:
            valid = valid and kinds == b"2TEZ" and client.diagnostic_fields(frames[-2][1]).get(b"C") == state.encode()
        elif plan_rows is not None:
            tree = []
            if kinds == b"2TDCZ":
                pending = [json.loads(rows[0][0])["plan"]]
                while pending:
                    node = pending.pop()
                    tree.append((node["nodeType"], node.get("actualRows", 0), node.get("heapRowsFetched")))
                    pending.extend(node["children"])
            if plan_rows:
                valid = valid and ("OidIndexPage", plan_rows, str(plan_rows)) in tree
                valid = valid and all(node != "TableScan" for node, _, _ in tree)
            else:
                valid = valid and tree and all(actual == 0 for _, actual, _ in tree)
            rows = tree
        else:
            valid = (valid and kinds == b"2T" + b"D" * len(expected) + b"CZ"
                     and rows == [[value] for value in expected]
                     and frames[-2] == (b"C", f"SELECT {len(expected)}\0".encode()))
        review.record(("Execute", name, inputs), valid, (kinds, field, rows, frames[-2]),
                      state or (expected, plan_rows))

    try:
        review.query("CREATE TABLE bound_page(k oid PRIMARY KEY, n integer)")
        review.query("INSERT INTO bound_page VALUES (0,1),(2147483648,2),(4294967295,3)")
        relation = int(client.data_row_values(client.simple_query(
            sock, "SELECT oid FROM pg_catalog.pg_class WHERE relname='bound_page'"))[0][0])
        origin = (b"object_number", relation, 1, 26, 4, -1, 0)
        sql = "SELECT k AS object_number FROM bound_page ORDER BY k LIMIT $1 OFFSET $2"
        page = prepare(sql, (), (20, 20), origin)
        for values, rows in (
            ((b"1", b"1"), [b"2147483648"]),
            ((b"2", None), [b"0", b"2147483648"]),
            ((None, b"1"), [b"2147483648", b"4294967295"]),
            ((b"0", b"9223372036854775807"), []),
            ((b"9223372036854775807", None), [b"0", b"2147483648", b"4294967295"]),
        ):
            execute(page, values, rows, origin)
        execute(page, (b"-1", None), [], origin, state="2201W")
        execute(page, (b"-1", b"-1"), [], origin, state="2201X")
        execute(page, (b"words", None), [], origin, state="22P02", bind_error=True)
        execute(page, (b"9223372036854775808", None), [], origin, state="22003", bind_error=True)
        execute(page, (struct.pack("!q", 2), struct.pack("!q", 1)),
                [struct.pack("!I", 2147483648), struct.pack("!I", 4294967295)], origin, binary=True)
        short = prepare(sql, (21, 23), (21, 23), origin)
        execute(short, (struct.pack("!h", 1), struct.pack("!i", 2)),
                [struct.pack("!I", 4294967295)], origin, binary=True)
        prepare(sql, (25, 20), (), origin, state="42804")
        bracket = prepare(sql.replace("$1", "($1)").replace("$2", "/* page $92 */ $2 ROW"), (), (20, 20), origin)
        execute(bracket, (b"1", b"1"), [b"2147483648"], origin)

        filtered = "SELECT k AS object_number FROM bound_page WHERE k >= $1 ORDER BY 1"
        fetch = " OFFSET $2 ROWS FETCH NEXT $3 ROWS WITH /* peer group */ TIES"
        scoped = prepare(filtered + fetch, (), (26, 20, 20), origin)
        execute(scoped, (b"2147483648", b"0", b"1"), [b"2147483648"], origin)
        execute(scoped, (b"2147483648", b"0", None), [b"2147483648", b"4294967295"], origin)
        execute(scoped, (struct.pack("!I", 2147483648), None, struct.pack("!q", 1)),
                [struct.pack("!I", 2147483648)], origin, binary=True)
        execute(scoped, (b"2147483648", b"1", b"-2"), [], origin, state="2201W")
        repeated = prepare(filtered + " LIMIT $1", (26,), (26,), origin)
        execute(repeated, (struct.pack("!I", 2147483648),),
                [struct.pack("!I", 2147483648), struct.pack("!I", 4294967295)], origin, binary=True)

        decimal = prepare(sql, (1700, 20), (1700, 20), origin)
        execute(decimal, (b"1.5", None), [b"0", b"2147483648"], origin)
        execute(decimal, (b"-0.5", None), [], origin, state="2201W")
        single = prepare(sql, (700, 20), (700, 20), origin)
        execute(single, (b"1.5", None), [b"0", b"2147483648"], origin)
        execute(single, (struct.pack("!f", 1.5), None),
                [struct.pack("!I", 0), struct.pack("!I", 2147483648)], origin, binary=True)
        execute(single, (b"words", None), [], origin, state="22P02", bind_error=True)

        argument = (b"page_size", 0, 0, 20, 8, -1, 0)
        shared = prepare("SELECT $1 AS page_size FROM bound_page ORDER BY k LIMIT $1",
                         (20,), (20,), argument)
        execute(shared, (struct.pack("!q", 1),), [struct.pack("!q", 1)], argument, binary=True)

        began = client.simple_query(sock, "BEGIN")
        review.record("BEGIN", began == [(b"C", b"BEGIN\0"), (b"Z", b"T")], began, b"T")
        execute(page, (b"1", b"-1"), [], origin, state="2201X", ready=b"E")
        rolled_back = client.simple_query(sock, "ROLLBACK")
        review.record("ROLLBACK", rolled_back == [(b"C", b"ROLLBACK\0"), (b"Z", b"I")], rolled_back, b"I")

        review.query("CREATE TABLE bound_peer(id integer PRIMARY KEY,k oid NOT NULL)")
        review.query("INSERT INTO bound_peer VALUES (1,0),(2,2147483648),(3,2147483648),"
                     "(4,2147483648),(5,4294967295)")
        review.query("CREATE INDEX bound_peer_key ON bound_peer(k)")
        relation = int(client.data_row_values(client.simple_query(
            sock, "SELECT oid FROM pg_catalog.pg_class WHERE relname='bound_peer'"))[0][0])
        peer_origin = (b"object_number", relation, 2, 26, 4, -1, 0)
        peers = "SELECT k AS object_number FROM bound_peer ORDER BY k OFFSET $1 ROWS FETCH NEXT ($2) ROWS WITH TIES"
        tied = prepare(peers, (), (20, 20), peer_origin)
        execute(tied, (b"1", b"1"), [b"2147483648"] * 3, peer_origin)
        execute(tied, (b"2", None), [b"2147483648", b"2147483648", b"4294967295"], peer_origin)

        if review.server is not None:
            plan_origin = (b"QUERY PLAN", 0, 0, 114, -1, -1, 0)
            explain = "EXPLAIN (ANALYZE, FORMAT JSON, TIMING FALSE) "
            plan = prepare(explain + sql, (), (20, 20), plan_origin)
            execute(plan, (b"1", b"1"), None, plan_origin, plan_rows=2)
            execute(plan, (b"0", b"2"), None, plan_origin, plan_rows=0)
            plan = prepare(explain + filtered + fetch, (), (26, 20, 20), plan_origin)
            execute(plan, (b"0", b"0", b"1"), None, plan_origin, plan_rows=2)
            plan = prepare(explain + peers, (), (20, 20), plan_origin)
            execute(plan, (b"1", b"1"), None, plan_origin, plan_rows=5)

        review.complete(50 if review.server is not None else 43)
    finally:
        review.close()


if __name__ == "__main__":
    main()
