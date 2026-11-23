#!/usr/bin/env python3
"""An ordered TIMETZ page uses only checked index entries where possible."""

import json

from compat.numeric_type_review import Review


def main():
    review = Review("TIMETZ_ORDERED_PAGE")
    client, sock = review.client, review.sock
    reference = review.server is None
    plans = 0

    def command(sql, tag):
        frames = client.simple_query(sock, sql)
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record(sql, frames == expected, frames, expected)

    def nodes(node):
        yield node
        for child in node["children"]:
            yield from nodes(child)

    def page(sql, candidates):
        nonlocal plans
        if reference:
            return
        plans += 1
        label = "EXPLAIN (ANALYZE, FORMAT JSON) " + sql
        reply = review.runner.decode_wire_result(client.simple_query(sock, label))
        tree = list(nodes(json.loads(reply[0][0][0])["plan"])) if not reply[1] else []
        used = [node for node in tree if node["nodeType"] == "ZonedClockIndexPage"
                and node.get("usedIndex") == "true"]
        table = any(node["nodeType"] == "TableScan" for node in tree)
        if candidates is None:
            valid = table and not used
        else:
            valid = (not table and len(used) == 1 and used[0].get("column") == "t"
                     and used[0].get("heapRowsFetched") == str(candidates)
                     and used[0].get("actualRows") == candidates)
        seen = [(node["nodeType"], node.get("actualRows"), node.get("heapRowsFetched"))
                for node in tree]
        review.record(label, valid, seen, candidates)

    def ids(sql, expected, candidates=None):
        review.query(sql, rows=[[str(item)] for item in expected], oids=[23])
        page(sql, candidates)

    try:
        command("CREATE TABLE tp_pk(t timetz(6) PRIMARY KEY, id integer)", "CREATE TABLE")
        command("INSERT INTO tp_pk VALUES "
                "('10:00:00.124001+02',1),('09:00:00.124001+01',2),"
                "('01:00:00+15:59:59',3),('24:00:00-15:59:59',4),('11:00:00+00',5),('00:01:00+00',6)",
                "INSERT 0 6")
        base = "SELECT id AS v FROM tp_pk"
        ids(base + " ORDER BY t LIMIT 2", [3, 6], 2)
        ids(base + " ORDER BY t DESC LIMIT 3", [4, 5, 2], 3)
        ids(base + " ORDER BY t LIMIT 2 OFFSET 2", [1, 2], 4)
        ids(base + " ORDER BY t ASC NULLS FIRST FETCH FIRST 2 ROWS ONLY", [3, 6], 2)
        ids("SELECT id+0 AS v FROM tp_pk WHERE t>='10:00:00.124001+02'"
            " ORDER BY t DESC LIMIT 2 OFFSET 1", [5, 2], 3)
        ids(base + " WHERE t>'24:00:00-15:59:59' ORDER BY t LIMIT 3", [], 0)
        ids(base + " WHERE t BETWEEN '11:00:00+00' AND '11:00:00+00' ORDER BY t LIMIT 3", [5], 1)
        ids(base + " ORDER BY t FETCH FIRST 2 ROWS WITH TIES", [3, 6])
        ids(base + " ORDER BY t,id LIMIT 2", [3, 6])
        review.query(base + " ORDER BY t LIMIT 0", rows=[], oids=[23])

        for ordering in ("clock", "1"):
            sql = f"SELECT t AS clock,id AS v FROM tp_pk ORDER BY {ordering} DESC LIMIT 2"
            review.query(sql, rows=[["24:00:00-15:59:59", "4"], ["11:00:00+00", "5"]], oids=[1266, 23])
            page(sql, 2)

        query = "SELECT id+0 AS v FROM tp_pk WHERE t>=$1 ORDER BY t LIMIT 2"
        review.parameter(query, ["10:00:00.124001+02"], [1266], [["1"], ["2"]], [23])

        command("CREATE TABLE tp_log(id integer PRIMARY KEY,t timetz(6))", "CREATE TABLE")
        command("INSERT INTO tp_log VALUES (1,'10:00:00.124001+02'),(2,NULL),"
                "(3,'24:00:00-15:59:59'),(4,'00:01:00+00')", "INSERT 0 4")
        command("CREATE INDEX tp_partial ON tp_log(t) WHERE id=1", "CREATE INDEX")
        base = "SELECT id AS v FROM tp_log"
        ids(base + " ORDER BY t LIMIT 2", [4, 1])
        command("DROP INDEX tp_partial", "DROP INDEX")
        command("CREATE INDEX tp_all ON tp_log(t DESC)", "CREATE INDEX")
        ids(base + " ORDER BY t LIMIT 2", [4, 1], 2)
        ids(base + " ORDER BY t DESC LIMIT 2", [2, 3])
        ids(base + " ORDER BY t DESC NULLS LAST LIMIT 2", [3, 1], 2)
        ids(base + " ORDER BY t LIMIT 4", [4, 1, 3, 2])
        ids(base + " WHERE t IS NOT NULL ORDER BY t DESC LIMIT 4", [3, 1, 4], 3)
        ids(base + " WHERE t>'00:01:00+00' ORDER BY t DESC LIMIT 3", [3, 1], 2)
        ids(base + " ORDER BY t LIMIT 700", [4, 1, 3, 2])
        command("REINDEX TABLE tp_log", "REINDEX")
        ids(base + " ORDER BY t ASC LIMIT 2", [4, 1], 2)
        command("DROP INDEX tp_all", "DROP INDEX")
        ids(base + " ORDER BY t ASC LIMIT 2", [4, 1])
        assert plans == (0 if reference else 21), plans
        review.complete(32 + plans)
    finally:
        review.close()


if __name__ == "__main__":
    main()
