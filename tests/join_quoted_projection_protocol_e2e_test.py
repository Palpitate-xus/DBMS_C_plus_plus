#!/usr/bin/env python3
"""Quoted JOIN column references must retain actual values, not become NULL."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "join_quoted_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in (
                'CREATE TABLE quoted_join_a (id INT, "MixedText" TEXT);',
                'CREATE TABLE quoted_join_b (id INT, "MixedText" TEXT);',
                "INSERT INTO quoted_join_a VALUES(1, 'a b'), (2, NULL);",
                "INSERT INTO quoted_join_b VALUES(1, 'NULL'), (3, 'right only');"):
            assert query(sql)[1] is None, sql
        cases = (
            ('SELECT l."id", r."id" FROM quoted_join_a l '
             'JOIN quoted_join_b r ON l.id=r.id;',
             [["1", "1"]], ["id", "id"], [23, 23]),
            ('SELECT "l"."id", "l"."MixedText" AS a, "r"."MixedText" AS b '
             'FROM quoted_join_a "l" LEFT JOIN quoted_join_b "r" '
             'ON "l"."id"="r"."id" ORDER BY "l"."id";',
             [["1", "a b", "NULL"], ["2", None, None]],
             ["id", "a", "b"], [23, 25, 25]),
            ('SELECT l."MixedText", r."MixedText" FROM quoted_join_a l '
             'FULL JOIN quoted_join_b r ON l.id=r.id ORDER BY l.id NULLS LAST, r.id;',
             [["a b", "NULL"], [None, None], [None, "right only"]],
             ["MixedText", "MixedText"], [25, 25]),
        )
        for sql, rows, names, types in cases:
            actual = query(sql)
            assert actual[1] is None, (sql, actual)
            assert actual[0] == rows and actual[3] == names and actual[5] == types, (sql, actual)
            assert actual[4] == f"SELECT {len(rows)}", (sql, actual)
        print("[JOIN QUOTED PROJECTION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
