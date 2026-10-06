#!/usr/bin/env python3
"""Comments and dollar-quoted predicate data must not disable row filtering."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("lex_predicate_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect_ids(clause, ids):
        sql = "SELECT id FROM lexical_rows " + clause + ";"
        result = query(sql)
        assert result[1] is None and result[0] == [[str(i)] for i in ids], (sql, result)
        assert result[3] == ["id"] and result[5] == [23] and result[4] == f"SELECT {len(ids)}", result

    try:
        for sql in ("CREATE TABLE lexical_rows(id INT,txt TEXT);",
                    "INSERT INTO lexical_rows VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,''),"
                    "(5,'/* literal */'),(6,'quote'' x'),(7,$tag$line\nbreak$tag$);"):
            assert query(sql)[1] is None, sql
        cases = (
            ("WHERE id=1 /* nosuchfn(id) */", [1]),
            ("WHERE id/* a /* nested */ b */=/* ignored */1", [1]),
            ("WHERE id=1 -- nosuchfn(id)\n ORDER BY id", [1]),
            ("WHERE id=1 -- nosuchfn(id)\r\n ORDER BY id", [1]),
            ("WHERE txt=$$a b$$", [1]),
            ("WHERE txt=$tag$a b$tag$", [1]),
            ("WHERE txt=$$NULL$$", [3]),
            ("WHERE txt=$$$$", [4]),
            ("WHERE txt=$$/* literal */$$", [5]),
            ("WHERE txt=$$quote' x$$", [6]),
            ("WHERE txt=$$line\nbreak$$", [7]),
            ("WHERE txt=$$not stored$$", []),
            ("WHERE txt IS NULL", [2]),
            ("WHERE txt=$$a b$$ /* ignored */ OR id=3 ORDER BY id", [1, 3]),
            ("WHERE txt='/* literal */'", [5]),
            ("WHERE txt='quote'' x'", [6]),
        )
        for case in cases:
            expect_ids(*case)
        # The same lexical copy feeds nested SELECTs and materializers.
        for sql in (
                "WITH c AS(SELECT id,txt FROM lexical_rows WHERE txt=$$a b$$ /* ignore */) SELECT c.* FROM c;",
                "SELECT d.* FROM(SELECT id,txt FROM lexical_rows WHERE txt=$$a b$$ /* ignore */)d;"):
            result = query(sql)
            assert result[1] is None and result[0] == [["1", "a b"]], (sql, result)
            assert result[3] == ["id", "txt"] and result[5] == [23, 25], result
        result = query("SELECT $tag$a b 'quote' /* literal */$tag$ AS datum FROM lexical_rows WHERE id=1;")
        assert result[1] is None and result[0] == [["a b 'quote' /* literal */"]], result
        assert result[3] == ["datum"] and result[5] == [25], result
        # AST-backed DML must keep its original NULL/data distinctions too.
        result = query("UPDATE lexical_rows SET txt=$$updated a b$$ WHERE id=3 RETURNING id,txt;")
        assert result[1] is None and result[0] == [["3", "updated a b"]], result
        result = query("DELETE FROM lexical_rows WHERE txt=$$updated a b$$ RETURNING id;")
        assert result[1] is None and result[0] == [["3"]], result
        expect_ids("ORDER BY id", [1, 2, 4, 5, 6, 7])
        print("[TABLE LEXICAL PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
