#!/usr/bin/env python3
"""Parse/Describe preserves delimited column identities in scalar metadata."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("quoted_describe_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ('CREATE TABLE quote_describe_rows("F" BIGINT,f INT);',
                    'INSERT INTO quote_describe_rows VALUES(2,1);'):
            result = runner.decode_wire_result(client.simple_query(server["sock"], sql))
            assert result[1] is None, (sql, result)
        described = 0
        def describe(sql, oids, attributes=None, modifiers=None):
            nonlocal described
            name = ("quoted_shape_" + str(described)).encode()
            described += 1
            server["sock"].sendall(client.typed(b"P", name+b"\0"+sql.encode()+b"\0\0\0") +
                                   client.typed(b"D", b"S"+name+b"\0") + client.typed(b"S"))
            messages = client.read_until_ready(server["sock"])
            assert not any(kind in (b"E", b"D") for kind, _ in messages), (sql, messages)
            fields = client.row_description_fields(messages)
            assert [field[3] for field in fields] == oids, (sql, fields)
            if attributes is not None:
                assert [field[2] for field in fields] == attributes, (sql, fields)
            if modifiers is not None:
                assert [field[5] for field in fields] == modifiers, (sql, fields)

        for sql, oids, attributes in (
            ('SELECT "F"+f FROM quote_describe_rows;', [20], [0]),
            ('SELECT f,"F" FROM quote_describe_rows;', [23, 20], [2, 1]),
            ('SELECT "F" AS f,f AS "F" FROM quote_describe_rows;', [20, 23], [1, 2]),
            ('SELECT F,"F" FROM quote_describe_rows;', [23, 20], [2, 1]),
            ('SELECT a.f,a."F" FROM quote_describe_rows a;', [23, 20], [2, 1]),
            ('SELECT "A".f,"A"."F" FROM quote_describe_rows "A";', [23, 20], [2, 1]),
            ('SELECT "A"."F"+"A".f FROM quote_describe_rows "A";', [20], [0]),
            ('SELECT CAST("F" AS BIGINT)+f,"F"::BIGINT+f FROM quote_describe_rows;', [20, 20], [0, 0]),
            ('SELECT COALESCE("F",f),CASE WHEN f=0 THEN f ELSE "F" END FROM quote_describe_rows;', [20, 20], [0, 0]),
            ('SELECT "F" IS NOT DISTINCT FROM f FROM quote_describe_rows;', [16], [0]),
            ('SELECT "F"+f FROM quote_describe_rows WHERE false;', [20], [0]),
            ('UPDATE quote_describe_rows SET f=9 RETURNING "F"+f;', [20], [0]),
        ):
            describe(sql, oids, attributes)
        describe('SELECT CAST("F" AS VARCHAR(4)) FROM quote_describe_rows;', [1043], [0], [8])
        for sql in ('CREATE TABLE quote_describe_effect(id INT);',
                    'CREATE FUNCTION quote_describe_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO quote_describe_effect VALUES(i); RETURN i; END;$$;'):
            result = runner.decode_wire_result(client.simple_query(server["sock"], sql))
            assert result[1] is None, (sql, result)
        describe('SELECT quote_describe_writer(f)+"F" FROM quote_describe_rows;', [20], [0])
        result = runner.decode_wire_result(client.simple_query(server["sock"], 'SELECT id FROM quote_describe_effect;'))
        assert result[1] is None and result[0] == [], result
        result = runner.decode_wire_result(client.simple_query(server["sock"], 'SELECT f FROM quote_describe_rows;'))
        assert result[1] is None and result[0] == [["1"]], result
        print("[QUOTED ARITHMETIC DESCRIBE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
