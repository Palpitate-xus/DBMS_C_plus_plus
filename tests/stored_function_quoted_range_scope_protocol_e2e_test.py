#!/usr/bin/env python3
"""Canonical source/range components must not fold quotes a second time."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('quoted_range_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b'Z', b'I'), (sql, messages[-1])
        return result

    def ok(sql, rows=None):
        result = query(sql)
        assert result[1] is None, (sql, result)
        if rows is not None:
            assert result[0] == rows, (sql, result)
        return result

    try:
        for sql in (
            'CREATE TABLE quoted_range_rows(id INT);',
            'CREATE TABLE quoted_range_empty(id INT);',
            'CREATE TABLE quoted_range_calls(id INT);',
            'INSERT INTO quoted_range_rows VALUES(1),(2);',
            'CREATE SCHEMA "RangeNS";',
            'CREATE TABLE "RangeNS"."Range.Table"(id INT);',
            'INSERT INTO "RangeNS"."Range.Table" VALUES(1),(2);',
            'CREATE FUNCTION quoted_range_fn(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT -arg $$;',
            'CREATE FUNCTION quoted_range_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO quoted_range_calls VALUES(arg); RETURN -arg; END; $$;',
        ):
            ok(sql)
        for sql in (
            'SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_fn("M".id);',
            'SELECT id FROM quoted_range_rows AS "m" ORDER BY quoted_range_fn(m.id);',
            'SELECT id FROM quoted_range_rows AS "Alias Space" ORDER BY quoted_range_fn("Alias Space".id);',
            'SELECT id FROM quoted_range_rows AS "Alias.Dot" ORDER BY quoted_range_fn("Alias.Dot".id);',
            'SELECT id FROM public.quoted_range_rows ORDER BY quoted_range_fn(public.quoted_range_rows.id);',
            'SELECT id FROM quoted_range_rows ORDER BY public.quoted_range_rows.id DESC;',
            'SELECT id FROM "RangeNS"."Range.Table" ORDER BY quoted_range_fn("RangeNS"."Range.Table".id);',
            'SELECT id FROM "RangeNS"."Range.Table" AS "M" ORDER BY quoted_range_fn("M".id);',
        ):
            ok(sql, [['2'], ['1']])
        for sql, state in (
            ('SELECT id FROM quoted_range_rows AS m ORDER BY quoted_range_fn("M".id);', '42P01'),
            ('SELECT quoted_range_fn("M".id) FROM quoted_range_rows AS m;', '42P01'),
            ('SELECT quoted_range_writer(id),CASE WHEN false THEN m.id ELSE id END FROM quoted_range_rows AS "M";', '42P01'),
            ('SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_fn(m.id);', '42P01'),
            ('SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_fn(quoted_range_rows.id);', '42P01'),
            ('SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_fn(public."M".id);', '42P01'),
            ('SELECT id FROM "RangeNS"."Range.Table" AS "M" ORDER BY quoted_range_fn("RangeNS"."Range.Table".id);', '42P01'),
            ('SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_fn("M".missing);', '42703'),
            ('SELECT id AS output_only FROM quoted_range_rows AS "M" ORDER BY "M".output_only;', '42703'),
            ('SELECT id FROM quoted_range_empty AS "M" ORDER BY quoted_range_writer(id),quoted_range_fn(m.id);', '42P01'),
            ('SELECT id FROM quoted_range_rows AS "M" ORDER BY quoted_range_writer(id),CASE WHEN false THEN "M".missing ELSE id END;', '42703'),
            ('SELECT id FROM quoted_range_empty AS "M" ORDER BY CASE WHEN false THEN quoted_range_missing("M".id) ELSE id END;', '42883'),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            ok('SELECT id FROM quoted_range_calls;', [])
        ok('SELECT id FROM quoted_range_rows AS "M" WHERE quoted_range_fn("M".id)<0 ORDER BY id;', [['1'], ['2']])
        for sql in (
            'SELECT id FROM quoted_range_rows AS "m" WHERE m.id=1;',
            'SELECT id FROM quoted_range_rows WHERE public.quoted_range_rows.id=1;',
            'SELECT id FROM quoted_range_rows AS "Alias Space" WHERE "Alias Space".id=1;',
            'SELECT id FROM "RangeNS"."Range.Table" WHERE "RangeNS"."Range.Table".id=1;',
            'SELECT id FROM quoted_range_rows AS "m" WHERE m.id=1 AND m.id<2;',
            'SELECT id FROM quoted_range_rows AS "m" GROUP BY m.id HAVING m.id=1;',
            'SELECT id FROM quoted_range_rows GROUP BY public.quoted_range_rows.id HAVING public.quoted_range_rows.id=1;',
        ):
            ok(sql, [['1']])
        ok('SELECT id FROM quoted_range_rows AS "M" GROUP BY "M".id ORDER BY "M".id;', [['1'], ['2']])
        print('[STORED FUNCTION QUOTED RANGE SCOPE] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
