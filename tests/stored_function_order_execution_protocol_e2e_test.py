#!/usr/bin/env python3
"""ORDER BY routines must execute with typed keys, NULLs and original errors."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('function_order_runner', root/'tests/compat/pg_diff_runner.py')
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

    def sink(rows):
        ok('SELECT id FROM function_order_sink ORDER BY id;', rows)
        ok('DELETE FROM function_order_sink;')

    try:
        for sql in (
            'CREATE TABLE function_order_sink(id INT PRIMARY KEY);',
            'CREATE TABLE function_order_driver(id INT,payload TEXT);',
            'CREATE TABLE function_order_empty(id INT);',
            'CREATE TABLE function_order_null_calls(payload TEXT);',
            "INSERT INTO function_order_driver VALUES(1,NULL),(2,'a b'),(10,'null');",
            'CREATE TABLE function_order_text_rows(id INT,payload TEXT);',
            "INSERT INTO function_order_text_rows VALUES(1,NULL),(2,''),(3,'a b'),(4,'10'),(5,'2'),(10,'null');",
            'CREATE FUNCTION function_order_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO function_order_sink VALUES(arg); RETURN -arg; END; $$;',
            "CREATE FUNCTION function_order_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO function_order_sink VALUES(arg); SELECT 'bad' INTO n; RETURN n; END; $$;",
            'CREATE FUNCTION function_order_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO function_order_sink VALUES(arg); SELECT 1 INTO STRICT n WHERE false; RETURN n; END; $$;',
            'CREATE FUNCTION "OrderCase"(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT -arg $$;',
            'CREATE FUNCTION ordercase(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT arg $$;',
            'CREATE FUNCTION function_order_text(arg TEXT) RETURNS TEXT LANGUAGE sql AS $$ SELECT arg $$;',
            'CREATE FUNCTION function_order_nullable(arg TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO function_order_null_calls VALUES(arg); RETURN arg; END; $$;',
        ):
            ok(sql)
        for sql, state in (
            ('SELECT d.id FROM function_order_driver d ORDER BY function_order_cast(60+d.id);', '22P02'),
            ('SELECT id FROM function_order_driver ORDER BY function_order_strict(id);', 'P0002'),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            sink([])
        for sql, rows, writes in (
            ('SELECT id FROM function_order_driver ORDER BY function_order_writer(id);', [['10'], ['2'], ['1']], [['1'], ['2'], ['10']]),
            ('SELECT id FROM function_order_driver ORDER BY function_order_writer(id) LIMIT 1 OFFSET 1;', [['2']], [['1'], ['2'], ['10']]),
            ('SELECT id FROM function_order_driver WHERE id=1 OR id=2 OR id=1 ORDER BY function_order_writer(id);', [['2'], ['1']], [['1'], ['2']]),
            ('SELECT id FROM function_order_driver ORDER BY function_order_writer(id),function_order_writer(id);', [['10'], ['2'], ['1']], [['1'], ['2'], ['10']]),
            ('SELECT function_order_writer(id) AS value FROM function_order_driver ORDER BY function_order_writer(id);', [['-10'], ['-2'], ['-1']], [['1'], ['2'], ['10']]),
            ('SELECT function_order_writer(id) AS value FROM function_order_driver d ORDER BY function_order_writer(d.id);', [['-10'], ['-2'], ['-1']], [['1'], ['2'], ['10']]),
            ('SELECT id FROM function_order_driver d ORDER BY function_order_writer(id),function_order_writer(d.id);', [['10'], ['2'], ['1']], [['1'], ['2'], ['10']]),
            ('SELECT function_order_writer(id) AS value FROM function_order_driver d ORDER BY public.function_order_writer(d.id);', [['-10'], ['-2'], ['-1']], [['1'], ['2'], ['10']]),
            ('SELECT id FROM function_order_driver d ORDER BY function_order_writer(id),public.function_order_writer(d.id);', [['10'], ['2'], ['1']], [['1'], ['2'], ['10']]),
            ('SELECT upper(payload),id FROM function_order_driver ORDER BY function_order_writer(id);', [['NULL', '10'], ['A B', '2'], [None, '1']], [['1'], ['2'], ['10']]),
            ('SELECT id FROM function_order_driver ORDER BY CASE WHEN false THEN function_order_writer(id) ELSE -id END;', [['10'], ['2'], ['1']], []),
            ('SELECT id FROM function_order_driver WHERE false ORDER BY function_order_writer(id);', [], []),
        ):
            ok(sql, rows)
            sink(writes)
        for sql in (
            'SELECT id FROM function_order_driver ORDER BY function_order_writer(id),function_order_missing(id);',
            'SELECT id FROM function_order_empty ORDER BY function_order_missing(id);',
            'SELECT id FROM function_order_driver ORDER BY CASE WHEN false THEN function_order_missing(id) ELSE id END;',
        ):
            result = query(sql)
            assert result[1] == '42883' and result[0] == [], (sql, result)
            sink([])
        ok('SELECT id FROM function_order_driver ORDER BY "OrderCase"(id),public.ordercase(id);', [['10'], ['2'], ['1']])
        expected = [['1'], ['2'], ['4'], ['5'], ['3'], ['10']]
        ok('SELECT id FROM function_order_text_rows ORDER BY function_order_text(payload) COLLATE "C" NULLS FIRST;', expected)
        ok('SELECT upper(payload),id FROM function_order_text_rows ORDER BY function_order_text(payload) COLLATE "C" NULLS FIRST;',
           [[None, '1'], ['', '2'], ['10', '4'], ['2', '5'], ['A B', '3'], ['NULL', '10']])
        ok("SELECT function_order_nullable(NULL) FROM function_order_driver WHERE id=1 ORDER BY function_order_nullable('null');", [[None]])
        ok('SELECT payload FROM function_order_null_calls ORDER BY payload NULLS FIRST;', [[None], ['null']])
        print('[STORED FUNCTION ORDER EXECUTION] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
