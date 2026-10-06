#!/usr/bin/env python3
"""Aggregate ORDER keys remain aggregate results, not scalar calls."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('aggregate_order_runner', root/'tests/compat/pg_diff_runner.py')
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
            'CREATE TABLE aggregate_order_rows(id INT,g INT,v INT);',
            'CREATE TABLE aggregate_order_empty(id INT,g INT,v INT);',
            'CREATE TABLE aggregate_order_calls(id INT);',
            'INSERT INTO aggregate_order_rows VALUES(1,1,1),(2,1,2),(3,2,4),(4,3,0),(5,3,0),(6,3,0);',
            'CREATE FUNCTION aggregate_order_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO aggregate_order_calls VALUES(arg); RETURN arg; END; $$;',
        ):
            ok(sql)
        ok('SELECT g,count(*) FROM aggregate_order_rows GROUP BY g ORDER BY count(*) DESC,g;', [['3','3'], ['1','2'], ['2','1']])
        ok('SELECT g,count(*) AS n FROM aggregate_order_rows GROUP BY g ORDER BY count(*) DESC,g;', [['3','3'], ['1','2'], ['2','1']])
        ok('SELECT g,count(DISTINCT v) FROM aggregate_order_rows GROUP BY g ORDER BY count(DISTINCT v) DESC,g;', [['1','2'], ['2','1'], ['3','1']])
        ok('SELECT g,count(*) FILTER (WHERE v>0) FROM aggregate_order_rows GROUP BY g ORDER BY count(*) FILTER (WHERE v>0) DESC,g;', [['1','2'], ['2','1'], ['3','0']])
        ok('SELECT g,sum(v) FROM aggregate_order_rows GROUP BY g ORDER BY sum(v) DESC;', [['2','4'], ['1','3'], ['3','0']])
        ok('SELECT g,min(v) FROM aggregate_order_rows GROUP BY g ORDER BY min(v) DESC;', [['2','4'], ['1','1'], ['3','0']])
        ok('SELECT g,max(v) FROM aggregate_order_rows GROUP BY g ORDER BY max(v) DESC;', [['2','4'], ['1','2'], ['3','0']])
        avg = ok('SELECT g,avg(v) FROM aggregate_order_rows GROUP BY g ORDER BY avg(v) DESC;')
        assert [row[0] for row in avg[0]] == ['2','1','3'], avg
        ok('CREATE FUNCTION count(arg INT) RETURNS INT LANGUAGE sql AS $$ SELECT -arg $$;')
        ok('SELECT id FROM aggregate_order_rows ORDER BY public.count(id);', [[str(i)] for i in range(6,0,-1)])
        for sql in (
            'SELECT g,count(*) FROM aggregate_order_empty GROUP BY g ORDER BY count(aggregate_order_missing(v));',
            'SELECT g,count(*) FROM aggregate_order_rows GROUP BY g ORDER BY aggregate_order_writer(g),count(aggregate_order_missing(v));',
            'SELECT g,count(*) FROM aggregate_order_empty GROUP BY g ORDER BY "COUNT"(*);',
        ):
            result = query(sql)
            assert result[1] == '42883' and result[0] == [] and result[4] is None, (sql, result)
            ok('SELECT id FROM aggregate_order_calls;', [])
        print('[AGGREGATE ORDER ROLE] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
