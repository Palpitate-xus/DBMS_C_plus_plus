#!/usr/bin/env python3
"""Omitted nullable inputs remain present as SQL NULL in the final NEW row."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('omitted_null_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, state=None, rows=None):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        assert result[1] == state, (sql, result, state)
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        return result

    try:
        query('CREATE TABLE omitted_checked(id INT PRIMARY KEY,v INT);')
        query('INSERT INTO omitted_checked VALUES(1,1);')
        query('ALTER TABLE omitted_checked ADD CONSTRAINT positive CHECK(v>0);')
        query('INSERT INTO omitted_checked(id) VALUES(2) RETURNING id,v;', rows=[['2',None]])
        query('INSERT INTO omitted_checked VALUES(3,NULL) RETURNING id,v;', rows=[['3',None]])
        query('INSERT INTO omitted_checked VALUES(4,-1);', state='23514')
        query('SELECT id,v FROM omitted_checked ORDER BY id;', rows=[['1','1'],['2',None],['3',None]])
        query('CREATE TABLE omitted_generated(id INT PRIMARY KEY,base INT,d INT DEFAULT 7,g INT GENERATED ALWAYS AS(base+1) STORED);')
        query('INSERT INTO omitted_generated(id) VALUES(1) RETURNING id,base,d,g;', rows=[['1',None,'7',None]])
        query('INSERT INTO omitted_generated(id,d) VALUES(2,NULL) RETURNING id,base,d,g;', rows=[['2',None,None,None]])
        query('CREATE TABLE omitted_text(id INT PRIMARY KEY,v TEXT);')
        query("INSERT INTO omitted_text(id) VALUES(1);")
        query("INSERT INTO omitted_text VALUES(2,''),(3,'NULL');")
        query('SELECT id,v FROM omitted_text ORDER BY id;', rows=[['1',None],['2',''],['3','NULL']])
        query('SELECT id FROM omitted_text WHERE v IS NULL;', rows=[['1']])
        query('SELECT id FROM omitted_text WHERE v IS NOT NULL ORDER BY id;', rows=[['2'],['3']])
        query('BEGIN;')
        query('INSERT INTO omitted_checked(id) VALUES(5);')
        query('ROLLBACK;')
        query('SELECT id FROM omitted_checked WHERE id=5;', rows=[])
        print('[INSERT OMITTED NULL PROTOCOL E2E] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
