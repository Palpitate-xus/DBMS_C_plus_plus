#!/usr/bin/env python3
"""Ordinary scalar predicates execute lazily and preserve query errors/cells."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('scalar_where_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = runner.start_ours(client)
    failures = []
    def query(sql, ready=b'I'):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b'Z', ready), (sql, messages[-1])
        return result
    def ok(sql, rows=None, ready=b'I'):
        result = query(sql, ready)
        assert result[1] is None, (sql, result)
        if rows is not None: assert result[0] == rows, (sql, result)
        return result
    def control(name, sql, state=None, rows=None, oid=None):
        result = query(sql)
        good = result[1] == state and (state is None or result[4] is None)
        if rows is not None: good = good and result[0] == rows
        if oid is not None: good = good and result[5] == oid
        if not good: failures.append((name, sql, 'expected', state, rows, oid, 'actual', result))
        print('SCALAR_WHERE', name, 'PASS' if good else 'FAIL', result, flush=True)
        return result
    def calls(): return int(ok("SELECT currval('scalar_where_seq');")[0][0][0])
    def expected_calls(name, before, count):
        actual = calls()
        if actual != before+count: failures.append((name, 'calls', actual-before, 'expected', count))
    try:
        for sql in (
            'CREATE TABLE scalar_where_rows(id INT,"ID" BIGINT,payload TEXT);',
            "INSERT INTO scalar_where_rows VALUES(1,2147483648,''),(2,2147483649,'null'),(3,2147483650,NULL);",
            'CREATE TABLE scalar_where_sink(id INT);',
            'CREATE SEQUENCE scalar_where_seq START 1;',
            "CREATE FUNCTION scalar_where_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO scalar_where_sink VALUES(arg) RETURNING id) SELECT id INTO STRICT n FROM scalar_where_sink WHERE id=-1; RETURN n; END; $$;",
            "CREATE FUNCTION scalar_where_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO scalar_where_sink VALUES(arg); SELECT 'bad' INTO n; RETURN n; END; $$;",
            "CREATE FUNCTION scalar_where_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO scalar_where_sink VALUES(arg); PERFORM nextval('scalar_where_seq'); RETURN arg; END; $$;",
        ): ok(sql)
        ok("SELECT nextval('scalar_where_seq');", [['1']])
        control('P0002', 'SELECT id FROM scalar_where_rows WHERE(SELECT scalar_where_strict(51))>0;', 'P0002', [])
        ok('SELECT id FROM scalar_where_sink;', [])
        control('22P02', 'SELECT id FROM scalar_where_rows WHERE(SELECT scalar_where_cast(52))>0;', '22P02', [])
        ok('SELECT id FROM scalar_where_sink;', [])
        control('correlated quoted', 'SELECT d.id,d."ID" FROM scalar_where_rows AS d WHERE(SELECT d."ID")=2147483649;', rows=[['2','2147483649']], oid=[23,20])
        control('two ancestor depths', 'SELECT d.id FROM scalar_where_rows d WHERE(SELECT(SELECT d.id))=2;', rows=[['2']], oid=[23])
        control('nested WITH', 'SELECT d.id FROM scalar_where_rows d WHERE(SELECT(WITH c AS(SELECT d.id AS v)SELECT c.v FROM c))=2;', rows=[['2']])
        control('empty text', "SELECT d.id,d.payload FROM scalar_where_rows d WHERE(SELECT d.payload)='';", rows=[['1','']], oid=[23,25])
        control('text null', "SELECT d.id,d.payload FROM scalar_where_rows d WHERE(SELECT d.payload)='null';", rows=[['2','null']])
        control('actual null', 'SELECT d.id,d.payload FROM scalar_where_rows d WHERE(SELECT d.payload) IS NULL;', rows=[['3',None]])
        control('zero-row unknown', 'SELECT id FROM scalar_where_rows WHERE(SELECT id FROM scalar_where_rows WHERE id=99)>0;', rows=[])
        control('distinct/offset', 'SELECT DISTINCT id%2 AS parity FROM scalar_where_rows d WHERE(SELECT d.id)>0 ORDER BY parity OFFSET 1 LIMIT 1;', rows=[['1']], oid=[23])
        before = calls()
        control('lazy CASE', 'SELECT id FROM scalar_where_rows WHERE CASE WHEN false THEN(SELECT scalar_where_writer(66))>0 ELSE true END;', rows=[['1'],['2'],['3']])
        expected_calls('lazy CASE', before, 0); ok('SELECT id FROM scalar_where_sink;', [])
        before = calls()
        control('unknown callee pre-effect', "SELECT scalar_where_writer(id) FROM scalar_where_rows WHERE CASE WHEN false THEN(SELECT missing_where_callee(nextval('scalar_where_seq')))>0 ELSE true END;", '42883', [])
        expected_calls('unknown callee pre-effect', before, 0); ok('SELECT id FROM scalar_where_sink;', [])
        control('unknown column empty', 'SELECT id FROM scalar_where_rows WHERE id<0 AND(SELECT missing_where_column)>0;', '42703', [])
        before = calls()
        control('width pre-effect', 'SELECT scalar_where_writer(id) FROM scalar_where_rows WHERE id<0 AND(SELECT 1,2)>0;', '42601', [])
        expected_calls('width pre-effect', before, 0)
        control('cardinality', 'SELECT id FROM scalar_where_rows WHERE(SELECT id FROM scalar_where_rows)>0;', '21000', [])
        before = calls()
        control('uncorrelated memo', 'SELECT id FROM scalar_where_rows WHERE(SELECT scalar_where_writer(70))>0;', rows=[['1'],['2'],['3']])
        expected_calls('uncorrelated memo', before, 1)
        ok('DELETE FROM scalar_where_sink;')
        before = calls()
        control('different sites', 'SELECT id FROM scalar_where_rows WHERE(SELECT scalar_where_writer(71))>0 AND(SELECT scalar_where_writer(71))>0;', rows=[['1'],['2'],['3']])
        expected_calls('different sites', before, 2)
        ok('DELETE FROM scalar_where_sink;')
        before = calls()
        control('correlated writer', 'SELECT d.id FROM scalar_where_rows d WHERE(SELECT scalar_where_writer(d.id))>0 ORDER BY d.id;', rows=[['1'],['2'],['3']])
        expected_calls('correlated writer', before, 3)
        ok('DELETE FROM scalar_where_sink;')
        # Existing multirow semantic roles must retain their own lowering,
        # not acquire a scalar child receiver/cardinality error.
        control('IN multirow adjacent', 'SELECT id FROM scalar_where_rows WHERE id IN(SELECT id FROM scalar_where_rows) ORDER BY id;', rows=[['1'],['2'],['3']])
        control('EXISTS adjacent', 'SELECT id FROM scalar_where_rows WHERE EXISTS(SELECT id FROM scalar_where_rows) ORDER BY id;', rows=[['1'],['2'],['3']])
        # A failed predicate function may not erase an earlier successful
        # command in an explicit transaction; ROLLBACK TO restores that cut.
        ok('BEGIN;', ready=b'T')
        ok('INSERT INTO scalar_where_sink VALUES(90);', ready=b'T')
        ok('SAVEPOINT keep;', ready=b'T')
        result = query('SELECT id FROM scalar_where_rows WHERE(SELECT scalar_where_strict(53))>0;', b'E')
        if result[1] != 'P0002': failures.append(('explicit predicate error',result))
        ok('ROLLBACK TO keep;', ready=b'T')
        ok('SELECT id FROM scalar_where_sink;', [['90']], b'T')
        ok('ROLLBACK;')
        ok('SELECT id FROM scalar_where_sink;', [])
        assert not failures, failures
        print('[ORDINARY SCALAR WHERE PROTOCOL] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__': main()
