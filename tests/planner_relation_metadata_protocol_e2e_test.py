#!/usr/bin/env python3
"""Ordinary plans refresh schema and AM metadata across real DDL changes."""
import importlib.util
import json
import os
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        'relation_metadata_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    trace = {'complete': False, 'commands': [], 'scans': []}

    def query(sql, wanted=None, types=None):
        messages = client.simple_query(server['sock'], sql)
        rows, state, message, _, tag, oids = runner.decode_wire_result(
            messages, include_types=True)
        trace['commands'].append({
            'sql': sql, 'rows': rows, 'state': state, 'tag': tag, 'oids': oids,
            'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert state is None and messages[-1] == (b'Z', b'I'), (sql, state, message)
        if wanted is not None:
            assert sorted(rows, key=lambda r: int(r[0])) == wanted, (sql, rows, wanted)
            assert tag == 'SELECT ' + str(len(wanted)), (sql, tag)
        if types is not None:
            assert oids == types, (sql, oids, types)
        return rows

    def counters():
        row = next(r for r in query('SELECT * FROM pg_catalog.pg_stat_tables')
                   if r[0] == 'plan_meta_rows')
        return [int(v) for v in row[1:5]]

    def scan(sql, wanted, indexed, physical_rows=6):
        before = counters()
        query(sql, wanted, [23, 23, 25])
        after = counters()
        delta = [b - a for a, b in zip(before, after)]
        expected = [0, 0, 1, len(wanted)] if indexed else [1, physical_rows, 0, 0]
        assert delta == expected, (sql, delta, expected)
        trace['scans'].append({'sql': sql, 'delta': delta})

    def explain(sql, kind):
        document = json.loads('\n'.join(r[0] for r in query('EXPLAIN (FORMAT JSON) ' + sql)))
        def nodes(node):
            yield node
            for child in node.get('children', []):
                yield from nodes(child)
        assert any(n['nodeType'] == kind for n in nodes(document['plan'])), (sql, document)

    def observe_like(sql):
        # LIKE can remain a typed-expression scan in the ordinary frontend.
        # Its SQL result does not require a particular access method. Keep
        # the actual path observation alongside the explicit GiST ranges.
        before = counters()
        query(sql, [['2', '7', '界']], [23, 23, 25])
        after = counters()
        trace.setdefault('like_access_observations', []).append({
            'sql': sql, 'delta': [b - a for a, b in zip(before, after)]})

    try:
        query('ALTER SYSTEM SET auto_analyze=off')
        query('SELECT pg_reload_conf()')
        query('SET random_page_cost=0.000001')
        query('CREATE TABLE plan_meta_rows(id integer PRIMARY KEY,k integer,tag text)')
        query("INSERT INTO plan_meta_rows VALUES(1,7,'a'),(2,7,'界'),(3,NULL,NULL),"
              "(4,0,''),(5,7,'NULL'),(6,8,'line\nbreak')")
        query('ANALYZE plan_meta_rows')
        hot = 'SELECT id,k,tag FROM plan_meta_rows WHERE k=7'
        expected = [['1', '7', 'a'], ['2', '7', '界'], ['5', '7', 'NULL']]
        scan(hot, expected, False)
        query('CREATE INDEX plan_meta_btree ON plan_meta_rows(k)')
        explain(hot, 'IndexScan')
        scan(hot, expected, True)
        scan(hot, expected, True)
        query('DROP INDEX plan_meta_btree')
        explain(hot, 'TableScan')
        scan(hot, expected, False)

        conjunction = 'SELECT id,k,tag FROM plan_meta_rows WHERE id=1 AND k=7'
        for method in ['hash', 'bloom']:
            index = 'plan_meta_' + method
            query('CREATE INDEX ' + index + ' ON plan_meta_rows USING ' + method + '(k)')
            explain(conjunction, 'BitmapHeapScan')
            scan(conjunction, [['1', '7', 'a']], True)
            query('DROP INDEX ' + index)
            explain(conjunction, 'IndexScan')
            scan(conjunction, [['1', '7', 'a']], True)

        prefix = "SELECT id,k,tag FROM plan_meta_rows WHERE tag LIKE '界%'"
        bounds = "SELECT id,k,tag FROM plan_meta_rows WHERE tag>='界' AND tag<='界'"
        query('CREATE INDEX plan_meta_gist ON plan_meta_rows USING gist(tag)')
        scan(bounds, [['2', '7', '界']], True)
        observe_like(prefix)
        query('DROP INDEX plan_meta_gist')
        scan(bounds, [['2', '7', '界']], False)
        observe_like(prefix)
        query('ALTER TABLE plan_meta_rows RENAME COLUMN tag TO message')
        query('SELECT id,k,message FROM plan_meta_rows WHERE k IS NULL',
              [['3', None, None]], [23, 23, 25])
        query('SELECT id,k,message FROM plan_meta_rows WHERE k=0',
              [['4', '0', '']], [23, 23, 25])
        query('DROP TABLE plan_meta_rows')
        query('CREATE TABLE plan_meta_rows(id integer PRIMARY KEY,k text,message text)')
        query("INSERT INTO plan_meta_rows VALUES(9,'new type',NULL)")
        query("SELECT id,k,message FROM plan_meta_rows WHERE k='new type'",
              [['9', 'new type', None]], [23, 25, 25])
        refreshed = 'SELECT id,k,message FROM plan_meta_rows WHERE id=9'
        explain(refreshed, 'IndexScan')
        query(refreshed, [['9', 'new type', None]], [23, 25, 25])
        trace['complete'] = True
    finally:
        runner.stop_ours(server)
        trace['server_exit'] = server['process'].returncode
        evidence = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
        if evidence:
            path = Path(evidence)
            path.mkdir(parents=True, exist_ok=True)
            (path / 'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, indent=2) + '\n')
        assert trace['server_exit'] == 0
    print('PLANNER_RELATION_METADATA_PROTOCOL commands=' + str(len(trace['commands'])) +
          ' actual_scans=' + str(len(trace['scans'])) + ' DDL_NULL_empty_UTF8_types=1')


if __name__ == '__main__':
    main()
