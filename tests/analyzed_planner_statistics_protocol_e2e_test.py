#!/usr/bin/env python3
"""Typed scan-cost evidence survives real ANALYZE, rollback and DDL refresh."""
import importlib.util
import json
import os
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('typed_statistics_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = runner.start_ours(client)
    trace = {'complete': False, 'commands': [], 'scans': []}
    data = [[str(i), 'hot\n|;,界' if i <= 240 else None if i <= 260 else
             'needle' if i == 261 else '' if i == 262 else 'NULL' if i == 263 else 'cold\n|;,'+str(i),
             'payload 界\n'+str(i)] for i in range(1, 321)]
    literal = lambda value: 'NULL' if value is None else "'"+value.replace("'", "''")+"'"

    def query(sql, ready=b'I'):
        messages = client.simple_query(server['sock'], sql)
        rows, state, message, _, tag, oids = runner.decode_wire_result(messages, include_types=True)
        trace['commands'].append({'sql': sql, 'rows': rows, 'state': state, 'tag': tag,
            'oids': oids, 'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert state is None and messages[-1] == (b'Z', ready), (sql, state, message)
        return rows, oids

    def counts(ready):
        rows, _ = query('SELECT * FROM pg_catalog.pg_stat_tables', ready)
        r = next(row for row in rows if row[0] == 'typed_wire')
        return [int(v) for v in r[1:5]]

    def scan(predicate, wanted, indexed, ready=b'I'):
        before = counts(ready)
        sql = 'SELECT id,k,pad FROM typed_wire WHERE '+predicate
        rows, oids = query(sql, ready)
        assert sorted(rows, key=lambda row: int(row[0])) == wanted and oids == [23, 25, 25], sql
        after = counts(ready); delta = [b-a for a, b in zip(before, after)]
        if indexed:
            assert delta[:3] == [0, 0, 1] and delta[3] >= len(rows), (sql, delta)
        else:
            assert delta == [1, 320, 0, 0], (sql, delta)
        trace['scans'].append({'sql': sql, 'delta': delta, 'indexed': indexed})

    try:
        query('ALTER SYSTEM SET auto_analyze=off')
        query('CREATE TABLE typed_wire(id integer PRIMARY KEY,k text,pad text)')
        query('INSERT INTO typed_wire VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in data))
        query('CREATE INDEX typed_wire_btree ON typed_wire(k)')
        query('CREATE INDEX typed_wire_gist ON typed_wire USING gist(k)')
        query('ANALYZE typed_wire')
        root = Path(server['dir'])/'info'
        images = {str(f.relative_to(root)): f.read_bytes() for f in root.rglob('*.dt')}
        images.update({n: (root/n).read_bytes() for n in ['.stats', '.analyzed_statistics']})
        hot = data[:240]
        scan("k LIKE 'hot%界'", hot, False)
        scan("k='hot\n|;,界'", hot, False)
        scan("k='needle'", [data[260]], True)
        scan("k LIKE 'needle%'", [data[260]], True)
        scan("k='absent'", [], True)
        scan('k IS NULL', data[240:260], False)
        scan("k!='hot\n|;,界'", data[260:], False)
        for predicate, wanted in [("k=''", [data[261]]), ("k='NULL'", [data[262]])]:
            rows, oids = query('SELECT id,k,pad FROM typed_wire WHERE '+predicate)
            assert rows == wanted and oids == [23, 25, 25]
        query('SET random_page_cost=0.000001')
        scan("k='hot\n|;,界'", hot, True)
        rows, _ = query("EXPLAIN (FORMAT JSON, ANALYZE TRUE) SELECT id,k,pad FROM typed_wire WHERE k='hot\n|;,界'")
        document = json.loads('\n'.join(row[0] for row in rows))
        def nodes(node):
            yield node
            for child in node.get('children', []): yield from nodes(child)
        indexes = [node for node in nodes(document['plan']) if node['nodeType'] == 'IndexScan']
        assert indexes and indexes[0]['rows'] == 240 and document['actualRows'] == 240, document
        query('SET random_page_cost=4')
        assert all((root/n).read_bytes() == raw for n, raw in images.items())
        # Typed evidence remains usable without the old display-oriented file.
        (root/'.stats').unlink()
        scan("k LIKE 'hot%界'", hot, False)
        scan("k='hot\n|;,界'", hot, False)
        (root/'.stats').write_bytes(images['.stats'])
        before_transaction = {name: (root/name).read_bytes() for name in ['.stats', '.analyzed_statistics']}
        trace['statistics_before_transaction'] = {name: raw.hex() for name, raw in before_transaction.items()}
        query('BEGIN', b'T')
        query("UPDATE typed_wire SET k='transient\n|;,界' WHERE id<=240", b'T')
        query('ANALYZE typed_wire', b'T')
        transient = [[row[0], 'transient\n|;,界', row[2]] for row in hot]
        scan("k LIKE 'transient%'", transient, False, b'T')
        trace['statistics_inside_transaction'] = {name: (root/name).read_bytes().hex() for name in before_transaction}
        query('ROLLBACK')
        trace['statistics_after_rollback'] = {name: (root/name).read_bytes().hex() for name in before_transaction}
        assert all((root/name).read_bytes() == raw for name, raw in before_transaction.items()), 'both ANALYZE images roll back byte for byte'
        scan("k LIKE 'hot%界'", hot, False)
        scan("k LIKE 'transient%'", [], True)
        query("UPDATE typed_wire SET k='replacement\n|;,界' WHERE id<=240")
        query('ANALYZE typed_wire')
        replacement = [[row[0], 'replacement\n|;,界', row[2]] for row in hot]
        scan("k LIKE 'replacement%'", replacement, False)
        scan("k LIKE 'hot%'", [], True)
        query('ALTER TABLE typed_wire RENAME COLUMN k TO renamed')
        query('ANALYZE typed_wire')
        rows, oids = query("SELECT id,renamed,pad FROM typed_wire WHERE renamed LIKE 'replacement%' ORDER BY id")
        assert rows == replacement and oids == [23, 25, 25]
        trace['complete'] = True
    finally:
        runner.stop_ours(server); trace['server_exit'] = server['process'].returncode
        evidence = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
        if evidence:
            target = Path(evidence); target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0
    print('ANALYZED_PLANNER_STATISTICS_PROTOCOL commands='+str(len(trace['commands']))+
          ' actual_scans='+str(len(trace['scans']))+' full_values_NULL_UTF8_rollback=1')


if __name__ == '__main__':
    main()
