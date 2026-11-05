#!/usr/bin/env python3
"""Literal LIKE candidates retain the complete typed predicate and snapshots."""
import importlib.util
import json
import os
import socket
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('like_prefix_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = runner.start_ours(client)
    trace = {'complete': False, 'commands': [], 'scans': []}
    other = None

    def query(sql, sock=None, ready=b'I', state=None):
        messages = client.simple_query(sock or server['sock'], sql)
        rows, actual, message, _, tag, types = runner.decode_wire_result(messages, include_types=True)
        trace['commands'].append({'sql': sql, 'rows': rows, 'state': actual, 'tag': tag,
            'oids': types, 'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert actual == state and messages[-1] == (b'Z', ready), (sql, actual, message)
        return rows, types

    def counts(ready=b'I'):
        rows, _ = query('SELECT * FROM pg_catalog.pg_stat_tables', ready=ready)
        r = next(r for r in rows if r[0] == 'like_wire')
        return [int(v) for v in r[1:5]]

    def scan(predicate, wanted, indexed, ready=b'I', columns='id,tag,pad', table='like_wire'):
        before = counts(ready)
        sql = 'SELECT '+columns+' FROM '+table+' WHERE '+predicate
        rows, types = query(sql, ready=ready)
        assert sorted(rows, key=lambda r: int(r[0])) == wanted, (sql, rows, wanted)
        assert types == [23, 25, 25], (sql, types)
        after = counts(ready); delta = [b-a for a, b in zip(before, after)]
        if indexed:
            assert delta[:3] == [0, 0, 1] and len(rows) <= delta[3] <= 24, (sql, delta)
        else:
            assert delta == [1, 24, 0, 0], (sql, delta)
        trace['scans'].append({'sql': sql, 'delta': delta, 'expected_index': indexed})

    tags = ['a', 'ab', 'a界', 'a界tail', 'aZtail', 'a_x', 'a%x', 'a\\x', 'b', None,
            '', 'NULL', '界', '界尾', '界\ntail', '界x', 'hiu', 'hitTail', 'hit', '~界',
            '~', 'a\nnewline', "a'quote", 'other']
    population = [[str(i+1), t, None if i % 3 == 0 else 'payload 界\n'+str(i)] for i, t in enumerate(tags)]
    literal = lambda v: 'NULL' if v is None else "'"+v.replace("'", "''")+"'"
    cases = [
        ("tag LIKE 'a%'", [i for i, t in enumerate(tags) if t is not None and t.startswith('a')], True),
        ("tag LIKE '界%'", [12, 13, 14, 15], True),
        ("tag LIKE 'a_%'", [1, 2, 3, 4, 5, 6, 7, 21, 22], True),
        ("tag LIKE 'a%tail'", [3, 4], True),
        ("tag LIKE 'a\\_%'", [5], True),
        ("tag LIKE 'a!_%' ESCAPE '!'", [5], True),
        ("tag LIKE 'a界_%' ESCAPE '界'", [5], True),
        ("tag LIKE 'a\\_%' ESCAPE ''", [7], True),
        ("tag LIKE 'a'", [0], True),
        ("tag LIKE 'hit%'", [17, 18], True),
        ("tag LIKE '~%'", [19, 20], True),
        ("tag LIKE 'a%' AND id>3", [3, 4, 5, 6, 7, 21, 22], True),
        ("tag LIKE 'a%' OR id=9", [0, 1, 2, 3, 4, 5, 6, 7, 8, 21, 22], False),
        ("tag NOT LIKE 'a%'", [8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 23], False),
        ("tag ILIKE 'a%'", [0, 1, 2, 3, 4, 5, 6, 7, 21, 22], False),
        ("tag LIKE '%a'", [0], False),
        ("tag LIKE '_%'", [i for i, t in enumerate(tags) if t], False),
        ("tag LIKE '%'", [i for i, t in enumerate(tags) if t is not None], False),
        ("tag LIKE NULL", [], False)]
    try:
        query('ALTER SYSTEM SET auto_analyze=off')
        query('SET random_page_cost=0.000001')
        query('CREATE TABLE like_wire(id integer PRIMARY KEY,tag text,pad text)')
        query('INSERT INTO like_wire VALUES'+','.join('('+','.join(literal(v) for v in r)+')' for r in population))
        query('ANALYZE like_wire')
        path = Path(server['dir'])/'info'
        for predicate, indices, _ in cases:
            rows, types = query('SELECT id,tag,pad FROM like_wire WHERE '+predicate)
            assert sorted(rows, key=lambda r: int(r[0])) == [population[i] for i in indices], predicate
            assert types == [23, 25, 25]
        query('CREATE INDEX like_wire_gist ON like_wire USING gist(tag)')
        frozen = {str(f.relative_to(path)): f.read_bytes() for f in path.rglob('*.dt')}
        frozen.update({n: (path/n).read_bytes() for n in ['.stats', '.analyzed_statistics']})
        for predicate, indices, indexed in cases:
            scan(predicate, [population[i] for i in indices], indexed)
        scan("x.tag LIKE '界%'", [population[i] for i in [12, 13, 14, 15]], True,
             columns='x.id,x.tag,x.pad', table='like_wire AS x')
        assert all((path/n).read_bytes() == raw for n, raw in frozen.items())
        # A nonconstant pattern keeps its expression owner; its row result
        # and actual heap access remain unchanged.
        scan('tag LIKE tag', [r for i, r in enumerate(population) if r[1] is not None and i != 7], False)
        for pattern, wanted in [('a_%', 9), ('ÿ%', 0), ('a'+chr(1)+'%', 0)]:
            rows, _ = query('EXPLAIN (FORMAT JSON, ANALYZE TRUE) SELECT id,tag,pad '
                            'FROM like_wire WHERE tag LIKE '+literal(pattern))
            document = json.loads('\n'.join(r[0] for r in rows))
            assert document['actualRows'] == wanted, (pattern, document)
            def nodes(node):
                yield node
                for child in node.get('children', []): yield from nodes(child)
            assert any(n['nodeType'] == 'GiSTScan' for n in nodes(document['plan'])), document
        query('SET random_page_cost=4')
        scan("tag LIKE 'a%'", [population[i] for i in cases[0][1]], False)
        query('SET random_page_cost=0.000001')
        other = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(other, 'alice', 'info')
        query('BEGIN ISOLATION LEVEL REPEATABLE READ', ready=b'T')
        rows, _ = query('SELECT id,tag,pad FROM like_wire ORDER BY id', ready=b'T')
        assert rows == population
        query("UPDATE like_wire SET tag='outside' WHERE id=19", sock=other)
        query("UPDATE like_wire SET tag='hitNew' WHERE id=24", sock=other)
        scan("tag LIKE 'hit%'", [population[17], population[18]], False, ready=b'T')
        query('COMMIT')
        latest = [r[:] for r in population]; latest[18][1] = 'outside'; latest[23][1] = 'hitNew'
        scan("tag LIKE 'hit%'", [latest[17], latest[23]], True)
        query('BEGIN', ready=b'T')
        query("UPDATE like_wire SET tag='hitOwn' WHERE id=9", ready=b'T')
        scan("tag LIKE 'hit%'", [latest[8][:1]+['hitOwn']+latest[8][2:], latest[17], latest[23]],
             False, ready=b'T')
        query('ROLLBACK')
        query('DROP INDEX like_wire_gist')
        scan("tag LIKE 'hit%'", [latest[17], latest[23]], False)
        query('CREATE TABLE like_varchar(id integer PRIMARY KEY,tag varchar(60))')
        query("INSERT INTO like_varchar VALUES(1,'a界'),(2,NULL),(3,''),(4,'界x'),(5,'other')")
        query('CREATE INDEX like_varchar_gist ON like_varchar USING gist(tag)')
        before = next(r for r in query('SELECT * FROM pg_catalog.pg_stat_tables')[0]
                      if r[0] == 'like_varchar')
        rows, types = query("SELECT id,tag FROM like_varchar WHERE tag LIKE '界%'")
        after = next(r for r in query('SELECT * FROM pg_catalog.pg_stat_tables')[0]
                     if r[0] == 'like_varchar')
        delta = [int(b)-int(a) for a, b in zip(before[1:5], after[1:5])]
        assert rows == [['4', '界x']] and types == [23, 1043] and delta == [0, 0, 1, 1]
        trace['scans'].append({'sql': "SELECT id,tag FROM like_varchar WHERE tag LIKE '界%'",
                               'delta': delta, 'expected_index': True})
        trace['complete'] = True
    finally:
        if other is not None: other.close()
        runner.stop_ours(server); trace['server_exit'] = server['process'].returncode
        evidence = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
        if evidence:
            target = Path(evidence); target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0
    print('LIKE_PREFIX_SCAN_PROTOCOL commands='+str(len(trace['commands']))+
          ' actual_scans='+str(len(trace['scans']))+' full_values_NULL_UTF8_snapshot=1')


if __name__ == '__main__':
    main()
