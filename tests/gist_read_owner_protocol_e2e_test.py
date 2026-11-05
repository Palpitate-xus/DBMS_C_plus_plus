#!/usr/bin/env python3
"""GiST-backed LIKE keeps complete visible values across read-owner changes."""
import importlib.util
import json
import os
import socket
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('gist_owner_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    trace = {'complete': False, 'commands': []}
    other = None
    population = [[str(i), '' if i == 1 else None if i == 2 or i % 19 == 0 else
                   'NULL' if i == 3 else 'needle_界\n|;,'+str(i) if i >= 125 else
                   'bulk\n|;,'+str(i), None if i % 3 == 0 else 'payload 界\n'+str(i)]
                  for i in range(1, 129)]

    def literal(value):
        return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"

    def query(sql, *, peer=False, ready=b'I'):
        messages = client.simple_query(other if peer else server['sock'], sql)
        rows, state, message, _, tag, oids = runner.decode_wire_result(messages, include_types=True)
        trace['commands'].append({'sql': sql, 'connection': 'other' if peer else 'primary',
                                  'rows': rows, 'state': state, 'tag': tag, 'oids': oids,
                                  'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert state is None and messages[-1] == (b'Z', ready), (sql, state, message)
        return rows, oids

    def select(predicate, wanted, *, ready=b'I'):
        rows, oids = query('SELECT id,k,pad FROM gist_owner_wire WHERE '+predicate, ready=ready)
        assert sorted(rows, key=lambda row: int(row[0])) == wanted, (predicate, rows, wanted)
        assert oids == [23, 25, 25], (predicate, oids)

    try:
        query('ALTER SYSTEM SET auto_analyze=off')
        query('SET random_page_cost=0.000001')
        query('CREATE TABLE gist_owner_wire(id integer PRIMARY KEY,k text,pad text)')
        query('INSERT INTO gist_owner_wire VALUES'+','.join(
            '('+','.join(literal(v) for v in row)+')' for row in population))
        query('CREATE INDEX gist_owner_wire_gist ON gist_owner_wire USING gist(k)')
        query('ANALYZE gist_owner_wire')
        root = Path(server['dir'])/'info'
        index = root/'gist_owner_wire_k.gist'
        images = {str(f.relative_to(root)): f.read_bytes() for f in root.rglob('*.dt')}
        images.update({name: (root/name).read_bytes() for name in ['.stats', '.analyzed_statistics']})
        needles = [row for row in population if row[1] and row[1].startswith('needle')]
        select("k LIKE 'needle%'", needles)
        query('BEGIN', ready=b'T')
        select("k LIKE 'needle%'", needles, ready=b'T')
        query('COMMIT')
        assert all((root/name).read_bytes() == raw for name, raw in images.items())
        query('BEGIN ISOLATION LEVEL SERIALIZABLE', ready=b'T')
        select("k LIKE 'needle%'", needles, ready=b'T')
        select("k LIKE 'absent%'", [], ready=b'T')
        query('COMMIT')

        other = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(other, 'alice', 'info')
        query('BEGIN ISOLATION LEVEL REPEATABLE READ', ready=b'T')
        select('id>=1', population, ready=b'T')
        query("UPDATE gist_owner_wire SET k='changed_界' WHERE id=127", peer=True)
        query('UPDATE gist_owner_wire SET k=NULL WHERE id=1', peer=True)
        query("UPDATE gist_owner_wire SET k='' WHERE id=2", peer=True)
        select("k LIKE 'needle%'", needles, ready=b'T')
        select("k=''", [population[0]], ready=b'T')
        select('k IS NULL', [row for row in population if row[1] is None], ready=b'T')
        query('COMMIT')
        latest = [row[:] for row in population]
        latest[0][1], latest[1][1], latest[126][1] = None, '', 'changed_界'
        current_needles = [row for row in latest if row[1] and row[1].startswith('needle')]
        select("k LIKE 'needle%'", current_needles)
        select("k=''", [latest[1]])
        select('k IS NULL', [row for row in latest if row[1] is None])

        query('BEGIN', ready=b'T')
        query("UPDATE gist_owner_wire SET k='needle_own',pad=NULL WHERE id=3", ready=b'T')
        own = [row[:] for row in latest]
        own[2][1:] = ['needle_own', None]
        select("k LIKE 'needle%'", [row for row in own if row[1] and row[1].startswith('needle')], ready=b'T')
        query('ROLLBACK')
        query('BEGIN', peer=True, ready=b'T')
        select("k LIKE 'needle%'", current_needles)
        query('ROLLBACK', peer=True)

        # Full checksum and absence controls operate only on this private server's file.
        committed_index = index.read_bytes()
        try:
            corrupt = bytearray(committed_index)
            corrupt[-1] ^= 1
            index.write_bytes(corrupt)
            select("k LIKE 'needle%'", current_needles)
            index.unlink()
            select("k LIKE 'needle%'", current_needles)
        finally:
            index.write_bytes(committed_index)
        select("k LIKE 'needle%'", current_needles)
        select("k LIKE '%'", [row for row in latest if row[1] is not None])
        select("k='NULL'", [latest[2]])
        select('id>=1', latest)
        trace['complete'] = True
    finally:
        if other is not None:
            other.close()
        runner.stop_ours(server)
        trace['server_exit'] = server['process'].returncode
        evidence = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
        if evidence:
            target = Path(evidence)
            target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0
    print('GIST_READ_OWNER_PROTOCOL commands='+str(len(trace['commands']))+
          ' full_values_NULL_UTF8_snapshots_corruption_absence=1')


if __name__ == '__main__':
    main()
