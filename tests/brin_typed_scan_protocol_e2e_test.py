#!/usr/bin/env python3
"""Typed BRIN pages keep original predicate, value/NULL and snapshot semantics."""
import hashlib
import importlib.util
import json
import os
import socket
import struct
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('brin_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    other = None
    trace = {'complete': False, 'commands': [], 'images': [], 'scan_assertions': 0}
    population = [[str(i), None if i % 17 == 0 else str(i-384),
                   None if i == 1 or i % 19 == 0 else '' if i == 2 else 'NULL' if i == 3 else 'row 界\n|;,'+str(i),
                   None if i % 11 == 0 else 'payload 界\n|;,'+str(i)+'x'*192]
                  for i in range(1, 769)]

    def literal(value):
        return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"

    def query(sql, *, peer=False, ready=b'I', state=None, tag=None, shared=True):
        messages = client.simple_query(other if peer else server['sock'], sql)
        rows, actual, message, _, actual_tag, oids = runner.decode_wire_result(messages, include_types=True)
        trace['commands'].append({'sql': sql, 'connection': 'other' if peer else 'primary',
                                  'rows': rows, 'state': actual, 'tag': actual_tag, 'oids': oids,
                                  'shared_pg_sql': shared,
                                  'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert actual == state and messages[-1] == (b'Z', ready), (sql, actual, message)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        return rows, oids

    def nodes(plan):
        if isinstance(plan, list):
            for value in plan:
                yield from nodes(value)
        elif isinstance(plan, dict):
            yield plan
            for value in plan.values():
                if isinstance(value, (dict, list)):
                    yield from nodes(value)

    def select(predicate, wanted, *, ready=b'I', used=None, fields='id,n,body,pad', table='brin_wire', oids=(23, 20, 25, 25)):
        sql = 'SELECT '+fields+' FROM '+table+' WHERE '+predicate
        rows, types = query(sql, ready=ready, tag='SELECT '+str(len(wanted)))
        assert sorted(rows, key=lambda row: int(row[0])) == wanted, (predicate, rows, wanted)
        assert types == list(oids), (predicate, types, oids)
        if used is not None:
            data, _ = query('EXPLAIN (ANALYZE,FORMAT JSON) '+sql, ready=ready, shared=False)
            plan = json.loads('\n'.join(row[0] for row in data))
            scans = [n for n in nodes(plan) if n.get('nodeType') == 'BrinScan']
            assert any(n.get('usedSummary') is True for n in scans) == used, (predicate, used, plan)
            if used:
                assert all(n['candidatePages'] >= 0 for n in scans)
            trace['scan_assertions'] += 1

    def image(name, phase, expected_width=None):
        raw = (Path(server['dir'])/'info'/name).read_bytes()
        trace['images'].append({'name': name, 'phase': phase, 'bytes': len(raw),
                                'sha256': hashlib.sha256(raw).hexdigest(), 'hex': raw.hex()})
        assert struct.unpack_from('<II', raw) == (0x5342524e, 3)
        checksum = 0xffffffff
        for byte in raw[:-4]:
            checksum ^= byte
            for _ in range(8):
                checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
        assert checksum ^ 0xffffffff == int.from_bytes(raw[-4:], 'little')
        count = struct.unpack_from('<Q', raw, 8)[0]
        width, pages, flags = struct.unpack_from('<III', raw, 16)
        identity = struct.unpack_from('<Q', raw, 36)[0]
        assert width and pages and not flags and identity
        if expected_width is not None:
            assert width == expected_width, (phase, width, expected_width)
        offset = 44+identity
        ranges = []
        for _ in range(count):
            first, last, state = struct.unpack_from('<III', raw, offset)
            offset += 12
            values = []
            for _ in range(2):
                length = struct.unpack_from('<Q', raw, offset)[0]
                offset += 8
                values.append(raw[offset:offset+length])
                offset += length
            assert 0 < first <= last < pages and 0 < state <= 7
            ranges.append((first, last, state, *values))
        assert offset == len(raw)-4
        return raw, ranges

    def matching(rows, predicate):
        return [row for row in rows if row[1] is not None and predicate(int(row[1]))]

    try:
        query('ALTER SYSTEM SET auto_analyze=off', shared=False)
        query('CREATE TABLE brin_wire(id integer PRIMARY KEY,n bigint,body text COLLATE "C",pad text)', tag='CREATE TABLE')
        query('INSERT INTO brin_wire VALUES'+','.join('('+','.join(literal(x) for x in row)+')' for row in population), tag='INSERT 0 768')
        query('CREATE INDEX brin_wire_n_idx ON brin_wire USING brin(n) WITH (pages_per_range=1)', tag='CREATE INDEX')
        image('brin_wire_n.brin', 'initial', 1)
        for predicate, test in [('n>=376', lambda n: n >= 376), ('376<=n', lambda n: n >= 376), ("n>='376'", lambda n:n>=376),
                                ('n>376', lambda n: n > 376), ('n<-375', lambda n: n < -375),
                                ('n<=-375', lambda n: n <= -375), ('n=10', lambda n: n == 10), ('10=n', lambda n:n==10),
                                ('n>99999', lambda n: n > 99999)]:
            select(predicate, matching(population, test), used=True)
        select('n>=376 AND body IS NOT NULL', [r for r in matching(population, lambda n: n >= 376) if r[2] is not None], used=True)
        select('n IS NULL', [r for r in population if r[1] is None])
        select('n IS NOT NULL', [r for r in population if r[1] is not None])
        query('CREATE INDEX brin_wire_body_idx ON brin_wire USING brin(body) WITH (pages_per_range=1)', tag='CREATE INDEX')
        select("body=''", [population[1]], used=True)
        select("body='NULL'", [population[2]], used=True)
        select("''=body", [population[1]], used=True)
        select("'NULL'=body", [population[2]], used=True)
        original, _ = image('brin_wire_n.brin', 'before-corruption', 1)
        path = Path(server['dir'])/'info/brin_wire_n.brin'
        damaged = bytearray(original)
        damaged[-1] ^= 1
        path.write_bytes(damaged)
        try:
            select('n>=376', matching(population, lambda n: n >= 376), used=False)
        finally:
            path.write_bytes(original)
        path.unlink()
        try:
            select('n>=376', matching(population, lambda n: n >= 376), used=False)
        finally:
            path.write_bytes(original)
        select('n>=376', matching(population, lambda n: n >= 376), used=True)
        other = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(other, 'alice', 'info')
        query('BEGIN ISOLATION LEVEL REPEATABLE READ', ready=b'T', tag='BEGIN')
        select('n>=376', matching(population, lambda n: n >= 376), ready=b'T', used=True)
        query('UPDATE brin_wire SET n=-999 WHERE id=768', peer=True, tag='UPDATE 1')
        query('UPDATE brin_wire SET n=9999 WHERE id=1', peer=True, tag='UPDATE 1')
        query('UPDATE brin_wire SET body=NULL WHERE id=2', peer=True, tag='UPDATE 1')
        image('brin_wire_n.brin', 'foreign-committed-width-retained', 1)
        select('n>=376', matching(population, lambda n: n >= 376), ready=b'T', used=False)
        select('id>=1', population, ready=b'T')
        query('COMMIT', tag='COMMIT')
        latest = [r.copy() for r in population]
        latest[767][1], latest[0][1], latest[1][2] = '-999', '9999', None
        select('n>=376', matching(latest, lambda n: n >= 376), used=True)
        query('BEGIN', ready=b'T', tag='BEGIN')
        query('UPDATE brin_wire SET n=-4000 WHERE id=2', ready=b'T', tag='UPDATE 1')
        temporary = [r.copy() for r in latest]
        temporary[1][1] = '-4000'
        select('n<=-3999', matching(temporary, lambda n: n <= -3999), ready=b'T', used=False)
        query('ROLLBACK', tag='ROLLBACK')
        image('brin_wire_n.brin', 'rollback-width-retained', 1)
        select('n>=376', matching(latest, lambda n: n >= 376), used=True)
        query('BEGIN ISOLATION LEVEL SERIALIZABLE', ready=b'T', tag='BEGIN')
        select('n>=376', matching(latest, lambda n: n >= 376), ready=b'T', used=False)
        select('n>99999', [], ready=b'T', used=False)
        query('COMMIT', tag='COMMIT')
        query('CREATE TABLE brin_default(id integer,n integer)', tag='CREATE TABLE')
        query('INSERT INTO brin_default VALUES(1,2),(2,10),(3,NULL)', tag='INSERT 0 3')
        query('CREATE INDEX brin_default_idx ON brin_default USING brin(n)', tag='CREATE INDEX')
        image('brin_default_n.brin', 'SQL-default-128', 128)
        select('n>=8', [['2', '10']], fields='id,n', table='brin_default', oids=(23, 23))
        for index, (value, width) in enumerate([('1.5', 2), ('2.5', 2), ('1e1', 10), ("'010'", 8), ('010', 10), ("'0x10'", 16), ("' 2 '", 2), ('131072.5', 131072)]):
            query('CREATE INDEX brin_value_'+str(index)+' ON brin_default USING brin(n) WITH(pages_per_range='+value+')', tag='CREATE INDEX')
            image('brin_default_n.brin', 'PG-reloption-'+value, width)
        for index, value in enumerate(['0', '-1', '131073', "'bad'", "'1.2junk'", '0.5', '131072.6', "'0x0'", "'08'", "'1e999'", "'NaN'", "'1kB'"]):
            query('CREATE INDEX brin_bad_'+str(index)+' ON brin_default USING brin(n) WITH(pages_per_range='+value+')', state='22023')
        query('CREATE INDEX brin_unknown ON brin_default USING brin(n) WITH(unknown_setting=1)', state='22023')
        query('CREATE INDEX brin_auto ON brin_default USING brin(n) WITH(autosummarize=true)', state='0A000', shared=False)
        query('CREATE TABLE brin_arrays(id integer PRIMARY KEY,a integer[],pad text)', tag='CREATE TABLE')
        arrays = [[str(i), None if i == 256 else '[0:1]={1,2}' if i % 7 == 0 else '{1,NULL}', 'array 界\n'+str(i)+'p'*256] for i in range(1, 257)]
        query('INSERT INTO brin_arrays VALUES'+','.join('('+','.join(literal(x) for x in row)+')' for row in arrays), tag='INSERT 0 256')
        query('CREATE INDEX brin_arrays_idx ON brin_arrays USING brin(a) WITH(pages_per_range=1)', tag='CREATE INDEX', shared=False)
        image('brin_arrays_a.brin', 'array-container-NULL', 1)
        query('BEGIN ISOLATION LEVEL READ COMMITTED', ready=b'T', tag='BEGIN')
        select('a IS NULL', [arrays[-1]], ready=b'T', used=True, fields='id,a,pad', table='brin_arrays', oids=(23, 1007, 25))
        select('a IS NOT NULL', arrays[:-1], ready=b'T', fields='id,a,pad', table='brin_arrays', oids=(23, 1007, 25))
        query('COMMIT', tag='COMMIT')
        select('id>=1', latest)
        trace['complete'] = True
    finally:
        if other is not None:
            other.close()
        runner.stop_ours(server)
        trace['server_exit'] = server['process'].returncode
        if os.environ.get('DBMS_TEST_EVIDENCE_DIR'):
            target = Path(os.environ['DBMS_TEST_EVIDENCE_DIR'])
            target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0
    print('BRIN_TYPED_SCAN_PROTOCOL commands='+str(len(trace['commands']))+' scan_assertions='+str(trace['scan_assertions'])+' full_values_NULL_snapshots_reloptions=1')


if __name__ == '__main__':
    main()
