#!/usr/bin/env python3
"""BRIN range configuration survives physical rewrites and transaction undo."""
import hashlib
import importlib.util
import json
import os
import struct
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('maintenance_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    trace = {'complete': False, 'commands': [], 'images': [], 'plans': [], 'failures': []}
    rows = [[str(i), None if i % 17 == 0 else str(i-384),
             None if i % 19 == 0 else '' if i == 2 else 'NULL' if i == 3 else 'row 界\n|;,'+str(i),
             None if i % 11 == 0 else 'payload 界\n|;,'+str(i)+'x'*192] for i in range(1, 769)]

    def literal(value):
        return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"

    def query(sql, ready=b'I', shared=True, tag=None):
        frames = client.simple_query(server['sock'], sql)
        values, state, message, _, actual_tag, oids = runner.decode_wire_result(frames, include_types=True)
        trace['commands'].append({'sql': sql, 'connection': 'primary', 'rows': values, 'state': state,
                                  'tag': actual_tag, 'oids': oids, 'shared_pg_sql': shared,
                                  'raw': [{'kind': k.decode(), 'body_hex': b.hex()} for k, b in frames]})
        assert state is None and frames[-1] == (b'Z', ready), (sql, state, message)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        return values, oids

    def population(wanted, fields='id,n,body,pad', table='brin_maint', ready=b'I', oids=(23, 20, 25, 25)):
        values, types = query('SELECT '+fields+' FROM '+table+' ORDER BY id', ready, tag='SELECT '+str(len(wanted)))
        assert values == wanted and types == list(oids), (table, fields, values, wanted, types)

    def image(table, phase, wanted_width):
        raw = (Path(server['dir'])/'info'/(table+'_n.brin')).read_bytes()
        assert struct.unpack_from('<II', raw) == (0x5342524e, 3)
        crc = 0xffffffff
        for byte in raw[:-4]:
            crc ^= byte
            for _ in range(8):
                crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
        assert crc ^ 0xffffffff == int.from_bytes(raw[-4:], 'little')
        count = struct.unpack_from('<Q', raw, 8)[0]
        width, extent, flags = struct.unpack_from('<III', raw, 16)
        length = struct.unpack_from('<Q', raw, 36)[0]
        assert width and extent and not flags and length
        position = 44+length
        previous = 0
        for _ in range(count):
            first, last, state = struct.unpack_from('<III', raw, position)
            position += 12
            assert previous < first <= last < extent and state in range(1, 8)
            previous = last
            for _ in range(2):
                size = struct.unpack_from('<Q', raw, position)[0]
                position += 8+size
                assert position <= len(raw)-4
        assert position == len(raw)-4
        trace['images'].append({'after_command_index': len(trace['commands'])-1, 'table': table, 'phase': phase, 'expected_width': wanted_width,
                                'width': width, 'extent': extent, 'ranges': count, 'bytes': len(raw),
                                'sha256': hashlib.sha256(raw).hexdigest(), 'hex': raw.hex()})
        if width != wanted_width:
            trace['failures'].append({'phase': phase, 'expected_width': wanted_width, 'actual_width': width})

    def nodes(value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from nodes(child)
        elif isinstance(value, list):
            for child in value:
                yield from nodes(child)

    def narrow(wanted, fields='id,n,body,pad', oids=(23, 20, 25, 25)):
        sql = 'SELECT '+fields+' FROM brin_maint WHERE n>=376'
        selected = [r for r in wanted if r[1] is not None and int(r[1]) >= 376]
        values, types = query(sql, tag='SELECT '+str(len(selected)))
        assert sorted(values, key=lambda r: int(r[0])) == selected and types == list(oids)
        values, _ = query('EXPLAIN (ANALYZE,FORMAT JSON) '+sql, shared=False)
        plan = json.loads('\n'.join(r[0] for r in values))
        used = any(n.get('nodeType') == 'BrinScan' and n.get('usedSummary') for n in nodes(plan))
        trace['plans'].append({'sql': sql, 'plan': plan, 'used': used})
        if not used:
            trace['failures'].append({'phase': 'narrow-after-maintenance', 'sql': sql, 'expected_summary': True})

    try:
        query('ALTER SYSTEM SET auto_analyze=off', shared=False)
        query('ALTER SYSTEM SET auto_vacuum=off', shared=False)
        query('SELECT pg_reload_conf()', shared=False)
        query('CREATE TABLE brin_maint(id integer PRIMARY KEY,n bigint,body text COLLATE "C",pad text)', tag='CREATE TABLE')
        query('INSERT INTO brin_maint VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in rows), tag='INSERT 0 768')
        query('CREATE INDEX brin_maint_idx ON brin_maint USING brin(n) WITH(pages_per_range=1)', tag='CREATE INDEX')
        image('brin_maint', 'created-width1', 1)
        population(rows)
        query('DELETE FROM brin_maint WHERE id=384', tag='DELETE 1')
        rows = [r for r in rows if r[0] != '384']
        query('VACUUM FULL brin_maint', tag='VACUUM')
        image('brin_maint', 'vacuum-full', 1)
        population(rows)
        narrow(rows)
        query('ALTER TABLE brin_maint ADD COLUMN extra integer DEFAULT 7', tag='ALTER TABLE')
        image('brin_maint', 'add-column', 1)
        population([r+['7'] for r in rows], fields='id,n,body,pad,extra', oids=(23, 20, 25, 25, 23))
        narrow([r+['7'] for r in rows], fields='id,n,body,pad,extra', oids=(23, 20, 25, 25, 23))
        query('ALTER TABLE brin_maint DROP COLUMN extra', tag='ALTER TABLE')
        image('brin_maint', 'drop-column', 1)
        population(rows)
        narrow(rows)
        query('BEGIN', ready=b'T', tag='BEGIN')
        query('TRUNCATE brin_maint', ready=b'T', tag='TRUNCATE TABLE')
        image('brin_maint', 'truncate-before-rollback', 1)
        population([], ready=b'T')
        query('ROLLBACK', tag='ROLLBACK')
        image('brin_maint', 'truncate-rollback', 1)
        population(rows)
        query('TRUNCATE brin_maint', tag='TRUNCATE TABLE')
        image('brin_maint', 'truncate-committed', 1)
        population([])
        query('INSERT INTO brin_maint VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in rows), tag='INSERT 0 767')
        image('brin_maint', 'insert-after-truncate', 1)
        population(rows)
        narrow(rows)
        query('CREATE TABLE brin_default(id integer,n integer)', tag='CREATE TABLE')
        query('INSERT INTO brin_default VALUES(1,2),(2,10),(3,NULL)', tag='INSERT 0 3')
        query('CREATE INDEX brin_default_idx ON brin_default USING brin(n)', tag='CREATE INDEX')
        image('brin_default', 'SQL-default128-created', 128)
        query('VACUUM FULL brin_default', tag='VACUUM')
        image('brin_default', 'SQL-default128-vacuum', 128)
        population([['1', '2'], ['2', '10'], ['3', None]], fields='id,n', table='brin_default', oids=(23, 23))
        query('TRUNCATE brin_default', tag='TRUNCATE TABLE')
        image('brin_default', 'SQL-default128-truncate', 128)
        query('CREATE TABLE brin_type(id integer,n integer)', tag='CREATE TABLE')
        query('INSERT INTO brin_type VALUES(1,2),(2,10),(3,NULL)', tag='INSERT 0 3')
        query('CREATE INDEX brin_type_idx ON brin_type USING brin(n) WITH(pages_per_range=3)', tag='CREATE INDEX')
        query('ALTER TABLE brin_type ALTER COLUMN n TYPE bigint', tag='ALTER TABLE')
        image('brin_type', 'type-rewrite-width3', 3)
        population([['1', '2'], ['2', '10'], ['3', None]], fields='id,n', table='brin_type', oids=(23, 20))
        trace['complete'] = not trace['failures']
    finally:
        runner.stop_ours(server)
        trace['server_exit'] = server['process'].returncode
        if os.environ.get('DBMS_TEST_EVIDENCE_DIR'):
            target = Path(os.environ['DBMS_TEST_EVIDENCE_DIR'])
            target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0, trace['failures']
    print('BRIN_MAINTENANCE commands='+str(len(trace['commands']))+' images='+str(len(trace['images']))+' full_values_NULL_types_widths=1')


if __name__ == '__main__':
    main()
