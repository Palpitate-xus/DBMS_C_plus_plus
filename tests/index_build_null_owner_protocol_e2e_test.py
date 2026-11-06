#!/usr/bin/env python3
"""CREATE/REINDEX and write maintenance retain full typed rows and physical NULLs."""
import importlib.util
import json
import os
import struct
from pathlib import Path


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('index_owner_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    trace = {'complete': False, 'commands': [], 'brin_images': {}}
    population = [[str(i), None if i == 1 or i % 17 == 0 else '' if i == 2 else
                   'NULL' if i == 3 else 'tok'+str(i)+' 界\n|;,',
                   None if i % 11 == 0 else str(i*10), '('+str(i)+','+str(i*2)+')',
                   None if i % 7 == 0 else 'payload 界\n'+str(i)] for i in range(1, 65)]

    def literal(value):
        return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"

    def query(sql, *, ready=b'I', tag=None, shared=True):
        messages = client.simple_query(server['sock'], sql)
        rows, state, message, _, actual_tag, oids = runner.decode_wire_result(messages, include_types=True)
        trace['commands'].append({'sql': sql, 'rows': rows, 'state': state, 'tag': actual_tag,
                                  'oids': oids, 'shared_pg_sql': shared,
                                  'raw': [{'kind': k.decode(), 'body_hex': v.hex()} for k, v in messages]})
        assert state is None and messages[-1] == (b'Z', ready), (sql, state, message)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        return rows, oids

    def select(wanted, ready=b'I'):
        rows, oids = query('SELECT id,body,score,location,pad FROM idx_owner', ready=ready,
                          tag='SELECT '+str(len(wanted)))
        assert sorted(rows, key=lambda row: int(row[0])) == wanted, (rows, wanted)
        assert oids == [23, 25, 23, 600, 25], oids

    def brin_image(name):
        raw = (Path(server['dir'])/'info'/name).read_bytes()
        trace['brin_images'][name] = raw.hex()
        assert len(raw) >= 20 and struct.unpack_from('<II', raw) == (0x5342524e, 3)
        checksum = 0xffffffff
        for byte in raw[:-4]:
            checksum ^= byte
            for _ in range(8):
                checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
        assert checksum ^ 0xffffffff == int.from_bytes(raw[-4:], 'little')
        # Independent V3 framing decoder; NULL flags are distinct from
        # non-NULL empty-string extrema. Do not infer NULL from text bytes.
        count = struct.unpack_from('<Q', raw, 8)[0]
        width, pages, flags = struct.unpack_from('<III', raw, 16)
        relation = struct.unpack_from('<Q', raw, 28)[0]
        identity_length = struct.unpack_from('<Q', raw, 36)[0]
        offset = 44+identity_length
        assert width and pages and not flags and relation and identity_length
        ranges = []
        for _ in range(count):
            first, last, state = struct.unpack_from('<III', raw, offset)
            offset += 12
            bounds = []
            for _ in range(2):
                length = struct.unpack_from('<Q', raw, offset)[0]
                offset += 8
                bounds.append(raw[offset:offset+length])
                offset += length
            assert 0 < first <= last < pages and 0 < state <= 7
            ranges.append((first, last, state, *bounds))
        assert offset == len(raw)-4
        return raw, ranges

    try:
        query('ALTER SYSTEM SET auto_analyze=off', shared=False)
        query('CREATE TABLE idx_owner(id integer PRIMARY KEY,body text,score integer,location point,pad text)', tag='CREATE TABLE')
        query('INSERT INTO idx_owner VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in population), tag='INSERT 0 64')
        select(population)
        for name, method, column, shared in [
                ('hash', 'hash', 'body', True), ('bloom', 'bloom', 'body', False),
                ('gin', 'gin', 'body', False), ('gist', 'gist', 'score', False),
                ('spgist', 'spgist', 'location', True), ('brin', 'brin', 'score', True),
                ('fulltext', 'fulltext', 'body', False)]:
            query('CREATE INDEX idx_owner_'+name+' ON idx_owner USING '+method+'('+column+')',
                  tag='CREATE INDEX', shared=shared)
        query('CREATE INDEX idx_owner_body ON idx_owner(body)', tag='CREATE INDEX')
        query('CREATE INDEX idx_owner_pair ON idx_owner(score,body)', tag='CREATE INDEX')
        query('REINDEX TABLE idx_owner', tag='REINDEX')
        select(population)
        query('BEGIN', ready=b'T', tag='BEGIN')
        query("UPDATE idx_owner SET body='temporary 界' WHERE id=1", ready=b'T', tag='UPDATE 1')
        query('UPDATE idx_owner SET score=NULL WHERE id=19', ready=b'T', tag='UPDATE 1')
        temporary = [row.copy() for row in population]
        temporary[0][1] = 'temporary 界'
        temporary[18][2] = None
        query('REINDEX TABLE idx_owner', ready=b'T', tag='REINDEX')
        select(temporary, b'T')
        query('ROLLBACK', tag='ROLLBACK')
        select(population)
        query('BEGIN', ready=b'T', tag='BEGIN')
        query("UPDATE idx_owner SET body='committed 界' WHERE id=1", ready=b'T', tag='UPDATE 1')
        query('UPDATE idx_owner SET body=NULL WHERE id=2', ready=b'T', tag='UPDATE 1')
        query('DELETE FROM idx_owner WHERE id=3', ready=b'T', tag='DELETE 1')
        added = ['65', 'tok65 界\n|;,', '650', '(65,130)', None]
        query('INSERT INTO idx_owner VALUES('+','.join(literal(v) for v in added)+')', ready=b'T', tag='INSERT 0 1')
        query('COMMIT', tag='COMMIT')
        latest = [row.copy() for row in population if row[0] != '3']+[added]
        latest[0][1] = 'committed 界'
        latest[1][1] = None
        select(latest)
        query('REINDEX TABLE idx_owner', tag='REINDEX')
        select(latest)
        query('CREATE TABLE idx_null(id integer PRIMARY KEY,n integer)', tag='CREATE TABLE')
        query('INSERT INTO idx_null VALUES(1,NULL),(2,NULL),(3,NULL)', tag='INSERT 0 3')
        query('CREATE INDEX idx_null_brin ON idx_null USING brin(n)', tag='CREATE INDEX')
        rows, oids = query('SELECT id,n FROM idx_null', tag='SELECT 3')
        assert sorted(rows) == [['1', None], ['2', None], ['3', None]] and oids == [23, 23]
        raw, ranges = brin_image('idx_null_n.brin')
        assert len(ranges) == 1 and ranges[0][2:] == (2, b'', b''), 'all-NULL range has only the NULL flag'
        query('CREATE TABLE idx_empty(id integer PRIMARY KEY,s text)', tag='CREATE TABLE')
        query("INSERT INTO idx_empty VALUES(1,NULL),(2,'')", tag='INSERT 0 2')
        query('CREATE INDEX idx_empty_brin ON idx_empty USING brin(s)', tag='CREATE INDEX')
        rows, oids = query('SELECT id,s FROM idx_empty', tag='SELECT 2')
        assert sorted(rows) == [['1', None], ['2', '']] and oids == [23, 25]
        raw, ranges = brin_image('idx_empty_s.brin')
        assert len(ranges) == 1 and ranges[0][2:] == (3, b'', b''), 'NULL plus genuine empty text has both flags'
        select(latest)
        trace['complete'] = True
    finally:
        runner.stop_ours(server)
        trace['server_exit'] = server['process'].returncode
        evidence = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
        if evidence:
            target = Path(evidence)
            target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'] and trace['server_exit'] == 0
    print('INDEX_BUILD_NULL_OWNER_PROTOCOL commands='+str(len(trace['commands']))+' full_values_NULL_point_tags_OIDs_CRC=1')


if __name__ == '__main__':
    main()
