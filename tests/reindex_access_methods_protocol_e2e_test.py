#!/usr/bin/env python3
"""Normal REINDEX TABLE replaces all defined AM files and preserves typed rows.

Raw responses and file images are retained before checking. Portable SQL is
marked shared_pg_sql; local Bloom/text-GiST, file damage and catalog OID
observations are not PostgreSQL numeric-OID or filesystem-equivalence claims.
"""
import hashlib
import importlib.util
import json
import os
import struct
from pathlib import Path


TABLE = 'reindex_am_wire'
QUOTED_TABLE = '"reindex_am_wire"'
FIELDS = ['id', 'n', 'txt', 'arr', 'p', 'pad']
OIDS = [23, 20, 25, 1007, 600, 25]
SIZES = [4, 8, -1, -1, 16, -1]
ROWS = [[str(i), None if i % 17 == 0 else str(i-96),
         None if i == 1 or i % 19 == 0 else '' if i == 2 or i % 31 == 0 else
         'NULL' if i == 3 else 'row 界\n|;,'+str(i),
         None if i % 19 == 0 else '{}' if i % 23 == 0 else
         '{'+str(i % 7)+',NULL,'+str(i % 11)+'}',
         None if i % 13 == 0 else '('+str(i)+','+str(i*2)+')',
         None if i % 11 == 0 else 'payload 界\n|;,'+str(i)+'x'*(6000 if i % 47 == 0 else 192)]
        for i in range(1, 193)]
INDEXES = [
    ('rxw_btree', 'btree', 'n', '_n.idx', True),
    ('rxw_hash', 'hash', 'txt', '_txt.hidx', True),
    ('rxw_bloom', 'bloom', 'txt', '_txt.bidx', False),
    ('rxw_gist', 'gist', 'txt', '_txt.gist', False),
    ('rxw_gin', 'gin', 'arr', '_arr.gin', True),
    ('rxw_spgist', 'spgist', 'p', '_p.spgist', True),
    ('rxw_brin', 'brin', 'n', '_n.brin', True),
]


def literal(value):
    return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"


def crc32c(raw):
    value = 0xffffffff
    for byte in raw:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def row_description(raw):
    count = struct.unpack_from('!H', raw)[0]
    fields, position = [], 2
    for _ in range(count):
        end = raw.index(b'\0', position)
        name = raw[position:end].decode('utf8')
        position = end+1
        origin, attribute, oid, size, modifier, fmt = struct.unpack_from('!IhIhih', raw, position)
        position += 18
        fields.append([name, origin, attribute, oid, size, modifier, fmt])
    assert position == len(raw)
    return fields


def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('reindex_am_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    database = Path(server['dir'])/'info'
    trace = {'complete': False, 'cohort_complete': False, 'commands': [], 'images': [],
             'definitions': [], 'file_actions': [], 'failures': [], 'harness_error': None}

    def check(good, label, **detail):
        if not good:
            trace['failures'].append({'label': label, **detail})

    def query(sql, *, ready=b'I', tag=None, shared=True):
        frames = client.simple_query(server['sock'], sql)
        record = {'sql': sql, 'connection': 'primary', 'shared_pg_sql': shared,
                  'raw': [{'kind': kind.decode(), 'body_hex': body.hex()} for kind, body in frames]}
        trace['commands'].append(record)  # Full raw is retained before all checks.
        values, state, message, _, actual_tag, oids = runner.decode_wire_result(frames, include_types=True)
        record.update(rows=values, state=state, message=message, tag=actual_tag, oids=oids)
        check(state is None, 'expected successful ordinary SQL', command=len(trace['commands'])-1, state=state)
        check(bool(frames) and frames[-1] == (b'Z', ready), 'complete ReadyForQuery state',
              command=len(trace['commands'])-1, expected=ready.decode())
        if tag is not None:
            check(actual_tag == tag, 'exact command tag', command=len(trace['commands'])-1,
                  actual=actual_tag, expected=tag)
        return values, oids, frames

    table_oid = None

    def population(wanted, phase, ready=b'I'):
        values, oids, frames = query('SELECT '+','.join(FIELDS)+' FROM '+QUOTED_TABLE+' ORDER BY id',
                                     ready=ready, tag='SELECT '+str(len(wanted)))
        check(values == wanted, 'every ordered value and SQL NULL', phase=phase,
              expected_rows=len(wanted), actual_rows=len(values))
        check(oids == OIDS, 'complete result type OIDs', phase=phase, actual=oids, expected=OIDS)
        descriptions = [body for kind, body in frames if kind == b'T']
        check(len(descriptions) == 1, 'one complete RowDescription', phase=phase)
        if len(descriptions) == 1:
            try:
                actual = row_description(descriptions[0])
                expected = [[name, table_oid, i+1, oid, size, -1, 0]
                            for i, (name, oid, size) in enumerate(zip(FIELDS, OIDS, SIZES))]
                trace['commands'][-1]['row_description'] = actual
                check(actual == expected, 'all seven RowDescription fields and real table origin',
                      phase=phase, actual=actual, expected=expected)
            except (ValueError, IndexError, struct.error, UnicodeError) as error:
                check(False, 'malformed result RowDescription', phase=phase, error=repr(error))
        return values

    def catalog(phase, ready=b'I'):
        result = {}
        for name in [TABLE]+[item[0] for item in INDEXES]:
            values, oids, _ = query("SELECT oid FROM pg_catalog.pg_class WHERE relname="+literal(name),
                                    ready=ready, tag='SELECT 1', shared=False)
            check(len(values) == 1 and len(values[0]) == 1 and values[0][0] is not None,
                  'genuine local catalog relation identity', phase=phase, name=name, actual=values)
            check(oids == [26], 'catalog OID field type', phase=phase, name=name, actual=oids)
            result[name] = values
        trace['definitions'].append({'phase': phase, 'catalog': result})
        return result

    def metadata():
        result = {}
        for suffix in ('.idxnames', '.secidx', '.hashidx', '.bloomidx', '.stc'):
            path = database/(TABLE+suffix)
            raw = path.read_bytes() if path.is_file() else None
            result[path.name] = None if raw is None else raw.hex()
            check(raw is not None and len(raw) > 0, 'real persistent definition file', name=path.name)
        return result

    paths = [database/(TABLE+'.idx')]+[database/(TABLE+item[3]) for item in INDEXES]

    def generation(path):
        if not path.is_file():
            return None
        stat = path.stat()
        return [stat.st_dev, stat.st_ino]

    def image(path, phase, width=1, hashes=7):
        raw = path.read_bytes() if path.is_file() else b''
        record = {'phase': phase, 'name': path.name, 'after_command_index': len(trace['commands'])-1,
                  'generation': generation(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                  'hex': raw.hex()}
        trace['images'].append(record)
        check(bool(raw), 'actual nonempty rebuilt main file', phase=phase, name=path.name)
        if path.suffix == '.idx':
            return  # Real Btree generation + full SQL rows; no invented footer CRC.
        try:
            check(len(raw) >= 4 and crc32c(raw[:-4]) == int.from_bytes(raw[-4:], 'little'),
                  'independent complete physical checksum', phase=phase, name=path.name)
            if path.suffix == '.hidx':
                magic, version = struct.unpack_from('<II', raw)
                check((magic, version) == (0x48494458, 2), 'current Hash format', phase=phase)
            elif path.suffix == '.bidx':
                magic, bits, actual_hashes, count = struct.unpack_from('<IIII', raw)
                record.update(bit_count=bits, hash_count=actual_hashes, key_count=count)
                check(magic == 0x324d4c42 and bits > 0 and actual_hashes == hashes and count > 0,
                      'current Bloom format and actual configuration', phase=phase,
                      actual_hashes=actual_hashes, expected_hashes=hashes)
            elif path.suffix in ('.gin', '.gist', '.spgist'):
                marker = {'.gin': b'\0DBMSG\0002', '.gist': b'\0DBMSS\0002', '.spgist': b'\0DBMSP\0002'}[path.suffix]
                version, count = struct.unpack_from('<IQ', raw, 8)
                record['entries'] = count
                check(raw[:8] == marker and version == 2 and count > 0,
                      'current nonempty AM frame', phase=phase, name=path.name)
                if raw[:8] != marker or version != 2 or count > max(0, len(raw)-24)//24:
                    raise ValueError('invalid AM header or bounded entry count')
                position = 20
                if path.suffix == '.spgist':
                    check(position+count*24 == len(raw)-4, 'complete SPGiST frame', phase=phase)
                else:
                    for _ in range(count):
                        if path.suffix == '.gin':
                            length = struct.unpack_from('<Q', raw, position)[0]
                            position += 8+length
                            n = struct.unpack_from('<Q', raw, position)[0]
                            position += 8+n*8
                        else:
                            _, low, high = struct.unpack_from('<QQQ', raw, position)
                            position += 24+low+high
                        if position > len(raw)-4:
                            raise ValueError('entry extends beyond checksum')
                    check(position == len(raw)-4, 'all AM entries consumed with no suffix', phase=phase, name=path.name)
            elif path.suffix == '.brin':
                magic, version, count = struct.unpack_from('<IIQ', raw)
                actual_width, extent, flags, relation, identity_length = struct.unpack_from('<IIIQQ', raw, 16)
                record.update(width=actual_width, extent=extent, relation=relation, ranges=count)
                check((magic, version) == (0x5342524e, 3) and actual_width == width and extent > 0
                      and flags == 0 and relation > 0 and identity_length > 0,
                      'current typed BRIN and retained/fallback width', phase=phase,
                      actual_width=actual_width, expected_width=width)
                position = 44+identity_length
                if (magic, version) != (0x5342524e, 3) or position > len(raw)-4 or count > max(0, len(raw)-48)//28:
                    raise ValueError('invalid BRIN header or bounded range count')
                previous = 0
                for _ in range(count):
                    first, last, flags = struct.unpack_from('<III', raw, position)
                    position += 12
                    check(previous < first <= last < extent and 0 < flags <= 7,
                          'complete ordered physical BRIN ranges', phase=phase)
                    previous = last
                    for _ in range(2):
                        length = struct.unpack_from('<Q', raw, position)[0]
                        position += 8+length
                check(position == len(raw)-4, 'all typed BRIN bounds consumed', phase=phase)
        except (ValueError, IndexError, OverflowError, struct.error) as error:
            check(False, 'invalid rebuilt AM framing', phase=phase, name=path.name, error=repr(error))

    def reindex(phase, *, ready=b'I', width=1, quoted=True):
        before = {str(path): generation(path) for path in paths}
        query('REINDEX TABLE '+(QUOTED_TABLE if quoted else TABLE), ready=ready, tag='REINDEX')
        for path in paths:
            after = generation(path)
            check(after is not None and after != before[str(path)],
                  'actual new physical generation, never heap-fallback-only success',
                  phase=phase, name=path.name, before=before[str(path)], after=after)
            image(path, phase, width=width)

    def snapshot_files():
        result = {path: path.read_bytes() if path.is_file() else None for path in paths}
        for path in list(paths):
            if path.suffix == '.idx':
                sidecar = Path(str(path)+'.tde')
                result[sidecar] = sidecar.read_bytes() if sidecar.is_file() else None
        return result

    def restore_files(saved, phase):
        for path, raw in saved.items():
            if raw is None:
                if path.exists():
                    path.unlink()
            else:
                path.write_bytes(raw)  # Preserve actual present-empty sidecars.
        trace['file_actions'].append({'phase': phase, 'action': 'restore_owned_fixture_for_next_distinct_case',
                                      'retry': False, 'files': [path.name for path in saved]})

    try:
        query('ALTER SYSTEM SET auto_analyze=off', shared=False)
        query('ALTER SYSTEM SET auto_vacuum=off', shared=False)
        query('SELECT pg_reload_conf()', shared=False)
        query('CREATE TABLE '+QUOTED_TABLE+'(id integer PRIMARY KEY,n bigint,txt text COLLATE "C",arr integer[],p point,pad text)', tag='CREATE TABLE')
        query('INSERT INTO '+QUOTED_TABLE+' VALUES'+','.join('('+','.join(literal(value) for value in row)+')' for row in ROWS), tag='INSERT 0 192')
        for name, method, key, _, shared in INDEXES:
            sql = 'CREATE INDEX '+name+' ON '+QUOTED_TABLE
            if method != 'btree':
                sql += ' USING '+method
            sql += '('+key+')'
            if method == 'brin':
                sql += ' WITH(pages_per_range=1)'
            query(sql, tag='CREATE INDEX', shared=shared)
        original_catalog = catalog('initial')
        found = original_catalog[TABLE]
        if len(found) == 1 and len(found[0]) == 1 and found[0][0] is not None:
            table_oid = int(found[0][0])
        original_metadata = metadata()
        population(ROWS, 'created')
        reindex('healthy-uppercase-unquoted', quoted=False)
        population(ROWS, 'healthy-uppercase-unquoted')
        reindex('healthy-uppercase-quoted')
        population(ROWS, 'healthy-uppercase-quoted')
        check(catalog('healthy') == original_catalog and metadata() == original_metadata,
              'local catalog OIDs and definition bytes survive healthy REINDEX')
        saved = snapshot_files()
        for action in ('corrupt', 'delete'):
            restore_files(saved, 'before-'+action)
            mutations = []
            trace['file_actions'].append({'phase': action, 'action': action,
                                          'files': mutations, 'shared_pg_sql': False})
            for path in paths:
                before = path.read_bytes() if path.is_file() else None
                mutations.append({'name': path.name, 'old_generation': generation(path),
                                  'before_hex': None if before is None else before.hex()})
                if action == 'corrupt':
                    path.write_bytes(b'independent corrupted index file\n')
                elif path.exists():
                    path.unlink()
                if path.suffix == '.idx' and action == 'delete':
                    sidecar = Path(str(path)+'.tde')
                    if sidecar.exists():
                        sidecar.unlink()
            reindex(action+'-all-named-datafiles', width=64)
            population(ROWS, action+'-all-named-datafiles')
            check(catalog(action) == original_catalog and metadata() == original_metadata,
                  'local catalog/definition identity survives damaged-file rebuild', phase=action)
            restore_files(saved, 'after-'+action)
        query('BEGIN', ready=b'T', tag='BEGIN')
        query('DELETE FROM '+QUOTED_TABLE+' WHERE id=2', ready=b'T', tag='DELETE 1')
        query('UPDATE '+QUOTED_TABLE+' SET txt=NULL WHERE id=31', ready=b'T', tag='UPDATE 1')
        query('UPDATE '+QUOTED_TABLE+" SET txt='' WHERE id=1", ready=b'T', tag='UPDATE 1')
        temporary = [row.copy() for row in ROWS if row[0] != '2']
        for row in temporary:
            if row[0] == '31':
                row[2] = None
            if row[0] == '1':
                row[2] = ''
        reindex('actual-empty-and-NULL-transaction', ready=b'T')
        population(temporary, 'before-real-rollback', ready=b'T')
        query('ROLLBACK', tag='ROLLBACK')
        population(ROWS, 'after-real-rollback')
        check(catalog('after-rollback') == original_catalog and metadata() == original_metadata,
              'real rollback retains every local OID and declaration')
        trace['cohort_complete'] = True
    except BaseException as error:
        trace['harness_error'] = {'type': type(error).__name__, 'message': str(error)}
    finally:
        try:
            runner.stop_ours(server)
        except BaseException as error:
            trace['stop_error'] = {'type': type(error).__name__, 'message': str(error)}
        trace['server_exit'] = server['process'].returncode
        trace['complete'] = trace['cohort_complete'] and not trace['failures'] and trace['harness_error'] is None and trace['server_exit'] == 0 and 'stop_error' not in trace
        if os.environ.get('DBMS_TEST_EVIDENCE_DIR'):
            target = Path(os.environ['DBMS_TEST_EVIDENCE_DIR'])
            target.mkdir(parents=True, exist_ok=True)
            (target/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'], {'failures': trace['failures'], 'harness_error': trace['harness_error'],
                               'server_exit': trace['server_exit'], 'stop_error': trace.get('stop_error')}
    print('REINDEX_AM_PROTOCOL commands='+str(len(trace['commands']))+' images='+str(len(trace['images']))+
          ' rows=192 all_values_NULL_7descriptor_fields_tags_Ready_generation_CRC=1')


if __name__ == '__main__':
    main()
