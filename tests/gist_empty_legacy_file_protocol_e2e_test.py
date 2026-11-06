#!/usr/bin/env python3
"""A zero-byte legacy file is not proof of an empty physical GiST generation.

Normal owned SimpleQuery, fixed 15s deadline. Corruption is a local fixture,
never a PostgreSQL format oracle. Every raw response precedes its assertions.
Valid checksummed V3 count-zero and valid nonempty legacy compatibility are
separate strict positives. Preparation supports --write-plan with zero SQL.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import struct

TABLE = 'gist_legacy_empty_wire'
NULL_TABLE = 'gist_legacy_allnull_wire'
FIELDS, OIDS, SIZES, MODS = ['id', 'k', 'pad'], [23, 25, 25], [4, -1, -1], [-1, -1, -1]
MODEL = [['1', '', ''], ['2', None, None], ['3', 'a', 'payload 界'],
         ['4', 'ab', None], ['5', 'b', 'NULL'], ['6', '\x7f', 'payload DEL'],
         ['7', '界', 'UTF8 界\n|;,'], ['8', 'NULL', '']]
NULL_MODEL = [['1', None, 'NULL'], ['2', None, None], ['3', None, '']]


def literal(value):
    return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"


def crc32c(raw):
    state = 0xffffffff
    for byte in raw:
        state ^= byte
        for _ in range(8):
            state = (state >> 1) ^ (0x82f63b78 if state & 1 else 0)
    return state ^ 0xffffffff


def decode_gist(raw):
    if len(raw) < 24 or raw[:8] not in (b'\0DBMSS\0002', b'\0DBMSS\0003'):
        raise ValueError('incomplete recognized binary generation')
    version, count = struct.unpack_from('<IQ', raw, 8)
    end = len(raw)-4
    if version != raw[7]-ord('0') or count > (end-20)//24:
        raise ValueError('magic/version/count mismatch')
    if crc32c(raw[:end]) != int.from_bytes(raw[end:], 'little'):
        raise ValueError('independent complete CRC32C')
    offset, entries, seen = 20, [], set()
    for _ in range(count):
        if end-offset < 24:
            raise ValueError('truncated fixed fields')
        rid, low, high = struct.unpack_from('<QQQ', raw, offset)
        offset += 24
        if not 0 < rid < (1 << 63) or rid in seen or low > end-offset or high > end-offset-low:
            raise ValueError('invalid RID/span/duplicate')
        seen.add(rid)
        a, b = raw[offset:offset+low], raw[offset+low:offset+low+high]
        offset += low+high
        entries.append((rid, a, b))
    if offset != end:
        raise ValueError('binary tail')
    return version, entries


def heap_key_witness(heap, rid):
    """Actual three-field heap schema: fixed int4 then two variable descriptors.

    RID low16 slot is zero based; never infer SQL visibility from xmin/xmax.
    The target key is non-TOAST; pad transport is verified independently by SQL.
    """
    if len(heap) < 8192 or struct.unpack_from('<I', heap)[0] != 0x44415441 or struct.unpack_from('<I', heap, 16)[0] != 2:
        raise ValueError('actual format2 heap needed')
    page, slot = rid >> 32, rid & 0xffff
    if page == 0 or page*8192+8192 > len(heap):
        raise ValueError('RID page outside actual heap')
    raw = heap[page*8192:(page+1)*8192]
    lower, upper, special, pv = struct.unpack_from('<HHHH', raw, 12)
    flags = struct.unpack_from('<H', raw, 10)[0]
    if pv & 255 not in (4, 5) or not 24 <= lower <= upper <= special <= 8192 or lower % 4:
        raise ValueError('page bounds')
    if pv & 255 == 5 and (not flags & 8 or struct.unpack_from('<I', raw, 20)[0] != page):
        raise ValueError('bound page identity')
    at = 24+slot*4
    if at+4 > lower:
        raise ValueError('RID slot outside real line pointers')
    off, packed = struct.unpack_from('<HH', raw, at)
    off &= 0x7fff
    length = packed & 0x3fff
    if packed >> 14 != 1 or length < 24 or off < upper or off+length > special:
        raise ValueError('normal tuple extent')
    tup = raw[off:off+length]
    natts, mask, hoff = struct.unpack_from('<HHB', tup, 18)
    if natts & 0x7ff != 3 or not 24 <= hoff < len(tup) or hoff+12 > len(tup):
        raise ValueError('actual three-field tuple')
    ident = struct.unpack_from('<i', tup, hoff)[0]
    if mask & 1 and not tup[23] & 2:
        return ident, None
    start, length = struct.unpack_from('<HH', tup, hoff+4)
    if start+length > len(tup)-hoff:
        raise ValueError('actual key descriptor span')
    return ident, tup[hoff+start:hoff+start+length]


def description(raw):
    if len(raw) < 2:
        raise ValueError('short RowDescription')
    count = struct.unpack_from('!H', raw)[0]
    out, at = [], 2
    for _ in range(count):
        end = raw.index(b'\0', at)
        name = raw[at:end].decode('utf8')
        at = end+1
        if len(raw)-at < 18:
            raise ValueError('short field tuple')
        out.append([name, *struct.unpack_from('!IhIhih', raw, at)])
        at += 18
    if at != len(raw):
        raise ValueError('RowDescription tail')
    return out


def build_actions():
    actions = []

    def q(name, sql, rows=None, tag=None, table=TABLE, shared=True, used=None, require_gist=False):
        actions.append(dict(kind='query', name=name, sql=sql, expected_rows=rows,
                            expected_tag=tag, expected_ready='I', table=table,
                            full=rows is not None, shared_pg_sql=shared))
        if used is not None:
            actions.append(dict(kind='explain', name=name+'-explain',
                                sql='EXPLAIN (ANALYZE,FORMAT JSON) '+sql,
                                expected_used_index=used, require_gist_node=require_gist, expected_tag='EXPLAIN', shared_pg_sql=False))

    def selected(phase, used, local):
        for suffix, predicate, ids in [('equal-a', "k='a'", {'3'}),
                                        ('range-a-c', "k>='a' AND k<'c'", {'3', '4', '5'}),
                                        ('prefix-a', "k LIKE 'a%'", {'3', '4'})]:
            wanted = [row[:] for row in MODEL if row[0] in ids]
            q(phase+'-'+suffix, 'SELECT id,k,pad FROM '+TABLE+' WHERE '+predicate+' ORDER BY id',
              wanted, 'SELECT '+str(len(wanted)), shared=not local, used=used, require_gist=True)

    q('auto-analyze-off', 'ALTER SYSTEM SET auto_analyze=off', tag='ALTER SYSTEM', shared=False)
    q('auto-vacuum-off', 'ALTER SYSTEM SET auto_vacuum=off', tag='ALTER SYSTEM', shared=False)
    q('reload', 'SELECT pg_reload_conf()', tag='SELECT 1', shared=False)
    q('cheap-index', 'SET random_page_cost=0.000001', tag='SET', shared=False)
    for table, model in [(TABLE, MODEL), (NULL_TABLE, NULL_MODEL)]:
        q(table+'-create', 'CREATE TABLE '+table+'(id integer PRIMARY KEY,k text COLLATE "C",pad text)', tag='CREATE TABLE', table=table)
        q(table+'-seed', 'INSERT INTO '+table+' VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in model), tag='INSERT 0 '+str(len(model)), table=table)
        q(table+'-index', 'CREATE INDEX '+table+'_g ON '+table+' USING gist(k)', tag='CREATE INDEX', table=table, shared=False)
        actions.append(dict(kind='anchor', name=table+'-actual-table-oid', table=table,
                            sql="SELECT oid FROM pg_catalog.pg_class WHERE relname='"+table+"'", expected_tag='SELECT 1', shared_pg_sql=False))
    q('checkpoint-images', 'CHECKPOINT', tag='CHECKPOINT', shared=False)
    actions.append(dict(kind='capture-original', name='healthy-V3-actual-RIDs', table=TABLE, shared_pg_sql=False))
    q('healthy-all8', 'SELECT id,k,pad FROM '+TABLE+' ORDER BY id', [r[:] for r in MODEL], 'SELECT 8')
    selected('healthy-V3', True, False)
    q('healthy-empty', 'SELECT id,k,pad FROM '+TABLE+" WHERE k='' ORDER BY id", [MODEL[0][:]], 'SELECT 1', used=True, require_gist=True)
    q('healthy-NULL', 'SELECT id,k,pad FROM '+TABLE+' WHERE k IS NULL ORDER BY id', [MODEL[1][:]], 'SELECT 1', used=False)
    for variant in ['zero-byte', 'partial-header', 'bad-CRC']:
        actions.append(dict(kind='damage', name=variant, variant=variant, shared_pg_sql=False))
        selected(variant, False, True)
        q(variant+'-all8', 'SELECT id,k,pad FROM '+TABLE+' ORDER BY id', [r[:] for r in MODEL], 'SELECT 8', shared=False)
        actions.append(dict(kind='restore-original', name=variant+'-restore-exact-V3', shared_pg_sql=False))
        selected(variant+'-restored', True, True)
    actions.append(dict(kind='legacy-nonempty', name='legal-nonempty-legacy-from-actual-RIDs', shared_pg_sql=False))
    selected('legal-legacy', True, True)
    q('legacy-omitted-empty-fallback', 'SELECT id,k,pad FROM '+TABLE+" WHERE k='' ORDER BY id", [MODEL[0][:]], 'SELECT 1', shared=False, used=False)
    actions.append(dict(kind='restore-original', name='legacy-restore-exact-V3', shared_pg_sql=False))
    selected('final-restored-V3', True, True)
    actions.append(dict(kind='check-zero-V3', name='all-NULL-valid-V3-zero-count', table=NULL_TABLE, shared_pg_sql=False))
    q('all-NULL-equal-a-index-empty', 'SELECT id,k,pad FROM '+NULL_TABLE+" WHERE k='a' ORDER BY id", [], 'SELECT 0', table=NULL_TABLE, used=True, require_gist=True)
    q('all-NULL-predicate-real-rows', 'SELECT id,k,pad FROM '+NULL_TABLE+' WHERE k IS NULL ORDER BY id', [r[:] for r in NULL_MODEL], 'SELECT 3', table=NULL_TABLE, used=False)
    q('final-all8', 'SELECT id,k,pad FROM '+TABLE+' ORDER BY id', [r[:] for r in MODEL], 'SELECT 8')
    actions.append(dict(kind='assert-restored', name='final-index-image-unchanged', shared_pg_sql=False))
    return actions


def find_repo():
    for parent in Path(__file__).resolve().parents:
        if (parent/'tests/compat/pg_diff_runner.py').is_file():
            return parent
    raise RuntimeError('real repository helpers not found')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--write-plan', type=Path)
    options = parser.parse_args()
    actions = build_actions()
    if options.write_plan:
        if options.write_plan.exists():
            raise ValueError('exclusive frozen plan required')
        options.write_plan.write_text(json.dumps(dict(schema='gist-empty-legacy-file-plan-v1', state='FROZEN_NOT_RUN', wire_deadline_seconds=15,
            fields=FIELDS, oids=OIDS, sizes=SIZES, type_modifiers=MODS, original_rows=MODEL, all_NULL_rows=NULL_MODEL,
            descriptors='All three actual origin attributes, seven fields, actual pg_class OID anchors independently read per table; no fabricated OIDs.',
            corruptions='Distinct local cases, each same original generation restored once; not retries. Valid V3 zero count remains an index-positive.',
            actions=actions), ensure_ascii=False, indent=2)+'\n')
        return
    repo = find_repo()
    spec = importlib.util.spec_from_file_location('gist_empty_legacy_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    if runner.wire_timeout() != 15:
        raise ValueError('frozen original wire deadline must remain15s')
    evidence = Path(os.environ.get('DBMS_TEST_EVIDENCE_DIR', Path.cwd()/'gist-empty-legacy-evidence'))
    evidence.mkdir(parents=True, exist_ok=True)
    binary = Path(runner.DBMS_MAIN).resolve()
    trace = dict(complete=False, cohort_complete=False, commands=[], images=[], actions=[], owners=[], failures=[], harness_error=None,
                 source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), binary=dict(path=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest()),
                 protocol_helper_sha256=hashlib.sha256((repo/'tests/postgres_protocol_test.py').read_bytes()).hexdigest())
    server, original, index = None, None, None
    table_oids = {}

    def check(ok, label, **details):
        if not ok:
            trace['failures'].append(dict(label=label, **details))

    def query(action):
        record = dict(name=action['name'], sql=action['sql'], shared_pg_sql=action['shared_pg_sql'], raw=[])
        trace['commands'].append(record)
        sock = server['sock']
        wire = bytearray()
        class Capture:
            def recv(self, size):
                data = sock.recv(size)
                wire.extend(data)
                return data
        request = client.typed(b'Q', action['sql'].encode()+b'\0')
        record['request_hex'] = request.hex()
        frames = []
        try:
            sock.sendall(request)
            while True:
                kind, body = client.read_message(Capture())
                frames.append((kind, body))
                record['raw'].append(dict(kind=kind.decode(), body_hex=body.hex()))
                if kind == b'Z':
                    break
        except BaseException:
            record['partial_response_wire_hex'] = wire.hex()
            raise
        record['response_wire_sha256'] = hashlib.sha256(wire).hexdigest()
        rows, state, message, _, tag, oids = runner.decode_wire_result(frames, include_types=True)
        record.update(rows=rows, state=state, message=message, tag=tag, oids=oids)
        check(state is None, 'successful SQL', name=record['name'], actual_state=state, message=message)
        check(bool(frames) and frames[-1] == (b'Z', b'I'), 'one terminal idle Ready', name=record['name'])
        check(sum(kind == b'Z' for kind, _ in frames) == 1, 'exact one Ready', name=record['name'])
        if action.get('expected_tag') is not None:
            wanted = action['expected_tag']
            check(tag == wanted and [b for k, b in frames if k == b'C'] == [(wanted+'\0').encode()], 'exact command tag/body', name=record['name'], expected=wanted, actual=tag)
        return record, frames

    def full(action, record, frames):
        check(record['rows'] == action['expected_rows'], 'all ordered cells/NULL/empty', name=action['name'], expected=action['expected_rows'], actual=record['rows'])
        check(record['oids'] == OIDS, 'all three actual type OIDs', name=action['name'], actual=record['oids'])
        expected = [[name, table_oids[action['table']], i+1, oid, size, mod, 0] for i, (name, oid, size, mod) in enumerate(zip(FIELDS, OIDS, SIZES, MODS))]
        headers = [body for kind, body in frames if kind == b'T']
        check(len(headers) == 1, 'one complete three-field RowDescription', name=action['name'], count=len(headers))
        if len(headers) == 1:
            actual = description(headers[0])
            record['row_description'] = actual
            check(actual == expected, 'all seven descriptor fields/actual relation origin', name=action['name'], expected=expected, actual=actual)
        decoded = []
        for kind, body in frames:
            if kind != b'D':
                continue
            if len(body) < 2 or struct.unpack_from('!H', body)[0] != 3:
                raise ValueError('complete three-field DataRow required')
            at, row = 2, []
            for _ in range(3):
                if len(body)-at < 4:
                    raise ValueError('short DataRow length')
                length = struct.unpack_from('!i', body, at)[0]
                at += 4
                if length < -1 or length > len(body)-at:
                    raise ValueError('invalid DataRow span')
                row.append(None if length == -1 else body[at:at+length].decode())
                if length >= 0:
                    at += length
            if at != len(body):
                raise ValueError('DataRow tail')
            decoded.append(row)
        check(decoded == action['expected_rows'], 'independent complete raw DataRows', name=action['name'])

    def explain(action, record):
        nodes = []
        def walk(value):
            if isinstance(value, dict):
                if value.get('nodeType') == 'GiSTScan':
                    nodes.append(value)
                for item in value.values():
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)
        if len(record['rows']) != 1 or len(record['rows'][0]) != 1:
            raise ValueError('one EXPLAIN JSON cell')
        walk(json.loads(record['rows'][0][0]))
        record['gist_nodes'] = nodes
        if action.get('require_gist_node'):
            check(bool(nodes), 'actual GiST attempt retained for probe', name=action['name'], nodes=nodes)
        if action['expected_used_index']:
            check(bool(nodes) and any(n.get('usedIndex') is True for n in nodes), 'genuine GiST usedIndex true', name=action['name'], nodes=nodes)
        else:
            check(all(n.get('usedIndex') is False for n in nodes), 'actual heap fallback/usedIndex false', name=action['name'], nodes=nodes)

    def image(phase, raw, table=TABLE):
        item = dict(phase=phase, table=table, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), hex=raw.hex())
        trace['images'].append(item)
        return item

    def prove_original(raw, table, expected):
        version, entries = decode_gist(raw)
        check(version == 3, 'real complete V3 producer', table=table, actual_version=version)
        heap = (Path(server['dir'])/'info'/(table+'.dt')).read_bytes()
        (evidence/(table+'-heap.bin')).write_bytes(heap)
        values = {}
        for rid, low, high in entries:
            ident, actual = heap_key_witness(heap, rid)
            check(ident not in values and actual is not None and low == high == actual, 'actual physical RID/key/NULL authority', table=table, rid=rid, id=ident)
            values[ident] = low
        wanted = {int(row[0]): row[1].encode() for row in expected if row[1] is not None}
        check(values == wanted, 'every actual nonNULL encoded key/empty included', table=table, expected_count=len(wanted), actual_count=len(values))
        return entries

    try:
        server = runner.start_ours(client)
        process = server['process']
        stat = Path('/proc', str(process.pid), 'stat').read_text()
        trace['owners'].append(dict(pid=process.pid, pgid=os.getpgid(process.pid), start_ticks=stat[stat.rfind(')')+2:].split()[19],
                                    cmdline=(Path('/proc', str(process.pid), 'cmdline')).read_bytes().hex(), data_dir=server['dir']))
        original_entries = []
        for action in actions:
            trace['actions'].append(dict(name=action['name'], kind=action['kind']))
            kind = action['kind']
            if kind in ('query', 'explain', 'anchor'):
                record, frames = query(action)
                if kind == 'anchor':
                    check(record['oids'] == [26] and len(record['rows']) == 1 and len(record['rows'][0]) == 1 and record['rows'][0][0] is not None, 'genuine pg_class relation identity', name=action['name'], rows=record['rows'], oids=record['oids'])
                    if len(record['rows']) == 1 and len(record['rows'][0]) == 1 and record['rows'][0][0] is not None:
                        table_oids[action['table']] = int(record['rows'][0][0])
                        check(table_oids[action['table']] > 0, 'nonzero actual table OID', table=action['table'])
                elif kind == 'explain':
                    explain(action, record)
                elif action['full']:
                    full(action, record, frames)
            elif kind == 'capture-original':
                index = Path(server['dir'])/'info'/(TABLE+'_k.gist')
                original = index.read_bytes()
                image(action['name'], original)
                original_entries = prove_original(original, TABLE, MODEL)
                check(any(low == high == b'' for _, low, high in original_entries), 'genuine encoded empty positive')
            elif kind == 'damage':
                if original is None or index.read_bytes() != original:
                    raise ValueError('each distinct damage starts from same actual saved generation')
                variant = action['variant']
                raw = b'' if variant == 'zero-byte' else original[:11] if variant == 'partial-header' else original[:-1]+bytes([original[-1]^1])
                index.write_bytes(raw)
                check(index.read_bytes() == raw, 'actual exact owned corruption bytes', name=action['name'])
                image(action['name'], raw)
            elif kind == 'restore-original':
                index.write_bytes(original)
                check(index.read_bytes() == original, 'same exact V3 restored once per distinct case', name=action['name'])
                image(action['name'], original)
            elif kind == 'legacy-nonempty':
                if index.read_bytes() != original:
                    raise ValueError('legacy fixture starts from same healthy V3')
                def quoted(value):
                    return '"'+value.decode('utf8').replace('\\', '\\\\').replace('"', '\\"')+'"'
                legacy = ''.join(str(rid)+' '+quoted(low)+' '+quoted(high)+'\n' for rid, low, high in original_entries if low or high).encode()
                check(bool(legacy), 'nonempty legal legacy positive, not zero-byte corruption')
                index.write_bytes(legacy)
                check(index.read_bytes() == legacy, 'actual legacy representation saved')
                image(action['name'], legacy)
            elif kind == 'check-zero-V3':
                raw = (Path(server['dir'])/'info'/(NULL_TABLE+'_k.gist')).read_bytes()
                item = image(action['name'], raw, NULL_TABLE)
                entries = prove_original(raw, NULL_TABLE, NULL_MODEL)
                check(len(raw) == 24 and entries == [], 'legal V3 exact24-byte zero-count frame')
                item['version'] = 3
            elif kind == 'assert-restored':
                check(index.read_bytes() == original, 'all local variants restored actual original bytes')
        trace['cohort_complete'] = True
    except BaseException as error:
        trace['harness_error'] = dict(type=type(error).__name__, message=str(error))
    finally:
        if index is not None and original is not None:
            try:
                index.write_bytes(original)
            except BaseException as error:
                trace['restore_error'] = dict(type=type(error).__name__, message=str(error))
        if server is not None:
            process = server['process']
            try:
                runner.stop_ours(server)
            except BaseException as error:
                trace['stop_error'] = dict(type=type(error).__name__, message=str(error))
            trace['server_exit'] = process.returncode
            if trace['owners']:
                trace['owners'][-1].update(exit=process.returncode, pid_gone=not Path('/proc', str(process.pid)).exists())
        trace['complete'] = trace['cohort_complete'] and not trace['failures'] and trace['harness_error'] is None and trace.get('server_exit') == 0 and 'stop_error' not in trace and 'restore_error' not in trace
        (evidence/'trace-v1.json').write_text(json.dumps(trace, ensure_ascii=False, separators=(',', ':'))+'\n')
    assert trace['complete'], dict(failures=trace['failures'], harness_error=trace['harness_error'], server_exit=trace.get('server_exit'))
    print('GIST_EMPTY_LEGACY_PROTOCOL commands='+str(len(trace['commands']))+' images='+str(len(trace['images']))+' full_values_NULL_origins_raw_usedIndex=1')


if __name__ == '__main__':
    main()
