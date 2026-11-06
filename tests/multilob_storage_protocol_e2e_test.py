#!/usr/bin/env python3
"""Full ordinary multiLOB SimpleQuery regression: fixed37+3; two normal owners.

Deterministic payloads, complete values/NULL/T7/origins/tags/Ready and raw wire;
header0 and closed all-file images. No network downloads or build artifacts.
Product-only SIGKILL/native backup roles are separate tests, not run here.
"""
import argparse, copy, hashlib, importlib.util, json, os, socket, struct
import subprocess, tempfile, time, traceback
from pathlib import Path

DDL = 'CREATE TABLE cv_sig(id INT PRIMARY KEY,n INT,tag TEXT,other_tag TEXT,bytes BYTEA,other_bytes BYTEA,"select" INT)'
def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def blob(seed, size, first, last):
    out = bytearray()
    x = seed
    for _ in range(size):
        x ^= (x << 13) & 0xffffffff
        x ^= x >> 17
        x ^= (x << 5) & 0xffffffff
        x &= 0xffffffff
        out.append(x & 255)
    out[:len(first)] = first
    out[-len(last):] = last
    return bytes(out)


def text7000(mark):
    a = ('界|' + mark + ";,'\n").encode()
    b = ('終' + mark).encode()
    return a + (mark.encode() * (7000 - len(a) - len(b))) + b



def make_plan():
    D = Path('.')
    raw_payloads = {}
    payloads = {}
    for name, raw in [('b64a', blob(0x13579bdf, 65536, b'\x00\xff', b'\xff\x00')),
                      ('b70a', blob(0x2468ace0, 71680, b'\xff\x00', b'\x00\xff')),
                      ('b64b', blob(0xd00dfeed, 65536, b'\x00\xff', b'\x00\xff')),
                      ('b70b', blob(0xc0ffee12, 71680, b'\xff\x00', b'\xff\x00')),
                      ('t7a', text7000('A')), ('t7b', text7000('B'))]:
        path = D / (name + ('.bin' if name[0] == 'b' else '.utf8'))
        raw_payloads[name] = raw
        payloads[name] = dict(path=path.name, bytes=len(raw), sha256=sha(raw),
                              role='bytea' if name[0] == 'b' else 'text')

    def ref(name):
        return dict(payload=name)

    rows = [
        [1, None, None, None, None, None, None],
        [2, 0, '', 'NULL', dict(hex=''), dict(hex='00ff'), 0],
        [3, -7, ref('t7a'), ref('t7b'), ref('b64a'), ref('b70a'), 7],
        [4, 2147483647, ref('t7b'), '', ref('b70a'), ref('b64a'), -2147483648],
        [5, 17, 'NULL', None, dict(hex='00ff00ff'), dict(hex=''), None],
        [6, -1, '', ref('t7a'), None, ref('b70a'), 8],
        [7, 1, ref('t7b'), None, ref('b64a'), None, 1],
    ]

    def literal(value):
        if value is None:
            return 'NULL'
        if isinstance(value, int):
            return str(value)
        if isinstance(value, dict):
            if 'hex' in value:
                return "'\\x" + value['hex'] + "'"
            name = value['payload']
            raw = raw_payloads[name]
            if payloads[name]['role'] == 'bytea':
                return "'\\x" + raw.hex() + "'"
            value = raw.decode()
        return "'" + value.replace("'", "''") + "'"

    def insert(rs):
        return 'INSERT INTO cv_sig(id,n,tag,other_tag,bytes,other_bytes,"select") VALUES' + ','.join('(' + ','.join(map(literal, row)) + ')' for row in rs)

    base_fields = [dict(name=name, relation='cv_sig', attribute=i + 1, oid=oid,
                        size=size, modifier=-1, format=0)
                   for i, (name, oid, size) in enumerate([
                       ('id', 23, 4), ('n', 23, 4), ('tag', 25, -1),
                       ('other_tag', 25, -1), ('bytes', 17, -1),
                       ('other_bytes', 17, -1), ('select', 23, 4)])]
    query = 'SELECT id,n,tag,other_tag,bytes,other_bytes,"select" FROM cv_sig ORDER BY id'
    snapshots = {}
    actions = []

    def action(name, sql, tag, ready='I', snapshot=None, fields=None,
               requires=('table',), establish=(), scope='shared'):
        actions.append(dict(id=name, scope=scope, sql=sql, expected_state=None,
                            expected_tag=tag, expected_ready=ready, snapshot=snapshot,
                            fields=fields, requires=list(requires), establish=list(establish)))

    def check(name, rs, ready='I', fields=None, sql=None):
        snapshots[name] = copy.deepcopy(rs)
        action(name, sql or query, 'SELECT ' + str(len(rs)), ready,
               name, copy.deepcopy(base_fields if fields is None else fields))

    action('schema.original_exact7', DDL, 'CREATE TABLE', requires=(), establish=('table',))
    action('seed.full7', insert(rows), 'INSERT 0 7', establish=('population',))
    check('seed.full7_values', rows)
    nrows = copy.deepcopy(rows)
    nrows[2][1] = 123
    action('update.physical_scalar_only', 'UPDATE cv_sig SET n=123 WHERE id=3', 'UPDATE 1')
    check('update.preserve_all_varlenas', nrows)
    for i, value in [(2, ref('t7b')), (3, ref('t7a')), (4, ref('b70b')), (5, ref('b64b'))]:
        nrows[2][i] = value
    action('update.all_four_varlenas', 'UPDATE cv_sig SET tag=' + literal(ref('t7b')) + ',other_tag=' + literal(ref('t7a')) + ',bytes=' + literal(ref('b70b')) + ',other_bytes=' + literal(ref('b64b')) + ' WHERE id=3', 'UPDATE 1')
    check('update.full_new_large_values', nrows)
    nrows[1][2:6] = [None, '', ref('b64b'), None]
    action('update.null_empty_large', 'UPDATE cv_sig SET tag=NULL,other_tag=\'\',bytes=' + literal(ref('b64b')) + ',other_bytes=NULL WHERE id=2', 'UPDATE 1')
    check('update.true_null_empty', nrows)
    saved = copy.deepcopy(nrows)
    action('rollback.begin', 'BEGIN', 'BEGIN', 'T')
    action('rollback.update', 'UPDATE cv_sig SET tag=\'rolled back\',bytes=' + literal(ref('b64a')) + ' WHERE id=3', 'UPDATE 1', 'T')
    action('rollback.delete', 'DELETE FROM cv_sig WHERE id=2', 'DELETE 1', 'T')
    txrow = [99, 99, ref('t7a'), ref('t7b'), ref('b64a'), ref('b70a'), 99]
    action('rollback.insert', insert([txrow]), 'INSERT 0 1', 'T')
    txrows = [copy.deepcopy(r) for r in nrows if r[0] != 2]
    txrows[1][2] = 'rolled back'
    txrows[1][4] = ref('b64a')
    txrows.append(txrow)
    check('rollback.own_full_rows', txrows, 'T')
    action('rollback.end', 'ROLLBACK', 'ROLLBACK')
    check('rollback.original_full_rows', saved)
    action('maintenance.create_index', 'CREATE INDEX cv_sig_n_idx ON cv_sig(n)', 'CREATE INDEX')
    action('maintenance.reindex', 'REINDEX TABLE cv_sig', 'REINDEX')
    check('maintenance.after_reindex', saved)
    action('maintenance.vacuum_full', 'VACUUM FULL cv_sig', 'VACUUM')
    check('maintenance.after_vacuum_full', saved)
    action('rewrite.add_bytea', 'ALTER TABLE cv_sig ADD COLUMN extra_bytes BYTEA', 'ALTER TABLE')
    eight_fields = copy.deepcopy(base_fields) + [dict(name='extra_bytes', relation='cv_sig', attribute=8, oid=17, size=-1, modifier=-1, format=0)]
    eight_rows = [copy.deepcopy(r) + [None] for r in saved]
    q8 = 'SELECT id,n,tag,other_tag,bytes,other_bytes,"select",extra_bytes FROM cv_sig ORDER BY id'
    check('rewrite.added_true_null', eight_rows, fields=eight_fields, sql=q8)
    action('rewrite.extra_bytea_update', 'UPDATE cv_sig SET extra_bytes=' + literal(ref('b70b')) + ' WHERE id=3', 'UPDATE 1')
    eight_rows[2][7] = ref('b70b')
    check('rewrite.extra_large_bytea', eight_rows, fields=eight_fields, sql=q8)
    action('rewrite.drop_bytea', 'ALTER TABLE cv_sig DROP COLUMN extra_bytes', 'ALTER TABLE')
    check('rewrite.drop_preserves_all7', saved)
    action('rewrite.integer_to_bigint', 'ALTER TABLE cv_sig ALTER COLUMN n TYPE BIGINT', 'ALTER TABLE')
    bigint_fields = copy.deepcopy(base_fields)
    bigint_fields[1].update(oid=20, size=8)
    check('rewrite.bigint_metadata_values', saved, fields=bigint_fields)
    action('rewrite.bigint_to_integer', 'ALTER TABLE cv_sig ALTER COLUMN n TYPE INTEGER', 'ALTER TABLE')
    check('rewrite.integer_metadata_values', saved)
    action('delete.large_tuple', 'DELETE FROM cv_sig WHERE id=4', 'DELETE 1')
    deleted = [r for r in saved if r[0] != 4]
    check('delete.other_large_tuples_retained', deleted)
    action('truncate.heap', 'TRUNCATE TABLE cv_sig', 'TRUNCATE TABLE')
    check('truncate.empty_full_descriptor', [])
    action('truncate.reseed', insert(rows), 'INSERT 0 7')
    check('truncate.reseed_full7', rows)
    cold = []
    snapshots['cold.original_full7'] = copy.deepcopy(rows)
    cold.append(dict(id='cold.original_full7', scope='shared', sql=query,
                     expected_state=None, expected_tag='SELECT 7', expected_ready='I',
                     snapshot='cold.original_full7', fields=copy.deepcopy(base_fields),
                     requires=['table', 'population'], establish=[]))
    coldrow = [8, 8, ref('t7a'), ref('t7b'), ref('b70b'), ref('b64b'), 8]
    cold.append(dict(id='cold.subsequent_large_insert', scope='shared', sql=insert([coldrow]),
                     expected_state=None, expected_tag='INSERT 0 1', expected_ready='I',
                     snapshot=None, fields=None, requires=['table', 'population'], establish=[]))
    snapshots['cold.after_subsequent_write'] = copy.deepcopy(rows) + [coldrow]
    cold.append(dict(id='cold.after_subsequent_write', scope='shared', sql=query,
                     expected_state=None, expected_tag='SELECT 8', expected_ready='I',
                     snapshot='cold.after_subsequent_write', fields=copy.deepcopy(base_fields),
                     requires=['table', 'population'], establish=[]))
    # Both crash roles start from a separately preserved, actually closed final
    # normal seed. No native engine may open this image before Main recovery.
    seed = copy.deepcopy(rows) + [coldrow]
    crashroles = []
    for kind in ['committed', 'uncommitted']:
        changed = copy.deepcopy(seed)
        changed[2][2:6] = [ref('t7b'), ref('t7a'), ref('b70b'), ref('b64b')]
        newrow = [101, 101, ref('t7b'), ref('t7a'), ref('b64b'), ref('b70b'), 101]
        beforekill = [dict(id=kind + '.begin', sql='BEGIN', expected_tag='BEGIN', expected_ready='T'),
                      dict(id=kind + '.update_all4', sql='UPDATE cv_sig SET tag=' + literal(ref('t7b')) + ',other_tag=' + literal(ref('t7a')) + ',bytes=' + literal(ref('b70b')) + ',other_bytes=' + literal(ref('b64b')) + ' WHERE id=3', expected_tag='UPDATE 1', expected_ready='T')]
        if kind == 'committed':
            beforekill += [dict(id=kind + '.insert', sql=insert([newrow]), expected_tag='INSERT 0 1', expected_ready='T'),
                           dict(id=kind + '.commit', sql='COMMIT', expected_tag='COMMIT', expected_ready='I')]
            expected = changed + [newrow]
        else:
            expected = copy.deepcopy(seed)
        snapshots[kind + '.after_Main_recovery'] = expected
        subsequent = [102, 102, ref('t7a'), ref('t7b'), ref('b70a'), ref('b64a'), 102]
        snapshots[kind + '.after_subsequent_write'] = expected + [subsequent]
        recovery = [dict(id=kind + '.after_Main_recovery', scope='product_WAL', sql=query, expected_state=None,
                         expected_tag='SELECT ' + str(len(expected)), expected_ready='I',
                         snapshot=kind + '.after_Main_recovery', fields=copy.deepcopy(base_fields), requires=['table', 'population'], establish=[]),
                    dict(id=kind + '.subsequent_write', scope='product_WAL', sql=insert([subsequent]), expected_state=None,
                         expected_tag='INSERT 0 1', expected_ready='I', snapshot=None, fields=None, requires=['table', 'population'], establish=[]),
                    dict(id=kind + '.after_subsequent_write', scope='product_WAL', sql=query, expected_state=None,
                         expected_tag='SELECT ' + str(len(expected) + 1), expected_ready='I',
                         snapshot=kind + '.after_subsequent_write', fields=copy.deepcopy(base_fields), requires=['table', 'population'], establish=[])]
        for a in beforekill:
            a.update(scope='product_WAL', expected_state=None, snapshot=None, fields=None, requires=['table', 'population'], establish=[])
        crashroles.append(dict(role=kind, seed='actual closed normal-role final data image; clone all files without fabrication',
                               before_kill=beforekill, kill_after_exact_ack=beforekill[-1]['id'],
                               no_read_or_checkpoint_between_ack_and_kill=True, intended_exit=-9,
                               recovery=recovery, recovery_first_reader='normal Main, no native Engine before it',
                               true_visible_RID_proof='separate public native cursor only after Main normal stop; file slots alone do not establish MVCC visibility'))

    plan = dict(schema='P29-multilob-protocol-fixed-v1', state='FROZEN_PREDICTIONS_NOT_RUN',
                exact_original_DDL=DDL, payloads=payloads, snapshots=snapshots,
                actions=actions, cold_actions=cold, product_crash_roles=crashroles,
                row_population=rows, wire=dict(protocol='SimpleQuery only', format=0, timeout_seconds=15,
                    descriptor_fields=7, NULL='signed length -1', empty='signed length 0',
                    bytea='hex text, every byte checked, not binary result format',
                    expected_main_frames='T/D*/C/Z for SELECT; C/Z for successful utility/DML; N retained separately',
                    normal_stop_deadline_seconds=10, startup_deadline_seconds=20),
                origins='each actual engine pg_class equality lookup by true relation name; one actual relation identity reused and cold-compared',
                header_witness='product after normal stop only: DataFileHeader valid checksum, formatVersion2, rowSize0; no SQL ctid invented',
                TOAST_witness='public native visible cursor/readToast raw bytes owned by separate native task; SQL and physical checks remain distinct',
                excluded=['new Bind/format1/format2 tests', 'PG backup or crash simulation', 'R15 diagnostics/retries', 'generic fault matrix', 'catalog OID fabrication'],
                predictions_policy='actual PG first differences preserved; subsequent oracle versions may only derive offline from captured actual PG, no resampling',
                native_backup_role='separate actual public physicalBackup/restore native fixture, not represented as portable PG SQL')
    return plan, raw_payloads

"""SimpleQuery/format0 receiver. Import has no server, engine, or filesystem side effects."""
import hashlib
import json
import struct
from pathlib import Path


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def row_description(body):
    if len(body) < 2:
        raise ValueError('short T count')
    count = struct.unpack_from('!H', body)[0]
    at, fields = 2, []
    for _ in range(count):
        end = body.index(b'\0', at)
        name = body[at:end].decode('utf-8')
        at = end + 1
        if len(body) - at < 18:
            raise ValueError('short T field')
        fields.append([name, *struct.unpack_from('!IhIhih', body, at)])
        at += 18
    if at != len(body):
        raise ValueError('T tail')
    return fields


def decode(wire):
    result = dict(rows=[], descriptors=[], tags=[], errors=[], notices=[], ready=[], frames=[])
    at = 0
    while at < len(wire):
        if len(wire) - at < 5:
            raise ValueError('truncated frame envelope')
        kind = chr(wire[at])
        size = struct.unpack_from('!I', wire, at + 1)[0]
        if size < 4 or size > len(wire) - at - 1:
            raise ValueError('invalid frame extent')
        body = wire[at + 5:at + 1 + size]
        result['frames'].append(dict(kind=kind, offset=at, length=size + 1,
                                     body_bytes=len(body), body_sha256=digest(body)))
        at += size + 1
        if kind == 'T':
            result['descriptors'].append(row_description(body))
        elif kind == 'D':
            if len(body) < 2:
                raise ValueError('short D count')
            n = struct.unpack_from('!H', body)[0]
            pos, row = 2, []
            for _ in range(n):
                if len(body) - pos < 4:
                    raise ValueError('short D field')
                length = struct.unpack_from('!i', body, pos)[0]
                pos += 4
                if length < -1 or length > len(body) - pos:
                    raise ValueError('invalid D field span')
                row.append(None if length == -1 else body[pos:pos + length].decode('utf-8'))
                if length >= 0:
                    pos += length
            if pos != len(body):
                raise ValueError('D tail')
            result['rows'].append(row)
        elif kind in ('E', 'N'):
            pos, fields = 0, {}
            while pos < len(body) and body[pos]:
                code = chr(body[pos])
                pos += 1
                end = body.index(b'\0', pos)
                if code in fields:
                    raise ValueError('duplicate error field')
                fields[code] = body[pos:end].decode('utf-8')
                pos = end + 1
            if body[pos:] != b'\0':
                raise ValueError('E/N tail')
            result['errors' if kind == 'E' else 'notices'].append(fields)
        elif kind == 'C':
            if not body.endswith(b'\0') or b'\0' in body[:-1]:
                raise ValueError('C tail')
            result['tags'].append(body[:-1].decode())
        elif kind == 'Z':
            if body not in (b'I', b'T', b'E'):
                raise ValueError('Z body')
            result['ready'].append(body.decode())
        elif kind in ('S', 'A'):
            # Full bytes remain in the wire image. These are not discarded or
            # confused with a result row, command, or Ready marker.
            pass
        else:
            raise ValueError('unexpected SimpleQuery response frame: ' + kind)
    if len(result['ready']) != 1 or not result['frames'] or result['frames'][-1]['kind'] != 'Z':
        raise ValueError('exactly one final Ready required')
    result['state'] = result['errors'][-1].get('C') if result['errors'] else None
    return result


class Receiver:
    def __init__(self, client, sock, trace, directory, plan, raw_payloads):
        self.client, self.sock, self.trace = client, sock, trace
        self.directory = Path(directory)
        self.plan = plan
        self.roles, self.origins = set(), {}
        self.sequence = 0
        self.payloads = {}
        for name, item in plan['payloads'].items():
            raw = raw_payloads[name]
            if len(raw) != item['bytes'] or digest(raw) != item['sha256']:
                raise ValueError('frozen payload drift: ' + name)
            self.payloads[name] = raw

    def save(self):
        self.trace['_save']()

    def request(self, sql, name, target):
        self.sequence += 1
        prefix = f'{self.sequence:04d}'
        request = self.client.typed(b'Q', sql.encode('utf-8') + b'\0')
        request_path = self.directory / (prefix + '.request.bin')
        response_path = self.directory / (prefix + '.response.bin')
        request_path.write_bytes(request)
        event = dict(id=name, sequence=self.sequence, sql=sql, complete=False,
                     request=dict(path=request_path.name, bytes=len(request), sha256=digest(request)),
                     response=dict(path=response_path.name, bytes=0))
        target.append(event)
        self.save()
        sock = self.sock
        with response_path.open('xb') as raw:
            class Capture:
                def recv(self, n):
                    data = sock.recv(n)
                    raw.write(data)
                    raw.flush()
                    return data
            try:
                sock.sendall(request)
                while True:
                    kind, _ = self.client.read_message(Capture())
                    if kind == b'Z':
                        break
            except BaseException:
                raw.flush()
                event['response'].update(bytes=raw.tell(), sha256=digest(response_path.read_bytes()))
                self.save()
                raise
        # Only decode or compare after all actual response bytes were saved.
        wire = response_path.read_bytes()
        event['response'].update(bytes=len(wire), sha256=digest(wire))
        event.update(decode(wire))
        event['complete'] = True
        self.save()
        return event

    def anchor(self, relation):
        if relation in self.origins:
            return self.origins[relation]
        event = self.request("SELECT oid FROM pg_catalog.pg_class WHERE relname='" + relation.replace("'", "''") + "'",
                             'actual-origin-' + relation, self.trace['controls'])
        fields = [f for d in event['descriptors'] for f in d]
        if (event['state'] is not None or event['ready'] != ['I'] or event['tags'] != ['SELECT 1']
                or len(event['rows']) != 1 or len(event['rows'][0]) != 1
                or not event['rows'][0][0].isdigit() or int(event['rows'][0][0]) <= 0
                or len(fields) != 1 or fields[0][3:] != [26, 4, -1, 0]):
            self.trace['mismatches'].append(dict(id='actual-origin-' + relation,
                                                 fields=['real_catalog_identity'], actual=event))
            self.save()
            return None
        self.origins[relation] = int(event['rows'][0][0])
        return self.origins[relation]

    def value(self, value):
        if value is None:
            return None
        if isinstance(value, dict):
            if 'hex' in value:
                return '\\x' + value['hex']
            name = value['payload']
            raw = self.payloads[name]
            return '\\x' + raw.hex() if self.plan['payloads'][name]['role'] == 'bytea' else raw.decode('utf-8')
        return str(value)

    def run(self, action):
        missing = [role for role in action['requires'] if role not in self.roles]
        if missing:
            self.trace['notrun'].append(dict(id=action['id'], reason='PREREQUISITE_FAILED', missing=missing,
                                             expected_action=action))
            self.save()
            return None
        origins = {f['relation']: self.anchor(f['relation']) for f in action['fields'] or []}
        event = self.request(action['sql'], action['id'], self.trace['observations'])
        differences = []
        if event['state'] != action['expected_state']:
            differences.append(dict(field='SQLSTATE', expected=action['expected_state'], actual=event['state']))
        if event['ready'] != [action['expected_ready']]:
            differences.append(dict(field='Ready', expected=action['expected_ready'], actual=event['ready']))
        expected_tags = [] if action['expected_tag'] is None else [action['expected_tag']]
        if event['tags'] != expected_tags:
            differences.append(dict(field='CommandComplete', expected=expected_tags, actual=event['tags']))
        if action['snapshot'] is None:
            expected_rows, expected_descriptors = [], []
        else:
            expected_rows = [[self.value(cell) for cell in row]
                             for row in self.plan['snapshots'][action['snapshot']]]
            expected_descriptors = [[[f['name'], origins[f['relation']], f['attribute'], f['oid'],
                                      f['size'], f['modifier'], f['format']] for f in action['fields']]]
        if event['rows'] != expected_rows:
            differences.append(dict(field='every_cell_NULL_and_length', expected_snapshot=action['snapshot'],
                                    expected_rows=len(expected_rows), actual_rows=len(event['rows']),
                                    expected_sha256=digest(json.dumps(expected_rows, ensure_ascii=False, separators=(',', ':')).encode()),
                                    actual_sha256=digest(json.dumps(event['rows'], ensure_ascii=False, separators=(',', ':')).encode())))
        if event['descriptors'] != expected_descriptors:
            differences.append(dict(field='complete_T7_real_origins', expected=expected_descriptors,
                                    actual=event['descriptors']))
        significant = ''.join(f['kind'] for f in event['frames'] if f['kind'] not in ('N', 'S', 'A'))
        if action['expected_state'] is not None:
            expected_frames = 'EZ'
        elif action['snapshot'] is not None:
            expected_frames = 'T' + 'D' * len(expected_rows) + 'CZ'
        else:
            expected_frames = 'CZ'
        if significant != expected_frames:
            differences.append(dict(field='complete_response_sequence', expected=expected_frames, actual=significant))
        event.update(snapshot=action['snapshot'], differences=differences, matches_prediction=not differences,
                     checked_row_count=len(expected_rows), checked_cell_count=sum(map(len, expected_rows)),
                     checked_NULL_count=sum(cell is None for row in expected_rows for cell in row))
        if differences:
            self.trace['mismatches'].append(dict(id=action['id'], differences=differences))
        else:
            self.roles.update(action['establish'])
        self.save()
        return event



def file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def find_repo():
    for parent in Path(__file__).resolve().parents:
        if (parent/'tests/postgres_protocol_test.py').is_file() and (parent/'src/commands/TableManage.h').is_file():
            return parent
    raise ValueError('actual repository protocol helper unavailable')


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--write-plan',type=Path)
    args=ap.parse_args()
    plan,raw_payloads=make_plan()
    plan_raw=(json.dumps(plan,ensure_ascii=False,indent=2)+'\n').encode()
    if hashlib.sha256(plan_raw).hexdigest()!='babc252a143acf3d54068d130de3c87c3f2cfe588003493b399b42d3d7136c25':
        raise ValueError('original frozen37+3 SQL/values/expectations changed')
    if args.write_plan:
        if args.write_plan.exists():raise ValueError('refuse prior plan overwrite')
        args.write_plan.write_bytes(plan_raw)
        print('MULTILOB_PROTOCOL PREPARED SQL0 '+str(args.write_plan));return
    repo=find_repo()
    spec=importlib.util.spec_from_file_location('multilob_registered_client',repo/'tests/postgres_protocol_test.py')
    client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
    binary=Path(os.environ.get('DBMS_MAIN',str(repo/'dbms_main'))).resolve()
    if not binary.is_file():raise ValueError('actual normalMain executable unavailable')
    pinned_binary=file_sha(binary)
    output=os.environ.get('DBMS_TEST_EVIDENCE_DIR')
    D=Path(output).resolve() if output else Path(tempfile.mkdtemp(prefix='dbms-multilob-protocol-'))
    if output:D.mkdir(parents=True,exist_ok=True)
    if (D/'trace-v1.json').exists() or (D/'data').exists():raise ValueError('exclusive new evidence/data directory required')
    (D/'wire').mkdir()
    (D/'fixed-plan-v1.json').write_bytes(plan_raw)
    data=D/'data';data.mkdir();(data/'info').mkdir();(data/'info/tlist.lst').write_bytes(b'')
    client.write_auth_catalog(str(data),'alice','secret')
    (data/'pg_hba.conf').write_text('host all alice 127.0.0.1/32 scram-sha-256\n')
    probe=socket.socket();probe.bind(('127.0.0.1',0));port=probe.getsockname()[1];probe.close()
    trace=dict(complete=False,cohort_complete=False,observations=[],controls=[],mismatches=[],notrun=[],owners=[],
               failure=None,actual_binary=dict(path=str(binary),sha256=pinned_binary),plan_sha256=hashlib.sha256(plan_raw).hexdigest(),
               protocol='SimpleQuery/text0 only',new_Bind_or_binary_format_coverage=False,normal_cold_restarts=0,
               source_sha256=file_sha(Path(__file__)))
    def save():(D/'trace-v1.json').write_text(json.dumps({k:v for k,v in trace.items() if k!='_save'},ensure_ascii=False,separators=(',',':'))+'\n')
    trace['_save']=save
    q=sock=read=None
    def start(phase):
        nonlocal q,sock
        if file_sha(binary)!=pinned_binary:raise ValueError('actual binary changed before owner start')
        env={k:v for k,v in os.environ.items() if not k.startswith('DBMS_')};env['DBMS_DATA_DIR']=str(data)
        q=subprocess.Popen([str(binary),'--data-dir',str(data),'--server',str(port),'--insecure'],cwd=data,env=env,
                           stdout=(D/(phase+'.stdout')).open('wb'),stderr=(D/(phase+'.stderr')).open('wb'),start_new_session=True)
        stat=Path('/proc',str(q.pid),'stat').read_text()
        owner=dict(phase=phase,pid=q.pid,pgid=os.getpgid(q.pid),start_ticks=stat[stat.rfind(')')+2:].split()[19],
                   exe=str(Path('/proc',str(q.pid),'exe').resolve()),cmdline_hex=Path('/proc',str(q.pid),'cmdline').read_bytes().hex(),data_directory=str(data))
        trace['owners'].append(owner);save()
        if owner['pgid']!=q.pid or owner['exe']!=str(binary):raise ValueError('actual owner group/executable mismatch')
        sock=socket.socket();sock.settimeout(15);deadline=time.monotonic()+20
        while True:
            try:sock.connect(('127.0.0.1',port));break
            except OSError:
                if q.poll() is not None or time.monotonic()>=deadline:raise
                time.sleep(.05)
        actual=sock;startup=D/(phase+'-startup.response.bin')
        with startup.open('xb') as raw:
            class Capture:
                def recv(self,n):
                    value=actual.recv(n);raw.write(value);raw.flush();return value
                def sendall(self,value):return actual.sendall(value)
            client.startup(Capture(),'alice','info')
        trace.setdefault('startups',[]).append(dict(phase=phase,path=str(startup),bytes=startup.stat().st_size,sha256=file_sha(startup)));save()
    def stop():
        nonlocal sock
        if sock is not None:sock.close();sock=None
        if q is None or not trace['owners'] or 'exit' in trace['owners'][-1]:return
        q.terminate()
        try:q.wait(timeout=10)
        except subprocess.TimeoutExpired:
            trace['owners'][-1]['failed_normal_stop_forced_kill']=True;q.kill();q.wait(timeout=10)
        trace['owners'][-1].update(exit=q.returncode,pid_gone=not Path('/proc',str(q.pid)).exists());save()
        if q.returncode!=0 or not trace['owners'][-1]['pid_gone']:raise RuntimeError('normal lifetime failure; no retry/cold restart')
    def closed(phase):
        if not trace['owners'] or trace['owners'][-1].get('exit')!=0 or not trace['owners'][-1].get('pid_gone'):
            raise ValueError('actual normal closed owner required')
        dest=D/'images'/phase;dest.mkdir(parents=True)
        files=[]
        for path in sorted(data.rglob('*')):
            if path.is_symlink():raise ValueError('actual closed source symlink')
            rel=path.relative_to(data)
            if path.is_dir():(dest/rel).mkdir(parents=True,exist_ok=True)
            elif path.is_file():
                raw=path.read_bytes();copy=dest/rel;copy.parent.mkdir(parents=True,exist_ok=True);copy.write_bytes(raw)
                if copy.read_bytes()!=raw:raise ValueError('actual closed bytes capture mismatch')
                files.append(dict(source=str(path),capture=str(copy),relative=str(rel),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
            else:raise ValueError('nonregular actual closed source')
        rec=dict(phase=phase,files=files,image=str(dest))
        if read is not None and 'table' in read.roles:
            heaps=list(dest.rglob('cv_sig.dt'))
            if len(heaps)!=1:raise ValueError('real cv_sig heap not unique')
            raw=heaps[0].read_bytes()[:24]
            if len(raw)!=24:raise ValueError('short real DataFileHeader')
            magic,pages,free,width,version,checksum=struct.unpack('<6I',raw)
            computed=2166136261
            for byte in raw[:20]:computed=((computed^byte)*16777619)&0xffffffff
            rec['heap_header']=dict(path=str(heaps[0]),raw_hex=raw.hex(),magic=magic,numPages=pages,freeListHead=free,rowSize=width,formatVersion=version,checksum=checksum,calculated_checksum=computed)
            if (magic,width,version,checksum)!=(0x44415441,0,2,computed):
                trace['mismatches'].append(dict(id='closed-'+phase+'-actual-header-zero',actual=rec['heap_header']))
        trace.setdefault('closed_images',[]).append(rec);save()
    try:
        start('initial')
        read=Receiver(client,sock,trace,D/'wire',plan,raw_payloads)
        for action in plan['actions']:read.run(action)
        trace['initial_origins']=dict(read.origins)
        stop();closed('initial')
        if {'table','population'}<=read.roles:
            start('cold');read.sock=sock;read.origins={}
            for action in plan['cold_actions']:read.run(action)
            trace['cold_origins']=dict(read.origins)
            if trace['cold_origins']!=trace['initial_origins']:
                trace['mismatches'].append(dict(id='actual_relation_origin_cold_durability',initial=trace['initial_origins'],cold=trace['cold_origins']))
            stop();closed('cold');trace['normal_cold_restarts']=1
        else:
            trace['notrun'].extend(dict(id=a['id'],reason='INITIAL_SCHEMA_OR_POPULATION_FAILED') for a in plan['cold_actions'])
        trace['cohort_complete']=True
    except BaseException as error:
        trace['failure']=dict(type=type(error).__name__,message=str(error),traceback=traceback.format_exc())
    finally:
        try:stop()
        except BaseException as error:trace['stop_failure']=dict(type=type(error).__name__,message=str(error))
        if q is not None and q.poll() is None:
            q.terminate()
            try:q.wait(timeout=10)
            except subprocess.TimeoutExpired:q.kill();q.wait(timeout=10)
            trace['startup_cleanup']=dict(exit=q.returncode,pid_gone=not Path('/proc',str(q.pid)).exists())
        trace['complete']=trace['cohort_complete'] and trace['failure'] is None and not trace.get('stop_failure') and bool(trace['owners']) and all(o.get('exit')==0 and o.get('pid_gone') for o in trace['owners'])
        trace['strict_pass']=trace['complete'] and not trace['mismatches'] and not trace['notrun'] and len(trace['observations'])==40 and trace['normal_cold_restarts']==1
        trace['binary_unchanged']=file_sha(binary)==pinned_binary
        save()
        print('MULTILOB_PROTOCOL '+json.dumps(dict(complete=trace['complete'],strict_pass=trace['strict_pass'],actions=len(trace['observations']),NOTRUN=len(trace['notrun']),differences=[x['id'] for x in trace['mismatches']],evidence=str(D))),flush=True)
    if not trace['strict_pass'] or not trace['binary_unchanged']:raise SystemExit(1)


if __name__=='__main__':main()
