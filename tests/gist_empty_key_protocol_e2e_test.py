#!/usr/bin/env python3
"""Real empty-text GiST coverage, normal SQL and actual read owners.

No PostgreSQL index format/plan equivalence is
claimed: shared_pg_sql carries portable SQL/full results; local files and
usedIndex are independent product assertions. All wire raw precedes checks.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import time

TABLE = 'gist_empty_wire'
FIELDS = ['id', 'k', 'n', 'v', 'pad', 'note']
OIDS = [23, 25, 23, 1043, 25, 25]
SIZES = [4, -1, 4, -1, -1, -1]
MODS = [-1, -1, -1, 68, -1, -1]
DEL, MAX = '\x7f', '\U0010ffff'


def literal(value):
    return 'NULL' if value is None else "'" + value.replace("'", "''") + "'"


def rows():
    special = {1: '', 2: None, 3: 'NULL', 4: '', 5: DEL, 6: 'tail'+DEL,
               7: 'a', 8: 'b', 9: 'needle_界', 10: MAX, 11: MAX+'界', 12: ' '}
    out = []
    for i in range(1, 193):
        key = special.get(i, None if i % 29 == 0 else 'bulk 界\n|;,'+str(i))
        out.append([str(i), key, str(i-96),
                    None if i == 2 or i % 31 == 0 else '' if i in (1, 4) else 'v'+str(i),
                    None if i % 17 == 0 else 'payload 界\n'+str(i)+'x'*(6000 if i % 47 == 0 else 48),
                    None if i % 13 == 0 else 'NULL' if i == 3 else 'note 界\n|;,'+str(i)])
    return out


def crc32c(raw):
    value = 0xffffffff
    for byte in raw:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def decode_gist(raw):
    if len(raw) < 24 or raw[:8] not in (b'\0DBMSS\0002', b'\0DBMSS\0003'):
        raise ValueError('unrecognized complete GiST V2/V3 frame')
    version, count = struct.unpack_from('<IQ', raw, 8)
    if version != raw[7]-ord('0') or count > (len(raw)-24)//24:
        raise ValueError('magic/version/count mismatch')
    if crc32c(raw[:-4]) != int.from_bytes(raw[-4:], 'little'):
        raise ValueError('independent CRC mismatch')
    position, entries, seen = 20, [], set()
    for _ in range(count):
        if position+24 > len(raw)-4:
            raise ValueError('truncated fixed entry')
        rid, low, high = struct.unpack_from('<QQQ', raw, position)
        position += 24
        if rid == 0 or rid > (1 << 63)-1 or rid in seen or low+high > len(raw)-4-position:
            raise ValueError('RID/duplicate/extent invalid')
        seen.add(rid)
        lo, hi = raw[position:position+low], raw[position+low:position+low+high]
        position += low+high
        entries.append((rid, lo, hi))
    if position != len(raw)-4:
        raise ValueError('unconsumed frame tail')
    return version, entries


def heap_key_witness(heap, rid, ordinal):
    """Read a real encoded RID's physical key, not a guessed id-to-RID map.

    Uses only this fixture's real format2 schema (fixed id/n, four var fields).
    Does not assert MVCC visibility from raw xmin/xmax; SQL snapshot results
    independently check visibility. Key columns are deliberately non-TOAST.
    """
    if len(heap) < 8192 or struct.unpack_from('<I', heap)[0] != 0x44415441 or struct.unpack_from('<I', heap, 16)[0] != 2:
        raise ValueError('actual format2 heap required')
    page, slot = rid >> 32, rid & 0xffff
    if not page or page*8192+8192 > len(heap):
        raise ValueError('RID outside actual heap')
    raw = heap[page*8192:(page+1)*8192]
    lower, upper, special, pv = struct.unpack_from('<HHHH', raw, 12)
    flags = struct.unpack_from('<H', raw, 10)[0]
    version = pv & 255
    if version not in (4, 5) or not (24 <= lower <= upper <= special <= 8192) or lower % 4:
        raise ValueError('heap page layout/extent')
    if version == 5 and (not flags & 8 or struct.unpack_from('<I', raw, 20)[0] != page):
        raise ValueError('bound page identity mismatch')
    at = 24+slot*4
    if at+4 > lower:
        raise ValueError('RID slot outside actual line-pointer array')
    off, length_flags = struct.unpack_from('<HH', raw, at)
    off &= 0x7fff
    length = length_flags & 0x3fff
    if length_flags >> 14 != 1 or length < 24 or off < upper or off+length > special:
        raise ValueError('RID does not address a normal complete tuple')
    tup = raw[off:off+length]
    natts, mask, hoff = struct.unpack_from('<HHB', tup, 18)
    if natts & 0x7ff != 6 or hoff < 24 or hoff >= len(tup):
        raise ValueError('real fixture tuple header')
    ident = struct.unpack_from('<i', tup, hoff)[0]
    is_null = bool(mask & 1 and not tup[23] & (1 << ordinal))
    if is_null:
        return ident, None
    var_index = {1: 0, 3: 1}[ordinal]
    arr = hoff+8+var_index*4
    if arr+4 > len(tup):
        raise ValueError('raw variable field descriptor missing')
    start, size = struct.unpack_from('<HH', tup, arr)
    if start+size > len(tup)-hoff:
        raise ValueError('raw selected key extent')
    return ident, tup[hoff+start:hoff+start+size]


def description(raw):
    n = struct.unpack_from('!H', raw)[0]
    out, pos = [], 2
    for _ in range(n):
        end = raw.index(b'\0', pos)
        name = raw[pos:end].decode('utf8')
        pos = end+1
        out.append([name, *struct.unpack_from('!IhIhih', raw, pos)])
        pos += 18
    if pos != len(raw):
        raise ValueError('RowDescription tail')
    return out


def build_actions():
    """Pure frozen SQL/value plan; product outcomes never adjust this oracle."""
    actions = []
    model = rows()
    mods, oids = MODS.copy(), OIDS.copy()

    def q(name, sql, wanted=None, tag=None, ready='I', peer=False,
          shared=True, full=False, plan_used=None):
        actions.append(dict(kind='query', name=name, sql=sql, expected_rows=wanted,
                            expected_tag=tag, expected_ready=ready, peer=peer,
                            shared_pg_sql=shared, full=full, oids=oids.copy(), mods=mods.copy()))
        if plan_used is not None:
            actions.append(dict(kind='explain', name=name, sql='EXPLAIN (ANALYZE,FORMAT JSON) '+sql,
                                expected_used_index=plan_used, expected_ready=ready, peer=peer,
                                shared_pg_sql=False))

    def population(name, wanted=None, ready='I'):
        q(name, 'SELECT '+','.join(FIELDS)+' FROM '+TABLE+' ORDER BY id',
          [r.copy() for r in (model if wanted is None else wanted)],
          'SELECT '+str(len(model if wanted is None else wanted)), ready, full=True)

    def selected(name, predicate, test, used=None, ready='I', wanted=None):
        population_model = model if wanted is None else wanted
        expected = [r.copy() for r in population_model if test(r)]
        q(name, 'SELECT '+','.join(FIELDS)+' FROM '+TABLE+' WHERE '+predicate+' ORDER BY id',
          expected, 'SELECT '+str(len(expected)), ready, full=True, plan_used=used)

    def image(name):
        q(name+'-checkpoint', 'CHECKPOINT', tag='CHECKPOINT', shared=False)
        actions.append(dict(kind='image', name=name, expected_rows=[r.copy() for r in model], shared_pg_sql=False))

    q('auto-analyze-off', 'ALTER SYSTEM SET auto_analyze=off', shared=False)
    q('auto-vacuum-off', 'ALTER SYSTEM SET auto_vacuum=off', shared=False)
    q('reload', 'SELECT pg_reload_conf()', shared=False)
    q('cheap-index', 'SET random_page_cost=0.000001', tag='SET', shared=False)
    q('create', 'CREATE TABLE '+TABLE+'(id integer PRIMARY KEY,k text COLLATE "C",n integer,v varchar(64) COLLATE "C",pad text,note text)', tag='CREATE TABLE')
    q('seed', 'INSERT INTO '+TABLE+' VALUES'+','.join('('+','.join(literal(v) for v in row)+')' for row in model), tag='INSERT 0 192')
    q('create-k-index', 'CREATE INDEX ge_k ON '+TABLE+' USING gist(k)', tag='CREATE INDEX')
    q('create-v-index', 'CREATE INDEX ge_v ON '+TABLE+' USING gist(v)', tag='CREATE INDEX')
    actions.append(dict(kind='anchor', table=TABLE, name='actual-table-oid', sql="SELECT oid FROM pg_catalog.pg_class WHERE relname='"+TABLE+"'", shared_pg_sql=False))
    population('initial-full')
    image('initial')
    cases = [
        ('equal-empty', "k=''", lambda r:r[1]=='', True),
        ('less-empty', "k<''", lambda r:r[1] is not None and r[1]<'', True),
        ('less-equal-empty', "k<=''", lambda r:r[1] is not None and r[1]<='', True),
        ('greater-empty', "k>''", lambda r:r[1] is not None and r[1]>'', True),
        ('finite-pair', "k>='a' AND k<'c'", lambda r:r[1] is not None and 'a'<=r[1]<'c', True),
        ('literal-NULL', "k='NULL'", lambda r:r[1]=='NULL', True),
        ('literal-DEL', 'k='+literal(DEL), lambda r:r[1]==DEL, True),
        ('suffix-DEL', 'k='+literal('tail'+DEL), lambda r:r[1]=='tail'+DEL, True),
        ('mixed-column', "k<='a' AND n>0", lambda r:r[1] is not None and r[1]<='a' and int(r[2])>0, True),
        ('varchar-empty', "v=''", lambda r:r[3]=='', None),
        ('prefix-UTF8', "k LIKE 'needle%'", lambda r:r[1] is not None and r[1].startswith('needle'), True),
        ('prefix-no-successor', 'k LIKE '+literal(MAX+'%'), lambda r:r[1] is not None and r[1].startswith(MAX), True),
        ('explicit-CAST-original-owner', "k=CAST('' AS text)", lambda r:r[1]=='', False),
        ('two-columns-filter', "k>='a' AND v<='v7'", lambda r:r[1] is not None and r[1]>='a' and r[3] is not None and r[3]<='v7', True),
        ('SQL-NULL-not-empty', 'k IS NULL', lambda r:r[1] is None, False),
        ('all-nonNULL-ineffective-bound', "k>=''", lambda r:r[1] is not None, False),
        ('strict-existing-boundary', "k>'a' AND k<='b'", lambda r:r[1] is not None and 'a'<r[1]<='b', True),
    ]
    for case in cases:
        selected(*case)
    q('default-locale-table', 'CREATE TABLE ge_locale(id integer,k text)', tag='CREATE TABLE')
    q('default-locale-seed', "INSERT INTO ge_locale VALUES(1,''),(2,NULL),(3,'a')", tag='INSERT 0 3')
    q('default-locale-index', 'CREATE INDEX ge_locale_k ON ge_locale USING gist(k)', tag='CREATE INDEX')
    actions.append(dict(kind='anchor',table='ge_locale',name='actual-locale-table-oid',sql="SELECT oid FROM pg_catalog.pg_class WHERE relname='ge_locale'",shared_pg_sql=False))
    q('default-locale-value', "SELECT id,k FROM ge_locale WHERE k='' ORDER BY id", [['1','']], 'SELECT 1', plan_used=False)
    q('begin-current', 'BEGIN', tag='BEGIN', ready='T')
    selected('current-owner-empty', "k=''", lambda r:r[1]=='', True, ready='T')
    q('commit-current', 'COMMIT', tag='COMMIT')
    q('begin-SSI', 'BEGIN ISOLATION LEVEL SERIALIZABLE', tag='BEGIN', ready='T')
    selected('SSI-matching', "k=''", lambda r:r[1]=='', False, ready='T')
    selected('SSI-empty-answer', "k='absent'", lambda r:r[1]=='absent', False, ready='T')
    q('commit-SSI', 'COMMIT', tag='COMMIT')
    actions.append(dict(kind='connect-peer', name='real-peer', shared_pg_sql=False))
    q('begin-historical', 'BEGIN ISOLATION LEVEL REPEATABLE READ', tag='BEGIN', ready='T')
    population('historical-acquire', ready='T')
    old = [r.copy() for r in model]
    q('peer-empty-to-null', 'UPDATE '+TABLE+' SET k=NULL WHERE id=1', tag='UPDATE 1', peer=True)
    q('peer-null-to-empty', 'UPDATE '+TABLE+" SET k='' WHERE id=2", tag='UPDATE 1', peer=True)
    q('peer-nonempty-change', 'UPDATE '+TABLE+" SET k='changed_界' WHERE id=9", tag='UPDATE 1', peer=True)
    model[0][1],model[1][1],model[8][1] = None,'','changed_界'
    selected('historical-still-empty', "k=''", lambda r:r[1]=='', False, ready='T', wanted=old)
    selected('historical-still-NULL', 'k IS NULL', lambda r:r[1] is None, False, ready='T', wanted=old)
    population('historical-still-all', old, ready='T')
    q('commit-historical', 'COMMIT', tag='COMMIT')
    population('current-after-peer')
    selected('current-new-empty', "k=''", lambda r:r[1]=='', True)
    q('begin-own-write', 'BEGIN', tag='BEGIN', ready='T')
    q('own-change', 'UPDATE '+TABLE+" SET k='needle_own',pad=NULL WHERE id=3", tag='UPDATE 1', ready='T')
    own = [r.copy() for r in model];own[2][1],own[2][4] = 'needle_own',None
    q('own-empty-to-NULL', 'UPDATE '+TABLE+' SET k=NULL WHERE id=2', tag='UPDATE 1', ready='T')
    q('own-NULL-to-empty', 'UPDATE '+TABLE+" SET k='' WHERE id=1", tag='UPDATE 1', ready='T')
    q('own-delete-empty', 'DELETE FROM '+TABLE+' WHERE id=4', tag='DELETE 1', ready='T')
    own[0][1],own[1][1] = '',None
    own = [r for r in own if r[0]!='4']
    selected('own-write-fallback', "k LIKE 'needle%'", lambda r:r[1] is not None and r[1].startswith('needle'), False, ready='T', wanted=own)
    population('own-all', own, ready='T')
    q('rollback-own-write', 'ROLLBACK', tag='ROLLBACK')
    population('rollback-exact')
    q('peer-active-begin', 'BEGIN', tag='BEGIN', ready='T', peer=True)
    selected('overlapping-owner', "k=''", lambda r:r[1]=='', False)
    q('peer-active-rollback', 'ROLLBACK', tag='ROLLBACK', peer=True)
    actions.append(dict(kind='damage-controls', name='V2-corrupt-missing', expected_rows=[r.copy() for r in model], oids=oids.copy(), mods=mods.copy(), shared_pg_sql=False))
    q('commit-insert', 'INSERT INTO '+TABLE+" VALUES(193,'',97,'','new pad',NULL)", tag='INSERT 0 1')
    model.append(['193','', '97','', 'new pad',None])
    selected('insert-empty-indexed', "k=''", lambda r:r[1]=='', True)
    q('commit-delete', 'DELETE FROM '+TABLE+' WHERE id=4', tag='DELETE 1')
    model = [r for r in model if r[0]!='4']
    q('commit-update', 'UPDATE '+TABLE+" SET k='',v=NULL WHERE id=3", tag='UPDATE 1')
    model[2][1],model[2][3] = '',None
    population('committed-mutations')
    actions.append(dict(kind='generation-before',name='reindex',shared_pg_sql=False))
    q('reindex', 'REINDEX TABLE '+TABLE, tag='REINDEX')
    image('reindex')
    population('reindex-all')
    actions.append(dict(kind='generation-before',name='vacuum-full',shared_pg_sql=False))
    q('vacuum-full', 'VACUUM FULL '+TABLE, tag='VACUUM')
    image('vacuum-full')
    population('vacuum-full-all')
    q('add-column', 'ALTER TABLE '+TABLE+' ADD COLUMN added integer DEFAULT 7', tag='ALTER TABLE')
    population('add-column-original-six')
    q('drop-column', 'ALTER TABLE '+TABLE+' DROP COLUMN added', tag='ALTER TABLE')
    image('add-drop')
    population('add-drop-all')
    q('alter-key-type', 'ALTER TABLE '+TABLE+' ALTER COLUMN k TYPE varchar(64)', tag='ALTER TABLE')
    oids[1],mods[1] = 1043,68
    population('alter-key-type-all')
    image('alter-key-type')
    actions.append(dict(kind='cold-restart', name='cold-normal-restart', shared_pg_sql=False))
    q('cold-cost-setting', 'SET random_page_cost=0.000001', tag='SET', shared=False)
    population('cold-all')
    selected('cold-empty', "k=''", lambda r:r[1]=='', None)
    q('truncate', 'TRUNCATE TABLE '+TABLE, tag='TRUNCATE TABLE')
    model = []
    population('truncate-zero')
    image('truncate')
    return actions


def find_repo():
    for parent in Path(__file__).resolve().parents:
        if (parent/'tests/compat/pg_diff_runner.py').is_file():
            return parent
    raise RuntimeError('real repository protocol helper not found')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--write-plan', type=Path)
    options = parser.parse_args()
    actions = build_actions()
    if options.write_plan:
        options.write_plan.write_text(json.dumps(dict(schema='gist-empty-wire-plan-v1', status='PREPARED_NOTRUN',
            wire_deadline_seconds=15, row_fields=FIELDS, initial_rows=rows(), actions=actions), ensure_ascii=False, indent=2)+'\n')
        return
    repo = find_repo()
    spec = importlib.util.spec_from_file_location('gist_empty_runner', repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    if runner.wire_timeout() != 15:
        raise ValueError('this frozen cohort requires original 15s wire deadline')
    trace = dict(complete=False, cohort_complete=False, commands=[], images=[], actions=[], failures=[], owners=[], harness_error=None)
    server, peer, table_oid = None,None,None
    table_oids={}
    binary=Path(runner.DBMS_MAIN).resolve()
    trace['binary']=dict(path=str(binary),sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    trace['source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    trace['protocol_helper_sha256']=hashlib.sha256((repo/'tests/postgres_protocol_test.py').read_bytes()).hexdigest()
    evidence = Path(os.environ.get('DBMS_TEST_EVIDENCE_DIR', Path.cwd()/'gist-empty-wire-evidence'))
    evidence.mkdir(parents=True,exist_ok=True)

    def check(ok,label,**details):
        if not ok:trace['failures'].append(dict(label=label,**details))

    def query(sql,name,ready='I',is_peer=False,shared=True,tag=None):
        record=dict(name=name,sql=sql,peer=is_peer,shared_pg_sql=shared,raw=[])
        trace['commands'].append(record)  # even an interrupted first response is retained
        sock=peer if is_peer else server['sock']
        wire=bytearray()
        class Capture:
            def recv(self,size):
                chunk=sock.recv(size);wire.extend(chunk);return chunk
        frames=[]
        request=client.typed(b'Q',sql.encode()+b'\0');record['request_hex']=request.hex()
        try:
            sock.sendall(request)
            while True:
                kind,body=client.read_message(Capture());frames.append((kind,body))
                record['raw'].append(dict(kind=kind.decode(),body_hex=body.hex()))
                if kind==b'Z':break
        except BaseException:
            record['partial_response_wire_hex']=wire.hex()
            raise
        record['response_wire_sha256']=hashlib.sha256(wire).hexdigest()
        values,state,message,_,actual_tag,oids = runner.decode_wire_result(frames,include_types=True)
        record.update(rows=values,state=state,message=message,tag=actual_tag,oids=oids)
        check(state is None,'successful SQL',name=name,state=state,message=message)
        check(bool(frames) and frames[-1]==(b'Z',ready.encode()),'exact terminal Ready',name=name,expected=ready)
        if tag is not None:
            check(actual_tag==tag,'exact command tag',name=name,expected=tag,actual=actual_tag)
            check([body for kind,body in frames if kind==b'C']==[(tag+'\0').encode()], 'exact CommandComplete body',name=name)
        return values,oids,frames,record

    def full(record,frames,wanted,oids,mods):
        check(record['rows']==wanted,'complete ordered rows/cells/NULL',name=record['name'],expected=wanted,actual=record['rows'])
        check(record['oids']==oids,'complete six type OIDs',name=record['name'],expected=oids,actual=record['oids'])
        for kind,body in frames:
            if kind!=b'D':continue
            try:
                count=struct.unpack_from('!H',body)[0];position=2
                if count!=len(FIELDS):raise ValueError('DataRow field count')
                for _ in range(count):
                    length=struct.unpack_from('!i',body,position)[0];position+=4
                    if length < -1 or (length>=0 and length>len(body)-position):raise ValueError('DataRow length')
                    if length>=0:position+=length
                if position!=len(body):raise ValueError('DataRow tail')
            except (ValueError,struct.error) as error:
                check(False,'complete six-field DataRow frame',name=record['name'],error=repr(error))
        headers=[body for kind,body in frames if kind==b'T']
        check(len(headers)==1,'one full RowDescription',name=record['name'],actual=len(headers))
        if len(headers)==1:
            try:
                actual=description(headers[0]);record['row_description']=actual
                expected=[[name,table_oid,i+1,oid,size,mod,0] for i,(name,oid,size,mod) in enumerate(zip(FIELDS,oids,SIZES,mods))]
                check(actual==expected,'six descriptor tuples/all seven fields',name=record['name'],expected=expected,actual=actual)
            except (ValueError,IndexError,struct.error,UnicodeError) as error:
                check(False,'malformed descriptor',name=record['name'],error=repr(error))

    def explain(action):
        values,_,_,record=query(action['sql'],action['name']+'-explain',action['expected_ready'],action['peer'],False)
        nodes=[]
        def walk(node):
            if isinstance(node,dict):
                if node.get('nodeType')=='GiSTScan':nodes.append(node)
                for item in node.values():walk(item)
            elif isinstance(node,list):
                for item in node:walk(item)
        try:
            if len(values)!=1 or len(values[0])!=1:raise ValueError('one actual JSON cell required')
            walk(json.loads(values[0][0]))
            record['gist_nodes']=nodes
            expected=action['expected_used_index']
            if expected:
                check(bool(nodes) and any(n.get('usedIndex') is True for n in nodes),'actual GiST usedIndex true',name=action['name'],nodes=nodes)
            else:
                check(all(n.get('usedIndex') is False for n in nodes),'actual fallback/heap, never nodeType-only index evidence',name=action['name'],nodes=nodes)
        except (ValueError,TypeError,IndexError) as error:
            check(False,'complete parseable EXPLAIN JSON',name=action['name'],error=repr(error))

    saved_generations={}

    def image(action):
        heap=(Path(server['dir'])/'info'/(TABLE+'.dt')).read_bytes()
        for column,ordinal in [('k',1),('v',3)]:
            path=Path(server['dir'])/'info'/(TABLE+'_'+column+'.gist')
            raw=path.read_bytes() if path.is_file() else b''
            item=dict(phase=action['name'],column=column,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),hex=raw.hex(),
                      heap_bytes=len(heap),heap_sha256=hashlib.sha256(heap).hexdigest(),generation=None)
            trace['images'].append(item)
            if path.is_file():
                stat=path.stat();item['generation']=[stat.st_dev,stat.st_ino]
                prior=saved_generations.get(action['name'],{}).get(column)
                if prior is not None:
                    check(item['generation']!=prior,'actual new physical index generation',phase=action['name'],column=column,before=prior,after=item['generation'])
            try:
                version,entries=decode_gist(raw);item['version']=version
                check(version==3,'complete physical nonNULL generation V3',phase=action['name'],column=column,actual_version=version)
                expected={int(r[0]):r[ordinal].encode() for r in action['expected_rows'] if r[ordinal] is not None}
                witnessed={}
                for rid,lo,hi in entries:
                    ident,value=heap_key_witness(heap,rid,ordinal)
                    check(ident not in witnessed,'one current encoded key per logical row',phase=action['name'],column=column,id=ident)
                    witnessed[ident]=lo
                    check(lo==hi==value and value is not None,'real heap RID/NULL/key bytes witness',phase=action['name'],column=column,rid=rid,id=ident)
                item['witnesses']=[dict(id=i,value_hex=v.hex()) for i,v in sorted(witnessed.items())]
                check(witnessed==expected,'all nonNULL actual key bytes including every empty RID',phase=action['name'],column=column,expected_count=len(expected),actual_count=len(witnessed))
            except (ValueError,IndexError,struct.error,OverflowError) as error:
                check(False,'complete independent V2/V3 frame and real RID witness',phase=action['name'],column=column,error=repr(error))
        (evidence/(action['name']+'-heap.bin')).write_bytes(heap)

    def owner(process,name):
        stat=Path('/proc')/str(process.pid)/'stat'
        item=dict(name=name,pid=process.pid,pgid=os.getpgid(process.pid),start_ticks=stat.read_text().split()[21],
                  cmdline=(Path('/proc')/str(process.pid)/'cmdline').read_bytes().hex())
        trace['owners'].append(item)
        return item

    def cold_restart():
        nonlocal peer,server
        if peer is not None:peer.close();peer=None
        server['sock'].close();server['sock']=None
        old=server['process'];old.terminate();old.wait(timeout=10)
        trace['owners'][-1].update(exit=old.returncode,pid_gone=not Path('/proc',str(old.pid)).exists())
        check(old.returncode==0,'normal pre-cold server exit',actual=old.returncode)
        if old.returncode!=0:raise RuntimeError('actual lifetime failure; no retry or R15 investigation')
        process=subprocess.Popen([runner.DBMS_MAIN,'--data-dir',server['dir'],'--server',str(server['port']),'--insecure'],
                                 cwd=server['dir'],stdout=(evidence/'cold.stdout').open('wb'),stderr=(evidence/'cold.stderr').open('wb'),start_new_session=True)
        server['process']=process;owner(process,'cold')
        sock=socket.socket();sock.settimeout(runner.wire_timeout());server['sock']=sock
        deadline=time.monotonic()+20
        while True:
            try:sock.connect(('127.0.0.1',server['port']));break
            except OSError:
                if process.poll() is not None or time.monotonic()>=deadline:raise
                time.sleep(.05)
        client.startup(sock,'alice','info')

    try:
        server=runner.start_ours(client);owner(server['process'],'initial')
        for action in actions:
            trace['actions'].append(dict(name=action['name'],kind=action['kind']))
            if action['kind']=='connect-peer':
                peer=socket.create_connection(('127.0.0.1',server['port']),timeout=runner.wire_timeout());client.startup(peer,'alice','info')
            elif action['kind']=='cold-restart':cold_restart()
            elif action['kind']=='image':image(action)
            elif action['kind']=='generation-before':
                saved_generations[action['name']]={}
                for column in ('k','v'):
                    path=Path(server['dir'])/'info'/(TABLE+'_'+column+'.gist')
                    if path.is_file():
                        st=path.stat();saved_generations[action['name']][column]=[st.st_dev,st.st_ino]
            elif action['kind']=='anchor':
                values,oids,_,_=query(action['sql'],action['name'],shared=False,tag='SELECT 1')
                check(oids==[26] and len(values)==1 and len(values[0])==1 and values[0][0] is not None,'actual catalog table identity',actual=values,oids=oids)
                if len(values)==1 and len(values[0])==1 and values[0][0] is not None:
                    table_oids[action['table']]=int(values[0][0])
                    if action['table']==TABLE:table_oid=table_oids[TABLE]
            elif action['kind']=='explain':explain(action)
            elif action['kind']=='damage-controls':
                path=Path(server['dir'])/'info'/(TABLE+'_k.gist');original=path.read_bytes()
                # Each mutation is a distinct owned case, restored once, never a retry.
                try:
                    version,entries=decode_gist(original)
                    payload=b''.join(struct.pack('<QQQ',rid,len(lo),len(hi))+lo+hi for rid,lo,hi in entries if lo or hi)
                    v2=b'\0DBMSS\0002'+struct.pack('<IQ',2,sum(1 for _,lo,hi in entries if lo or hi))+payload
                    v2+=struct.pack('<I',crc32c(v2))
                    variants=[('legacy-V2-omitted-empty',v2),('bad-CRC',original[:-1]+bytes([original[-1]^1])),('missing',None)]
                    for name,raw in variants:
                        if raw is None:path.unlink()
                        else:path.write_bytes(raw)
                        trace['images'].append(dict(phase=name,column='k',hex=None if raw is None else raw.hex(),sha256=None if raw is None else hashlib.sha256(raw).hexdigest()))
                        sql='SELECT '+','.join(FIELDS)+' FROM '+TABLE+" WHERE k='' ORDER BY id"
                        wanted=[r.copy() for r in action['expected_rows'] if r[1]=='']
                        _,_,frames,record=query(sql,name,tag='SELECT '+str(len(wanted)),shared=False)
                        full(record,frames,wanted,action['oids'],action['mods'])
                        explain(dict(sql='EXPLAIN (ANALYZE,FORMAT JSON) '+sql,name=name,expected_used_index=False,expected_ready='I',peer=False))
                        path.write_bytes(original)
                finally:path.write_bytes(original)
            else:
                _,_,frames,record=query(action['sql'],action['name'],action['expected_ready'],action['peer'],action['shared_pg_sql'],action['expected_tag'])
                if action['expected_rows'] is not None:
                    check(record['rows']==action['expected_rows'],'every expected value/NULL',name=action['name'],expected=action['expected_rows'],actual=record['rows'])
                if action['full']:full(record,frames,action['expected_rows'],action['oids'],action['mods'])
                if action['name']=='default-locale-value':
                    expected=[['id',table_oids.get('ge_locale'),1,23,4,-1,0],['k',table_oids.get('ge_locale'),2,25,-1,-1,0]]
                    headers=[body for kind,body in frames if kind==b'T']
                    check(len(headers)==1 and description(headers[0])==expected,'default locale complete actual descriptor',actual=None if len(headers)!=1 else description(headers[0]),expected=expected)
        trace['cohort_complete']=True
    except BaseException as error:
        trace['harness_error']=dict(type=type(error).__name__,message=str(error))
    finally:
        if peer is not None:peer.close()
        if server is not None:
            process=server['process']
            try:runner.stop_ours(server)
            except BaseException as error:trace['stop_error']=dict(type=type(error).__name__,message=str(error))
            trace['server_exit']=process.returncode
            trace['owners'][-1].update(exit=process.returncode,pid_gone=not Path('/proc',str(process.pid)).exists())
        trace['complete']=trace['cohort_complete'] and not trace['failures'] and trace['harness_error'] is None and trace.get('server_exit')==0 and 'stop_error' not in trace
        (evidence/'trace-v1.json').write_text(json.dumps(trace,ensure_ascii=False,separators=(',',':'))+'\n')
    assert trace['complete'],dict(failures=trace['failures'],harness_error=trace['harness_error'],server_exit=trace.get('server_exit'))
    print('GIST_EMPTY_PROTOCOL commands='+str(len(trace['commands']))+' images='+str(len(trace['images']))+' complete_values_NULL_descriptors_V3_realRID_owners=1')


if __name__=='__main__':
    main()
