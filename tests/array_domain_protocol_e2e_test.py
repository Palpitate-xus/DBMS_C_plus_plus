#!/usr/bin/env python3
"""Validate whole-array domains through the ordinary SQL text protocol.

Pass --reference18 with a private PGREF endpoint to check the same expectations
on PostgreSQL 18.6. The project server uses a private data directory and is
restarted against the same files for the cold checks.
"""

import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from compat import pg_diff_runner as runner
from compat.regtype_projection_protocol import (
    group_members, interrupted, owns_listener, process_identity,
)


class ProjectServer:
    def __init__(self, client, data):
        self.client = client
        self.data = data
        self.process = None
        self.sock = None
        self.stops = []
        (data / 'info').mkdir()
        (data / 'info/tlist.lst').touch()
        client.write_auth_catalog(str(data), 'alice', 'secret')
        (data / 'pg_hba.conf').write_text(
            'host all alice 127.0.0.1/32 scram-sha-256\n', encoding='utf-8')

    def start(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        program = str(Path(runner.DBMS_MAIN).resolve())
        with (self.data / f'server-{len(self.stops)}.log').open('xb') as log:
            self.process = subprocess.Popen(
                [program, '--data-dir', str(self.data), '--server', str(port), '--insecure'],
                cwd=self.data, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        pid = self.process.pid
        deadline = time.monotonic() + 30
        while not owns_listener(pid, port):
            assert self.process.poll() is None, ('server exited', self.process.returncode)
            assert time.monotonic() < deadline, ('private listener timeout', pid)
            time.sleep(.05)
        assert (Path('/proc') / str(pid) / 'exe').resolve() == Path(program)
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=runner.wire_timeout())
        self.client.startup(self.sock, 'alice', 'info')
        return self.sock

    def stop(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None
        if self.process is None:
            return
        process, self.process = self.process, None
        pid = process.pid
        forced = False
        if process.poll() is None:
            process.terminate()
        try:
            code = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            forced = True
            owner = process_identity(pid)
            assert owner['pgid'] == owner['sid'] == pid
            os.killpg(pid, signal.SIGKILL)
            code = process.wait(timeout=20)
        remaining = group_members(pid)
        self.stops.append({'pid': pid, 'exit': code, 'forced': forced, 'group_after': remaining})
        assert code == 0 and not forced and not remaining, ('private server shutdown', self.stops)


def reference_connection(client):
    for key in ('PGREF_HOST', 'PGREF_PORT', 'PGREF_PASSWORD', 'PGREF_PID'):
        assert os.environ.get(key), f'{key} is required for the private reference'
    host, port, user, database, password = runner._reference_connection_settings()
    assert host == '127.0.0.1' and owns_listener(int(os.environ['PGREF_PID']), port)
    sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
    try:
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    except BaseException:
        sock.close()
        raise
    return sock


class Checks:
    def __init__(self, client, evidence):
        self.client = client
        self.evidence = evidence
        self.sock = None
        self.records = []

    def query(self, label, sql, *, tag, rows=None, oids=None, names=None, state=None,
              ready='I', wire_layout=None, capture_oid=False, require_success=False):
        messages = self.client.simple_query(self.sock, sql)
        actual_rows, status, message, headers, command_tag, actual_oids = (
            runner.decode_wire_result(messages, include_types=True))
        kinds = [kind for kind, _ in messages]
        fields = self.client.row_description_fields(messages) if b'T' in kinds else []
        matched = (status == state and command_tag == tag and
                   messages[-1] == (b'Z', ready.encode()) and kinds.count(b'Z') == 1)
        if state is not None:
            matched &= kinds.count(b'E') == 1 and not any(k in kinds for k in (b'D', b'C'))
            if b'T' in kinds and b'E' in kinds:
                matched &= kinds.index(b'T') < kinds.index(b'E')
        else:
            matched &= kinds.count(b'C') == 1 and b'E' not in kinds
        if oids is None:
            matched &= b'T' not in kinds and not actual_rows
        else:
            matched &= (kinds.count(b'T') == 1 and actual_oids == oids and
                        headers == names and all(field[6] == 0 for field in fields))
        if wire_layout is not None:
            matched &= [[field[4], field[5]] for field in fields] == wire_layout
        if not capture_oid:
            matched &= actual_rows == (rows or [])
        else:
            matched &= (len(actual_rows) == 1 and len(actual_rows[0]) == 1 and
                        actual_rows[0][0] is not None and actual_rows[0][0].isdigit() and
                        10000 <= int(actual_rows[0][0]) <= 4294967295)
        record = {
            'label': label, 'sql': sql, 'pass': bool(matched), 'rows': actual_rows,
            'types': actual_oids, 'headers': headers, 'state': status, 'message': message,
            'tag': command_tag, 'ready': messages[-1][1].decode(),
            'fields': [[field[0].decode(), *field[1:]] for field in fields],
            'expected': {'rows': rows, 'oids': oids, 'names': names, 'state': state,
                         'tag': tag, 'ready': ready, 'wire_layout': wire_layout,
                         'capture_oid': capture_oid},
        }
        if self.evidence:
            stem = self.evidence / f'{len(self.records):03d}-{label}'
            stem.with_suffix('.request.bin').write_bytes(self.client.typed(b'Q', sql.encode() + b'\0'))
            stem.with_suffix('.response.bin').write_bytes(
                b''.join(self.client.typed(kind, body) for kind, body in messages))
        self.records.append(record)
        if not matched:
            print('ARRAY_DOMAIN_DIFFERENCE', json.dumps(record), flush=True)
        if capture_oid or require_success:
            assert status is None, ('required setup failed', record)
        if capture_oid:
            assert matched, ('actual catalog identity required', record)
            return actual_rows[0][0]
        return None


def workload(checks, prefix, reconnect):
    q = checks.query
    refs, numbers, child, strings, limited, table, rolled = (
        f'{prefix}_{suffix}' for suffix in (
            'refs', 'numbers', 'child', 'strings', 'limited', 'rows', 'rolled'))

    def ddl(label, sql, tag, ready='I'):
        q(label, sql, tag=tag, ready=ready, require_success=True)

    def select(label, sql, rows, oids, names, ready='I', wire_layout=None):
        q(label, sql, tag=f'SELECT {len(rows)}', rows=rows, oids=oids, names=names,
          ready=ready, wire_layout=wire_layout)

    def oid(label, catalog, key, name):
        return q(label, f"SELECT oid FROM {catalog} WHERE {key}='{name}'", tag='SELECT 1',
                 names=['oid'], oids=[26], capture_oid=True)

    def type_metadata(label, domain, base, dims, notnull, type_modifier='-1'):
        names = ['typbasetype', 'typndims', 'typlen', 'typbyval', 'typcategory',
                 'typelem', 'typnotnull', 'typtypmod', 'typtype', 'typstorage']
        select(label, f"SELECT {','.join(names)} FROM pg_type WHERE typname='{domain}'",
               [[base, dims, '-1', 'f', 'A', '0', notnull, type_modifier, 'd', 'x']],
               [26, 23, 21, 16, 18, 26, 16, 23, 18, 18], names)

    ddl('create-refs', f'CREATE DOMAIN {refs} AS pg_catalog.regtype[]', 'CREATE DOMAIN')
    refs_oid = oid('refs-identity', 'pg_type', 'typname', refs)
    type_metadata('refs-base', refs, '2211', '1', 'f')
    ddl('create-numbers', f"CREATE DOMAIN {numbers} AS integer[] DEFAULT '{{3,NULL,5}}' NOT NULL "
        'CONSTRAINT at_most_three CHECK (cardinality(VALUE) <= 3)', 'CREATE DOMAIN')
    numbers_oid = oid('numbers-identity', 'pg_type', 'typname', numbers)
    type_metadata('numbers-base', numbers, '1007', '1', 't')
    ddl('create-child', f'CREATE DOMAIN {child} AS {numbers} '
        'CONSTRAINT at_least_two CHECK (cardinality(VALUE) >= 2)', 'CREATE DOMAIN')
    child_oid = oid('child-identity', 'pg_type', 'typname', child)
    type_metadata('child-base', child, numbers_oid, '0', 'f')
    ddl('create-strings', f'CREATE DOMAIN {strings} AS text[][]', 'CREATE DOMAIN')
    strings_oid = oid('strings-identity', 'pg_type', 'typname', strings)
    type_metadata('strings-base', strings, '1009', '2', 'f')
    ddl('create-limited', f'CREATE DOMAIN {limited} AS character varying(2)[]', 'CREATE DOMAIN')
    limited_oid = oid('limited-identity', 'pg_type', 'typname', limited)
    type_metadata('limited-base', limited, '1015', '1', 'f', type_modifier='6')
    assert len({refs_oid, numbers_oid, child_oid, strings_oid, limited_oid}) == 5

    ddl('create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY, counts {child}, '
        f'refs {refs}, words {strings}, labels {limited})', 'CREATE TABLE')
    table_oid = oid('relation-identity', 'pg_class', 'relname', table)

    def attribute_metadata(label):
        names = ['attname', 'atttypid', 'attndims', 'attlen', 'atttypmod']
        select(label, f"SELECT {','.join(names)} FROM pg_attribute WHERE attrelid={table_oid} "
               'AND attnum>0 ORDER BY attnum',
               [['id', '23', '0', '4', '-1'], ['counts', child_oid, '0', '-1', '-1'],
                ['refs', refs_oid, '0', '-1', '-1'], ['words', strings_oid, '0', '-1', '-1'],
                ['labels', limited_oid, '0', '-1', '-1']],
               [19, 26, 21, 21, 23], names)

    attribute_metadata('attribute-metadata')
    names = ['id', 'counts', 'refs', 'words', 'labels']
    oids = [23, 1007, 2211, 1009, 1015]
    wire_layout = [[4, -1], [-1, -1], [-1, -1], [-1, -1], [-1, 6]]
    projection = f'SELECT * FROM {table} ORDER BY id'
    select('empty-projection', projection, [], oids, names, wire_layout=wire_layout)
    first = ['1', '{3,NULL,5}', None, None, None]
    q('default-insert', f'INSERT INTO {table}(id) VALUES (1) RETURNING *',
      tag='INSERT 0 1', rows=[first], oids=oids, names=names, wire_layout=wire_layout)
    second = ['2', '[-1:0]={1,2}', '[0:2]={integer,NULL,-}', '{"NULL",NULL,""}', '{xy,NULL,z}']
    q('explicit-insert', f"INSERT INTO {table} VALUES (2,'[-1:0]={{1,2}}',"
      "'[0:2]={integer,NULL,-}','{\"NULL\",NULL,\"\"}','{xy,NULL,z}')", tag='INSERT 0 1')
    select('stored-projection', projection, [first, second], oids, names, wire_layout=wire_layout)
    select('actual-domain-type', f'SELECT pg_typeof(counts)::oid AS counts_type FROM {table} WHERE id=2',
           [[child_oid]], [26], ['counts_type'])
    select('array-expression', f'SELECT array_dims(counts) AS dims,cardinality(counts) AS items, '
           f'counts[-1] AS first,refs::text AS ref_text FROM {table} WHERE id=2',
           [['[-1:0]', '2', '1', second[2]]], [25, 23, 23, 25], ['dims', 'items', 'first', 'ref_text'])
    select('explicit-domain-cast', f"SELECT '{{2,3}}'::{child} AS counts", [['{2,3}']], [1007], ['counts'])
    q('explicit-domain-cast-check', f"SELECT '{{2}}'::{child} AS counts",
      tag=None, oids=[1007], names=['counts'], state='23514')

    updated = ['1', '{{7,NULL,8}}', '{text,4294967295}', '{}', None]
    q('array-update', f"UPDATE {table} SET counts='{{{{7,NULL,8}}}}',refs='{{text,4294967295}}',"
      "words='{}' WHERE id=1 RETURNING *", tag='UPDATE 1', rows=[updated], oids=oids, names=names,
      wire_layout=wire_layout)
    q('base-check-rejects', f"INSERT INTO {table}(id,counts) VALUES(3,'{{1,2,3,4}}')", tag=None, state='23514')
    q('child-check-rejects', f"UPDATE {table} SET counts='{{1}}' WHERE id=2", tag=None, state='23514')
    q('null-rejects', f'UPDATE {table} SET counts=NULL WHERE id=2', tag=None, state='23502')
    q('invalid-element-rejects', f"INSERT INTO {table}(id,counts) VALUES(3,'{{1,nope}}')",
      tag=None, state='22P02')
    q('typmod-rejects', f"UPDATE {table} SET labels='{{abc}}' WHERE id=2", tag=None, state='22001')
    select('outer-null', f'SELECT b.counts,b.refs FROM {table} a LEFT JOIN {table} b ON b.id=-1 WHERE a.id=1',
           [[None, None]], [1007, 2211], ['counts', 'refs'])
    select('ordered-distinct-refs', f'SELECT DISTINCT refs FROM {table} ORDER BY refs',
           [[second[2]], [updated[2]]], [2211], ['refs'])

    ddl('begin', 'BEGIN', 'BEGIN', 'T')
    ddl('savepoint', 'SAVEPOINT hold_array', 'SAVEPOINT', 'T')
    q('insert-select', f'INSERT INTO {table} SELECT 3,counts,refs,words,labels '
      f'FROM {table} WHERE id=2', tag='INSERT 0 1', ready='T')
    select('copied-domain-values', f'SELECT * FROM {table} WHERE id=3',
           [['3', *second[1:]]], oids, names, ready='T')
    ddl('rollback-copy', 'ROLLBACK TO SAVEPOINT hold_array', 'ROLLBACK', 'T')
    q('atomic-insert-error', f"INSERT INTO {table}(id,counts) VALUES(3,'{{1,2}}'),(4,'{{0}}')",
      tag=None, state='23514', ready='E')
    q('failed-transaction', projection, tag=None, state='25P02', ready='E')
    ddl('rollback-error', 'ROLLBACK TO SAVEPOINT hold_array', 'ROLLBACK', 'T')
    select('failed-insert-left-no-row', projection, [updated, second], oids, names, ready='T')
    ddl('create-rolled-domain', f'CREATE DOMAIN {rolled} AS text[]', 'CREATE DOMAIN', 'T')
    q('update-with-default', f'UPDATE {table} SET counts=DEFAULT WHERE id=1', tag='UPDATE 1', ready='T')
    rolled_default = ['1', first[1], *updated[2:]]
    select('default-in-transaction', projection, [rolled_default, second], oids, names, ready='T')
    ddl('rollback-ddl-and-data', 'ROLLBACK TO SAVEPOINT hold_array', 'ROLLBACK', 'T')
    select('rolled-domain-removed', f"SELECT oid FROM pg_type WHERE typname='{rolled}'",
           [], [26], ['oid'], ready='T')
    ddl('commit-after-rollback', 'COMMIT', 'COMMIT')

    reconnect()
    attribute_metadata('attribute-metadata-reopen')
    type_metadata('refs-base-reopen', refs, '2211', '1', 'f')
    type_metadata('child-base-reopen', child, numbers_oid, '0', 'f')
    type_metadata('limited-base-reopen', limited, '1015', '1', 'f', type_modifier='6')
    select('reopened-data', projection, [updated, second], oids, names, wire_layout=wire_layout)
    q('reopened-constraint', f"UPDATE {table} SET counts='{{0}}' WHERE id=2", tag=None, state='23514')
    q('dependent-drop', f'DROP DOMAIN {refs}', tag=None, state='2BP01')
    ddl('drop-table', f'DROP TABLE {table}', 'DROP TABLE')
    for name in (child, numbers, refs, strings, limited):
        ddl(f'drop-{name.rsplit("_", 1)[1]}', f'DROP DOMAIN {name}', 'DROP DOMAIN')


def check_assignment_evaluates_once(checks, prefix):
    q = checks.query
    sequence = f'{prefix}_check_seq'
    domain, table = f'{prefix}_check_domain', f'{prefix}_check_rows'

    def counter(label, expected):
        q(label, f"SELECT currval('{sequence}') AS calls", tag='SELECT 1',
          rows=[[str(expected)]], names=['calls'], oids=[20])

    q('once-create-sequence', f'CREATE SEQUENCE {sequence}', tag='CREATE SEQUENCE',
      require_success=True)
    q('once-create-domain', f"CREATE DOMAIN {domain} AS integer[] CHECK (nextval('{sequence}') > 0)",
      tag='CREATE DOMAIN', require_success=True)
    q('once-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain})',
      tag='CREATE TABLE', require_success=True)
    q('once-implicit-domain-assignment', f"INSERT INTO {table} VALUES(1,'{{1}}')", tag='INSERT 0 1')
    counter('once-implicit-check', 1)
    q('once-explicit-domain-cast', f'INSERT INTO {table} VALUES(2,ARRAY[2]::{domain})',
      tag='INSERT 0 1')
    counter('once-explicit-check', 2)
    q('once-unrelated-update', f'UPDATE {table} SET id=3 WHERE id=2', tag='UPDATE 1')
    counter('once-unchanged-not-rechecked', 2)
    q('once-bare-projection', f'SELECT v FROM {table} WHERE id=3', tag='SELECT 1',
      rows=[['{2}']], names=['v'], oids=[1007])
    counter('once-read-not-rechecked', 2)
    q('once-explicit-domain-update', f'UPDATE {table} SET v=ARRAY[4]::{domain} WHERE id=3', tag='UPDATE 1')
    counter('once-update-cast-check', 3)
    q('once-same-domain-update', f'UPDATE {table} SET v=v WHERE id=3', tag='UPDATE 1')
    counter('once-same-domain-update-no-check', 3)
    q('once-same-domain-insert-select', f'INSERT INTO {table} SELECT 4,v FROM {table} WHERE id=3',
      tag='INSERT 0 1')
    counter('once-same-domain-copy-no-check', 3)
    q('once-stored-values', f'SELECT * FROM {table} ORDER BY id', tag='SELECT 3',
      rows=[['1', '{1}'], ['3', '{4}'], ['4', '{4}']], names=['id', 'v'], oids=[23, 1007])
    q('once-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('once-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')
    q('once-drop-sequence', f'DROP SEQUENCE {sequence}', tag='DROP SEQUENCE')


def check_slice_assignment(checks, prefix):
    q = checks.query
    domain, table = f'{prefix}_slice_domain', f'{prefix}_slice_rows'
    projection = f'SELECT id,v FROM {table} ORDER BY id'

    def stored_value(label, value, ready='I'):
        q(label, projection, tag='SELECT 1', rows=[['1', value]], names=['id', 'v'],
          oids=[23, 1007], ready=ready)

    q('slice-create-domain', f'CREATE DOMAIN {domain} AS integer[] CHECK(cardinality(VALUE)>=2)',
      tag='CREATE DOMAIN', require_success=True)
    q('slice-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain})',
      tag='CREATE TABLE', require_success=True)
    q('slice-seed', f"INSERT INTO {table} VALUES(1,'{{1,2,3}}')", tag='INSERT 0 1')
    q('slice-return', f'SELECT v[1:1] AS smaller FROM {table}', tag='SELECT 1',
      rows=[['{1}']], names=['smaller'], oids=[1007])
    q('slice-type', f'SELECT pg_typeof(v[1:1])::oid AS slice_type FROM {table}', tag='SELECT 1',
      rows=[['1007']], names=['slice_type'], oids=[26])
    q('slice-update-must-check', f'UPDATE {table} SET v=v[1:1] WHERE id=1', tag=None, state='23514')
    stored_value('slice-failed-update-keeps-row', '{1,2,3}')
    q('slice-cast-must-check', f'SELECT v[1:1]::{domain} AS invalid_slice FROM {table}',
      tag=None, state='23514', names=['invalid_slice'], oids=[1007], wire_layout=[[-1, -1]])
    stored_value('slice-failed-cast-keeps-row', '{1,2,3}')
    q('slice-copy-must-check', f'INSERT INTO {table} SELECT 2,v[1:1] FROM {table} WHERE id=1',
      tag=None, state='23514')
    stored_value('slice-failed-copy-keeps-original', '{1,2,3}')
    q('slice-begin', 'BEGIN', tag='BEGIN', ready='T', require_success=True)
    q('slice-savepoint', 'SAVEPOINT keep_value', tag='SAVEPOINT', ready='T', require_success=True)
    q('slice-transaction-update-must-check', f'UPDATE {table} SET v=v[1:1] WHERE id=1',
      tag=None, state='23514', ready='E')
    q('slice-restore-savepoint', 'ROLLBACK TO SAVEPOINT keep_value', tag='ROLLBACK', ready='T')
    stored_value('slice-rollback-keeps-row', '{1,2,3}', ready='T')
    q('slice-valid-cast', f'SELECT v[2:3]::{domain} AS valid_slice FROM {table}', tag='SELECT 1',
      rows=[['{2,3}']], names=['valid_slice'], oids=[1007], ready='T')
    q('slice-valid-update', f'UPDATE {table} SET v=v[2:3] WHERE id=1 RETURNING v', tag='UPDATE 1',
      rows=[['{2,3}']], names=['v'], oids=[1007], ready='T')
    q('slice-commit', 'COMMIT', tag='COMMIT')
    stored_value('slice-committed-value', '{2,3}')
    q('slice-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('slice-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')


def check_nullif_assignment(checks, prefix):
    q = checks.query
    for suffix, constraint, state in [('nn', 'NOT NULL', '23502'),
                                      ('ck', 'CHECK(VALUE IS NOT NULL)', '23514')]:
        domain, table = f'{prefix}_{suffix}_domain', f'{prefix}_{suffix}_rows'
        tag = 'nullif-' + suffix
        q(f'{tag}-create-domain', f'CREATE DOMAIN {domain} AS integer[] {constraint}',
          tag='CREATE DOMAIN', require_success=True)
        q(f'{tag}-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain})',
          tag='CREATE TABLE', require_success=True)
        q(f'{tag}-seed', f"INSERT INTO {table} VALUES(1,'{{1,2}}')", tag='INSERT 0 1')
        q(f'{tag}-result-type', f'SELECT pg_typeof(NULLIF(v,v))::oid AS nullif_type FROM {table}',
          tag='SELECT 1', rows=[['1007']], names=['nullif_type'], oids=[26])
        q(f'{tag}-equal-values', f'SELECT NULLIF(v,v) AS absent FROM {table}',
          tag='SELECT 1', rows=[[None]], names=['absent'], oids=[1007], wire_layout=[[-1, -1]])
        q(f'{tag}-unequal-values', f"SELECT NULLIF(v,'{{3}}'::integer[]) AS present FROM {table}",
          tag='SELECT 1', rows=[['{1,2}']], names=['present'], oids=[1007], wire_layout=[[-1, -1]])
        q(f'{tag}-explicit-null-control', f'SELECT NULL::{domain} AS absent',
          tag=None, state=state, names=['absent'], oids=[1007], wire_layout=[[-1, -1]])
        q(f'{tag}-recast-must-check', f'SELECT NULLIF(v,v)::{domain} AS absent FROM {table}',
          tag=None, state=state, names=['absent'], oids=[1007], wire_layout=[[-1, -1]])
        q(f'{tag}-update-must-check', f'UPDATE {table} SET v=NULLIF(v,v) WHERE id=1',
          tag=None, state=state)
        q(f'{tag}-original-preserved', f'SELECT id,v FROM {table} ORDER BY id',
          tag='SELECT 1', rows=[['1', '{1,2}']], names=['id', 'v'], oids=[23, 1007])
        q(f'{tag}-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
        q(f'{tag}-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')


def check_conditional_assignment(checks, prefix):
    q = checks.query
    sequence = f'{prefix}_cond_seq'
    domain, table = f'{prefix}_cond_domain', f'{prefix}_cond_rows'

    def counter(label, expected):
        q(label, f"SELECT currval('{sequence}') AS calls", tag='SELECT 1',
          rows=[[str(expected)]], names=['calls'], oids=[20])

    q('cond-create-sequence', f'CREATE SEQUENCE {sequence}', tag='CREATE SEQUENCE', require_success=True)
    q('cond-create-domain', f"CREATE DOMAIN {domain} AS integer[] "
      f"CHECK(VALUE IS NULL OR nextval('{sequence}')>0)", tag='CREATE DOMAIN', require_success=True)
    q('cond-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain},a integer[])',
      tag='CREATE TABLE', require_success=True)
    q('cond-seed', f"INSERT INTO {table} VALUES(1,'{{1,2}}','{{3,4}}'),(2,NULL,'{{5,6}}')", tag='INSERT 0 2')
    counter('cond-initial-check', 1)

    case = 'CASE WHEN id=1 THEN v ELSE a END'
    coalesce = 'COALESCE(v,a)'
    q('cond-mixed-types', f'SELECT pg_typeof({case})::oid AS case_type, '
      f'pg_typeof({coalesce})::oid AS coalesce_type FROM {table} ORDER BY id',
      tag='SELECT 2', rows=[['1007', '1007'], ['1007', '1007']],
      names=['case_type', 'coalesce_type'], oids=[26, 26])
    q('cond-mixed-reads', f'SELECT {case} AS candidate, {coalesce} AS fallback FROM {table} ORDER BY id',
      tag='SELECT 2', rows=[['{1,2}', '{1,2}'], ['{5,6}', '{5,6}']],
      names=['candidate', 'fallback'], oids=[1007, 1007])
    counter('cond-reads-no-check', 1)
    q('cond-case-update', f'UPDATE {table} SET v={case} WHERE id=1', tag='UPDATE 1')
    counter('cond-case-must-check', 2)
    q('cond-coalesce-update', f'UPDATE {table} SET v={coalesce} WHERE id=1', tag='UPDATE 1')
    counter('cond-coalesce-must-check', 3)
    q('cond-coalesce-null-branch', f'UPDATE {table} SET v={coalesce} WHERE id=2', tag='UPDATE 1')
    counter('cond-base-branch-must-check', 4)
    q('cond-case-recast', f'SELECT ({case})::{domain} AS v FROM {table} WHERE id=1',
      tag='SELECT 1', rows=[['{1,2}']], names=['v'], oids=[1007], wire_layout=[[-1, -1]])
    counter('cond-case-cast-must-check', 5)
    q('cond-coalesce-recast', f'SELECT {coalesce}::{domain} AS v FROM {table} WHERE id=1',
      tag='SELECT 1', rows=[['{1,2}']], names=['v'], oids=[1007], wire_layout=[[-1, -1]])
    counter('cond-coalesce-cast-must-check', 6)
    q('cond-stored-values', f'SELECT id,v,a FROM {table} ORDER BY id', tag='SELECT 2',
      rows=[['1', '{1,2}', '{3,4}'], ['2', '{5,6}', '{5,6}']], names=['id', 'v', 'a'],
      oids=[23, 1007, 1007])
    q('cond-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('cond-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')
    q('cond-drop-sequence', f'DROP SEQUENCE {sequence}', tag='DROP SEQUENCE')


def check_same_domain_conditionals(checks, prefix):
    q = checks.query
    sequence = f'{prefix}_same_seq'
    domain, table = f'{prefix}_same_domain', f'{prefix}_same_rows'

    def unchanged_counter(label):
        q(label, f"SELECT currval('{sequence}') AS calls", tag='SELECT 1',
          rows=[['2']], names=['calls'], oids=[20])

    q('same-create-sequence', f'CREATE SEQUENCE {sequence}', tag='CREATE SEQUENCE', require_success=True)
    q('same-create-domain', f"CREATE DOMAIN {domain} AS integer[] CHECK(nextval('{sequence}')>0)",
      tag='CREATE DOMAIN', require_success=True)
    oid = q('same-domain-oid', f"SELECT oid FROM pg_type WHERE typname='{domain}'", tag='SELECT 1',
            oids=[26], names=['oid'], capture_oid=True)
    q('same-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain},other {domain})',
      tag='CREATE TABLE', require_success=True)
    q('same-seed', f"INSERT INTO {table} VALUES(1,'{{1,2}}','{{3,4}}')", tag='INSERT 0 1')
    unchanged_counter('same-seed-checks')

    case = 'CASE WHEN id=1 THEN v ELSE other END'
    coalesce = 'COALESCE(v,other)'
    q('same-result-types', f'SELECT pg_typeof({case})::oid AS case_type, '
      f'pg_typeof({coalesce})::oid AS coalesce_type FROM {table} WHERE id=1',
      tag='SELECT 1', rows=[[oid, oid]], oids=[26, 26], names=['case_type', 'coalesce_type'])
    q('same-case-update', f'UPDATE {table} SET v={case} WHERE id=1', tag='UPDATE 1')
    unchanged_counter('same-case-does-not-recheck')
    q('same-coalesce-update', f'UPDATE {table} SET v={coalesce} WHERE id=1', tag='UPDATE 1')
    unchanged_counter('same-coalesce-does-not-recheck')
    q('same-case-cast', f'SELECT ({case})::{domain} AS v FROM {table} WHERE id=1',
      tag='SELECT 1', rows=[['{1,2}']], oids=[1007], names=['v'])
    unchanged_counter('same-case-cast-does-not-recheck')
    q('same-coalesce-cast', f'SELECT {coalesce}::{domain} AS v FROM {table} WHERE id=1',
      tag='SELECT 1', rows=[['{1,2}']], oids=[1007], names=['v'])
    unchanged_counter('same-coalesce-cast-does-not-recheck')
    q('same-copy', f'INSERT INTO {table} SELECT 2,{case},{coalesce} FROM {table} WHERE id=1',
      tag='INSERT 0 1')
    unchanged_counter('same-copy-does-not-recheck')
    q('same-stored-copies', f'SELECT id,v,other FROM {table} ORDER BY id', tag='SELECT 2',
      rows=[['1', '{1,2}', '{3,4}'], ['2', '{1,2}', '{1,2}']], names=['id', 'v', 'other'],
      oids=[23, 1007, 1007])
    q('same-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('same-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')
    q('same-drop-sequence', f'DROP SEQUENCE {sequence}', tag='DROP SEQUENCE')


def check_declared_element_widths(checks, prefix):
    q = checks.query
    char, bit, varbit, table = (f'{prefix}_{suffix}' for suffix in
                               ('width_char', 'width_bit', 'width_varbit', 'width_rows'))

    def cells(label, name, oid, modifier, value, row=1):
        q(label, f'SELECT {name} FROM {table} WHERE id={row}', tag='SELECT 1',
          names=[name], oids=[oid], rows=[[value]], wire_layout=[[-1, modifier]])

    q('width-create-char', f"CREATE DOMAIN {char} AS character(3)[] DEFAULT '{{xy}}'",
      tag='CREATE DOMAIN', require_success=True)
    q('width-create-bit', f'CREATE DOMAIN {bit} AS bit(3)[]', tag='CREATE DOMAIN', require_success=True)
    q('width-create-varbit', f'CREATE DOMAIN {varbit} AS bit varying(3)[]',
      tag='CREATE DOMAIN', require_success=True)
    q('width-create-table', f'CREATE TABLE {table}(id int PRIMARY KEY,c {char},b {bit},v {varbit})',
      tag='CREATE TABLE', require_success=True)
    q('width-insert', f"INSERT INTO {table} VALUES (1,'{{a,ab,abc,NULL}}',"
      "'{101,011,NULL}','{0,01,101,NULL}')", tag='INSERT 0 1')
    cells('width-inserted-char', 'c', 1014, 7, '{"a  ","ab ",abc,NULL}')
    cells('width-inserted-bit', 'b', 1561, 3, '{101,011,NULL}')
    cells('width-inserted-varbit', 'v', 1563, 3, '{0,01,101,NULL}')
    q('width-default-char', f'INSERT INTO {table}(id) VALUES (2)', tag='INSERT 0 1')
    cells('width-default-char-read', 'c', 1014, 7, '{"xy "}', row=2)
    q('width-update-char', f"UPDATE {table} SET c='[0:1]={{z,za}}' WHERE id=1", tag='UPDATE 1')
    q('width-update-bit', f"UPDATE {table} SET b='{{000,001}}' WHERE id=1", tag='UPDATE 1')
    q('width-update-varbit', f"UPDATE {table} SET v='{{10,110}}' WHERE id=1", tag='UPDATE 1')
    cells('width-updated-char', 'c', 1014, 7, '[0:1]={"z  ","za "}')
    cells('width-updated-bit', 'b', 1561, 3, '{000,001}')
    cells('width-updated-varbit', 'v', 1563, 3, '{10,110}')
    q('width-bad-insert-char', f"INSERT INTO {table}(id,c) VALUES(3,'{{toolong}}')", tag=None, state='22001')
    q('width-bad-insert-bit', f"INSERT INTO {table}(id,b) VALUES(4,'{{11}}')", tag=None, state='22026')
    q('width-bad-insert-varbit', f"INSERT INTO {table}(id,v) VALUES(5,'{{1101}}')", tag=None, state='22001')
    q('width-bad-update-char', f"UPDATE {table} SET c='{{toolong}}' WHERE id=1", tag=None, state='22001')
    q('width-bad-update-bit', f"UPDATE {table} SET b='{{1111}}' WHERE id=1", tag=None, state='22026')
    q('width-bad-update-varbit', f"UPDATE {table} SET v='{{1101}}' WHERE id=1", tag=None, state='22001')
    q('width-row-count', f'SELECT count(*) AS total FROM {table}', tag='SELECT 1',
      rows=[['2']], names=['total'], oids=[20])
    cells('width-preserved-char', 'c', 1014, 7, '[0:1]={"z  ","za "}')
    cells('width-preserved-bit', 'b', 1561, 3, '{000,001}')
    cells('width-preserved-varbit', 'v', 1563, 3, '{10,110}')
    q('width-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    for suffix, name in [('char', char), ('bit', bit), ('varbit', varbit)]:
        q(f'width-drop-{suffix}', f'DROP DOMAIN {name}', tag='DROP DOMAIN')


def check_same_domain_case_paths(checks, prefix):
    q = checks.query
    sequence = f'{prefix}_case_path_seq'
    domain, table = f'{prefix}_case_path_domain', f'{prefix}_case_path_rows'

    def unchanged_counter(label):
        q(label, f"SELECT currval('{sequence}') AS calls", tag='SELECT 1',
          rows=[['2']], names=['calls'], oids=[20])

    q('case-path-sequence', f'CREATE SEQUENCE {sequence}', tag='CREATE SEQUENCE', require_success=True)
    q('case-path-domain', f"CREATE DOMAIN {domain} AS integer[] CHECK(nextval('{sequence}')>0)",
      tag='CREATE DOMAIN', require_success=True)
    q('case-path-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain},other {domain})',
      tag='CREATE TABLE', require_success=True)
    q('case-path-seed', f"INSERT INTO {table} VALUES(1,'{{1,2}}','{{3,4}}')", tag='INSERT 0 1')
    unchanged_counter('case-path-seed-checks')

    case = 'CASE WHEN id=1 THEN v ELSE other END'
    forms = [
        ('bare-column-cast', f'v::{domain}'),
        ('standard-outer-cast', f'CAST({case} AS {domain})'),
        ('cast-arms', f'CASE WHEN id=1 THEN v::{domain} ELSE other::{domain} END'),
    ]
    for label, expression in forms:
        q(f'case-path-{label}', f'SELECT {expression} AS v FROM {table} WHERE id=1',
          tag='SELECT 1', rows=[['{1,2}']], names=['v'], oids=[1007])
        unchanged_counter(f'case-path-{label}-without-new-check')
    q('case-path-insert-select-cast', f'INSERT INTO {table} SELECT 2,({case})::{domain},other '
      f'FROM {table} WHERE id=1', tag='INSERT 0 1')
    unchanged_counter('case-path-insert-select-without-new-check')
    q('case-path-update-cast', f'UPDATE {table} SET v=({case})::{domain} WHERE id=1', tag='UPDATE 1')
    unchanged_counter('case-path-update-without-new-check')
    q('case-path-stored-rows', f'SELECT id,v,other FROM {table} ORDER BY id', tag='SELECT 2',
      rows=[['1', '{1,2}', '{3,4}'], ['2', '{1,2}', '{3,4}']], names=['id', 'v', 'other'],
      oids=[23, 1007, 1007])
    q('case-path-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('case-path-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')
    q('case-path-drop-sequence', f'DROP SEQUENCE {sequence}', tag='DROP SEQUENCE')


def check_default_input_validation(checks, prefix):
    domain = prefix + '_default_domain'
    invalid_domain = prefix + '_bad_default_domain'
    table = prefix + '_default_rows'
    sequence = prefix + '_default_seq'
    q = checks.query

    q('default-bad-create', f"CREATE DOMAIN {invalid_domain} AS integer[] DEFAULT '{{bad}}'",
      tag=None, state='22P02')
    q('default-bad-domain-absent', f"SELECT typname FROM pg_type WHERE typname='{invalid_domain}'",
      tag='SELECT 0', rows=[], names=['typname'], oids=[19])
    q('default-domain', f"CREATE DOMAIN {domain} AS integer[] DEFAULT '{{1,2}}' "
      'CHECK(cardinality(VALUE)>=2)', tag='CREATE DOMAIN', require_success=True)
    q('default-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,v {domain})', tag='CREATE TABLE',
      require_success=True)
    q('default-seed', f'INSERT INTO {table}(id) VALUES(1)', tag='INSERT 0 1')
    q('default-transaction', 'BEGIN', tag='BEGIN', ready='T')
    q('default-savepoint', 'SAVEPOINT before_invalid_default', tag='SAVEPOINT', ready='T')
    q('default-bad-alter', f"ALTER DOMAIN {domain} SET DEFAULT '{{bad}}'", tag=None,
      state='22P02', ready='E')
    q('default-rollback', 'ROLLBACK TO before_invalid_default', tag='ROLLBACK', ready='T')
    q('default-old-value', f'INSERT INTO {table}(id) VALUES(2)', tag='INSERT 0 1', ready='T')
    q('default-commit', 'COMMIT', tag='COMMIT')
    q('default-check-at-write', f"ALTER DOMAIN {domain} SET DEFAULT '{{}}'", tag='ALTER DOMAIN')
    q('default-check-on-insert', f'INSERT INTO {table}(id) VALUES(3)', tag=None, state='23514')
    q('default-sequence', f'CREATE SEQUENCE {sequence}', tag='CREATE SEQUENCE', require_success=True)
    q('default-sequence-start', f"SELECT nextval('{sequence}') AS n", tag='SELECT 1',
      rows=[['1']], names=['n'], oids=[20])
    q('default-volatile-declaration', f"ALTER DOMAIN {domain} SET DEFAULT ARRAY[nextval('{sequence}')::int,9]",
      tag='ALTER DOMAIN')
    q('default-volatile-deferred', f"SELECT currval('{sequence}') AS n", tag='SELECT 1',
      rows=[['1']], names=['n'], oids=[20])
    q('default-volatile-write', f'INSERT INTO {table}(id) VALUES(4)', tag='INSERT 0 1')
    q('default-volatile-called-once', f"SELECT currval('{sequence}') AS n", tag='SELECT 1',
      rows=[['2']], names=['n'], oids=[20])
    q('default-results', f'SELECT id,v FROM {table} ORDER BY id', tag='SELECT 3',
      rows=[['1', '{1,2}'], ['2', '{1,2}'], ['4', '{2,9}']], names=['id', 'v'], oids=[23, 1007])
    q('default-drop-table', f'DROP TABLE {table}', tag='DROP TABLE')
    q('default-drop-domain', f'DROP DOMAIN {domain}', tag='DROP DOMAIN')
    q('default-drop-sequence', f'DROP SEQUENCE {sequence}', tag='DROP SEQUENCE')


def main():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    reference = sys.argv[1:] == ['--reference18']
    assert reference or not sys.argv[1:], 'expected --reference18 or no arguments'
    client = runner.load_protocol_client()
    evidence_env = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
    evidence = Path(evidence_env) if evidence_env else None
    if evidence is not None:
        evidence.mkdir(parents=True, exist_ok=False)
    prefix = 'arrdomain_' + uuid.uuid4().hex[:12]
    checks = Checks(client, evidence)
    with tempfile.TemporaryDirectory(prefix='dbms-array-domain-') as directory:
        server = None if reference else ProjectServer(client, Path(directory))

        def reconnect():
            if server is not None:
                server.stop()
                checks.sock = server.start()
            else:
                if checks.sock:
                    checks.sock.close()
                    checks.sock = None
                checks.sock = reference_connection(client)

        checks_passed = shutdown_passed = False
        try:
            reconnect()
            workload(checks, prefix, reconnect)
            check_assignment_evaluates_once(checks, prefix)
            check_slice_assignment(checks, prefix)
            check_nullif_assignment(checks, prefix)
            check_conditional_assignment(checks, prefix)
            check_same_domain_conditionals(checks, prefix)
            check_declared_element_widths(checks, prefix)
            check_same_domain_case_paths(checks, prefix)
            check_default_input_validation(checks, prefix)
            assert all(record['pass'] for record in checks.records), 'array domain protocol differences'
            checks_passed = True
        finally:
            try:
                if server:
                    server.stop()
                elif checks.sock:
                    # The caller owns the private reference service. Clean up
                    # only objects whose names belong to this test invocation.
                    client.simple_query(checks.sock, 'ROLLBACK')
                    for suffix in ('rows', 'check_rows', 'slice_rows', 'nn_rows', 'ck_rows', 'cond_rows',
                                   'same_rows', 'width_rows', 'case_path_rows', 'default_rows'):
                        client.simple_query(checks.sock, f'DROP TABLE IF EXISTS {prefix}_{suffix}')
                    for suffix in ('child', 'numbers', 'refs', 'strings', 'limited', 'rolled',
                                   'check_domain', 'slice_domain', 'nn_domain', 'ck_domain', 'cond_domain',
                                   'same_domain', 'width_char', 'width_bit', 'width_varbit', 'case_path_domain',
                                   'default_domain', 'bad_default_domain'):
                        client.simple_query(checks.sock, f'DROP DOMAIN IF EXISTS {prefix}_{suffix}')
                    for suffix in ('check_seq', 'cond_seq', 'same_seq', 'case_path_seq', 'default_seq'):
                        client.simple_query(checks.sock, f'DROP SEQUENCE IF EXISTS {prefix}_{suffix}')
                    checks.sock.close()
                shutdown_passed = True
            finally:
                if evidence is not None:
                    source = Path(__file__)
                    program = None if reference else Path(runner.DBMS_MAIN).resolve()
                    report = {
                        'complete': checks_passed and shutdown_passed, 'prefix': prefix,
                        'reference': 'PostgreSQL 18.6' if reference else None,
                        'program': str(program) if program else None,
                        'program_sha256': hashlib.sha256(program.read_bytes()).hexdigest() if program else None,
                        'test_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
                        'checks': len(checks.records), 'passes': sum(row['pass'] for row in checks.records),
                        'stops': server.stops if server else [], 'records': checks.records,
                    }
                    (evidence / 'summary.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print('ARRAY_DOMAIN_PROTOCOL_PASS', len(checks.records))


if __name__ == '__main__':
    main()
