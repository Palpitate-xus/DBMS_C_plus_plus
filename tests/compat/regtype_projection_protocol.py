#!/usr/bin/env python3
"""Direct REGTYPE projections retain object names, OID zero, NULLs and arrays.

Only simple-query messages are used. --reference18 requires an explicitly
identified private PostgreSQL 18.6 endpoint. That caller-owned reference is
not restarted; the project test uses its own data directory and cold restart.
No prepared statements, Bind, portals, format-2 output or routine conventions are tested.
Unsupported ALTER TYPE RENAME behavior is recorded separately, not allowlisted here.
"""
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid


def steps_for(prefix, *, include_inheritance):
    table, saved, defaults, custom = (prefix + suffix for suffix in ('_rows', '_saved', '_defaults', '_custom'))
    domain, schema = prefix + '_ref', prefix + '_scope'
    parent, child, array_domain = prefix + '_parent', prefix + '_child', prefix + '_ref_array'
    type_name = schema + '."Mixed Type"'
    out = []

    def literal(text):
        return "'" + text.replace("'", "''") + "'"

    def array(values, lower=1):
        def token(value):
            if value is None:
                return 'NULL'
            if not value or value == 'NULL' or any(c.isspace() or c in '{}",\\' for c in value):
                escape = chr(92)
                return '"' + value.replace(escape, escape * 2).replace('"', escape + '"') + '"'
            return value
        body = '{' + ','.join(token(value) for value in values) + '}'
        return ('[%d:%d]=' % (lower, lower + len(values) - 1) if values and lower != 1 else '') + body

    def add(label, sql, rows=None, oids=None, ready='I', setup=False, headers=None, unordered=False, capture=None):
        out.append(dict(label=label, sql=sql, rows=rows, oids=oids, state=None,
                        ready=ready, setup=setup, headers=headers, unordered=unordered, capture=capture))

    add('domain', 'CREATE DOMAIN ' + domain + ' AS regtype', setup=True)
    add('table', 'CREATE TABLE ' + table + '(id integer PRIMARY KEY,t text,r regtype,ra regtype[],d ' + domain + ')', setup=True)
    add('data', 'INSERT INTO ' + table + " VALUES"
        "(1,'ab','integer','[0:2]={integer,NULL,-}','text'),"
        "(2,'','text','{}','-'),(3,'NULL','-',NULL,NULL),"
        "(4,NULL,NULL,'{text}','integer'),"
        "(5,'unknown','4294967295','{4294967295,NULL}','4294967295')", setup=True)
    values = [['1','ab','integer',array(['integer', None, '-'], 0),'text'],
              ['2','','text','{}','-'], ['3','NULL','-',None,None],
              ['4',None,None,'{text}','integer'],
              ['5','unknown','4294967295','{4294967295,NULL}','4294967295']]
    scalar = [[row[2]] for row in values]
    arrays = [[row[3]] for row in values]
    domains = [[row[4]] for row in values]
    source = ' FROM ' + table
    ordered = source + ' ORDER BY id'
    add('direct-scalar', 'SELECT r' + ordered, scalar, [2206], headers=['r'])
    add('direct-zero', 'SELECT r' + source + ' WHERE id=3', [['-']], [2206])
    add('direct-null', 'SELECT r' + source + ' WHERE id=4', [[None]], [2206])
    add('direct-unknown', 'SELECT r' + source + ' WHERE id=5', [['4294967295']], [2206])
    add('direct-scalar-unordered', 'SELECT id,r' + source, [[r[0],r[2]] for r in values], [23,2206], unordered=True)
    add('aliased-scalar', 'SELECT r AS chosen' + ordered, scalar, [2206], headers=['chosen'])
    add('qualified-scalar', 'SELECT x.r FROM ' + table + ' AS x ORDER BY x.id', scalar, [2206], headers=['r'])
    add('direct-array', 'SELECT ra' + ordered, arrays, [2211], headers=['ra'])
    add('direct-domain', 'SELECT d' + ordered, domains, [2206], headers=['d'])
    add('full-wildcard', 'SELECT *' + ordered, values, [23,25,2206,2211,2206], headers=['id','t','r','ra','d'])
    add('explicit-all', 'SELECT id,t,r,ra,d' + ordered, values, [23,25,2206,2211,2206])
    add('reordered-subset', 'SELECT d,r,id,ra' + ordered,
        [[r[4],r[2],r[0],r[3]] for r in values], [2206,2206,23,2211])
    add('mixed-function-control', 'SELECT upper(t),r' + ordered,
        [[None if r[1] is None else r[1].upper(), r[2]] for r in values], [25,2206])
    add('cast-control', 'SELECT r::text,ra::text,d::text' + ordered,
        [[r[2],r[3],r[4]] for r in values], [25,25,25])
    add('empty-projection', 'SELECT r,ra,d' + source + ' WHERE id<0', [], [2206,2211,2206])
    add('joined-scalars', 'SELECT a.id,a.r,b.r FROM ' + table + ' a JOIN ' + table +
        ' b ON a.id=b.id WHERE a.id<=3 ORDER BY a.id', [[r[0],r[2],r[2]] for r in values[:3]], [23,2206,2206])
    add('outer-null-identity', 'SELECT a.id,a.r,b.r FROM ' + table + ' a LEFT JOIN ' + table +
        ' b ON b.id=0 WHERE a.id<=3 ORDER BY a.id', [[r[0],r[2],None] for r in values[:3]], [23,2206,2206])
    add('derived-subset', 'SELECT x.id,x.r FROM (SELECT id,r FROM ' + table + ') x ORDER BY x.id',
        [[r[0],r[2]] for r in values], [23,2206])
    add('cte-control', 'WITH x AS (SELECT id,r,ra FROM ' + table + ') SELECT id,r,ra FROM x ORDER BY id',
        [[r[0],r[2],r[3]] for r in values], [23,2206,2211])
    add('presented-values', "SELECT r FROM (VALUES ('integer'::regtype),('text'::regtype),('0'::regtype),(NULL::regtype)) AS x(r)",
        [['integer'],['text'],['-'],[None]], [2206], unordered=True)
    add('limited-subset', 'SELECT r' + ordered + ' LIMIT 2 OFFSET 1', [['text'],['-']], [2206])
    add('reference-filter', 'SELECT r' + source + " WHERE r='integer'::regtype", [['integer']], [2206])
    add('repeated-projection', 'SELECT r AS first_reference,r AS second_reference,'
        'ra AS first_array,ra AS second_array,d AS first_domain,d AS second_domain' + ordered,
        [[r[2],r[2],r[3],r[3],r[4],r[4]] for r in values], [2206,2206,2211,2211,2206,2206],
        headers=['first_reference','second_reference','first_array','second_array','first_domain','second_domain'])
    add('joined-arrays-domains', 'SELECT a.id,a.ra,b.ra,a.d,b.d FROM ' + table +
        ' a JOIN ' + table + ' b ON a.id=b.id ORDER BY a.id',
        [[r[0],r[3],r[3],r[4],r[4]] for r in values], [23,2211,2211,2206,2206])
    add('repeated-join-projection', 'SELECT a.r,a.r,b.ra,b.ra,a.d,b.d FROM ' + table +
        ' a JOIN ' + table + ' b ON a.id=b.id ORDER BY a.id',
        [[r[2],r[2],r[3],r[3],r[4],r[4]] for r in values], [2206,2206,2211,2211,2206,2206],
        headers=['r','r','ra','ra','d','d'])
    add('outer-null-array-domain', 'SELECT a.id,b.ra,b.d,a.ra,a.d FROM ' + table +
        ' a LEFT JOIN ' + table + ' b ON b.id=0 ORDER BY a.id',
        [[r[0],None,None,r[3],r[4]] for r in values], [23,2211,2206,2211,2206])
    add('three-table-projection', 'SELECT a.id,a.r,b.ra,c.d FROM ' + table +
        ' a JOIN ' + table + ' b ON a.id=b.id JOIN ' + table + ' c ON b.id=c.id ORDER BY a.id',
        [[r[0],r[2],r[3],r[4]] for r in values], [23,2206,2211,2206])
    add('using-projection', 'SELECT id,a.r,b.ra,b.d FROM ' + table +
        ' a JOIN ' + table + ' b USING(id) ORDER BY id',
        [[r[0],r[2],r[3],r[4]] for r in values], [23,2206,2211,2206])
    add('three-table-using', 'SELECT id,a.r,b.ra,c.d FROM ' + table +
        ' a JOIN ' + table + ' b USING(id) JOIN ' + table + ' c USING(id) ORDER BY id',
        [[r[0],r[2],r[3],r[4]] for r in values], [23,2206,2211,2206])
    # NATURAL compares every common column. Whole SQL NULLs prevent a match;
    # matching array NULL elements still compare equal, including lower bounds.
    add('natural-projection', 'SELECT id,r,ra,d FROM ' + table +
        ' a NATURAL JOIN ' + table + ' b ORDER BY id',
        [[r[0],r[2],r[3],r[4]] for r in (values[0],values[1],values[4])], [23,2206,2211,2206])
    add('computed-reference-display', "SELECT coalesce(r,'integer'::regtype),"
        "CASE WHEN r IS NULL THEN 'text'::regtype ELSE r END,r,ra,d" + ordered,
        [[r[2] if r[2] is not None else 'integer',r[2] if r[2] is not None else 'text',r[2],r[3],r[4]]
         for r in values], [2206,2206,2206,2211,2206])
    add('computed-array-display', "SELECT ARRAY[r,NULL::regtype],coalesce(ra,ARRAY['integer'::regtype]),ra" + ordered,
        [[array([r[2],None]),r[3] if r[3] is not None else '{integer}',r[3]] for r in values], [2211,2211,2211])
    add('same-type-cast-display', 'SELECT r::regtype,ra::regtype[],d::regtype' + ordered,
        [[r[2],r[3],r[4]] for r in values], [2206,2211,2206])
    # Reference ordering uses unsigned OIDs, before names are rendered. The
    # unknown numeric display sorts differently from its actual large OID.
    reference_order = [['-'],['integer'],['text'],['4294967295'],[None]]
    add('distinct-reference-order', 'SELECT DISTINCT r' + source + ' ORDER BY r', reference_order, [2206])
    add('distinct-domain-order', 'SELECT DISTINCT d' + source + ' ORDER BY d', reference_order, [2206])
    add('distinct-array-order', 'SELECT DISTINCT ra' + source + ' ORDER BY ra',
        [['{}'],[values[0][3]],['{text}'],['{4294967295,NULL}'],[None]], [2211])
    add('reference-oid-order', 'SELECT id,r' + source + ' ORDER BY r',
        [[r[0],r[2]] for r in (values[2],values[0],values[1],values[4],values[3])], [23,2206])
    add('reference-descending-order', 'SELECT id,r' + source + ' ORDER BY r DESC NULLS FIRST',
        [[r[0],r[2]] for r in (values[3],values[4],values[1],values[0],values[2])], [23,2206])
    add('zero-limit', 'SELECT r,ra,d' + ordered + ' LIMIT 0', [], [2206,2211,2206])
    add('partial-offset', 'SELECT r,ra,d' + ordered + ' LIMIT 10 OFFSET 3',
        [[r[2],r[3],r[4]] for r in values[3:]], [2206,2211,2206])
    add('exhausted-offset', 'SELECT r,ra,d' + ordered + ' OFFSET 5', [], [2206,2211,2206])
    add('zero-limit-offset', 'SELECT r,ra,d' + ordered + ' LIMIT 0 OFFSET 2', [], [2206,2211,2206])
    add('locked-reference-projection', 'SELECT r,ra,d' + ordered + ' FOR UPDATE',
        [[r[2],r[3],r[4]] for r in values], [2206,2211,2206])
    add('locked-limited-projection', 'SELECT r,ra,d' + ordered + ' LIMIT 2 OFFSET 2 FOR UPDATE',
        [[r[2],r[3],r[4]] for r in values[2:4]], [2206,2211,2206])
    add('cte-repeated-arrays', 'WITH x AS (SELECT id,ra,d FROM ' + table +
        ') SELECT ra,d,ra FROM x ORDER BY id', [[r[3],r[4],r[3]] for r in values], [2211,2206,2211])
    add('cte-computed-display', "WITH x AS (SELECT id,coalesce(r,'integer'::regtype) AS chosen,"
        'ARRAY[r,NULL::regtype] AS refs FROM ' + table + ') SELECT chosen,refs FROM x ORDER BY id',
        [[r[2] if r[2] is not None else 'integer',array([r[2],None])] for r in values], [2206,2211])
    if include_inheritance:
        # Interleave the physical parent and child rows, so concatenating two
        # individually sorted scans cannot satisfy the global ORDER/LIMIT result.
        add('inherit-array-domain', 'CREATE DOMAIN ' + array_domain + ' AS regtype[]', setup=True)
        add('inherit-parent-table', 'CREATE TABLE ' + parent +
            '(id integer,r regtype,ra regtype[],d ' + domain + ',da ' + array_domain + ')', setup=True)
        add('inherit-child-table', 'CREATE TABLE ' + child + '(extra text) INHERITS (' + parent + ')', setup=True)
        add('inherit-parent-one', 'INSERT INTO ' + parent +
            " VALUES(1,'-','[0:2]={integer,NULL,-}','text','{text,NULL}')", setup=True)
        add('inherit-parent-four', 'INSERT INTO ' + parent + ' VALUES(4,NULL,NULL,NULL,NULL)', setup=True)
        add('inherit-parent-six', 'INSERT INTO ' + parent + " VALUES(6,'text','{}','-','{}')", setup=True)
        add('inherit-child-two', 'INSERT INTO ' + child +
            " VALUES(2,'boolean','{boolean}','integer','[0:1]={integer,NULL}','child two')", setup=True)
        add('inherit-child-three', 'INSERT INTO ' + child +
            " VALUES(3,'4294967295','{4294967295,NULL}','4294967295','{4294967295}','child three')", setup=True)
        add('inherit-child-five', 'INSERT INTO ' + child +
            " VALUES(5,'integer','{text}','boolean','{boolean,NULL,-}','child five')", setup=True)
        inherited = [
            ['1','-',array(['integer',None,'-'],0),'text','{text,NULL}'],
            ['2','boolean','{boolean}','integer',array(['integer',None],0)],
            ['3','4294967295','{4294967295,NULL}','4294967295','{4294967295}'],
            ['4',None,None,None,None],
            ['5','integer','{text}','boolean','{boolean,NULL,-}'],
            ['6','text','{}','-','{}']]
        inherit_types = [23,2206,2211,2206,2211]
        inherit_columns = 'SELECT id,r,ra,d,da FROM '
        add('inherit-global-order', inherit_columns + parent + ' ORDER BY id', inherited, inherit_types,
            headers=['id','r','ra','d','da'])
        add('inherit-parent-wildcard', 'SELECT * FROM ' + parent + ' ORDER BY id', inherited, inherit_types,
            headers=['id','r','ra','d','da'])
        add('inherit-only-parent', inherit_columns + 'ONLY ' + parent + ' ORDER BY id',
            [inherited[i] for i in (0,3,5)], inherit_types)
        add('inherit-child-direct', inherit_columns + child + ' ORDER BY id',
            [inherited[i] for i in (1,2,4)], inherit_types)
        add('inherit-or-global-order', inherit_columns + parent + ' WHERE id<=2 OR id>=5 ORDER BY id',
            [inherited[i] for i in (0,1,4,5)], inherit_types)
        add('inherit-overlapping-or', inherit_columns + parent + ' WHERE id>=2 OR id>=5 ORDER BY id',
            inherited[1:], inherit_types)
        add('inherit-global-limit-offset', inherit_columns + parent + ' ORDER BY id LIMIT 3 OFFSET 1',
            inherited[1:4], inherit_types)
        add('inherit-or-limit-offset', inherit_columns + parent +
            ' WHERE id<=3 OR id>=5 ORDER BY id DESC LIMIT 3 OFFSET 1',
            [inherited[i] for i in (4,2,1)], inherit_types)
        add('inherit-only-limit-offset', inherit_columns + 'ONLY ' + parent + ' ORDER BY id LIMIT 1 OFFSET 1',
            [inherited[3]], inherit_types)
        add('inherit-reordered-domain-arrays', 'SELECT da,d,ra,r,id FROM ' + parent + ' ORDER BY id',
            [[r[4],r[3],r[2],r[1],r[0]] for r in inherited], [2211,2206,2211,2206,23])
    add('begin', 'BEGIN', ready='T')
    add('update-before-savepoint', 'UPDATE ' + table + " SET r='bigint',ra='{text}' WHERE id=1", ready='T')
    add('savepoint', 'SAVEPOINT regtype_projection', ready='T')
    add('update-after-savepoint', 'UPDATE ' + table + " SET r='boolean' WHERE id=1", ready='T')
    add('pending-scalar', 'SELECT r' + source + ' WHERE id=1', [['boolean']], [2206], ready='T')
    add('rollback-savepoint', 'ROLLBACK TO SAVEPOINT regtype_projection', ready='T')
    add('savepoint-restored', 'SELECT r,ra' + source + ' WHERE id=1', [['bigint','{text}']], [2206,2211], ready='T')
    add('rollback', 'ROLLBACK')
    add('rollback-restored', 'SELECT r,ra' + source + ' WHERE id=1', [['integer',values[0][3]]], [2206,2211])
    add('saved-table', 'CREATE TABLE ' + saved + '(id integer PRIMARY KEY,r regtype,ra regtype[],d ' + domain + ')', setup=True)
    add('insert-select', 'INSERT INTO ' + saved + ' SELECT id,r,ra,d' + source, setup=True)
    stored = [[r[0],r[2],r[3],r[4]] for r in values]
    add('stored-values', 'SELECT id,r,ra,d FROM ' + saved + ' ORDER BY id', stored, [23,2206,2211,2206])
    add('default-table', 'CREATE TABLE ' + defaults + "(id integer,r regtype DEFAULT 'integer',"
        "ra regtype[] DEFAULT ARRAY['text'::regtype,NULL::regtype])", setup=True)
    add('default-insert', 'INSERT INTO ' + defaults + '(id) VALUES(1)', setup=True)
    add('default-values', 'SELECT id,r,ra FROM ' + defaults, [['1','integer','{text,NULL}']], [23,2206,2211])
    add('schema', 'CREATE SCHEMA ' + schema, setup=True)
    add('custom-type', 'CREATE TYPE ' + type_name + " AS ENUM('one')", setup=True)
    add('capture-custom-oid', 'SELECT ' + literal(type_name) + '::regtype::oid', oids=[26], capture='custom', setup=True)
    add('custom-table', 'CREATE TABLE ' + custom + '(id integer,r regtype,ra regtype[])', setup=True)
    add('custom-insert', 'INSERT INTO ' + custom + ' VALUES(1,' + literal(type_name) + '::regtype,ARRAY[' +
        literal(type_name) + '::regtype,NULL::regtype])', setup=True)
    add('custom-hidden-scalar', 'SELECT r FROM ' + custom, [[type_name]], [2206])
    add('custom-hidden-array', 'SELECT ra FROM ' + custom, [[array([type_name,None])]], [2211])
    add('visible-path', 'SET search_path=' + schema + ',pg_catalog,public')
    add('custom-visible-scalar', 'SELECT r FROM public.' + custom, [['"Mixed Type"']], [2206])
    add('custom-visible-array', 'SELECT ra FROM public.' + custom, [[array(['"Mixed Type"',None])]], [2211])
    add('restore-path', 'SET search_path=public,pg_catalog')
    # This DROP case is independent of ALTER TYPE RENAME support, so an
    # unsupported rename cannot mask ordinary dangling-reference output.
    drop_name = schema + '."Drop Type"'
    add('independent-drop-type', 'CREATE TYPE ' + drop_name + " AS ENUM('one')", setup=True)
    add('capture-drop-oid', 'SELECT ' + literal(drop_name) + '::regtype::oid', oids=[26], capture='dropped', setup=True)
    add('independent-drop-insert', 'INSERT INTO ' + custom + ' VALUES(2,' + literal(drop_name) + '::regtype,ARRAY[' +
        literal(drop_name) + '::regtype,NULL::regtype])', setup=True)
    add('independent-drop', 'DROP TYPE ' + drop_name)
    add('independent-dropped-reference', 'SELECT r,ra FROM ' + custom + ' WHERE id=2',
        lambda o: [[o['dropped'],array([o['dropped'],None])]], [2206,2211])
    out.append({'restart': True})
    add('cold-source', 'SELECT id,t,r,ra,d' + ordered, values, [23,25,2206,2211,2206])
    add('cold-stored-values', 'SELECT id,r,ra,d FROM ' + saved + ' ORDER BY id', stored, [23,2206,2211,2206])
    add('cold-default-values', 'SELECT id,r,ra FROM ' + defaults, [['1','integer','{text,NULL}']], [23,2206,2211])
    add('cold-custom-reference', 'SELECT r,ra FROM ' + custom + ' WHERE id=1',
        [[type_name,array([type_name,None])]], [2206,2211])
    add('cold-independent-drop', 'SELECT r,ra FROM ' + custom + ' WHERE id=2',
        lambda o: [[o['dropped'],array([o['dropped'],None])]], [2206,2211])
    add('cold-direct-subset', 'SELECT r,ra,d' + ordered,
        [[r[2],r[3],r[4]] for r in values], [2206,2211,2206])
    if include_inheritance:
        add('cold-inherit-global-order', inherit_columns + parent + ' ORDER BY id', inherited, inherit_types)
        add('cold-inherit-only-parent', inherit_columns + 'ONLY ' + parent + ' ORDER BY id',
            [inherited[i] for i in (0,3,5)], inherit_types)
        add('cold-inherit-or-limit-offset', inherit_columns + parent +
            ' WHERE id<=3 OR id>=5 ORDER BY id DESC LIMIT 3 OFFSET 1',
            [inherited[i] for i in (4,2,1)], inherit_types)
    tables = [table,saved,defaults,custom]
    cleanup = ['DROP DOMAIN IF EXISTS ' + domain,
               'DROP SCHEMA IF EXISTS ' + schema + ' CASCADE']
    if include_inheritance:
        tables.extend([parent,child])
        cleanup.insert(0, 'DROP DOMAIN IF EXISTS ' + array_domain)
    return out, tables, cleanup


def process_identity(pid):
    directory = Path('/proc') / str(pid)
    fields = (directory / 'stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': pid, 'pgid': int(fields[2]), 'sid': int(fields[3]),
            'start_ticks': fields[19], 'uid': directory.stat().st_uid}


def group_members(pgid):
    members = []
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            identity = process_identity(int(directory.name))
            if identity['pgid'] == pgid:
                members.append(identity['pid'])
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return members


def owns_listener(pid, port):
    assert (Path('/proc') / str(pid)).stat().st_uid == os.geteuid()
    inodes = set()
    for entry in (Path('/proc') / str(pid) / 'fd').iterdir():
        try:
            value = os.readlink(entry)
        except FileNotFoundError:
            continue
        if value.startswith('socket:[') and value.endswith(']'):
            inodes.add(value[8:-1])
    for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
        fields = line.split()
        host, value = fields[1].split(':')
        if (host in ('0100007F', '00000000') and int(value, 16) == port and
                fields[3] == '0A' and fields[9] in inodes):
            return True
    return False


def run(steps_provider, *, entry_file, cohort, expected_checks):
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('scalar_argument_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    work = None if reference else Path(tempfile.mkdtemp(prefix='dbms-regtype-projection-'))
    evidence_name = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
    evidence = Path(evidence_name) if evidence_name else None
    if evidence is not None:
        evidence.mkdir(parents=True, exist_ok=False)
    process = sock = owner = None
    checks = generation = 0
    failures, waits, records = [], [], []
    symbols = {}
    commands, tables, cleanup = steps_provider('regproj_' + uuid.uuid4().hex[:10])
    sql_commands = [command for command in commands if not command.get('restart')]
    assert len(sql_commands) == expected_checks, (cohort, len(sql_commands), expected_checks)
    entry_path = Path(entry_file).resolve()
    support_path = Path(__file__).resolve()
    sources = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
               for path in (entry_path, support_path)}

    def check(command):
        nonlocal checks
        sql = command['sql']
        messages = client.simple_query(sock, sql)
        value = runner.decode_wire_result(messages, include_types=True)
        expected_ready = command['ready'].encode()
        expected_rows = command['rows'](symbols) if callable(command['rows']) else command['rows']
        actual_rows = value[0]
        if command['unordered'] and expected_rows is not None:
            rows_match = collections.Counter(map(tuple, actual_rows)) == collections.Counter(map(tuple, expected_rows))
        else:
            rows_match = expected_rows is None or actual_rows == expected_rows
        good = (value[1] == command['state'] and messages[-1] == (b'Z', expected_ready) and
                rows_match and
                (command['headers'] is None or value[3] == command['headers']) and
                (command['oids'] is None or value[5] == command['oids']))
        words = sql.lstrip().split()
        keyword = command.get('statement_kind', words[0].upper())
        if keyword in ('SELECT', 'WITH'):
            expected_tag = 'SELECT ' + str(len(actual_rows))
        elif keyword == 'INSERT':
            expected_tag = 'INSERT 0 ' + ('5' if command['label'] in ('data', 'insert-select') else '1')
        elif keyword == 'UPDATE':
            expected_tag = 'UPDATE 1'
        elif keyword in ('CREATE', 'DROP'):
            expected_tag = keyword + ' ' + words[1].upper()
        else:
            expected_tag = keyword
        if command['state'] is None:
            good = (good and value[4] == expected_tag and sum(kind == b'C' for kind, _ in messages) == 1
                    and not any(kind == b'E' for kind, _ in messages))
            if keyword not in ('SELECT', 'WITH'):
                good = good and actual_rows == [] and not any(kind in (b'T', b'D') for kind, _ in messages)
        else:
            expected_tag = None
            good = (good and actual_rows == [] and value[4] is None and
                    sum(kind == b'E' for kind, _ in messages) == 1 and
                    not any(kind in (b'C', b'T', b'D') for kind, _ in messages))
        if command['capture'] is not None:
            valid_oid = value[1] is None and len(actual_rows) == 1 and len(actual_rows[0]) == 1 and actual_rows[0][0].isdigit() and int(actual_rows[0][0]) > 1000
            good = good and valid_oid
            if valid_oid: symbols[command['capture']] = actual_rows[0][0]
            assert valid_oid, ('actual custom type identity', value)
        record = {'label': command['label'], 'sql': sql, 'ok': good,
                  'result': value, 'ready': messages[-1][1].decode(), 'expected_rows': expected_rows,
                  'expected_tag': expected_tag}
        records.append(record)
        if evidence is not None:
            stem = '%03d-%s' % (checks, command['label'])
            request = b'Q' + struct.pack('!I', len(sql.encode()) + 5) + sql.encode() + b'\0'
            raw = b''.join(kind + struct.pack('!I', len(body) + 4) + body for kind, body in messages)
            (evidence / (stem + '.request.bin')).write_bytes(request)
            (evidence / (stem + '.response.bin')).write_bytes(raw)
            (evidence / 'queries.json').write_text(json.dumps(records, indent=2) + '\n')
        checks += 1
        if not good:
            failures.append(record)
        assert not command['setup'] or value[1] is None, ('setup failed', record)

    def start():
        nonlocal process, sock, owner, generation
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        log = work / ('server-%d.log' % generation)
        generation += 1
        with log.open('xb') as output:
            process = subprocess.Popen([runner.DBMS_MAIN, '--data-dir', str(work), '--server', str(port), '--insecure'],
                                       cwd=work, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        owner = process_identity(process.pid)
        assert owner['pgid'] == owner['sid'] == process.pid and owner['uid'] == os.geteuid()
        deadline = time.monotonic() + 30
        while True:
            assert process.poll() is None, ('private server exited', process.returncode, str(log))
            assert process_identity(process.pid) == owner
            try:
                if owns_listener(process.pid, port):
                    break
            except FileNotFoundError:
                pass
            assert time.monotonic() < deadline, ('private listener timeout', str(log))
            time.sleep(.05)
        assert (Path('/proc') / str(process.pid) / 'exe').resolve() == Path(runner.DBMS_MAIN).resolve()
        sock = socket.create_connection(('127.0.0.1', port), timeout=runner.wire_timeout())
        client.startup(sock, 'alice', 'info')

    def stop():
        nonlocal process, sock, owner
        if sock is not None:
            sock.close()
            sock = None
        if process is None:
            return
        # start_new_session gave this child its own group before exec. An
        # interruption can arrive after Popen but before owner is assigned.
        group = process.pid
        forced = False
        if process.poll() is None:
            if owner is None:
                owner = process_identity(process.pid)
                assert owner['pgid'] == owner['sid'] == process.pid and owner['uid'] == os.geteuid()
            assert process_identity(process.pid) == owner
            process.terminate()
        try:
            code = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            forced = True
            assert process_identity(process.pid) == owner
            os.killpg(owner['pgid'], signal.SIGKILL)
            code = process.wait(timeout=20)
        gone = not (Path('/proc') / str(process.pid)).exists()
        remaining = group_members(group)
        waits.append({'pid': process.pid, 'exit': code, 'forced': forced, 'pid_gone': gone, 'group_after': remaining})
        process = owner = None
        assert code == 0 and not forced and gone and not remaining, ('private server shutdown', waits)

    succeeded = False
    try:
        if reference:
            required = ('PGREF_HOST', 'PGREF_PORT', 'PGREF_PASSWORD', 'PGREF_PID')
            assert all(os.environ.get(key) for key in required), 'private PGREF endpoint and PID required'
            assert os.environ['PGREF_HOST'] == '127.0.0.1'
            host, port, user, database, password = runner._reference_connection_settings()
            assert owns_listener(int(os.environ['PGREF_PID']), port), 'reference PID does not own listener'
            sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
            client.startup_reference(sock, user, database, password)
            runner.verify_reference_version(client, sock)
        else:
            (work / 'info').mkdir()
            (work / 'info/tlist.lst').write_bytes(b'')
            client.write_auth_catalog(str(work), 'alice', 'secret')
            (work / 'pg_hba.conf').write_text('host all alice 127.0.0.1/32 scram-sha-256\n')
            start()
        for command in commands:
            if command.get('restart'):
                if not reference:
                    stop()
                    start()
                continue
            check(command)
        assert not failures, failures
        succeeded = True
        print('REGTYPE_DIRECT_PROJECTION_PASS checks=%d restarts=%d' % (checks, 0 if reference else 1), flush=True)
    finally:
        if reference and sock is not None:
            # These unique names belong solely to this run on the explicitly
            # identified private reference; do not touch other objects.
            client.simple_query(sock, 'ROLLBACK')
            for table in reversed(tables):
                client.simple_query(sock, 'DROP TABLE IF EXISTS ' + table)
            for sql in cleanup:
                client.simple_query(sock, sql)
            sock.close()
        elif process is not None:
            stop()
        if evidence is not None:
            summary = {'success': succeeded, 'checks': checks, 'failures': failures, 'waits': waits,
                       'program': runner.DBMS_MAIN if not reference else 'PostgreSQL 18.6',
                       'test_sha256': sources[str(entry_path)], 'symbols': symbols,
                       'entry_path': str(entry_path), 'support_path': str(support_path),
                       'support_sha256': sources[str(support_path)], 'source_files': sources,
                       'cohort': cohort, 'planned_checks': expected_checks,
                       'not_run': [{'label': command['label'], 'sql': command['sql']}
                                   for command in sql_commands[checks:]]}
            if not reference:
                summary['program_sha256'] = hashlib.sha256(Path(runner.DBMS_MAIN).read_bytes()).hexdigest()
            (evidence / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        if work is not None:
            if succeeded:
                shutil.rmtree(work)
            else:
                print('REGTYPE_DIRECT_PROJECTION_FAILURE_EVIDENCE', str(work), flush=True)


def interrupted(signum, frame):
    # Give the finally block one uninterrupted chance to stop its private
    # child; the server is in a separate session from the test runner.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise SystemExit(128 + signum)
