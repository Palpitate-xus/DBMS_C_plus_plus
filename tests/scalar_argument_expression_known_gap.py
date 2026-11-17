#!/usr/bin/env python3
"""Ordinary scalar arguments retain expressions, NULLs, effects and errors.

Only simple-query messages are used. --reference18 requires an explicitly
identified private PostgreSQL 18.6 endpoint. That caller-owned reference is
not restarted; the project test uses its own data directory and cold restart.
No POSITION/native routine conventions or unselected CAST analysis is tested.
"""
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


def steps(prefix):
    """Expected values/effects are checked against the private PG 18.6 oracle."""
    table, saved, defaults = (prefix + suffix for suffix in ('_rows', '_saved', '_defaults'))
    sequences = {name: prefix + '_' + name for name in (
        'null', 'nonnull', 'repeated', 'nested', 'literal', 'conditional',
        'dead', 'null_error', 'value_error', 'transaction_error')}
    out = []

    def add(label, sql, rows=None, oids=None, state=None, ready='I', setup=False):
        out.append(dict(label=label, sql=sql, rows=rows, oids=oids,
                        state=state, ready=ready, setup=setup))

    add('table', 'CREATE TABLE ' + table + '(id integer PRIMARY KEY,t text,"Odd Name" text)', setup=True)
    add('data', "INSERT INTO " + table + " VALUES(1,NULL,NULL),(2,'',''),"
        "(3,'a''b, (x)','q''R'),(4,'AbA',' ab ')", setup=True)
    for name, sequence in sequences.items():
        add('sequence-' + name, 'CREATE SEQUENCE ' + sequence, setup=True)
    source = ' FROM ' + table
    ordered = source + ' ORDER BY id'
    one = source + ' WHERE id=4'
    null_row = source + ' WHERE id=1'
    add('plain-upper', 'SELECT upper(t)' + ordered, [[None], [''], ["A'B, (X)"], ['ABA']], [25])
    add('plain-length', 'SELECT length(t)' + ordered, [[None], ['0'], ['8'], ['3']], [23])
    add('escaped-literal', "SELECT upper('we''ird'),length('we''ird')" + one, [["WE'IRD", '6']], [25, 23])
    add('literal-concatenation', "SELECT replace('a'||'','x','y')" + one, [['a']], [25])
    add('column-concatenation', "SELECT upper('prefix:' || t)" + ordered,
        [[None], ['PREFIX:'], ["PREFIX:A'B, (X)"], ['PREFIX:ABA']], [25])
    add('concatenation-length', "SELECT length('pre:' || t)" + ordered,
        [[None], ['4'], ['12'], ['7']], [23])
    add('quoted-comma-parentheses', "SELECT replace('a,b(c)''d','b(c)','X')" + one, [["a,X'd"]], [25])
    add('nested-replace', "SELECT upper(replace(t,'a','z'))" + ordered,
        [[None], [''], ["Z'B, (X)"], ['ABA']], [25])
    add('nested-lower', "SELECT replace(lower(t),'a','x')" + ordered,
        [[None], [''], ["x'b, (x)"], ['xbx']], [25])
    add('quoted-identifier', 'SELECT upper("Odd Name")' + ordered,
        [[None], [''], ["Q'R"], [' AB ']], [25])
    add('strict-quoted-identifier', 'SELECT replace("Odd Name",\'q\',\'x\')' + ordered,
        [[None], [''], ["x'R"], [' ab ']], [25])
    add('identifier-in-expression', 'SELECT length("Odd Name" || \':\' || coalesce(t,\'n\'))' + ordered,
        [[None], ['1'], ['12'], ['8']], [23])
    add('concatenated-arguments', "SELECT replace(t,'a'||'','z'||'')" + ordered,
        [[None], [''], ["z'b, (x)"], ['AbA']], [25])
    add('runtime-case', "SELECT upper(CASE WHEN id=1 THEN 'fallback' ELSE t END)" + ordered,
        [['FALLBACK'], [''], ["A'B, (X)"], ['ABA']], [25])
    add('literal-delimiters', "SELECT upper('first,second'),length('a(b)c')" + one,
        [['FIRST,SECOND', '5']], [25, 23])
    add('arithmetic-argument', 'SELECT length((id+7)::text)' + ordered,
        [['1'], ['1'], ['2'], ['2']], [23])
    add('empty-source', "SELECT upper('we''ird'),length('pre:'||t)" + source + ' WHERE id<0', [], [25, 23])

    def nextval(name):
        return "nextval('" + sequences[name] + "')::text"

    def counter(name, wanted):
        add('counter-' + name, "SELECT nextval('" + sequences[name] + "')", [[str(wanted)]], [20])

    for name, suffix, expected in (('null', null_row, None), ('nonnull', one, 'AbA')):
        value = nextval(name)
        add('strict-runtime-' + name, 'SELECT replace(t,' + value + ',' + value + ')' + suffix, [[expected]], [25])
        counter(name, 3)
    value = nextval('repeated')
    call = 'replace(t,' + value + ',' + value + ')'
    add('distinct-call-occurrences', 'SELECT ' + call + ',' + call + one, [['AbA', 'AbA']], [25, 25])
    counter('repeated', 5)
    value = nextval('nested')
    add('nested-call-effects', 'SELECT upper(replace(t,' + value + ',' + value + '))' + one, [['ABA']], [25])
    counter('nested', 3)
    # PostgreSQL folds a STRICT call with a literal NULL before runtime. This
    # is intentionally distinct from a runtime NULL read from column t above.
    value = nextval('literal')
    add('strict-literal-null', 'SELECT replace(NULL,' + value + ',' + value + ')' + null_row, [[None]], [25])
    counter('literal', 1)
    add('conditional-argument', "SELECT replace(t,CASE WHEN id=4 THEN 'A' ELSE " +
        nextval('conditional') + " END,'x')" + one, [['xbx']], [25])
    counter('conditional', 1)
    value = nextval('dead')
    add('limit-zero-effects', 'SELECT replace(t,' + value + ',' + value + ')' + source + ' LIMIT 0', [], [25])
    counter('dead', 1)
    # Runtime integer division uses the selected row's id; it does not ask
    # the planner to analyze an invalid CAST in an unselected expression.
    for name, suffix, denominator in (('null_error', null_row, 'id-1'), ('value_error', one, 'id-4')):
        add('later-argument-' + name, 'SELECT replace(t,' + nextval(name) + ',(1/(' + denominator + '))::text)' + suffix,
            state='22012')
        counter(name, 2)
    add('transaction-begin', 'BEGIN', ready='T')
    add('transaction-error', 'SELECT replace(t,' + nextval('transaction_error') + ',(1/(id-1))::text)' + null_row,
        state='22012', ready='E')
    add('transaction-stays-failed', 'SELECT 1', state='25P02', ready='E')
    add('transaction-rollback', 'ROLLBACK')
    counter('transaction_error', 2)

    add('saved-table', 'CREATE TABLE ' + saved + '(id integer PRIMARY KEY,t text)', setup=True)
    add('save-expression', 'INSERT INTO ' + saved + " SELECT id,upper('prefix:'||t)" + source, setup=True)
    add('saved-values', 'SELECT id,t FROM ' + saved + ' ORDER BY id',
        [['1', None], ['2', 'PREFIX:'], ['3', "PREFIX:A'B, (X)"], ['4', 'PREFIX:ABA']], [23, 25])
    add('update-expression', 'UPDATE ' + saved + " SET t=replace(lower(t),'a','x') WHERE id=4", setup=True)
    add('default-table', 'CREATE TABLE ' + defaults + "(id integer PRIMARY KEY,t text DEFAULT upper('we''ird'))", setup=True)
    add('default-insert', 'INSERT INTO ' + defaults + '(id) VALUES(1)', setup=True)
    add('write-begin', 'BEGIN', ready='T')
    add('write-before-rollback', 'UPDATE ' + saved + " SET t=upper('rollback:'||t) WHERE id=4", ready='T')
    add('write-rollback', 'ROLLBACK')
    out.append({'restart': True})
    add('cold-original-values', 'SELECT id,t,"Odd Name" FROM ' + table + ' ORDER BY id',
        [['1', None, None], ['2', '', ''], ['3', "a'b, (x)", "q'R"], ['4', 'AbA', ' ab ']], [23, 25, 25])
    add('cold-saved-values', 'SELECT id,t FROM ' + saved + ' ORDER BY id',
        [['1', None], ['2', 'PREFIX:'], ['3', "PREFIX:A'B, (X)"], ['4', 'prefix:xbx']], [23, 25])
    add('cold-default-values', 'SELECT id,t FROM ' + defaults, [['1', "WE'IRD"]], [23, 25])
    add('cold-expression', "SELECT upper('prefix:'||t)" + ordered,
        [[None], ['PREFIX:'], ["PREFIX:A'B, (X)"], ['PREFIX:ABA']], [25])
    counter('null', 4)
    counter('null_error', 3)
    return out, [table, saved, defaults], list(sequences.values())


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


def main():
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('scalar_argument_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    work = None if reference else Path(tempfile.mkdtemp(prefix='dbms-scalar-arguments-'))
    evidence_name = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
    evidence = Path(evidence_name) if evidence_name else None
    if evidence is not None:
        evidence.mkdir(parents=True, exist_ok=False)
    process = sock = owner = None
    checks = generation = 0
    failures, waits, records = [], [], []
    commands, tables, sequences = steps('scalar_args_' + uuid.uuid4().hex[:10])

    def check(command):
        nonlocal checks
        sql = command['sql']
        messages = client.simple_query(sock, sql)
        value = runner.decode_wire_result(messages, include_types=True)
        expected_ready = command['ready'].encode()
        good = (value[1] == command['state'] and messages[-1] == (b'Z', expected_ready) and
                (command['rows'] is None or value[0] == command['rows']) and
                (command['oids'] is None or value[5] == command['oids']))
        if command['state'] is not None:
            good = (good and value[0] == [] and value[4] is None and
                    sum(kind == b'E' for kind, _ in messages) == 1)
        record = {'label': command['label'], 'sql': sql, 'ok': good,
                  'result': value, 'ready': messages[-1][1].decode()}
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
        print('SCALAR_ARGUMENT_EXPRESSION_PASS checks=%d restarts=%d' % (checks, 0 if reference else 1), flush=True)
    finally:
        if reference and sock is not None:
            # These unique names belong solely to this run on the explicitly
            # identified private reference; do not touch other objects.
            client.simple_query(sock, 'ROLLBACK')
            for table in reversed(tables):
                client.simple_query(sock, 'DROP TABLE IF EXISTS ' + table)
            for sequence in sequences:
                client.simple_query(sock, 'DROP SEQUENCE IF EXISTS ' + sequence)
            sock.close()
        elif process is not None:
            stop()
        if evidence is not None:
            summary = {'success': succeeded, 'checks': checks, 'failures': failures, 'waits': waits,
                       'program': runner.DBMS_MAIN if not reference else 'PostgreSQL 18.6',
                       'test_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
            if not reference:
                summary['program_sha256'] = hashlib.sha256(Path(runner.DBMS_MAIN).read_bytes()).hexdigest()
            (evidence / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        if work is not None:
            if succeeded:
                shutil.rmtree(work)
            else:
                print('SCALAR_ARGUMENT_FAILURE_EVIDENCE', str(work), flush=True)


if __name__ == '__main__':
    def interrupted(signum, frame):
        # Give the finally block one uninterrupted chance to stop its private
        # child; the server is in a separate session from the test runner.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    main()
