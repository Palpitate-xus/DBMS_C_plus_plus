"""Real integer source comparisons validate genuine UNKNOWN input before rows."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('integer_input_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    failures = []; controls = 0
    schema = 'integer_input_' + uuid.uuid4().hex[:16]
    table = '"' + schema + '".bits'
    def check(sql, messages, rows=None, types=None, state=None, tag=None):
        nonlocal controls
        controls += 1
        result = runner.decode_wire_result(messages, include_types=True)
        print('INTEGER_UNKNOWN', sql, result, flush=True)
        if (result[1] != state or rows is not None and result[0] != rows or
                types is not None and result[5] != types or tag is not None and result[4] != tag):
            failures.append((sql, result, rows, types, state, tag))
        if state and (result[0] or result[4] is not None): failures.append(('partial publication', sql, result))
        if types is not None and state is None:
            fields = client.row_description_fields(messages)
            if len(fields) != len(types): failures.append(('field count', sql, fields, types))
            for field, oid in zip(fields, types):
                if field[3:] != (oid, {16: 1, 20: 8, 23: 4}[oid], -1, 0):
                    failures.append(('field OID/width/typmod/format', sql, field, oid))
                if sql.startswith('SELECT id FROM ') and (field[0] != b'id' or not field[1] or field[2] != 1):
                    failures.append(('real physical id origin', sql, field))
        return result
    def query(sql, **expected): return check(sql, client.simple_query(sock, sql), **expected)
    def error(sql, state='22P02', extended=False):
        if reference: query('SAVEPOINT integer_error')
        if extended:
            # Literal conversion belongs to Parse, not Bind or first Execute.
            sock.sendall(client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            check('PARSE ' + sql, messages, state=state)
            if any(kind == b'1' for kind, _ in messages): failures.append(('invalid literal accepted at Parse', sql, messages))
        else: query(sql, state=state)
        if reference: query('ROLLBACK TO integer_error'); query('RELEASE integer_error')
    def parameter(sql, oid, datum, rows, state=None):
        if reference: query('SAVEPOINT integer_parameter')
        sock.sendall(client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, oid)) +
                     client.typed(b'D', b'S\0') + client.typed(b'S'))
        parsed = client.read_until_ready(sock)
        check('PARSE PARAM ' + sql, parsed, state=None, types=[23])
        if not any(kind == b'1' for kind, _ in parsed): failures.append(('no ParseComplete', sql, parsed))
        if not any(kind == b't' and body == struct.pack('!HI', 1, oid) for kind, body in parsed):
            failures.append(('true declared parameter identity', sql, oid, parsed))
        encoded = None if datum is None else datum.encode()
        bind = b'\0\0' + struct.pack('!HH', 0, 1) + struct.pack('!i', -1 if encoded is None else len(encoded))
        if encoded is not None: bind += encoded
        bind += struct.pack('!H', 0)
        sock.sendall(client.typed(b'B', bind) + client.typed(b'D', b'P\0') +
                     client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        check('BIND EXECUTE ' + sql, messages, rows=rows, state=state,
              types=None if state else [23], tag=None if state else 'SELECT ' + str(len(rows)))
        if reference: query('ROLLBACK TO integer_parameter'); query('RELEASE integer_parameter')
    try:
        if reference: query('BEGIN')
        query('CREATE SCHEMA "' + schema + '"')
        query('CREATE TABLE ' + table + ' (id INT PRIMARY KEY,i INT,j INT,b BIGINT,"odd""i" INT)')
        query('CREATE INDEX bits_i ON ' + table + ' (i)')
        operators = ['=', '<>', '<', '>', '<=', '>=']
        columns = ['id', 'i', 'j', 'b', '"odd""i"']
        for populated in (False, True):
            if populated: query('INSERT INTO ' + table + ' VALUES (1,1,1,1,1),(2,2,2,2,2),(3,NULL,NULL,NULL,NULL)')
            for column in columns:
                for op in operators:
                    for reverse in (False, True):
                        predicate = "''" + op + column if reverse else column + op + "''"
                        error('SELECT id FROM ' + table + ' WHERE ' + predicate + ' ORDER BY id')
            # All invalid integer input is prepared, including out of range;
            # source size, WHERE false and LIMIT 0 cannot suppress input casts.
            for predicate, state in [('i=\'x\'', '22P02'), ('i=\'NULL\'', '22P02'),
                                     ('i=\'1.0\'', '22P02'), ('b=\'9e2\'', '22P02'), ('i=\'it\'\'s\'', '22P02'),
                                     ('i=\'2147483648\'', '22003'),
                                     ('b=\'9223372036854775808\'', '22003')]:
                error('SELECT id FROM ' + table + ' WHERE ' + predicate, state)
                error('SELECT ' + predicate + ' FROM ' + table + ' WHERE FALSE', state)
                error('SELECT id FROM ' + table + ' WHERE ' + predicate + ' LIMIT 0', state)
            for column in columns:
                query('SELECT id FROM ' + table + ' WHERE ' + column + "='1' ORDER BY id",
                      rows=[['1']] if populated else [], types=[23])
                query('SELECT id FROM ' + table + ' WHERE ' + column + '=NULL ORDER BY id', rows=[], types=[23])
                for op in operators:
                    for reverse in (False, True):
                        predicate = "'1'" + op + column if reverse else column + op + "'1'"
                        rows = []
                        if populated:
                            for row_id in (1, 2, 3):
                                value = row_id if column == 'id' or row_id != 3 else None
                                if value is None: continue
                                left, right = (1, value) if reverse else (value, 1)
                                if {'=': left == right, '<>': left != right, '<': left < right,
                                    '>': left > right, '<=': left <= right, '>=': left >= right}[op]:
                                    rows.append([str(row_id)])
                        query('SELECT id FROM ' + table + ' WHERE ' + predicate + ' ORDER BY id', rows=rows, types=[23])
            error('SELECT id FROM ' + table + " WHERE i='' ORDER BY id", extended=True)
            error('SELECT id FROM ' + table + " WHERE ''<b ORDER BY id", extended=True)
            error('SELECT id FROM ' + table + " WHERE i='' AND j=$1 ORDER BY id", extended=True)
            parameter('SELECT id FROM ' + table + ' WHERE i=$1 ORDER BY id', 23, '1', [['1']] if populated else [])
            parameter('SELECT id FROM ' + table + ' WHERE b=$1 ORDER BY id', 20, None, [])
            parameter('SELECT id FROM ' + table + ' WHERE i=$1 ORDER BY id', 23, '', [], '22P02')
        query('CREATE SEQUENCE "' + schema + '".effects')
        # Pure preparation must reject the predicate before running any
        # volatile projection. Distinct call sites remain distinct at runtime.
        error('SELECT nextval(\'' + schema + '.effects\') FROM ' + table + " WHERE i='' LIMIT 0")
        query("SELECT nextval('" + schema + ".effects')", rows=[['1']], types=[20])
        effects = '"' + schema + '".writes'
        writer = 'integer_writer_' + uuid.uuid4().hex[:12]
        if reference: query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
        query('CREATE TABLE ' + effects + ' (id INT)')
        query('CREATE FUNCTION ' + writer + '(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO ' + effects + ' VALUES(p); RETURN p; END$$')
        error('SELECT ' + writer + '(id) FROM ' + table + " WHERE i='' LIMIT 0")
        query('SELECT id FROM ' + effects, rows=[], types=[23])
        query('SELECT ' + writer + '(id),' + writer + '(id) FROM ' + table + " WHERE i='1'",
              rows=[['1', '1']], types=[23, 23])
        query('SELECT id FROM ' + effects + ' ORDER BY id', rows=[['1'], ['1']], types=[23])
        query("SELECT nextval('" + schema + ".effects')", rows=[['2']], types=[20])
        quoted = '"' + schema + '".quoted_values'
        query('CREATE TABLE ' + quoted + '("ID" INT PRIMARY KEY,"i I" INT,"a""b" TEXT,"I" BIGINT,i INT)')
        query('INSERT INTO ' + quoted + " VALUES(1,1,'',99,11),(2,NULL,'x',100,12),(3,2,NULL,NULL,NULL)")
        for predicate, rows in [('"i I"=\'1\'', [['1']]), ('"a""b"=\'\'', [['1']]),
                                ('"a""b"=\'x\'', [['2']]), ('"a""b" IS NULL', [['3']]),
                                ('"I"=\'99\'', [['1']]), ('i=\'11\'', [['1']]), ('"I"=NULL', [])]:
            query('SELECT "ID" FROM ' + quoted + ' WHERE ' + predicate + ' ORDER BY "ID"', rows=rows, types=[23])
        error('SELECT "ID" FROM ' + quoted + ' WHERE "I"=\'\' LIMIT 0')
        exact = '"' + schema + '".exact_bigints'
        query('CREATE TABLE ' + exact + '(id INT,b BIGINT)')
        query('INSERT INTO ' + exact + ' VALUES(1,9007199254740992),(2,9007199254740993),(3,NULL)')
        for reverse in (False, True):
            predicate = "'9007199254740993'=b" if reverse else "b='9007199254740993'"
            query('SELECT id FROM ' + exact + ' WHERE ' + predicate + ' ORDER BY id', rows=[['2']], types=[23])
        print('INTEGER_UNKNOWN_COMPLETE', controls, 'failures', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try: client.simple_query(sock, 'ROLLBACK')
            finally: sock.close()
        else: runner.stop_ours(server)


if __name__ == '__main__': main()
