"""A NULL Bind datum retains its real declared builtin type at execution."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('typed_null_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = '--reference18' in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    failures = []; controls = 0
    schema = 'typed_null_' + uuid.uuid4().hex[:12]
    table = '"' + schema + '".rows'
    types = [(16, 'boolean', 1), (20, 'bigint', 8), (21, 'smallint', 2),
             (23, 'integer', 4), (25, 'text', -1), (700, 'real', 4),
             (701, 'double precision', 8), (1082, 'date', 4),
             (1114, 'timestamp', 8), (1184, 'timestamptz', 8), (1700, 'numeric', -1)]

    def check(label, messages, rows=None, descriptor=None, tag=None, column_name='value'):
        nonlocal controls
        controls += 1
        actual = runner.decode_wire_result(messages, include_types=True)
        print('TYPED_NULL', label, actual, flush=True)
        if actual[1] or rows is not None and actual[0] != rows or tag is not None and actual[4] != tag:
            failures.append((label, actual, rows, tag))
        if descriptor is not None:
            oid, width, result_format = descriptor
            fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
            if len(fields) != 1 or fields[0] != (column_name.encode(), 0, 0, oid, width, -1, result_format):
                failures.append(('declared NULL descriptor', label, fields, descriptor))
        return actual

    def query(sql, **expected):
        return check(sql, client.simple_query(sock, sql), **expected)

    def extended(sql, oid, width, value=None, parameter_format=0, result_format=0,
                 rows=None, before_execute=None, column_name='value', parameter_oid=None):
        declared_oid = oid if parameter_oid is None else parameter_oid
        query('SAVEPOINT null_binding')
        name = ('null_' + uuid.uuid4().hex[:12]).encode(); portal = name + b'_portal'
        sock.sendall(client.typed(b'P', name + b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, declared_oid)) +
                     client.typed(b'D', b'S' + name + b'\0') + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        parsed = check('PARSE DESCRIBE ' + sql, messages, descriptor=(oid, width, 0), column_name=column_name)
        if not any(kind == b'1' for kind, _ in messages):
            failures.append(('missing ParseComplete', sql, messages))
        if [payload for kind, payload in messages if kind == b't'] != [struct.pack('!HI', 1, declared_oid)]:
            failures.append(('real ParameterDescription OID', sql, messages, declared_oid))
        if parsed[1]:
            query('ROLLBACK TO SAVEPOINT null_binding'); query('RELEASE SAVEPOINT null_binding'); return
        raw = None if value is None else value if isinstance(value, bytes) else value.encode()
        bound = struct.pack('!HHH', 1, parameter_format, 1)
        bound += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        bound += struct.pack('!HH', 1, result_format)
        sock.sendall(client.typed(b'B', portal + b'\0' + name + b'\0' + bound) +
                     client.typed(b'D', b'P' + portal + b'\0') + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        bound_result = check('BIND DESCRIBE ' + sql, messages, descriptor=(oid, width, result_format), column_name=column_name)
        if not any(kind == b'2' for kind, _ in messages):
            failures.append(('missing BindComplete', sql, messages))
        if bound_result[1]:
            query('ROLLBACK TO SAVEPOINT null_binding'); query('RELEASE SAVEPOINT null_binding'); return
        if before_execute:
            before_execute()
        sock.sendall(client.typed(b'E', portal + b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        expected_rows = [[None]] if rows is None else rows
        executed = check('EXECUTE ' + sql, messages, rows=expected_rows, tag='SELECT ' + str(len(expected_rows)))
        if not executed[1]:
            sock.sendall(client.typed(b'C', b'P' + portal + b'\0') +
                         client.typed(b'C', b'S' + name + b'\0') + client.typed(b'S'))
            closed = client.read_until_ready(sock); check('CLOSE ' + sql, closed)
            if sum(kind == b'3' for kind, _ in closed) != 2:
                failures.append(('real CloseComplete lifecycle', sql, closed))
        # Recover the same explicit savepoint on both servers so a real XX000
        # never contaminates subsequent OID/NULL controls with 25P02 cascades.
        query('ROLLBACK TO SAVEPOINT null_binding'); query('RELEASE SAVEPOINT null_binding')

    try:
        query('BEGIN')
        query('CREATE SCHEMA "' + schema + '"')
        query('CREATE TABLE ' + table + '(id INTEGER PRIMARY KEY,"ID" BIGINT,p TEXT)')
        for oid, type_name, width in types:
            query('SELECT CAST(NULL AS ' + type_name + ') AS value', rows=[[None]], descriptor=(oid, width, 0), tag='SELECT 1')
            for wire_format in (0, 1):
                extended('SELECT $1 AS value', oid, width, parameter_format=wire_format, result_format=wire_format)
                extended('VALUES($1)', oid, width, parameter_format=wire_format, column_name='column1')
        for populated in (False, True):
            if populated:
                query('INSERT INTO ' + table + " VALUES(1,9007199254740993,NULL),(2,NULL,'NULL')")
            for oid, type_name, width in types:
                body = '(SELECT $1 FROM ' + table + ' ORDER BY id FETCH FIRST 1 ROW WITH TIES)'
                for wire_format in (0, 1):
                    extended('SELECT CAST(' + body + ' AS ' + type_name + ') AS value', oid, width,
                             parameter_format=wire_format, result_format=wire_format)
                extended('SELECT CAST(' + body + ' AS ' + type_name + ') AS value WHERE false', oid, width, rows=[])
                extended('SELECT CAST(' + body + ' AS ' + type_name + ') AS value LIMIT 0', oid, width, rows=[])
        for value in ('', 'NULL', 'O\'Brien $1 NULL'):
            extended('SELECT $1 AS value', 25, -1, value=value, rows=[[value]])
        extended('SELECT $1 AS value', 20, 8, value='9007199254740993', rows=[['9007199254740993']])
        extended('SELECT $1 AS value', 23, 4, value=struct.pack('!i', 7), parameter_format=1, rows=[['7']])
        extended('SELECT CAST(CASE WHEN $1 IS NULL THEN $1 ELSE $1 END AS INTEGER) AS value', 23, 4)
        extended("SELECT 'NULL $1 CAST(NULL AS integer)' AS value", 25, -1, parameter_oid=23,
                 rows=[['NULL $1 CAST(NULL AS integer)']])
        sequence = '"' + schema + '".effects'; writer = 'typed_null_writer_' + uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE ' + sequence)
        if reference:
            query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
        query('CREATE FUNCTION ' + writer + '(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\'' +
              schema + '.effects\'); RETURN p; END$$')
        query("SELECT nextval('" + schema + ".effects') AS value", rows=[['1']], descriptor=(20, 8, 0))
        no_effects = lambda: query("SELECT currval('" + schema + ".effects') AS value", rows=[['1']], descriptor=(20, 8, 0))
        extended('SELECT CAST((SELECT ' + writer + '($1) FROM ' + table +
                 ' ORDER BY id FETCH FIRST 1 ROW WITH TIES) AS INTEGER) AS value', 23, 4, before_execute=no_effects)
        query("SELECT currval('" + schema + ".effects') AS value", rows=[['3']], descriptor=(20, 8, 0))
        print('TYPED_NULL_COMPLETE', controls, 'failures', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, 'ROLLBACK')
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == '__main__':
    main()
