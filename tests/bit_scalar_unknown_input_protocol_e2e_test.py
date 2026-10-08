"""Real physical BIT columns consume prepared UNKNOWN input, not raw prefixes."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('bit_scalar_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = '--reference18' in sys.argv; server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password); runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    schema = 'bit_scalar_' + uuid.uuid4().hex[:12]; table = '"' + schema + '".rows'
    values = ['01', '001', '', None, '0001', '1', '0', '00']
    inputs = [('b01', '01'), ('B001', '001'), ('b', ''), ('B', ''), ('x1', '0001'),
              ('x', ''), ('X', ''), ('X0aF', '000010101111'), ('01', '01'), ('', '')]
    failures = []; controls = 0

    def query(sql, rows=None, state=None, oid=None):
        nonlocal controls
        controls += 1; actual = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print('BIT_SCALAR_INPUT', sql, actual, flush=True)
        if actual[1] != state or rows is not None and actual[0] != rows or oid is not None and actual[5] != [oid]:
            failures.append((sql, actual, rows, state, oid))
        return actual

    def error(sql):
        query('SAVEPOINT scalar_bad_input'); query(sql, rows=[], state='22P02')
        query('ROLLBACK TO SAVEPOINT scalar_bad_input'); query('RELEASE SAVEPOINT scalar_bad_input')

    def match(op, lhs, rhs):
        return {'=': lhs == rhs, '<>': lhs != rhs, '!=': lhs != rhs, '<': lhs < rhs,
                '>': lhs > rhs, '<=': lhs <= rhs, '>=': lhs >= rhs}[op]

    try:
        query('BEGIN'); query('CREATE SCHEMA "' + schema + '"')
        query('CREATE TABLE ' + table + '(id INTEGER PRIMARY KEY,v VARBIT,u VARBIT,p TEXT)')
        query('CREATE INDEX bit_scalar_v ON ' + table + '(v)')
        for populated in (False, True):
            if populated:
                for i, value in enumerate(values):
                    literal = 'NULL' if value is None else "B'" + value + "'"
                    query('INSERT INTO ' + table + ' VALUES(' + str(i + 1) + ',' + literal + ',' + literal + ", 'b01')")
            for column in ('v', 'u'):
                for raw, bits in inputs:
                    for op in ('=', '<>', '!=', '<', '>', '<=', '>='):
                        expected = [[str(i + 1)] for i, value in enumerate(values) if value is not None and match(op, value, bits)] if populated else []
                        query('SELECT id FROM ' + table + ' WHERE ' + column + op + "'" + raw + "' ORDER BY id", expected, oid=23)
                for invalid in ('xg', 'b02', 'x 1', 'b 01', "B'01'", 'NULL'):
                    literal = "'" + invalid.replace("'", "''") + "'"
                    error('SELECT id FROM ' + table + ' WHERE ' + column + '=' + literal + ' ORDER BY id')
            query('SELECT id FROM ' + table + " WHERE p='b01' ORDER BY id", [[str(i + 1)] for i in range(len(values))] if populated else [], oid=23)
        query('SELECT id FROM ' + table + " WHERE v='b01' OR v='x1' ORDER BY id", [['1'], ['5']], oid=23)
        query('SELECT id FROM ' + table + " WHERE v='b01' LIMIT 1", [['1']], oid=23)
        query('SELECT id FROM ' + table + " WHERE v='b01' LIMIT 0", [], oid=23)
        for suffix in (' LIMIT 0', ' AND FALSE', ' AND id=99', ' OR id=1'):
            error('SELECT id FROM ' + table + " WHERE v='xg'" + suffix)
        quoted = '"' + schema + '".quoted_rows'
        query('CREATE TABLE ' + quoted + '(k BIT(4) PRIMARY KEY,id INTEGER,"V Space" VARBIT,"Q""Bit" BIT(4))')
        query('INSERT INTO ' + quoted + " VALUES(B'0001',1,B'01',B'0001'),(B'0100',2,NULL,NULL)")
        for column, raw, expected in [('k','x1',[['1']]),('k','b01',[]),('k','b',[]),
                                     ('"V Space"','b01',[['1']]),('"Q""Bit"','x1',[['1']])]:
            query('SELECT id FROM ' + quoted + ' WHERE ' + column + "='" + raw + "' ORDER BY id", expected, oid=23)
        domain = '"' + schema + '".varbit'; domain_rows = '"' + schema + '".domain_rows'
        query('CREATE DOMAIN ' + domain + ' AS TEXT')
        query('CREATE TABLE ' + domain_rows + '(id INTEGER PRIMARY KEY,d ' + domain + ')')
        query('INSERT INTO ' + domain_rows + " VALUES(1,'b01'),(2,'xg'),(3,NULL)")
        query('SELECT id FROM ' + domain_rows + " WHERE d='b01' ORDER BY id", [['1']], oid=23)
        query('SELECT id FROM ' + domain_rows + " WHERE d='xg' ORDER BY id", [['2']], oid=23)
        sequence = '"' + schema + '".effects'; writer = 'bit_scalar_writer_' + uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE ' + sequence)
        if reference:
            query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
        query('CREATE FUNCTION ' + writer + '(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\'' +
              schema + '.effects\'); RETURN p; END$$')
        query("SELECT nextval('" + schema + ".effects')", [['1']], oid=20)
        error('SELECT ' + writer + '(id) FROM ' + table + " WHERE v='xg'")
        query("SELECT currval('" + schema + ".effects')", [['1']], oid=20)
        query('SELECT ' + writer + '(id) FROM ' + table + " WHERE v='b01' LIMIT 1", [['1']], oid=23)
        query("SELECT currval('" + schema + ".effects')", [['2']], oid=20)
        print('BIT_SCALAR_INPUT_COMPLETE', controls, 'failures', len(failures), failures, flush=True)
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
