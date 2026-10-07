"""Direct declared TEXT parameters describe their builtin variable width."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('text_width_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); reference = '--reference18' in sys.argv; server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    failures = []; controls = 0

    def check(label, messages, rows=None, name=None, wire_format=0):
        nonlocal controls
        controls += 1; actual = runner.decode_wire_result(messages, include_types=True)
        print('TEXT_PARAMETER_WIDTH', label, actual, flush=True)
        if actual[1] or rows is not None and (actual[0] != rows or actual[4] != 'SELECT 1'):
            failures.append((label, actual, rows))
        if name is not None:
            fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
            if fields != [(name.encode(), 0, 0, 25, -1, -1, wire_format)]:
                failures.append(('builtin TEXT width, not default Column{}', label, fields))
        return actual

    def query(sql):
        return check(sql, client.simple_query(sock, sql))

    try:
        query('BEGIN')
        for sql, column_name in [('SELECT $1 AS value', 'value'), ('VALUES($1)', 'column1')]:
            for value in (None, '', 'NULL', "O'Brien $1 NULL"):
                for wire_format in (0, 1):
                    query('SAVEPOINT text_width')
                    statement = ('text_' + uuid.uuid4().hex[:12]).encode(); portal = statement + b'_p'
                    sock.sendall(client.typed(b'P', statement + b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, 25)) +
                                 client.typed(b'D', b'S' + statement + b'\0') + client.typed(b'S'))
                    messages = client.read_until_ready(sock)
                    check('PARSE DESCRIBE ' + sql, messages, name=column_name)
                    if not any(kind == b'1' for kind, _ in messages) or \
                            [payload for kind, payload in messages if kind == b't'] != [struct.pack('!HI', 1, 25)]:
                        failures.append(('ParseComplete and real declared OID', sql, messages))
                    raw = None if value is None else value.encode()
                    bound = struct.pack('!HHH', 1, wire_format, 1) + struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                    bound += struct.pack('!HH', 1, wire_format)
                    sock.sendall(client.typed(b'B', portal + b'\0' + statement + b'\0' + bound) +
                                 client.typed(b'D', b'P' + portal + b'\0') + client.typed(b'S'))
                    messages = client.read_until_ready(sock)
                    check('BIND DESCRIBE ' + sql, messages, name=column_name, wire_format=wire_format)
                    if not any(kind == b'2' for kind, _ in messages):
                        failures.append(('BindComplete', sql, messages))
                    sock.sendall(client.typed(b'E', portal + b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
                    executed = check('EXECUTE ' + sql, client.read_until_ready(sock), rows=[[value]])
                    if not executed[1]:
                        sock.sendall(client.typed(b'C', b'P' + portal + b'\0') +
                                     client.typed(b'C', b'S' + statement + b'\0') + client.typed(b'S'))
                        closed = client.read_until_ready(sock); check('CLOSE ' + sql, closed)
                        if sum(kind == b'3' for kind, _ in closed) != 2:
                            failures.append(('CloseComplete', sql, closed))
                    query('ROLLBACK TO SAVEPOINT text_width'); query('RELEASE SAVEPOINT text_width')
        print('TEXT_PARAMETER_WIDTH_COMPLETE', controls, 'failures', len(failures), failures, flush=True)
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
