#!/usr/bin/env python3
"""Whole-AST pure extended queries keep a logical, promotable transaction."""
import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('extended_literal_pgdiff',
        root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    first = server['sock']
    second = None
    serial = 0

    def query(sock, sql, rows=None, state=None, ready=b'I'):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b'Z', ready), (sql, messages[-1])
        if rows is not None: assert result[0] == rows, (sql, result)
        return messages

    def parse(name, sql):
        return client.typed(b'P', name.encode() + b'\0' + sql.encode() + b'\0\0\0')

    def bind(portal, statement):
        return client.typed(b'B', portal.encode() + b'\0' + statement.encode() + b'\0' +
            struct.pack('!HHH', 0, 0, 0))

    def describe(kind, name):
        return client.typed(b'D', kind + name.encode() + b'\0')

    def execute(portal):
        return client.typed(b'E', portal.encode() + b'\0' + struct.pack('!I', 0))

    def exchange(label, frames, rows=None, state=None, ready=b'I', kinds=None):
        second.sendall(b''.join(frames) + client.typed(b'S'))
        messages = client.read_until_ready(second)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (label, result)
        assert messages[-1] == (b'Z', ready), (label, messages[-1])
        if rows is not None: assert result[0] == rows, (label, result)
        if kinds is not None: assert [kind for kind, _ in messages] == kinds, (label, messages)
        print('[EXTENDED LITERAL DEMAND]', label, state or 'OK', flush=True)
        return messages

    def extended(label, sql, rows=None, state=None, ready=b'I', kinds=None):
        nonlocal serial
        serial += 1
        name, portal = 'demand_' + str(serial), 'portal_' + str(serial)
        return exchange(label, [parse(name, sql), bind(portal, name), execute(portal)],
            rows, state, ready, kinds)

    def open_virtual(name):
        second.sendall(parse(name, 'SELECT 1;') + client.typed(b'H'))
        assert client.read_message(second) == (b'1', b'')

    try:
        second = socket.create_connection(('127.0.0.1', server['port']), timeout=15)
        second.settimeout(15)
        client.startup(second, 'alice', 'info')
        query(first, 'CREATE TABLE ext_demand_rows(id INT PRIMARY KEY);')
        query(first, 'INSERT INTO ext_demand_rows VALUES(1);')
        query(first, 'CREATE SEQUENCE ext_demand_sequence;')
        query(first, "CREATE FUNCTION ext_demand_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN SELECT nextval('ext_demand_sequence') INTO n; INSERT INTO ext_demand_rows(id) VALUES(arg); RETURN arg; END; $$;")
        query(first, "CREATE FUNCTION ext_demand_failure(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n TEXT; q INT; BEGIN SELECT ext_demand_writer(arg) INTO n; SELECT CAST(n || 'bad' AS INT) INTO q; RETURN arg; END; $$;")
        query(second, 'SET lock_timeout=50;')
        messages = query(second, "SET application_name='extended-literal-demand';")
        assert (b'S', b'application_name\0extended-literal-demand\0') in messages, messages
        exchange('preparse_existing', [parse('existing_constant', 'SELECT 1;')], kinds=[b'1', b'Z'])
        query(first, 'BEGIN;', ready=b'T')
        query(first, 'ALTER TABLE ext_demand_rows ADD COLUMN v INT;', ready=b'T')
        exchange('describe_existing_under_lock', [describe(b'S', 'existing_constant')],
            kinds=[b't', b'T', b'Z'])
        exchange('full_constant_under_lock', [parse('new_constant', 'SELECT 1;'),
            describe(b'S', 'new_constant'), bind('new_portal', 'new_constant'),
            describe(b'P', 'new_portal'), execute('new_portal')], rows=[['1']])
        exchange('existing_bind_under_lock', [bind('old_portal', 'existing_constant'),
            describe(b'P', 'old_portal'), execute('old_portal')], rows=[['1']])
        for sql, rows in (
            ('SELECT NULL;', [[None]]), ('SELECT 1+2*3;', [['7']]),
            ("SELECT 'writer() FROM ext_demand_rows';", [['writer() FROM ext_demand_rows']]),
            ('SELECT 1 WHERE FALSE;', []), ('SELECT 1 LIMIT 0;', []),
            ('SELECT DISTINCT 1 ORDER BY 1 OFFSET 1;', []),
        ):
            extended(sql, sql, rows=rows)
        extended('bind_expression_error_not_lock_error', 'SELECT 1/0;',
            rows=[], state='22012', kinds=[b'1', b'E', b'Z'])
        extended('post_error_constant_recovery', 'SELECT 1;', rows=[['1']])
        # Parse/Bind/Describe can complete without Execute. Sync still ends
        # their snapshot and expires the portal rather than leaking T state.
        second.sendall(parse('flush_only', 'SELECT 1;') + bind('flush_portal', 'flush_only') +
            describe(b'P', 'flush_portal') + client.typed(b'H'))
        assert [client.read_message(second)[0] for _ in range(3)] == [b'1', b'2', b'T']
        exchange('planning_only_sync', [], kinds=[b'Z'])
        exchange('planning_portal_expires', [execute('flush_portal')],
            rows=[], state='34000', kinds=[b'E', b'Z'])
        # A later non-pure Parse must upgrade the *same* logical transaction
        # before catalog/CTE/function work; lock failure must discard it safely.
        open_virtual('before_upgrade')
        for sql in ('SELECT id FROM ext_demand_rows;', 'SELECT ext_demand_writer(90);',
            "SELECT CAST('1' AS INT);", 'WITH q AS(SELECT ext_demand_writer(91)) SELECT 1;'):
            extended('physical_owner_still_required: ' + sql, sql, rows=[], state='55P03')
            query(first, 'SELECT id FROM ext_demand_rows;', [['1']], ready=b'T')
        open_virtual('before_explicit')
        extended('user_BEGIN_promotes_protocol_block', 'BEGIN;', ready=b'T')
        query(second, 'SELECT 1;', [['1']], ready=b'T')
        query(second, 'SELECT id FROM ext_demand_rows;', state='55P03', ready=b'E')
        query(second, 'ROLLBACK;')
        query(first, 'ROLLBACK;')
        query(second, 'SELECT 1;', [['1']])
        messages = query(second, "SET application_name='after-ownership-promotion';")
        assert (b'S', b'application_name\0after-ownership-promotion\0') in messages, messages
        query(first, 'SELECT id FROM ext_demand_rows;', [['1']])
        query(second, "SELECT nextval('ext_demand_sequence');", [['1']])
        extended('post_upgrade_writer_executes_once', 'SELECT ext_demand_writer(2);', rows=[['2']])
        query(first, 'SELECT id FROM ext_demand_rows ORDER BY id;', [['1'], ['2']])
        # Actual function writes remain one atomic calling statement after
        # promotion; sequence advancement is intentionally nontransactional.
        extended('promoted_writer_error_rolls_back_rows',
            "SELECT ext_demand_failure(3);", state='22P02')
        query(first, 'SELECT id FROM ext_demand_rows ORDER BY id;', [['1'], ['2']])
        query(second, "SELECT currval('ext_demand_sequence');", [['3']])
        open_virtual('read_only_promotion')
        query(second, 'BEGIN READ ONLY;', ready=b'T')
        query(second, 'INSERT INTO ext_demand_rows(id) VALUES(4);', state='25006', ready=b'E')
        query(second, 'ROLLBACK;')
        open_virtual('snapshot_characteristics')
        query(second, 'BEGIN ISOLATION LEVEL SERIALIZABLE;', state='25001', ready=b'I')
        query(second, 'ROLLBACK;')
        open_virtual('savepoint_promotion')
        query(second, 'BEGIN;', ready=b'T')
        query(second, 'SAVEPOINT child;', ready=b'T')
        query(second, 'INSERT INTO ext_demand_rows(id) VALUES(4);', ready=b'T')
        query(second, 'ROLLBACK TO child;', ready=b'T')
        query(second, 'RELEASE child;', ready=b'T')
        query(second, 'COMMIT;')
        query(first, 'SELECT id FROM ext_demand_rows ORDER BY id;', [['1'], ['2']])
        open_virtual('advisory_promotion')
        query(second, 'BEGIN;', ready=b'T')
        query(second, 'SELECT pg_advisory_xact_lock(723918);', ready=b'T')
        query(first, 'SELECT pg_try_advisory_xact_lock(723918);', [['f']])
        query(second, 'ROLLBACK;')
        query(first, 'SELECT pg_try_advisory_xact_lock(723918);', [['t']])
        query(first, 'LISTEN extended_literal_channel;')
        open_virtual('notification_promotion')
        query(second, 'BEGIN;', ready=b'T')
        query(second, "NOTIFY extended_literal_channel, 'promoted';", ready=b'T')
        assert not any(kind == b'A' for kind, _ in query(first, 'SELECT 1;', [['1']]))
        query(second, 'COMMIT;')
        messages = query(first, 'SELECT 1;', [['1']])
        assert any(kind == b'A' and b'extended_literal_channel\0promoted\0' in payload
            for kind, payload in messages), messages
        query(first, 'UNLISTEN extended_literal_channel;')
        # An idle, never-promoted backend can disconnect without acquiring
        # the database mutex or leaving an active xid/portal behind.
        open_virtual('disconnect_virtual')
        second.close()
        second = socket.create_connection(('127.0.0.1', server['port']), timeout=15)
        second.settimeout(15)
        client.startup(second, 'alice', 'info')
        query(second, 'SELECT id FROM ext_demand_rows ORDER BY id;', [['1'], ['2']])
        print('[EXTENDED LITERAL TRANSACTION DEMAND PROTOCOL E2E] passed', flush=True)
    finally:
        if second is not None: second.close()
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
