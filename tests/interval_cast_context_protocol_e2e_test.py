#!/usr/bin/env python3
"""PG18-owned cast/input contexts, including the complete retained matrices."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import json
from pathlib import Path
import struct
import sys
import uuid


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_CAST_CONTEXT_PROTOCOL')
    client, sock = review.client, review.sock
    corpus = json.loads((Path(__file__).parent / 'compat/interval_cast_context_pg18_expected.json').read_text())
    assert corpus['version'] == '180006' and len(corpus['cases']) == 696
    schema = 'interval_context_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    owned = False

    def operation(mode, sql, expected, inputs=(), values=(), input_binary=False):
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(inputs))
            parse += b''.join(struct.pack('!I', oid) for oid in inputs)
            packet = client.typed(b'P', parse)
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                bind = b'\0\0' + struct.pack('!HHH', 1, int(input_binary), len(values))
                for value in values:
                    raw = None if value is None else value if input_binary else value.encode()
                    bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                bind += struct.pack('!HH', 1, int(mode == 'binary'))
                packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        rows, valid = strict_data_rows(messages)
        states = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        state = expected['describeState'] if mode == 'describe' else expected['state']
        if state:
            # A reached runtime cast can fail after RowDescription. Preserve
            # the exact reference metadata instead of requiring Parse failure.
            oids = expected.get('describeOids', expected['oids']) if mode == 'describe' else expected['oids']
            modifiers = expected.get('describeModifiers', expected['modifiers']) if mode == 'describe' else expected['modifiers']
            desired = [(oid, 16 if oid == 1186 else -1, modifier, int(mode == 'binary')) for oid, modifier in zip(oids, modifiers)]
            good = states == [state] and not rows and not tags and [field[3:] for field in fields] == desired
        else:
            binary = mode == 'binary'
            if mode == 'describe':
                wanted = []
            elif binary:
                wanted = [[None if value is None else bytes.fromhex(value) for value in row] for row in expected['binaryRows']]
            else:
                wanted = [[None if value is None else value.encode() for value in row] for row in expected['rows']]
            metadata = [(field[3], field[4], field[5], field[6]) for field in fields]
            oids = expected.get('describeOids', expected['oids']) if mode == 'describe' else expected['oids']
            modifiers = expected.get('describeModifiers', expected['modifiers']) if mode == 'describe' else expected['modifiers']
            desired = [(oid, 16 if oid == 1186 else -1, modifier, int(binary)) for oid, modifier in zip(oids, modifiers)]
            good = valid and not states and rows == wanted and metadata == desired
            good = good and tags == ([] if mode == 'describe' else ['SELECT ' + str(len(wanted))])
        good = good and messages[-1:] == [(b'Z', b'I')]
        if mode != 'simple':
            parse_complete = expected.get('describeParseComplete', 0 if state else 1) if mode == 'describe' else expected.get('parseComplete', 0 if state else 1)
            good = good and sum(kind == b'1' for kind, _ in messages) == parse_complete
        if mode == 'describe':
            good = good and not any(kind == b'2' for kind, _ in messages)
        review.record((mode, sql, inputs), good, (rows, states, fields, tags), expected)

    try:
        for case in corpus['cases']:
            for mode in ('simple', 'text', 'binary', 'describe'):
                operation(mode, case['sql'], case['expected'])
        for source_type in ('text', 'varchar', 'character', 'name', 'interval', 'time'):
            value = '00:00:01' if source_type in ('interval', 'time') else '1'
            oid = {'text': 25, 'varchar': 1043, 'character': 1042, 'name': 19, 'interval': 1186, 'time': 1083}[source_type]
            expected = dict(state=None, describeState=None, rows=[['00:00:00']], binaryRows=[[bytes(16).hex()]], oids=[1186], modifiers=[(8 << 16) | 65535])
            for mode in ('text', 'binary', 'describe'):
                operation(mode, 'SELECT CAST($1 AS interval DAY) AS v', expected, (oid,), (value,))
        previous = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == previous, 'private schema ownership not confirmed'
        owned = True
        review.query('CREATE TABLE ' + table + '(id integer,t text,v varchar,c char(3),i interval,tm time,a text[])', rows=[], tag='CREATE TABLE')
        review.query('INSERT INTO ' + table + " VALUES(1,'1','1','1','1 second','00:00:01','[-2:-1]={1,NULL}'),(2,NULL,NULL,NULL,NULL,NULL,NULL)", rows=[], tag='INSERT 0 2')
        # NAME is a declared cast/parameter role, not the user-deferred NAME
        # physical-column DDL module.
        for column in ('t', 'v', 'c', 'CAST(t AS name)', 'i', 'tm'):
            expected = dict(state=None, describeState=None, rows=[['00:00:00'], [None]], binaryRows=[[bytes(16).hex()], [None]], oids=[1186], modifiers=[(8 << 16) | 65535])
            for mode in ('simple', 'text', 'binary', 'describe'):
                operation(mode, 'SELECT CAST(' + column + ' AS interval DAY) AS v FROM ' + table + ' ORDER BY id', expected)
            for suffix in (' WHERE false', ' LIMIT 0'):
                empty = dict(expected, rows=[], binaryRows=[])
                for mode in ('simple', 'binary'):
                    operation(mode, 'SELECT CAST(' + column + ' AS interval DAY) AS v FROM ' + table + suffix, empty)
        expected = dict(state=None, describeState=None, rows=[['[-2:-1]={00:00:00,NULL}'], [None]],
                        binaryRows=[[(struct.pack('!iii', 1, 1, 1186) + struct.pack('!ii', 2, -2) + struct.pack('!i', 16) + bytes(16) + struct.pack('!i', -1)).hex()], [None]],
                        oids=[1187], modifiers=[(8 << 16) | 65535])
        for mode in ('simple', 'text', 'binary', 'describe'):
            operation(mode, 'SELECT CAST(a AS interval DAY[]) AS v FROM ' + table + ' ORDER BY id', expected)
        review.query('CREATE SEQUENCE ' + identifier(schema) + '.effects_', rows=[], tag='CREATE SEQUENCE')
        sequence = "'" + identifier(schema) + '.effects_' + "'"
        effect = 'SELECT CAST(CAST(nextval(' + sequence + ') AS text) AS interval DAY) AS v'
        expected = dict(state=None, describeState=None, rows=[['00:00:00']], binaryRows=[[bytes(16).hex()]], oids=[1186], modifiers=[(8 << 16) | 65535])
        operation('describe', effect, expected)
        review.query('SELECT currval(' + sequence + ')', state='55000')
        operation('binary', effect, expected)
        review.query('SELECT currval(' + sequence + ')', rows=[['1']], oids=[20])
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(2863)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
