#!/usr/bin/env python3
"""Immutable actual-PG interval EXTRACT/date_part values and complete wire checks."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import copy
import json
from pathlib import Path
import struct
import sys
import uuid


def literal(value):
    return 'NULL' if value is None else "'" + value.replace("'", "''") + "'"


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_EXTRACT_PROTOCOL')
    client, sock = review.client, review.sock
    corpus = json.loads((Path(__file__).parent / 'compat/interval_extract_pg18_expected.json').read_text())
    assert corpus['version'] == '180006' and len(corpus['cases']) == 996
    index = {(row['function'], row['unit'], row['value']): row for row in corpus['cases'] if row['group'] != 'signature'}
    units = ('year', 'month', 'quarter', 'decade', 'century', 'millennium', 'week', 'day', 'hour', 'minute', 'second', 'milliseconds', 'microseconds', 'epoch')
    schema = 'interval_extract_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    owned = False

    def operation(mode, sql, expected, inputs=(), values=(), input_binary=False):
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
            state, fields, parse_count = expected['state'], expected['simpleFields'], None
            wanted = [[None if value is None else value.encode() for value in row] for row in expected['rows']]
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(inputs))
            parse += b''.join(struct.pack('!I', oid) for oid in inputs)
            packet = client.typed(b'P', parse)
            if mode == 'describe':
                state, fields, parse_count = expected['describeState'], expected['describeFields'], expected['describeParseComplete']
                wanted = []
                packet += client.typed(b'D', b'S\0')
            else:
                spec = expected[mode]
                state, fields, parse_count = spec['state'], spec['fields'], spec['parseComplete']
                wanted = [[None if value is None else bytes.fromhex(value) for value in row] for row in spec['rows']]
                bind = b'\0\0' + struct.pack('!HHH', 1, int(input_binary), len(values))
                for value in values:
                    raw = None if value is None else value if isinstance(value, bytes) else value.encode()
                    bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                bind += struct.pack('!HH', 1, int(mode == 'binary'))
                packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        actual_rows, valid = strict_data_rows(messages)
        states = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        actual_fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = valid and states == ([state] if state else []) and actual_rows == wanted
        good = good and [list(field[3:]) for field in actual_fields] == fields
        good = good and tags == ([] if state or mode == 'describe' else ['SELECT ' + str(len(wanted))])
        good = good and messages[-1:] == [(b'Z', b'I')]
        if parse_count is not None:
            good = good and sum(kind == b'1' for kind, _ in messages) == parse_count
            good = good and sum(kind == b'2' for kind, _ in messages) == (0 if state or mode == 'describe' else 1)
        if mode == 'describe':
            good = good and not any(kind in (b'2', b'D', b'C') for kind, _ in messages)
        review.record((mode, sql, inputs, input_binary), good, (actual_rows, states, actual_fields, tags), expected)

    def parameter_expected(row):
        expected = copy.deepcopy(row['expected'])
        oid, size = (1700, -1) if row['function'] == 'extract' else (701, 8)
        expected.update(describeState=None, describeFields=[[oid, size, -1, 0]], describeParseComplete=1)
        for mode in ('text', 'binary'):
            # PG's fresh unnamed custom plan folds constant input during Bind.
            # A unit error there precedes BindComplete/portal RowDescription;
            # statement Describe itself still has a pure fixed declaration.
            expected[mode].update(fields=[] if expected['state'] else [[oid, size, -1, int(mode == 'binary')]], parseComplete=1)
        return expected

    try:
        for row in corpus['cases']:
            for mode in ('simple', 'text', 'binary', 'describe'):
                operation(mode, row['sql'], row['expected'])
        binary_values = {'1 year': struct.pack('!qii', 0, 0, 12), '-1 month': struct.pack('!qii', 0, 0, -1),
                         'infinity': struct.pack('!qii', 2**63-1, 2**31-1, 2**31-1), None: None}
        for function in ('extract', 'date_part'):
            for unit in ('epoch', 'quarter', 'week', 'second'):
                sql = 'SELECT ' + ('EXTRACT(' + literal(unit) + ' FROM $1)' if function == 'extract' else 'date_part(' + literal(unit) + ',$1)') + ' AS v'
                for value in ('1 year', '-1 month', 'infinity', None):
                    row = index[(function, unit, value)]
                    for binary_input in (False, True):
                        for mode in ('text', 'binary', 'describe'):
                            operation(mode, sql, parameter_expected(row), (1186,), (binary_values[value] if binary_input else value,), binary_input)
            row = index[(function, 'nonesuch', '-13 months 15 days -61234567 microseconds')]
            sql = 'SELECT ' + ("EXTRACT('nonesuch' FROM $1)" if function == 'extract' else "date_part('nonesuch',$1)") + ' AS v'
            for mode in ('text', 'binary', 'describe'):
                operation(mode, sql, parameter_expected(row), (1186,), (row['value'],))
        for unit, lookup in (('year', 'YEAR'), ('YEARS', 'years'), ('microsecond_extra', 'microsecond_extra'),
                             ('timezone', 'timezone'), ('nonesuch', 'nonesuch'), (None, None)):
            for value in ('-13 months 15 days -61234567 microseconds', 'infinity', None):
                row = index[('date_part', lookup, '1 year' if lookup is None and value is not None and value != 'infinity' else value)]
                for mode in ('text', 'binary', 'describe'):
                    operation(mode, 'SELECT date_part($1,$2) AS v', parameter_expected(row), (25, 1186), (unit, value))
        before = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == before, 'private schema ownership not confirmed'
        owned = True
        review.query('CREATE TABLE ' + table + '(id integer,i interval)', rows=[], tag='CREATE TABLE')
        table_values = ('1 year', '-13 months 14 days -3600000001 microseconds', 'infinity', '-infinity', None)
        review.query('INSERT INTO ' + table + ' VALUES' + ','.join('(' + str(i+1) + ',' + literal(value) + ')' for i, value in enumerate(table_values)), rows=[], tag='INSERT 0 5')
        for function in ('extract', 'date_part'):
            for unit in units:
                sample = index[(function, unit, '1 year')]
                expected = parameter_expected(sample)
                expected['rows'] = [index[(function, unit, value)]['expected']['rows'][0] for value in table_values]
                for mode in ('text', 'binary'):
                    expected[mode]['rows'] = [index[(function, unit, value)]['expected'][mode]['rows'][0] for value in table_values]
                expression = ('EXTRACT(' + literal(unit) + ' FROM i)' if function == 'extract' else 'date_part(' + literal(unit) + ',i)')
                sql = 'SELECT ' + expression + ' AS v FROM ' + table
                for mode in ('simple', 'text', 'binary', 'describe'):
                    operation(mode, sql + ' ORDER BY id', expected)
                empty = copy.deepcopy(expected)
                empty['rows'] = []
                empty['text']['rows'] = []
                empty['binary']['rows'] = []
                for suffix in (' WHERE false', ' LIMIT 0'):
                    for mode in ('simple', 'binary'):
                        operation(mode, sql + suffix, empty)
        for function in ('extract', 'date_part'):
            sequence = identifier(schema) + '.' + function + '_effects'
            review.query('CREATE SEQUENCE ' + sequence, rows=[], tag='CREATE SEQUENCE')
            argument = "CAST(CAST(nextval('" + sequence + "') AS text)||' years' AS interval)"
            expression = "EXTRACT('epoch' FROM " + argument + ')' if function == 'extract' else "date_part('epoch'," + argument + ')'
            sql = 'SELECT ' + expression + ' AS v'
            expected = parameter_expected(index[(function, 'epoch', '1 year')])
            operation('describe', sql, expected)
            review.query("SELECT currval('" + sequence + "')", state='55000')
            operation('binary', sql, expected)
            review.query("SELECT currval('" + sequence + "')", rows=[['1']], oids=[20])
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(4474)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
