#!/usr/bin/env python3
"""PG18.6 window key semantics, genuine type identities and lazy comparisons.

Simple and valid Extended text/binary protocol; every Execute has quota zero.
The frozen reference contains complete raw cell bytes, descriptors, SQLSTATE,
message ordering and error prefixes. This is a correctness test, not a cost
assertion or a claim that window stages obey work_mem.
"""
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import tempfile

from compat.numeric_type_review import Review

LABEL = 'WINDOW_INTEGER_KEY_CACHE_PROTOCOL'
EXPECTED_CONTROLS = 143
SMALL = [-32768, -32767, -1, 0, 1, 32766, 32767, None]
INTEGER = [-2147483648, -2147483647, -1, 0, 1, 2147483646, 2147483647, None]
BIGINT = [-9223372036854775808, -9223372036854775807, -9007199254740993,
          -9007199254740992, -1, 0, 1, 9007199254740992, 9007199254740993,
          9223372036854775806, 9223372036854775807, None]


def literal(value):
    return 'NULL' if value is None else "'" + str(value).replace("'", "''") + "'"


def cases():
    result = []

    def add(label, sql, mode='simple', inputs=(), input_oids=()):
        result.append(dict(label=label, sql=sql, mode=mode,
                           inputs=list(inputs), input_oids=list(input_oids)))

    add('enum declaration', "CREATE TYPE wkc_mood AS ENUM ('zeta','','NULL','alpha')")
    add('domain declaration', 'CREATE DOMAIN wkc_domain AS BIGINT')
    add('physical source declaration', 'CREATE TABLE wkc_rows(id INTEGER,p SMALLINT,s SMALLINT,i INTEGER,b BIGINT,n NUMERIC(30,0),f DOUBLE PRECISION,e wkc_mood,d wkc_domain,t TEXT COLLATE "C")')
    rows = []
    for witness in range(48):
        b = BIGINT[(witness * 5) % len(BIGINT)]
        values = (witness + 1, witness % 4, SMALL[witness % 8], INTEGER[(witness * 3) % 8],
                  b, b, ['-Infinity', '-1', '-0', '0', '1', 'Infinity', 'NaN', None][witness % 8],
                  None if witness % 7 == 0 else ['zeta', '', 'NULL', 'alpha'][witness % 4],
                  b, ['', 'NULL', 'β', 'c', 'c', 'a', ' spaced ', None][witness % 8])
        rows.append('(' + ','.join(literal(value) for value in values) + ')')
    add('48 physical rows', 'INSERT INTO wkc_rows VALUES ' + ','.join(rows))
    add('actual enum identity', "SELECT oid FROM pg_type WHERE typname='wkc_mood'")
    add('actual domain identity', "SELECT oid FROM pg_type WHERE typname='wkc_domain'")
    add('genuine physical origin', 'SELECT id FROM wkc_rows LIMIT 0')
    add('domain output flatten descriptor', 'SELECT d FROM wkc_rows LIMIT 0')

    families = []
    for column in ('s', 'i', 'b'):
        for direction in ('ASC', 'DESC'):
            for nulls in ('FIRST', 'LAST'):
                key = column + ' ' + direction + ' NULLS ' + nulls
                families.append((column + ' ' + direction + ' NULLS ' + nulls,
                    'SELECT id,' + column + ' AS k,rank() OVER(ORDER BY ' + key + ') AS r,'
                    'dense_rank() OVER(ORDER BY ' + key + ') AS d,lag(' + column + ',0) OVER(ORDER BY id) AS v FROM wkc_rows ORDER BY id'))
    families += [
        ('multikey widths and peers', 'SELECT id,row_number() OVER(PARTITION BY p ORDER BY s,i,b,id) AS rn,rank() OVER(PARTITION BY p ORDER BY s,i,b) AS r,dense_rank() OVER(PARTITION BY p ORDER BY s,i,b) AS d FROM wkc_rows ORDER BY id'),
        ('same keys different frames', 'SELECT id,count(*) OVER(PARTITION BY p ORDER BY i GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS c,first_value(i) OVER(PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS f,last_value(i) OVER(PARTITION BY p ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l FROM wkc_rows ORDER BY id'),
        ('different stage keys', 'SELECT id,lag(b,1,0::bigint) OVER(PARTITION BY p ORDER BY s DESC NULLS FIRST,id) AS x,lead(i,1,0) OVER(PARTITION BY p ORDER BY b ASC NULLS LAST,id) AS y,rank() OVER(PARTITION BY p ORDER BY s) AS r FROM wkc_rows ORDER BY id'),
        ('int64 range arithmetic', 'SELECT id,count(*) OVER(ORDER BY b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c,first_value(b) OVER(ORDER BY b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS f FROM wkc_rows ORDER BY id'),
    ]
    for column, name in [('n', 'numeric fallback'), ('f', 'float NaN fallback'),
                         ('e', 'enum fallback'), ('d', 'genuine domain fallback'), ('t', 'C collation fallback')]:
        families.append((name, 'SELECT id,' + column + ' AS k,rank() OVER(ORDER BY ' + column + ' NULLS LAST) AS r,dense_rank() OVER(ORDER BY ' + column + ' NULLS LAST) AS d,lag(' + column + ',0) OVER(ORDER BY id) AS v FROM wkc_rows ORDER BY id'))
    families += [
        ('regtype array fallback', "SELECT id,a AS k,rank() OVER(ORDER BY a NULLS LAST) AS r,lag(a,0) OVER(ORDER BY id) AS v FROM (VALUES(1,ARRAY['boolean'::regtype,'bigint'::regtype]),(2,ARRAY['bigint'::regtype,'boolean'::regtype]),(3,ARRAY[NULL::regtype]),(4,NULL::regtype[])) AS s(id,a) ORDER BY id"),
        ('nested record fallback', "SELECT id,a AS k,rank() OVER(ORDER BY a NULLS LAST) AS r,lag(a,0) OVER(ORDER BY id) AS v FROM (VALUES(1,ROW(9007199254740993::bigint,ROW('x'::text,1::integer))),(2,ROW(9007199254740994::bigint,ROW(NULL::text,2::integer))),(3,ROW(NULL::bigint,ROW(''::text,3::integer))),(4,NULL::record)) AS s(id,a) ORDER BY id"),
        ('empty record versus NULL fallback', 'SELECT id,a AS k,rank() OVER(ORDER BY a NULLS LAST) AS r,lag(a,0) OVER(ORDER BY id) AS v FROM (VALUES(1,ROW()),(2,NULL::record),(3,ROW())) AS s(id,a) ORDER BY id'),
        ('argument error pruned by LIMIT1', 'SELECT id,lag(1/(id-3),0) OVER(ORDER BY id) AS v FROM wkc_rows ORDER BY id LIMIT 1'),
        ('argument error exact two row prefix', 'SELECT id,lag(1/(id-3),0) OVER(ORDER BY id) AS v FROM wkc_rows ORDER BY id LIMIT 3'),
        ('secondary expression still evaluated', 'SELECT id,rank() OVER(ORDER BY id,1/(id-3)) AS r FROM wkc_rows LIMIT 1'),
        ('unreached secondary record comparison', "SELECT id,rank() OVER(ORDER BY id,ROW('1'::json)) AS r FROM wkc_rows ORDER BY id"),
        ('reached secondary record comparison error', "SELECT id,rank() OVER(ORDER BY p,ROW('1'::json)) AS r FROM wkc_rows ORDER BY id"),
        ('singleton unsupported record comparison not demanded', "SELECT id,rank() OVER(ORDER BY ROW('1'::json)) AS r FROM wkc_rows WHERE id=1"),
        ('empty unsupported record comparison not demanded', "SELECT id,rank() OVER(ORDER BY ROW('1'::json)) AS r FROM wkc_rows WHERE false"),
    ]
    assert len(families) == 31
    for mode in ('simple', 'text', 'binary'):
        for label, sql in families:
            add(mode + ' ' + label, sql, mode)
        for index, shape in enumerate(('two argument sites', 'volatile order key', 'zero volatile order key')):
            sequence = 'wkc_' + mode + '_' + str(index)
            add(mode + ' ' + shape + ' create', 'CREATE SEQUENCE ' + sequence)
            add(mode + ' ' + shape + ' seed', "SELECT nextval('" + sequence + "')")
            if index == 0:
                sql = "SELECT id,lag(nextval('" + sequence + "'),0) OVER(PARTITION BY p ORDER BY b,id) AS a,lag(nextval('" + sequence + "'),0) OVER(PARTITION BY p ORDER BY b,id) AS b FROM wkc_rows ORDER BY id"
            else:
                sql = "SELECT id,lag(b,0) OVER(ORDER BY nextval('" + sequence + "')) AS v FROM wkc_rows ORDER BY id" + (' LIMIT 0' if index == 2 else '')
            add(mode + ' ' + shape, sql, mode)
            add(mode + ' ' + shape + ' post', "SELECT nextval('" + sequence + "')")
    for column, oid, value in [('s', 21, '-32768'), ('i', 23, '2147483647'), ('b', 20, '9007199254740993')]:
        for mode in ('text', 'binary'):
            add(mode + ' exact declared ' + column + ' parameter',
                'SELECT id,' + column + ' AS k,rank() OVER(ORDER BY ' + column + ') AS r FROM wkc_rows WHERE ' + column + '=$1 ORDER BY id',
                mode, [value], [oid])
    assert len(result) == EXPECTED_CONTROLS
    return result


def clean(value):
    if isinstance(value, bytes):
        return {'hex': value.hex()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, dict):
        return {(key.hex() if isinstance(key, bytes) else str(key)): clean(item) for key, item in value.items()}
    return value


def observe(sock, wire, case, index):
    if case['mode'] == 'simple':
        return wire.simple_query(sock, case['sql'])
    name = ('wkc_' + str(index)).encode()
    parse = name + b'\0' + case['sql'].encode() + b'\0' + struct.pack('!H', len(case['input_oids']))
    parse += b''.join(struct.pack('!I', oid) for oid in case['input_oids'])
    binary = case['mode'] == 'binary'
    bind = b'\0' + name + b'\0' + (struct.pack('!HH', 1, 1) if binary else struct.pack('!H', 0))
    bind += struct.pack('!H', len(case['inputs']))
    for value, oid in zip(case['inputs'], case['input_oids']):
        raw = None if value is None else struct.pack({21: '!h', 23: '!i', 20: '!q'}[oid], int(value)) if binary else value.encode()
        bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
    bind += struct.pack('!HH', 1, 1) if binary else struct.pack('!H', 0)
    sock.sendall(wire.typed(b'P', parse) + wire.typed(b'D', b'S' + name + b'\0') +
                 wire.typed(b'B', bind) + wire.typed(b'D', b'P\0') +
                 wire.typed(b'E', b'\0' + struct.pack('!I', 0)) + wire.typed(b'S'))
    return wire.read_until_ready(sock)


def decode(wire, messages):
    return dict(rows=clean(wire.data_row_values(messages)),
                descriptions=clean([wire.row_description_fields([message]) for message in messages if message[0] == b'T']),
                errors=clean([wire.diagnostic_fields(body) for kind, body in messages if kind == b'E']),
                states=[wire.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E'],
                tags=[body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C'],
                parameters=[[int.from_bytes(body[2 + 4 * i:6 + 4 * i], 'big') for i in range(int.from_bytes(body[:2], 'big'))] for kind, body in messages if kind == b't'],
                kinds=[kind.decode('ascii') for kind, body in messages],
                ready=[body.hex() for kind, body in messages if kind == b'Z'], raw=clean(messages))


def normalize(observations):
    by_label = {item['case']['label']: item['actual'] for item in observations}
    types = {}
    identities = {}
    for name, label in [('wkc_mood', 'actual enum identity'), ('wkc_domain', 'actual domain identity')]:
        actual = by_label[label]
        rows = actual['rows']
        if actual['states'] == [] and len(rows) == 1 and len(rows[0]) == 1 and rows[0][0] is not None:
            oid = int(bytes.fromhex(rows[0][0]['hex']).decode())
            if oid > 0:
                types[oid] = 'type:' + name
                identities[label] = [[{'identity': 'type:' + name}]]
    origin = by_label['genuine physical origin']
    fields = origin['descriptions'][0] if len(origin['descriptions']) == 1 else []
    table = fields[0][1] if len(fields) == 1 and fields[0][1] > 0 and fields[0][2:] == [1, 23, 4, -1, 0] else None
    normalized = []
    for item in observations:
        actual = item['actual']
        descriptions = []
        for description in actual['descriptions']:
            fields = []
            for field in description:
                field = list(field)
                if field[1]:
                    field[1] = 'relation:wkc_rows' if field[1] == table else 'UNRESOLVED_ORIGIN:' + str(field[1])
                field[3] = types.get(field[3], field[3])
                fields.append(field)
            descriptions.append(fields)
        normalized.append(dict(rows=identities.get(item['case']['label'], actual['rows']),
                               descriptions=descriptions, states=actual['states'], tags=actual['tags'],
                               parameters=actual['parameters'], kinds=actual['kinds'], ready=actual['ready']))
    return normalized


def collect(sock, wire, matrix):
    observations = []
    for index, case in enumerate(matrix, 1):
        observations.append(dict(control=index, case=case, actual=decode(wire, observe(sock, wire, case, index))))
    return observations


def main():
    fixture = json.loads((Path(__file__).with_name('compat') / 'window_integer_key_cache_pg18_expected.json').read_text())
    matrix = cases()
    assert [item['case'] for item in fixture['cases']] == matrix
    review = Review(LABEL)
    observations = []
    output = Path(os.environ['WINDOW_INTEGER_KEY_CACHE_OUTPUT_DIR']) if os.environ.get('WINDOW_INTEGER_KEY_CACHE_OUTPUT_DIR') else Path(tempfile.mkdtemp(prefix='dbms-window-integer-key-'))
    output.mkdir(parents=True, exist_ok=True)
    try:
        observations = collect(review.sock, review.client, matrix)
        normalized = normalize(observations)
        for item, actual, reference in zip(observations, normalized, fixture['cases']):
            expected = reference['expected']
            item['normalized'] = actual
            item['match'] = actual == expected
            review.record(item['case']['label'], item['match'], actual, expected)
        review.complete(EXPECTED_CONTROLS)
    finally:
        destination = output / ('reference-observations.json' if '--reference18' in sys.argv else 'observations.json')
        destination.write_text(json.dumps(dict(controls=review.controls, failures=len(review.failures),
            source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), observations=observations), ensure_ascii=False, indent=2) + '\n')
        print(LABEL + '_OBSERVATIONS', str(destination), flush=True)
        review.close()


if __name__ == '__main__':
    main()
