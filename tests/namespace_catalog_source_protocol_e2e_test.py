#!/usr/bin/env python3
"""Database-owned pg_namespace rows, binding and typed protocol consumers."""
from compat.numeric_type_review import Review
import struct
import sys
import uuid


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    reference = sys.argv[1:] == ['--reference18']
    r = Review('NAMESPACE_CATALOG_SOURCE_PROTOCOL')
    token = uuid.uuid4().hex[:8]
    schemas = ['n225_' + token, 'N225_' + token, 'dot.' + token,
               '界' + token, 'q"' + token, 'space ' + token]
    relation = 'items_' + token
    pending = 'pending_' + token
    sources = ['pg_namespace', 'pg_catalog.pg_namespace',
               '"pg_catalog"."pg_namespace"']
    owned = []

    def command(sql, state=None, ready=b'I'):
        messages = r.client.simple_query(r.sock, sql)
        result = r.runner.decode_wire_result(messages, include_types=True)
        valid = (result[1] == state and result[0] == [] and result[3] == [] and
                 result[5] == [] and messages[-1] == (b'Z', ready) and
                 sum(kind == b'C' for kind, _ in messages) == (0 if state else 1))
        r.record(sql, valid, result, ('command', state, ready))

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def binary(oid, name):
        sql = 'SELECT oid AS id,nspname AS label FROM pg_catalog.pg_namespace WHERE oid=$1'
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, 26)
        bind = (b'\0\0' + struct.pack('!HHH', 1, 1, 1) +
                struct.pack('!iI', 4, oid) + struct.pack('!HH', 1, 1))
        r.sock.sendall(r.client.typed(b'P', parse) + r.client.typed(b'B', bind) +
                       r.client.typed(b'D', b'P\0') +
                       r.client.typed(b'E', b'\0' + struct.pack('!I', 0)) + r.client.typed(b'S'))
        messages = r.client.read_until_ready(r.sock)
        fields = (r.client.row_description_fields(messages)
                  if any(kind == b'T' for kind, _ in messages) else [])
        raw = name.encode()
        expected = struct.pack('!HiIi', 2, 4, oid, len(raw)) + raw
        valid = ([kind for kind, _ in messages] == [b'1', b'2', b'T', b'D', b'C', b'Z'] and
                 [field[3] for field in fields] == [26, 19] and
                 all(field[-1] == 1 for field in fields) and
                 [body for kind, body in messages if kind == b'D'] == [expected] and
                 messages[-1] == (b'Z', b'I'))
        r.record(('BINARY NAMESPACE', name), valid, (messages, fields), expected)

    try:
        for i, schema in enumerate(schemas):
            command('CREATE SCHEMA ' + ident(schema))
            owned.append(schema)
            command('CREATE TABLE ' + ident(schema) + '.' + ident(relation) + '(n integer)')
            command('INSERT INTO ' + ident(schema) + '.' + ident(relation) + ' VALUES(' + str(i) + ')')
        for schema in schemas:
            value = literal(schema)
            identity_sql = ('SELECT oid,nspname,nspowner FROM pg_catalog.pg_namespace '
                            'WHERE nspname=' + value)
            result = r.runner.decode_wire_result(r.client.simple_query(r.sock, identity_sql), include_types=True)
            valid = (result[1] is None and result[5] == [26, 19, 26] and len(result[0]) == 1 and
                     result[0][0][1] == schema and result[0][0][0].isdigit() and
                     int(result[0][0][0]) > 0 and result[0][0][2].isdigit() and int(result[0][0][2]) > 0)
            r.record(('ACTUAL NAMESPACE IDENTITY', schema), valid, result, ('one actual nonzero identity', schema))
            # Keep the owner assertion above. An invalid owner must not
            # replace a real OID by zero in the separate binary codec test.
            valid_oid = (result[1] is None and len(result[0]) == 1 and
                         result[0][0][0].isdigit() and int(result[0][0][0]) > 0)
            oid = int(result[0][0][0]) if valid_oid else 0
            for source in sources:
                sql = 'SELECT nspname AS label FROM ' + source + ' WHERE nspname=' + value
                r.query(sql, rows=[[schema]], oids=[19])
                r.describe(sql, oids=[19])
                r.parameter('SELECT nspname AS label FROM ' + source + ' WHERE nspname::text=$1',
                            [schema], [25], [[schema]], [19])
                r.query('SELECT n.nspname::text AS label,n.oid>0 AS present,n.nspowner>0 AS owned '
                        'FROM ' + source + ' AS n WHERE n.nspname=' + value,
                        rows=[[schema, 't', 't']], oids=[25, 16, 16])
                r.query('SELECT oid,nspname,nspowner FROM ' + source + ' WHERE FALSE', rows=[], oids=[26, 19, 26])
                r.describe('SELECT oid,nspname,nspowner FROM ' + source + ' LIMIT 0', oids=[26, 19, 26])
                r.query('SELECT count(*) AS n FROM ' + source + ' WHERE nspname=' + value,
                        rows=[['1']], oids=[20])
            binary(oid, schema)
            join = ('SELECT c.relname::text AS relation,n.nspname::text AS schema '
                    'FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                    'WHERE n.nspname=' + value + ' AND c.relname=' + literal(relation))
            r.query(join, rows=[[relation, schema]], oids=[25, 25])
            r.describe(join, oids=[25, 25])
            r.parameter(join.replace('n.nspname=' + value, 'n.nspname::text=$1'),
                        [schema], [25], [[relation, schema]], [25, 25])
            r.query('SELECT count(*) AS n FROM pg_catalog.pg_namespace a '
                    'JOIN pg_catalog.pg_namespace b ON a.oid=b.oid WHERE a.nspname=' + value,
                    rows=[['1']], oids=[20])
            r.query('SELECT nspname FROM (SELECT nspname FROM pg_namespace WHERE nspname=' + value + ') AS s',
                    rows=[[schema]], oids=[19])
            r.query('WITH x AS (SELECT nspname FROM pg_namespace WHERE nspname=' + value + ') SELECT * FROM x',
                    rows=[[schema]], oids=[19])
            r.query('SELECT n.nspname::text AS name,b.nspname::text AS absent '
                    'FROM pg_namespace n LEFT JOIN pg_namespace b ON b.oid=0 WHERE n.nspname=' + value,
                    rows=[[schema, None]], oids=[25, 25])
        for source in sources:
            r.query('SELECT nspname FROM ' + source + ' WHERE nspname=' + literal(pending), rows=[], oids=[19])
            r.query('SELECT count(*) AS n FROM ' + source + ' WHERE nspname=' + literal(pending), rows=[['0']], oids=[20])
            r.query('SELECT missing_column FROM ' + source + ' WHERE FALSE', state='42703')
            r.describe('SELECT missing_column FROM ' + source + ' WHERE FALSE', state='42703')
            r.query('SELECT wrong.nspname FROM ' + source + ' WHERE FALSE', state='42P01')
            r.query('SELECT nspname FROM ' + source + ' WHERE nspname', state='42804')
        command('BEGIN', ready=b'T')
        command('CREATE SCHEMA ' + ident(pending), ready=b'T')
        r.query('SELECT nspname FROM pg_namespace WHERE nspname=' + literal(pending), rows=[[pending]], oids=[19])
        command('ROLLBACK')
        r.query('SELECT nspname FROM pg_namespace WHERE nspname=' + literal(pending), rows=[], oids=[19])
        # A relation wildcard is not COUNT(*). ACL storage is not owned yet;
        # fail explicitly, including dead/empty and nested query shapes.
        unsupported = [
            'SELECT * FROM pg_namespace LIMIT 0',
            'SELECT n.* FROM pg_catalog.pg_namespace n WHERE FALSE',
            'SELECT nspacl FROM pg_namespace WHERE FALSE',
            'SELECT count(nspacl) FROM pg_namespace WHERE FALSE',
            'SELECT nspname FROM pg_namespace WHERE nspacl IS NULL',
            'SELECT * FROM (SELECT * FROM pg_namespace WHERE FALSE) AS x',
            'WITH x AS (SELECT * FROM pg_namespace WHERE FALSE) SELECT count(*) FROM x',
        ]
        unsupported += [
            'SELECT count(*) FROM pg_namespace a NATURAL JOIN pg_namespace b',
            'SELECT count(*) FROM pg_namespace a NATURAL LEFT JOIN pg_namespace b',
            'SELECT a.nspname FROM pg_namespace a JOIN pg_namespace b USING(nspacl) WHERE FALSE',
            'SELECT n.* FROM pg_namespace AS n(a,b,c) WHERE FALSE',
            'SELECT a FROM pg_namespace AS n(a,b,c,d) WHERE FALSE',
        ]
        for sql in unsupported:
            r.query(sql, state=None if reference else '0A000')
            if not reference:
                r.describe(sql, state='0A000')
        sql = 'SELECT nspacl FROM pg_namespace AS n(nspacl) WHERE FALSE'
        r.query(sql, state='42702')
        r.describe(sql, state='42702')
        sql = 'SELECT * FROM (SELECT nspname FROM pg_namespace WHERE FALSE) AS d'
        r.query(sql, rows=[], oids=[19])
        r.describe(sql, oids=[19])
        # A real same-named user table must keep the existing project lookup
        # contract. Explicit pg_catalog continues to select the owned source.
        if not reference:
            command('CREATE TABLE public.pg_namespace(nspacl integer)')
            command('INSERT INTO public.pg_namespace VALUES(225)')
            r.query('SELECT * FROM pg_namespace', rows=[['225']], oids=[23])
            r.query('SELECT nspacl FROM public.pg_namespace', rows=[['225']], oids=[23])
            r.query('SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname=' + literal(schemas[0]),
                    rows=[[schemas[0]]], oids=[19])
            command('DROP TABLE public.pg_namespace')
    finally:
        command('ROLLBACK')
        command('DROP SCHEMA IF EXISTS ' + ident(pending) + ' CASCADE')
        for schema in reversed(owned):
            command('DROP SCHEMA IF EXISTS ' + ident(schema) + ' CASCADE')
        r.close()
    r.complete(245 if reference else 263)


if __name__ == '__main__':
    main()
