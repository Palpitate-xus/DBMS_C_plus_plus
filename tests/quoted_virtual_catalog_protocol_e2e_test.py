#!/usr/bin/env python3
"""Canonical known virtual catalogs retain quoted source and alias identities."""
from compat.numeric_type_review import Review
import sys
import uuid


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    reference = sys.argv[1:] == ['--reference18']
    r = Review('QUOTED_VIRTUAL_CATALOG_PROTOCOL')
    user, database = ((r.runner._reference_connection_settings()[2], r.runner._reference_connection_settings()[3])
                      if reference else ('alice', 'info'))
    private = 'v229_' + uuid.uuid4().hex[:8]
    cases = [('pg_class', 'relname', 19, private),
             ('pg_settings', 'name', 25, 'statement_timeout'),
             ('pg_stat_activity', 'usename', 19, user),
             ('pg_database', 'datname', 19, database),
             ('pg_type', 'typname', 19, 'int4')]
    owned = []

    def command(sql):
        messages = r.client.simple_query(r.sock, sql)
        result = r.runner.decode_wire_result(messages, include_types=True)
        valid = (result[1] is None and result[0] == [] and result[3] == [] and result[5] == [] and
                 messages[-1] == (b'Z', b'I') and sum(kind == b'C' for kind, _ in messages) == 1)
        r.record(sql, valid, result, 'successful command')

    try:
        command('CREATE TABLE public.' + ident(private) + '(n integer)')
        owned.append(private)
        for catalog, _, _, _ in cases:
            command('CREATE TABLE public.' + ident(catalog) + '(n integer)')
            owned.append(catalog)
            command('INSERT INTO public.' + ident(catalog) + ' VALUES(909)')
        for catalog, field, oid, value in cases:
            forms = ['pg_catalog.' + catalog, '"pg_catalog".' + catalog,
                     'pg_catalog.' + ident(catalog), '"pg_catalog".' + ident(catalog),
                     '"pg_catalog" . ' + ident(catalog)]
            for source in forms:
                for alias in ['', 'q229', ident('Alias "界 Name')]:
                    relation = source + (' AS ' + alias if alias else '')
                    prefix = alias + '.' if alias else ''
                    predicate = prefix + field + '=' + literal(value)
                    if catalog == 'pg_stat_activity':
                        predicate += ' AND ' + prefix + 'datname=' + literal(database)
                    sql = 'SELECT ' + prefix + field + ' AS v FROM ' + relation
                    r.query(sql + ' WHERE ' + predicate, rows=[[value]], oids=[oid])
                    r.query(sql + ' WHERE FALSE', rows=[], oids=[oid])
                r.query('SELECT wrong.' + field + ' AS v FROM ' + source + ' WHERE FALSE', state='42P01')
                # Full PostgreSQL catalog schemas are not fabricated here.
                r.query('SELECT * FROM ' + source + ' WHERE FALSE', state=None if reference else '0A000')
                if catalog != 'pg_type':
                    # Legacy typed subsets do not own full PG ordinal alias
                    # metadata; reject instead of shifting supported fields.
                    r.query('SELECT renamed FROM ' + source + ' AS q(renamed) WHERE FALSE',
                            state=None if reference else '0A000')
                else:
                    # The genuine pg_type binder already owns its first OID
                    # field. Do not reject a correctly retained prefix alias.
                    r.query('SELECT renamed AS v FROM ' + source + " AS q(renamed) WHERE typname='int4'",
                            rows=[['23']], oids=[26])
            # Explicit public qualification must never select a virtual
            # catalog with the same basename or borrow its column schema.
            r.query('SELECT n AS v FROM public.' + ident(catalog), rows=[['909']], oids=[23])
    finally:
        for name in reversed(owned):
            command('DROP TABLE public.' + ident(name))
        r.close()
    r.complete(247)


if __name__ == '__main__':
    main()
