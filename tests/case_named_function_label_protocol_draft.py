#!/usr/bin/env python3
"""Diagnostic: an ordinary CASE_WHEN callee is not a parser CASE node."""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, identifier
import sys
import uuid


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('CASE_NAMED_FUNCTION_LABEL_DIAGNOSTIC')
    wire = Wire(review)
    schema = 'case_name_' + uuid.uuid4().hex
    owner = identifier(schema)
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned
        wire.command('CREATE FUNCTION ' + owner + '.case_when(p integer) RETURNS integer '
                     'LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+1$$', 'CREATE FUNCTION')
        wire.command('SET search_path=public,' + schema + ',pg_catalog', 'SET',
                     parameter_status='public, ' + schema + ', pg_catalog')
        for callee in ('case_when', '"case_when"', owner + '.case_when', owner + '."case_when"'):
            for mode in MODES:
                for alias in (False, True):
                    wire.name = 'v' if alias else 'case_when'
                    wire.query(mode, 'SELECT ' + callee + '(-2)' + (' AS v' if alias else ''), (-1,), 23)
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(28)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
