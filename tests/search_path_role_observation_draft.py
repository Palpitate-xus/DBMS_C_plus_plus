#!/usr/bin/env python3
"""Read actual private-instance default/role behavior; not a compatibility pass."""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import identifier
import sys
import uuid

if sys.argv[1:] == ['--reference']:
    sys.argv[1:] = ['--reference18']
review = Review('SEARCH_PATH_ROLE_OBSERVATION_NOT_A_COMPATIBILITY_PASS')
token = uuid.uuid4().hex
role = 'r210_role_' + token
missing = 'r210_empty_' + token
created = []
schemas = []
table = 'public.r210_probe'


def observe(sql, required=False):
    messages = review.client.simple_query(review.sock, sql)
    result = review.runner.decode_wire_result(messages, include_types=True)
    print('R210_OBSERVE', repr(sql), repr(result),
          'statuses', [body.hex() for kind, body in messages if kind == b'S'], flush=True)
    if required:
        assert result[1] is None, (sql, result)
    return result


try:
    user = observe('SELECT current_user;', True)[0][0][0]
    observe('SHOW search_path;', True)
    for name in (role, missing):
        observe('CREATE ROLE ' + name + ';', True)
        created.append(name)
    observe('CREATE SCHEMA ' + identifier(user) + ';', True)
    schemas.append(identifier(user))
    observe('CREATE SCHEMA ' + role + ' AUTHORIZATION ' + role + ';', True)
    schemas.append(role)
    for owner, value in (('public', 11), (identifier(user), 22), (role, 33)):
        qualified = owner + '.' + table.split('.', 1)[1]
        observe('CREATE TABLE ' + qualified + '(v integer);', True)
        observe('INSERT INTO ' + qualified + ' VALUES(' + str(value) + ');', True)
    # Public qualification has a separately retained GRANT lookup failure;
    # the unqualified public target here is the same existing object.
    observe('GRANT SELECT ON ' + table.split('.', 1)[1] + ' TO ' + role + ',' + missing + ';', True)
    observe('GRANT SELECT ON ' + role + '.' + table.split('.', 1)[1] + ' TO ' + role + ';', True)
    observe('SET search_path TO "$user", public;', True)
    for value in ('none', role, missing, 'none'):
        observe('SET ROLE ' + value + ';', True)
        for sql in ('SELECT current_user;', 'SHOW search_path;', 'SELECT current_schema;',
                    'SELECT current_schema();', 'SELECT pg_catalog.current_schema();',
                    'SELECT v+0 AS v FROM ' + table.split('.', 1)[1] + ';'):
            observe(sql)
finally:
    try:
        observe('RESET ROLE;')
        observe('DROP TABLE IF EXISTS ' + table + ';')
        for schema in reversed(schemas):
            observe('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE;')
        for name in reversed(created):
            observe('DROP ROLE IF EXISTS ' + name + ';')
    finally:
        review.close()
