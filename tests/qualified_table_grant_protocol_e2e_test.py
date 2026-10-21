#!/usr/bin/env python3
"""GRANT/REVOKE resolve one table identity before checking or changing its ACL.

Exercises actual reads under non-owner roles; accepting an administrative GRANT
response alone cannot establish that the intended table received permission.
"""
from compat.numeric_type_review import Review
import struct
import sys
import uuid


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('QUALIFIED_TABLE_GRANT_PROTOCOL')
    token = uuid.uuid4().hex[:8]
    schemas = ('g219_' + token + '_a', 'G219_' + token + '_B')
    role, recipient = 'g219_' + token + '_r', 'g219_' + token + '_s'
    tables = ('g219_' + token, 'Mixed_' + token, 'dot.' + token, 'quo"' + token)
    client, sock = review.client, review.sock
    owned_schemas, owned_roles, owned_public = [], [], False

    def command(sql, state=None):
        messages = client.simple_query(sock, sql)
        result = review.runner.decode_wire_result(messages, include_types=True)
        valid = (result[1] == state and result[0] == [] and result[3] == [] and
                 result[5] == [] and messages[-1] == (b'Z', b'I') and
                 sum(kind == b'C' for kind, _ in messages) == (0 if state else 1))
        if state:
            valid = valid and result[4] is None
        review.record(sql, valid, result, ('command', state))
        return valid

    def prepare(sql):
        name = ('g219_' + str(review.controls)).encode()
        payload = name + b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
        sock.sendall(client.typed(b'P', payload) + client.typed(b'D', b'S' + name + b'\0') + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        expected = [(b'1', b''), (b't', b'\0\0'), (b'n', b''), (b'Z', b'I')]
        review.record('PREPARE ' + sql, messages == expected, messages, expected)
        return name

    def execute(name, sql, binary):
        bind = b'\0' + name + b'\0' + struct.pack('!HHHh', 0, 0, 1, int(binary))
        sock.sendall(client.typed(b'B', bind) + client.typed(b'D', b'P\0') +
                     client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        result = review.runner.decode_wire_result(messages, include_types=True)
        valid = ([kind for kind, _ in messages] == [b'2', b'n', b'C', b'Z'] and
                 messages[0][1] == messages[1][1] == b'' and messages[-1] == (b'Z', b'I') and
                 result[1] is None and result[0] == [] and result[3] == [] and result[5] == [])
        review.record(('EXECUTE', sql, binary), valid, (messages, result), 'BindComplete/NoData/CommandComplete/Ready')

    def read(who, table, value=None, column='n'):
        command('SET ROLE ' + who)
        review.query('SELECT ' + column + ' FROM ' + table,
                     rows=None if value is None else [[str(value)]],
                     oids=None if value is None else [23],
                     state='42501' if value is None else None)
        command('RESET ROLE')

    try:
        for who in (role, recipient):
            assert command('CREATE ROLE ' + who), 'private role not owned'
            owned_roles.append(who)
        for schema in schemas:
            assert command('CREATE SCHEMA ' + identifier(schema)), 'private schema not owned'
            owned_schemas.append(schema)
            for who in (role, recipient):
                command('GRANT USAGE ON SCHEMA ' + identifier(schema) + ' TO ' + who)
        for i, table in enumerate(tables):
            for j, schema in enumerate(schemas):
                target = identifier(schema) + '.' + identifier(table)
                command('CREATE TABLE ' + target + '(n integer,hidden integer)')
                command('INSERT INTO ' + target + ' VALUES(' + str(10 + i + 100*j) + ',99)')
        public = 'public.' + identifier(tables[0])
        assert command('CREATE TABLE ' + public + '(n integer,hidden integer)'), 'private public table not owned'
        owned_public = True
        command('INSERT INTO ' + public + ' VALUES(777,99)')
        # Explicit qualification must not follow the session's other namespace.
        command('SET search_path=' + identifier(schemas[1]) + ',public')
        for i, table in enumerate(tables):
            target = identifier(schemas[0]) + '.' + identifier(table)
            decoy = identifier(schemas[1]) + '.' + identifier(table)
            folded = schemas[0].upper() + '.' + (table.upper() if i == 0 else identifier(table))
            for mode, spelling in (('simple', target), ('extended', folded), ('binary', target)):
                read(role, target)
                read(role, decoy)
                grant = 'GRANT SELECT ON ' + ('TABLE ' if mode == 'extended' else '') + spelling + ' TO ' + role
                revoke = 'REVOKE SELECT ON ' + ('TABLE ' if mode != 'extended' else '') + target + ' FROM ' + role
                if mode != 'simple':
                    statement = prepare(grant)
                    read(role, target)  # Parse/Describe do not apply a grant.
                    execute(statement, grant, mode == 'binary')
                else:
                    command(grant)
                read(role, target, 10 + i)
                read(role, decoy)
                if mode != 'simple':
                    statement = prepare(revoke)
                    read(role, target, 10 + i)  # Nor do they revoke early.
                    execute(statement, revoke, mode == 'binary')
                else:
                    command(revoke)
                read(role, target)
                read(role, decoy)

        target = identifier(schemas[0]) + '.' + identifier(tables[0])
        decoy = identifier(schemas[1]) + '.' + identifier(tables[0])
        command('SET search_path=' + identifier(schemas[0]) + ',public')
        command('GRANT SELECT ON ' + identifier(tables[0]) + ' TO ' + role)
        read(role, target, 10)
        read(role, decoy)
        read(role, public)
        command('REVOKE SELECT ON TABLE ' + target + ' FROM ' + role)
        read(role, target)
        command('GRANT SELECT ON TABLE ' + public + ' TO ' + role)
        read(role, public, 777)
        read(role, target)
        command('REVOKE SELECT ON ' + public + ' FROM ' + role)
        read(role, public)
        command('GRANT SELECT(n) ON ' + target + ' TO ' + role)
        read(role, target, 10)
        read(role, target, column='hidden')
        command('REVOKE SELECT(n) ON TABLE ' + target + ' FROM ' + role)
        read(role, target)

        command('GRANT SELECT ON ' + target + ' TO ' + role + ' WITH GRANT OPTION')
        command('SET ROLE ' + role)
        command('GRANT SELECT ON TABLE ' + target + ' TO ' + recipient)
        command('RESET ROLE')
        read(recipient, target, 10)
        read(recipient, decoy)
        command('SET ROLE ' + role)
        command('REVOKE SELECT ON ' + target + ' FROM ' + recipient)
        command('RESET ROLE')
        read(recipient, target)
        command('REVOKE SELECT ON TABLE ' + target + ' FROM ' + role)
        read(role, target)
        for verb, preposition in (('GRANT', 'TO'), ('REVOKE', 'FROM')):
            for prefix in ('', 'TABLE '):
                for table, state in ((identifier(schemas[0]) + '.missing', '42P01'),
                                     ('g219_missing_' + token + '.missing', '3F000')):
                    command(verb + ' SELECT ON ' + prefix + table + ' ' + preposition + ' ' + role, state)
    finally:
        command('RESET ROLE')
        command('SET search_path=public')
        if owned_public:
            command('DROP TABLE ' + public)
        for schema in reversed(owned_schemas):
            command('DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        for who in reversed(owned_roles):
            command('DROP ROLE ' + who)
        review.close()
    review.complete(403)


if __name__ == '__main__':
    main()
