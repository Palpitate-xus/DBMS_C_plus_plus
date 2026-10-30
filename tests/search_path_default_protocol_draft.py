#!/usr/bin/env python3
"""Actual default/reset/startup search_path and effective-role namespace.

Private instances only. Does not exercise the skipped default protocol main,
ACL equivalence or RESET ALL authorization behavior under a switched role.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, WIDTHS, identifier
import socket
import struct
import sys
import uuid
from types import SimpleNamespace

DEFAULT = '"$user", public'
WIDTHS[19] = 64
EXPECTED_CONTROLS = 625


class PathWire(Wire):
    ready = b'I'

    def collect(self, label, producer, validator, expected):
        try:
            actual = producer()
            # The reusable row/command codec expects idle, while this fixture
            # also owns open transactions. Check the real state first; only
            # then adapt that one field for the shared codec. Record originals.
            valid_state = actual[-1:] == [(b'Z', self.ready)]
            normalized = [(kind, b'I' if kind == b'Z' else body) for kind, body in actual]
            good = valid_state and validator(normalized)
        except Exception as error:
            actual, good = (type(error).__name__, str(error)), False
        self.review.record(label, good, actual, expected)

    def command(self, sql, tag, notices=False, parameter_status=None):
        if tag == 'BEGIN':
            self.ready = b'T'
        elif tag == 'COMMIT' or (tag == 'ROLLBACK' and not sql.startswith('ROLLBACK TO')):
            self.ready = b'I'
        return super().command(sql, tag, notices, parameter_status)

    def show(self, mode, wanted):
        self.name = 'search_path'
        sql = 'SHOW search_path'
        binary = mode == 'binary'
        producer = (lambda: self.client.simple_query(self.sock, sql)) if mode == 'simple' else (
            lambda: self.exchange(self.parse(b'', sql) + self.bind(b'', binary_output=binary)))

        def valid(messages):
            if [body for kind, body in messages if kind == b'C'] != [b'SHOW\0']:
                return False
            normalized = [(kind, b'SELECT 1\0' if kind == b'C' else body) for kind, body in messages]
            return self.result_ok(normalized, (wanted,), 25, binary=binary,
                                  parsed=mode != 'simple', bound=mode != 'simple', simple=mode == 'simple')
        self.collect((mode, sql), producer, valid, (wanted, 25, 'SHOW'))
        self.name = 'v'

    def role(self, sql, status):
        def valid(messages):
            reports = [body for kind, body in messages if kind == b'S']
            wanted = [] if status is None else [b'is_superuser\0' + status.encode() + b'\0']
            return (reports == wanted and [kind for kind, _ in messages if kind != b'S'] == [b'C', b'Z'] and
                    [body for kind, body in messages if kind == b'C'] ==
                    [(b'RESET' if sql.startswith('RESET') else b'SET') + b'\0'] and messages[-1:] == [(b'Z', b'I')])
        self.collect(sql, lambda: self.client.simple_query(self.sock, sql), valid, ('role', status))


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    reference = bool(sys.argv[1:])
    review = Review('SEARCH_PATH_DEFAULT_PROTOCOL')
    wire = PathWire(review)
    if reference:
        host, port, user, database, password = review.runner._reference_connection_settings()
    else:
        host, port, user, database, password = '127.0.0.1', review.server['port'], 'alice', 'info', 'secret'
    role = 'r210_role_' + uuid.uuid4().hex[:12]
    missing = 'r210_empty_' + uuid.uuid4().hex[:12]
    table, function = 'r210_default_rows', 'r210_default_pick'
    owners, roles, public_owned = [], [], False

    def schemas(w, wanted):
        for mode in MODES:
            w.query(mode, 'SELECT pg_catalog.current_schema() AS v', (wanted,), 19)

    def state(path, schema, picked=None, identity=user):
        for mode in MODES:
            wire.show(mode, path)
            wire.query(mode, "SELECT current_setting('search_path') AS v", (path,), 25)
            wire.query(mode, 'SELECT current_user AS v', (identity,), 19)
            for expression in ('current_schema', 'current_schema()', 'pg_catalog.current_schema()'):
                wire.query(mode, 'SELECT ' + expression + ' AS v', (schema,), 19)
            if picked is not None:
                wire.query(mode, 'SELECT v+0 AS v FROM ' + table, (picked,), 23)
                wire.query(mode, 'SELECT ' + function + '() AS v', (picked,), 23)

    def startup(parameters, wanted):
        sock = socket.create_connection((host, port), timeout=review.runner.wire_timeout())
        child = PathWire(SimpleNamespace(client=review.client, sock=sock, record=review.record))
        application = 'r210-startup'

        def produce():
            child.client.send_startup_packet(sock, user, database, application, 196608, {},
                                            dict({'TimeZone': 'UTC'}, **parameters), False)
            child.client.complete_startup_authentication(sock, user, password)
            return child.client.read_until_ready(sock)

        def valid(messages):
            statuses = {}
            for kind, body in messages:
                if kind != b'S':
                    continue
                parts = body.split(b'\0')
                if len(parts) != 3 or parts[-1] or parts[0] in statuses:
                    return False
                statuses[parts[0]] = parts[1]
            expected = {
                b'server_version': b'18.6' if reference else b'18.0 DBMS-C++ 0.2.0',
                b'server_encoding': b'UTF8', b'client_encoding': b'UTF8',
                b'application_name': application.encode(), b'DateStyle': b'ISO, MDY',
                b'IntervalStyle': b'postgres', b'is_superuser': b'on',
                b'session_authorization': user.encode(), b'default_transaction_read_only': b'off',
                b'in_hot_standby': b'off', b'integer_datetimes': b'on',
                b'standard_conforming_strings': b'on', b'scram_iterations': b'4096',
                b'search_path': wanted.encode(), b'TimeZone': b'UTC'}
            return (statuses == expected and messages[:1] == [(b'R', struct.pack('!I', 0))] and
                    messages[-1:] == [(b'Z', b'I')] and
                    sum(kind == b'K' and len(body) == 8 for kind, body in messages) == 1 and
                    all(kind in (b'R', b'S', b'K', b'Z') for kind, _ in messages))
        child.collect(('STARTUP', parameters), produce, valid, wanted)
        return sock, child

    try:
        for name in (role, missing):
            wire.command('CREATE ROLE ' + name, 'CREATE ROLE')
            roles.append(name)
        wire.command('CREATE SCHEMA ' + identifier(user), 'CREATE SCHEMA')
        owners.append(identifier(user))
        wire.command('CREATE SCHEMA ' + role + ' AUTHORIZATION ' + role, 'CREATE SCHEMA')
        owners.append(role)
        for owner, value in (('public', 11), (identifier(user), 22)):
            wire.command('CREATE TABLE ' + owner + '.' + table + '(v integer)', 'CREATE TABLE')
            wire.command('INSERT INTO ' + owner + '.' + table + ' VALUES(' + str(value) + ')', 'INSERT 0 1')
            wire.command('CREATE FUNCTION ' + owner + '.' + function + '() RETURNS integer '
                         'LANGUAGE SQL IMMUTABLE AS $$SELECT ' + str(value) + '$$', 'CREATE FUNCTION')
        public_owned = True
        state(DEFAULT, user, 22)
        wire.command('SET search_path TO pg_catalog', 'SET', parameter_status='pg_catalog')
        wire.command('RESET search_path', 'RESET', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        wire.command('SET search_path TO public', 'SET', parameter_status='public')
        wire.command('SET search_path TO DEFAULT', 'SET', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        wire.command('RESET ALL', 'RESET', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        wire.command('BEGIN', 'BEGIN')
        wire.command('SET LOCAL search_path TO public', 'SET', parameter_status='public')
        state('public', 'public', 11)
        wire.command('RESET search_path', 'RESET', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        wire.command('ROLLBACK', 'ROLLBACK', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        wire.command('BEGIN', 'BEGIN')
        wire.command('SET search_path TO public', 'SET', parameter_status='public')
        wire.command('SAVEPOINT path_reset', 'SAVEPOINT')
        wire.command('SET LOCAL search_path TO pg_catalog', 'SET', parameter_status='pg_catalog')
        wire.command('ROLLBACK TO path_reset', 'ROLLBACK', parameter_status='public')
        for mode in MODES:
            wire.show(mode, 'public')
        wire.command('COMMIT', 'COMMIT', parameter_status='public')
        for mode in MODES:
            wire.show(mode, 'public')
        wire.command('RESET search_path', 'RESET', parameter_status=DEFAULT)
        state(DEFAULT, user, 22)
        for index, target in enumerate((role, missing)):
            wire.role('SET ROLE ' + target, 'off' if index == 0 else None)
            state(DEFAULT, role if target == role else 'public', identity=target)
            wire.command('RESET search_path', 'RESET', parameter_status=DEFAULT)
            state(DEFAULT, role if target == role else 'public', identity=target)
        wire.role('RESET ROLE', 'on')
        state(DEFAULT, user, 22)
        cases = (
            ({}, DEFAULT, user), ({'search_path': 'public'}, 'public', 'public'),
            ({'search_path': 'pg_catalog, public'}, 'pg_catalog, public', 'pg_catalog'),
            ({'options': '-c search_path=pg_catalog'}, 'pg_catalog', 'pg_catalog'),
            ({'search_path': 'pg_catalog', 'options': '-c search_path=public'}, 'public', 'public'),
            ({'search_path': ''}, '', None),
            ({'options': '-c search_path=\"$user\",\ public'}, DEFAULT, user))
        for parameters, wanted, schema in cases:
            sock, child = startup(parameters, wanted)
            try:
                for mode in MODES:
                    child.show(mode, wanted)
                    child.query(mode, "SELECT current_setting('search_path') AS v", (wanted,), 25)
                schemas(child, schema)
                child.command('SET search_path TO pg_catalog', 'SET', parameter_status='pg_catalog')
                child.command('RESET search_path', 'RESET', parameter_status=wanted)
                for mode in MODES:
                    child.show(mode, wanted)
                schemas(child, schema)
                child.command('SET search_path TO public', 'SET', parameter_status='public')
                child.command('SET search_path TO DEFAULT', 'SET', parameter_status=wanted)
                for mode in MODES:
                    child.show(mode, wanted)
                schemas(child, schema)
                child.command('RESET ALL', 'RESET', parameter_status=wanted)
                for mode in MODES:
                    child.show(mode, wanted)
                schemas(child, schema)
                child.command('BEGIN', 'BEGIN')
                child.command('SET LOCAL search_path TO public', 'SET', parameter_status='public')
                child.command('ROLLBACK', 'ROLLBACK', parameter_status=wanted)
                for mode in MODES:
                    child.show(mode, wanted)
                schemas(child, schema)
            finally:
                sock.close()
        for owner in reversed(owners):
            wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owners.clear()
        wire.command('DROP FUNCTION public.' + function + '()', 'DROP FUNCTION')
        wire.command('DROP TABLE public.' + table, 'DROP TABLE')
        public_owned = False
        for target in reversed(roles):
            wire.command('DROP ROLE ' + target, 'DROP ROLE')
        roles.clear()
        review.complete(EXPECTED_CONTROLS)
    finally:
        review.client.simple_query(review.sock, 'RESET ROLE')
        for owner in reversed(owners):
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        if public_owned:
            review.client.simple_query(review.sock, 'DROP FUNCTION IF EXISTS public.' + function + '()')
            review.client.simple_query(review.sock, 'DROP TABLE IF EXISTS public.' + table)
        for target in reversed(roles):
            review.client.simple_query(review.sock, 'DROP ROLE IF EXISTS ' + target)
        review.close()


if __name__ == '__main__':
    main()
