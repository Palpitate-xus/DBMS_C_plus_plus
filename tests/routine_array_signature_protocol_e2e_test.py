#!/usr/bin/env python3
"""Builtin array routine declarations retain actual parameter/return identity."""
import importlib.util
import socket
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('routine_array_runner', root/'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
c = r.load_protocol_client()
reference = sys.argv[1:] == ['--reference18']
assert not sys.argv[1:] or reference
server = None
if reference:
    host, port, user, database, password = r._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=r.wire_timeout())
    c.startup_reference(sock, user, database, password=password)
    r.verify_reference_version(c, sock)
else:
    server = r.start_ours(c); sock = server['sock']
failures = []

def query(sql, rows=None, oid=None, error=None):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('ROUTINE_ARRAY_SQL', sql, result, flush=True)
    if result[1] != error or (rows is not None and result[0] != rows) or (oid is not None and result[5] != oid):
        failures.append((sql, result, rows, oid, error))
    return result

def reject(sql, state):
    query('SAVEPOINT routine_array_error')
    query(sql, error=state)
    query('ROLLBACK TO SAVEPOINT routine_array_error')
    query('RELEASE SAVEPOINT routine_array_error')

try:
    query('BEGIN')
    # Original quoted builtin-like name: do not replace it with a scalar guard.
    query('CREATE FUNCTION "Unnest"(p INT[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 77; END$$')
    query('SELECT "Unnest"(ARRAY[1,2])', [['77']], [23])
    query('SELECT "Unnest"(CAST(NULL AS INT[]))', [['77']], [23])
    query('CREATE FUNCTION routine_array_echo(p INT[]) RETURNS INT[] LANGUAGE plpgsql AS $$BEGIN RETURN p; END$$')
    query('SELECT routine_array_echo(ARRAY[1,2])', [['{1,2}']], [1007])
    query('SELECT routine_array_echo(ARRAY[]::INT[])', [['{}']], [1007])
    query('SELECT routine_array_echo(CAST(NULL AS INT[]))', [[None]], [1007])
    query('CREATE FUNCTION routine_array_text(p TEXT[]) RETURNS TEXT[] LANGUAGE plpgsql AS $$BEGIN RETURN p; END$$')
    query("SELECT routine_array_text(ARRAY['NULL',''])", [['{"NULL",""}']], [1009])
    query('CREATE FUNCTION routine_array_multi(p INT[],q TEXT) RETURNS INT[] LANGUAGE plpgsql AS $$BEGIN RETURN p; END$$')
    query("SELECT routine_array_multi(ARRAY[3,4],'unchanged')", [['{3,4}']], [1007])
    query('CREATE OR REPLACE FUNCTION "Unnest"(p INTEGER[][]) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RETURN 78; END$$')
    query('SELECT "Unnest"(ARRAY[1,2])', [['78']], [23])
    reject('CREATE OR REPLACE FUNCTION routine_array_echo(p INT[]) RETURNS TEXT[] LANGUAGE plpgsql AS $$BEGIN RETURN p; END$$', '42P13')
    query('SELECT routine_array_echo(ARRAY[1,2])', [['{1,2}']], [1007])
    reject('CREATE FUNCTION routine_array_missing(p imaginary[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 1; END$$', '42704')
    reject('CREATE FUNCTION routine_array_void(p VOID[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 1; END$$', '42704')
    query('ROLLBACK')
finally:
    try: c.simple_query(sock, 'ROLLBACK')
    finally:
        if server: r.stop_ours(server)
        else: sock.close()
print('ROUTINE_ARRAY_FAILURES', failures, flush=True)
assert not failures, failures
