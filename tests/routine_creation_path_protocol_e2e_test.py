#!/usr/bin/env python3
"""CREATE FUNCTION chooses the real creation namespace, not implicit pg_catalog."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('routine_creation_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h,p,u,d,pw = r._reference_connection_settings()
        sock = socket.create_connection((h,p), timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,password=pw); r.verify_reference_version(c,sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    failures = []
    def query(sql, rows=None, state=None):
        result = r.decode_wire_result(c.simple_query(sock,sql), include_types=True)
        print('ROUTINE_CREATION_PATH',sql,result,flush=True)
        if result[1] != state or (rows is not None and (result[0] != rows or result[5] != [23])):
            failures.append((sql,result,rows,state))
    def error(sql,state):
        query('SAVEPOINT routine_creation_error')
        try: query(sql,state=state)
        finally:
            query('ROLLBACK TO routine_creation_error')
            query('RELEASE routine_creation_error')
    try:
        query('BEGIN')
        query('CREATE SCHEMA routine_create_a')
        query('CREATE SCHEMA "Routine.Create.B"')
        query('SET LOCAL search_path=routine_create_missing,"Routine.Create.B",routine_create_a,public')
        query('CREATE FUNCTION creation_chosen() RETURNS INT LANGUAGE SQL AS $$SELECT 11$$')
        query('SELECT "Routine.Create.B".creation_chosen()', [['11']])
        error('SELECT public.creation_chosen() WHERE false','42883')
        query('SET LOCAL search_path=routine_create_a,"Routine.Create.B",public')
        query('CREATE FUNCTION creation_chosen() RETURNS INT LANGUAGE SQL AS $$SELECT 22$$')
        query('SELECT creation_chosen()', [['22']])
        query('SELECT "Routine.Create.B".creation_chosen()', [['11']])
        query('SET LOCAL search_path=routine_create_missing,"Routine.Create.B",routine_create_a')
        query('CREATE OR REPLACE FUNCTION creation_chosen() RETURNS INT LANGUAGE SQL AS $$SELECT 33$$')
        query('SELECT creation_chosen()', [['33']])
        query('SELECT routine_create_a.creation_chosen()', [['22']])
        query('SET LOCAL search_path=public')
        query('CREATE FUNCTION creation_chosen() RETURNS INT LANGUAGE SQL AS $$SELECT 44$$')
        query('SELECT public.creation_chosen()', [['44']])
        query("SET LOCAL search_path=''")
        error('CREATE FUNCTION creation_no_path() RETURNS INT LANGUAGE SQL AS $$SELECT 55$$','3F000')
        query('SET LOCAL search_path=routine_create_missing')
        error('CREATE FUNCTION creation_missing_path() RETURNS INT LANGUAGE SQL AS $$SELECT 55$$','3F000')
        query('SET LOCAL search_path=routine_create_a')
        query('SAVEPOINT routine_creation_undo')
        query('CREATE FUNCTION creation_undone() RETURNS INT LANGUAGE SQL AS $$SELECT 66$$')
        query('SELECT routine_create_a.creation_undone()', [['66']])
        query('ROLLBACK TO routine_creation_undo')
        query('RELEASE routine_creation_undo')
        error('SELECT routine_create_a.creation_undone() WHERE false','42883')
        query('SELECT public.creation_chosen()', [['44']])
        query('CREATE FUNCTION public.creation_explicit() RETURNS INT LANGUAGE SQL AS $$SELECT 77$$')
        query('SELECT public.creation_explicit()', [['77']])
        query('ROLLBACK')
        for failure in failures: print('ROUTINE_CREATION_PATH_FAILURE',failure,flush=True)
        assert not failures, f'{len(failures)} whole creation-path assertions failed'
        print('[ROUTINE CREATION PATH '+('PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
    finally:
        if server: r.stop_ours(server)
        else:
            try: c.simple_query(sock,'ROLLBACK')
            finally: sock.close()


if __name__ == '__main__': main()
