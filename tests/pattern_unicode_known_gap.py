#!/usr/bin/env python3
"""Unfiltered Unicode, literal-metacharacter and escape-demand diagnostics."""
import importlib.util
import socket
import sys
from pathlib import Path

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('pattern_unicode_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference=sys.argv[1:]==['--reference18']
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    cases=[
        ("SELECT 'é' SIMILAR TO '_';",None,'t'),
        ("SELECT '中' NOT SIMILAR TO '_';",None,'f'),
        ("SELECT 'éé' SIMILAR TO '_{2}';",None,'t'),
        ("SELECT 'abc' SIMILAR TO 'a.c';",None,'f'),
        ("SELECT '^a$' SIMILAR TO '^a$';",None,'t'),
        ("SELECT 'a_' SIMILAR TO 'aé_' ESCAPE 'é';",None,'t'),
        ("SELECT 'a%' SIMILAR TO 'aé%' ESCAPE 'é';",None,'t'),
        ("SELECT E'a\\nb' SIMILAR TO 'a_b';",None,'t'),
        ("SELECT 'É' ILIKE 'é';",None,'t'),
        ("SELECT 'É_' ILIKE 'é#_' ESCAPE '#';",None,'t'),
        ("SELECT 'ab' LIKE E'a\\\\';",'22025',None),
        ("SELECT 'a' LIKE E'a\\\\';",None,'f'),
        ("SELECT NULL LIKE E'a\\\\';",None,None),
    ]
    failures=[]
    try:
        assert runner.decode_wire_result(client.simple_query(sock,'BEGIN;'))[1] is None
        for sql,state,value in cases:
            client.simple_query(sock,'SAVEPOINT unicode_pattern_case;')
            actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            expected=([] if state else [[value]],state,None if state else [16])
            observed=(actual[0],actual[1],None if state else actual[5])
            print('PATTERN_UNICODE',sql,actual,flush=True)
            if observed!=expected:
                failures.append((sql,observed,expected))
            client.simple_query(sock,'ROLLBACK TO unicode_pattern_case;')
            client.simple_query(sock,'RELEASE unicode_pattern_case;')
        print('PATTERN_UNICODE_FAILURES',failures,flush=True)
        assert not failures,failures
    finally:
        try:client.simple_query(sock,'ROLLBACK;')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()

if __name__=='__main__':main()
