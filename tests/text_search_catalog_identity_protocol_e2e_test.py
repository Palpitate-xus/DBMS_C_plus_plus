#!/usr/bin/env python3
"""Registered text-search types need real catalog and wire identities."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('ts_identity_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    failures, checks = [], 0

    def check(sql, rows, oids=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] is None and result[0] == rows
        if oids is not None:
            valid = valid and result[5] == oids
        checks += 1
        print('TEXT_SEARCH_CATALOG_IDENTITY', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids))

    try:
        for name, oid, array, storage in (('tsvector', 3614, 3643, 'x'), ('tsquery', 3615, 3645, 'p')):
            for prefix in ('', 'pg_catalog.', '"pg_catalog".'):
                check('SELECT NULL::' + prefix + name + ',NULL::' + prefix + name + '[]',
                      [[None, None]], [oid, array])
            for item_oid, item_name, category, element, backlink, store in (
                    (oid, name, 'U', 0, array, storage), (array, '_' + name, 'A', oid, 0, 'x')):
                check('SELECT oid,typname,typnamespace,typlen,typbyval,typtype,typcategory,typelem,typarray,typalign,typstorage '
                      'FROM pg_catalog.pg_type WHERE oid=' + str(item_oid),
                      [[str(item_oid), item_name, '11', '-1', 'f', 'b', category,
                        str(element), str(backlink), 'i', store]])
        print('TEXT_SEARCH_CATALOG_IDENTITY_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 10 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
