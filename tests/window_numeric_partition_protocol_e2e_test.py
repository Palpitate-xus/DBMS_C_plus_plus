#!/usr/bin/env python3
"""Window partition equality uses SQL NUMERIC values, not their scale spelling."""
import importlib.util
from decimal import Decimal
from pathlib import Path
import socket
import sys


DATA = [
    ('1.0', '01'), ('1.00', '01'), ('1', '01'), ('2', '01'),
    (None, ''), (None, ''), ('1', '1'),
    ('9007199254740993.0', '01'), ('9007199254740992.00', '01'),
    ('9007199254740993.000', '01'), ('9007199254740992.000', '01'),
    ('-0.0', '01'), ('0.00', '01'),
    ('12345678901234567890.12345678901', None),
    ('12345678901234567890.123456789010', None),
    ('12345678901234567890.12345678902', None),
]


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('window_partition_runner', root/'tests/compat/pg_diff_runner.py')
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
    failures, checks = [], 0

    def query(sql):
        return r.decode_wire_result(c.simple_query(sock, sql), include_types=True)

    try:
        result = query('CREATE TEMP TABLE numeric_partitions(id INT,n NUMERIC,t TEXT)')
        assert result[1] is None, result
        values = ','.join(f"({i},{n if n is not None else 'NULL'},"
                          + ('NULL' if t is None else "'"+t+"'") + ')'
                          for i, (n, t) in enumerate(DATA, 1))
        result = query('INSERT INTO numeric_partitions VALUES' + values)
        assert result[1] is None, result
        for partition in ['n', 'n,t', 't']:
            for limit in [len(DATA), 6, 0]:
                selected = [(i, n, t) for i, (n, t) in enumerate(DATA, 1) if i <= limit]
                def key(row):
                    _, n, t = row
                    number = Decimal(n) if n is not None else None
                    return (number, t) if partition == 'n,t' else number if partition == 'n' else t
                expected = []
                for row in selected:
                    members = [item[0] for item in selected if key(item) == key(row)]
                    expected.append([str(row[0]), str(len(members)), str(members.index(row[0])+1)])
                for legacy in [False, True]:
                    over = f'PARTITION BY {partition}'
                    extra = f',nth_value(id,1) OVER({over} ORDER BY id) AS first' if legacy else ''
                    sql = (f'SELECT id,count(*) OVER({over}) AS total,'
                           f'row_number() OVER({over} ORDER BY id) AS position{extra} '
                           f'FROM numeric_partitions WHERE id<={limit}')
                    wanted = [row[:] for row in expected]
                    if legacy:
                        for target, row in zip(wanted, selected):
                            target.append(str(next(item[0] for item in selected if key(item) == key(row))))
                    result = query(sql)
                    # There is deliberately no final SQL ORDER BY: compare the
                    # exact row bag, not an order the statement does not promise.
                    good = (result[1] is None and sorted(result[0]) == sorted(wanted)
                            and result[3] == ['id','total','position'] + (['first'] if legacy else [])
                            and result[4] == 'SELECT '+str(len(wanted))
                            and result[5] == [23,20,20] + ([23] if legacy else []))
                    checks += 1
                    print('WINDOW_NUMERIC_PARTITION', sql, result, 'PASS', good, flush=True)
                    if not good: failures.append((sql, result, wanted))
        print('WINDOW_NUMERIC_PARTITION_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 18 and not failures, failures
    finally:
        if server: r.stop_ours(server)
        else: sock.close()


if __name__ == '__main__':
    main()
