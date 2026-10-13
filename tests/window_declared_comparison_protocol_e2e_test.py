#!/usr/bin/env python3
"""Typed window ordering, peer equality and MIN/MAX preserve actual values."""
import importlib.util
from pathlib import Path
import socket
import sys


CASES = [
    ('text_rows', 'TEXT COLLATE "C"', [('2',5),('10',4),('1',3),('01',2),('',0),
                         (None,99),('NULL',6),('hello world',7),('2',5)]),
    ('bigint_rows', 'BIGINT', [('9007199254740993',3),('9007199254740992',2),
                             ('9223372036854775807',4),('-9223372036854775808',0),
                             ('-9007199254740993',1),(None,99),('9007199254740993',3)]),
    ('numeric_rows', 'NUMERIC', [('9007199254740993.00002',3),('9007199254740993.00001',2),
                               ('9007199254740992.99999',1),('-12345678901234567890.123456789',0),
                               (None,99),('9007199254740993.00002',3)]),
    ('special_rows', 'NUMERIC', [('NaN',4),('Infinity',3),('-Infinity',0),('-1',1),
                               ('0',2),('NaN',4),(None,99)]),
    ('null_rows', 'TEXT', [(None,99),(None,99)]),
    ('literal_rows', 'TEXT', [('',0),('NULL',1),(None,99)]),
    ('collated_rows', 'TEXT COLLATE "C"', [('10',0),('2',1),('A',2),('a',3),(None,99)]),
]


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('window_declared_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host,port), timeout=r.wire_timeout())
        c.startup_reference(sock,user,database,password=password); r.verify_reference_version(c,sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    failures, checks = [], 0
    def query(sql): return r.decode_wire_result(c.simple_query(sock,sql), include_types=True)
    try:
        for table, type_name, data in CASES:
            result = query(f'CREATE TEMP TABLE {table}(id INT,k {type_name})')
            assert result[1] is None, result
            values = ','.join(f'({i},'+('NULL' if value is None else "'"+value+"'")+')'
                              for i,(value,_) in enumerate(data,1))
            result = query(f'INSERT INTO {table} VALUES'+values)
            assert result[1] is None, result
            real = [(value,group) for value,group in data if value is not None]
            minimum = min(real,key=lambda row:row[1])[0] if real else None
            maximum = max(real,key=lambda row:row[1])[0] if real else None
            for ascending in [True,False]:
                direction = 'ASC' if ascending else 'DESC'
                def key(row):
                    value,group = row
                    return (100 if ascending else -100) if value is None else group if ascending else -group
                # Explicit fixture group ranks are independent of a numeric
                # parser or production comparison implementation.
                order = sorted(range(len(data)),key=lambda index:key(data[index]))
                wanted = []
                for index in order:
                    current = key(data[index])
                    before = sum(key(row)<current for row in data)
                    dense = len({key(row) for row in data if key(row)<current})+1
                    through = sum(key(row)<=current for row in data)
                    wanted.append([str(index+1),str(before+1),str(dense),str(through),str(len(real)),minimum,maximum])
                for legacy in [False,True]:
                    extra = f',nth_value(k,1) OVER(ORDER BY k {direction}) AS first' if legacy else ''
                    sql = (f'SELECT id,rank() OVER(ORDER BY k {direction}) AS r,'
                           f'dense_rank() OVER(ORDER BY k {direction}) AS d,'
                           f'count(*) OVER(ORDER BY k {direction}) AS peers,'
                           f'count(k) OVER() AS non_null,'
                           f'min(k) OVER() AS lo,max(k) OVER() AS hi{extra} FROM {table}')
                    expected = [row[:] for row in wanted]
                    if legacy:
                        for row in expected: row.append(data[order[0]][0])
                    result = query(sql)
                    oid = 25 if type_name.startswith('TEXT') else {'BIGINT':20,'NUMERIC':1700}[type_name]
                    # No final SQL ORDER BY is specified. Equal sort peers
                    # may appear in either physical order; every exact row,
                    # rank, peer count and nullable result must still match.
                    good = (result[1] is None and sorted(result[0]) == sorted(expected)
                            and result[3] == ['id','r','d','peers','non_null','lo','hi'] + (['first'] if legacy else [])
                            and result[4] == 'SELECT '+str(len(data))
                            and result[5] == [23,20,20,20,20,oid,oid] + ([oid] if legacy else []))
                    checks += 1
                    print('WINDOW_DECLARED_COMPARISON',sql,result,'PASS',good,flush=True)
                    if not good: failures.append((sql,result,expected))
        print('WINDOW_DECLARED_PROTOCOL_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks == 28 and not failures, failures
    finally:
        if server: r.stop_ours(server)
        else: sock.close()


if __name__ == '__main__': main()
