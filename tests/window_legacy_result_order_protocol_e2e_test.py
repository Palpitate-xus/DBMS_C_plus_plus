#!/usr/bin/env python3
"""R33: final ORDER BY is independent of legacy window partition order."""
import importlib.util
import os
import re
import socket
import subprocess
import sys
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path


def cli_control(root):
    spec = importlib.util.spec_from_file_location('r33_cli_fixture',root/'tests/window_e2e_test.py')
    helper = importlib.util.module_from_spec(spec); spec.loader.exec_module(helper)
    with tempfile.TemporaryDirectory(prefix='dbms-r33-cli-') as directory:
        base = Path(directory)
        catalog = base/'info/pg_catalog'; catalog.mkdir(parents=True)
        (catalog/'pg_authid.cat').write_text('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'
                                            % helper.scram_verifier('admin'),encoding='utf-8')
        (base/'info/tlist.lst').write_bytes(b'')
        script = '\n'.join([
            'admin admin','CREATE DATABASE r33_cli','USE DATABASE r33_cli',
            'CREATE TABLE items(id INT,p TEXT,v BIGINT)',
            "INSERT INTO items VALUES(1,'b',10),(2,'a',20)",
            'SELECT id,nth_value(v,1) OVER(PARTITION BY p ORDER BY id) AS first FROM items ORDER BY id',
            'exit',''])
        result = subprocess.run([os.environ.get('DBMS_MAIN',str(root/'dbms_main')),'--data-dir',directory],
            input=script,text=True,cwd=directory,capture_output=True,timeout=30,
            env=dict(os.environ,DBMS_COMPATIBILITY_MODE='extended'))
        rows = [line.strip() for line in result.stdout.splitlines() if re.fullmatch(r'\s*[12]\s+(10|20)\s*',line)]
        good = result.returncode == 0 and rows == ['1 10','2 20'] and 'ERROR' not in result.stdout
        print('WINDOW_LEGACY_CLI_SORT',result.returncode,rows,'PASS',good,flush=True)
        assert good,(result.stdout,result.stderr,rows)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "window_order_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]

    def query(sql):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result)
        return result

    try:
        query("CREATE TEMP TABLE exact_ints(id INT,p TEXT,v BIGINT)")
        query("INSERT INTO exact_ints VALUES"
              "(1,'maximum',9223372036854775807),(2,'maximum',9223372036854775807),"
              "(3,'minimum',-9223372036854775808),(4,'minimum',-9223372036854775808),"
              "(5,'cancel',9223372036854775807),(6,'cancel',-9223372036854775808),"
              "(7,'null',NULL),(8,'null',NULL)")
        window = ("PARTITION BY p ORDER BY id ROWS BETWEEN "
                  "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
        # Retain the original R11 discovery SQL and exact ordered row assertion.
        sql = (f"SELECT id,sum(v) OVER ({window}) AS total,"
               f"avg(v) OVER ({window}) AS average,"
               f"nth_value(v,1) OVER ({window}) AS first FROM exact_ints ORDER BY id")
        result = query(sql)
        expected = [
            ["1", "18446744073709551614", "9223372036854775807", "9223372036854775807"],
            ["2", "18446744073709551614", "9223372036854775807", "9223372036854775807"],
            ["3", "-18446744073709551616", "-9223372036854775808", "-9223372036854775808"],
            ["4", "-18446744073709551616", "-9223372036854775808", "-9223372036854775808"],
            ["5", "-1", "-0.5", "9223372036854775807"],
            ["6", "-1", "-0.5", "9223372036854775807"],
            ["7", None, None, None], ["8", None, None, None],
        ]
        numeric = lambda rows: [[None if cell is None else Decimal(cell) for cell in row] for row in rows]
        valid = (numeric(result[0]) == numeric(expected)
                 and result[3] == ["id", "total", "average", "first"]
                 and result[4] == "SELECT 8" and result[5] == [23, 1700, 1700, 20])
        print("WINDOW_LEGACY_RESULT_ORDER", sql, result, "match", valid, flush=True)
        failures = []
        if not valid: failures.append((sql, result, expected))
        checks = 1
        body = sql.rsplit(' ORDER BY id', 1)[0]
        by_id = {row[0]: row for row in expected}
        cases = [
            ('ORDER BY id DESC', [8,7,6,5,4,3,2,1]),
            ('ORDER BY p ASC,id DESC', [6,5,2,1,4,3,8,7]),
            ('ORDER BY p DESC,id ASC', [7,8,3,4,1,2,5,6]),
            ('ORDER BY total DESC NULLS LAST,id DESC', [2,1,6,5,4,3,8,7]),
            ('ORDER BY total ASC NULLS FIRST,id ASC', [7,8,3,4,5,6,1,2]),
            ('ORDER BY average ASC NULLS LAST,id ASC', [3,4,5,6,1,2,7,8]),
            ('ORDER BY first DESC NULLS LAST,id ASC', [1,2,5,6,3,4,7,8]),
            ('ORDER BY 1 DESC LIMIT 3 OFFSET 2', [6,5,4]),
            ('ORDER BY id DESC LIMIT 3 OFFSET 2', [6,5,4]),
            ('ORDER BY length(p),id', [7,8,5,6,1,2,3,4]),
            ('ORDER BY abs(id-5),id DESC', [5,6,4,7,3,8,2,1]),
            ('ORDER BY id+0 DESC', [8,7,6,5,4,3,2,1]),
            ('ORDER BY p COLLATE "C" ASC,id DESC', [6,5,2,1,4,3,8,7]),
            ('ORDER BY id LIMIT 0', []),
        ]
        for suffix, ids in cases:
            candidate = body+' '+suffix
            messages = c.simple_query(sock, candidate)
            result = r.decode_wire_result(messages, include_types=True)
            wanted = [by_id[str(i)] for i in ids]
            valid = (result[1] is None and numeric(result[0]) == numeric(wanted)
                     and result[3] == ['id','total','average','first']
                     and result[4] == 'SELECT '+str(len(ids)) and result[5] == [23,1700,1700,20])
            checks += 1
            print('WINDOW_LEGACY_SORT', candidate, result, 'PASS', valid, flush=True)
            if not valid: failures.append((candidate,result,wanted))
        sequence = 'r33_sort_'+uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE '+sequence)
        try:
            for suffix, ids in [
                    (f"ORDER BY nextval('{sequence}'),nextval('{sequence}')",[5,6,1,2,3,4,7,8]),
                    (f"ORDER BY nextval('{sequence}') LIMIT 0",[])]:
                candidate = body+' '+suffix
                result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
                valid = (result[1] is None and numeric(result[0]) == numeric([by_id[str(i)] for i in ids])
                         and result[3] == ['id','total','average','first']
                         and result[4] == 'SELECT '+str(len(ids)) and result[5] == [23,1700,1700,20])
                checks += 1
                print('WINDOW_LEGACY_SORT_VOLATILE', candidate, result, 'PASS', valid, flush=True)
                if not valid: failures.append((candidate,result))
                result = r.decode_wire_result(c.simple_query(sock,f"SELECT currval('{sequence}')"),include_types=True)
                checks += 1
                valid = result[1] is None and result[0] == [['8']] and result[5] == [20]
                print('WINDOW_LEGACY_SORT_ONCE',result,'PASS',valid,flush=True)
                if not valid: failures.append(('currval',result))
            candidate = body+f" ORDER BY nextval('{sequence}'),1/0"
            result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
            checks += 1
            valid = result[1] == '22012'
            print('WINDOW_LEGACY_SORT_CONSTANT_ERROR',result,'PASS',valid,flush=True)
            if not valid: failures.append((candidate,result))
            result = r.decode_wire_result(c.simple_query(sock,f"SELECT currval('{sequence}')"),include_types=True)
            checks += 1
            valid = result[1] is None and result[0] == [['8']]
            print('WINDOW_LEGACY_SORT_ERROR_BEFORE_EFFECT',result,'PASS',valid,flush=True)
            if not valid: failures.append(('currval after constant error',result))
        finally:
            query('DROP SEQUENCE '+sequence)
        for ordinal, state in [('0','42P10'),('5','42P10'),('99999999999999999999999999','42601'),
                ('-1','42P10'),('1.5','42601'),('NULL','42601'),('TRUE','42601'),("'1'",'42601'),
                ('2147483648','42601'),('-2147483648','42601'),('1e0','42601')]:
            candidate = body+' ORDER BY '+ordinal
            result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
            checks += 1
            valid = result[1] == state
            print('WINDOW_LEGACY_SORT_ORDINAL_ERROR',result,'PASS',valid,flush=True)
            if not valid: failures.append((candidate,result))
        for expression in ['+5','1::int']:
            candidate = body+' ORDER BY '+expression
            result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
            checks += 1
            valid = (result[1] is None and sorted(numeric(result[0])) == sorted(numeric(expected))
                     and result[3] == ['id','total','average','first']
                     and result[4] == 'SELECT 8' and result[5] == [23,1700,1700,20])
            print('WINDOW_LEGACY_SORT_CONSTANT_EXPRESSION',result,'PASS',valid,flush=True)
            if not valid: failures.append((candidate,result))
        for alias in ['NULL','1','sum result','sum+result','a.b',
                      'sum-result','sum(result)','sum/result','sum*result']:
            candidate = body.replace(' AS total',f' AS "{alias}"')+f' ORDER BY "{alias}" DESC NULLS LAST,id DESC'
            result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
            checks += 1
            ids = [2,1,6,5,4,3,8,7]
            valid = (result[1] is None and numeric(result[0]) == numeric([by_id[str(i)] for i in ids])
                     and result[3] == ['id',alias,'average','first']
                     and result[4] == 'SELECT 8' and result[5] == [23,1700,1700,20])
            print('WINDOW_LEGACY_SORT_QUOTED_ALIAS',result,'PASS',valid,flush=True)
            if not valid: failures.append((candidate,result))
        query('CREATE TEMP TABLE keyword_keys(id INT,"NULL" INT,v BIGINT)')
        query('INSERT INTO keyword_keys VALUES(1,20,1),(2,10,2)')
        candidate = 'SELECT id,nth_value(v,1) OVER(ORDER BY id) AS first FROM keyword_keys ORDER BY "NULL"'
        result = r.decode_wire_result(c.simple_query(sock,candidate),include_types=True)
        checks += 1
        valid = (result[1] is None and result[0] == [['2','1'],['1','1']]
                 and result[3] == ['id','first'] and result[4] == 'SELECT 2' and result[5] == [23,20])
        print('WINDOW_LEGACY_SORT_QUOTED_SOURCE',result,'PASS',valid,flush=True)
        if not valid: failures.append((candidate,result))
        print('WINDOW_LEGACY_SORT_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks == 44 and not failures, failures
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()
    if not reference:
        cli_control(root)


if __name__ == "__main__":
    main()
