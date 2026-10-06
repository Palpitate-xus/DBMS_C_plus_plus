#!/usr/bin/env python3
"""Storage accepts precise finite interval values; error classification is a separate gate."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("interval_storage_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference="--reference" in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        version=runner.decode_wire_result(client.simple_query(sock,"SHOW server_version_num;"))
        assert version[1] is None and version[0]==[["170002"]],version
        server={"sock":sock}
        print("PG17.2 diagnostic reference",version[0],flush=True)
    else:
        server=runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"],sql),include_types=True)

    try:
        assert query("CREATE TEMP TABLE interval_storage_rows(id INT PRIMARY KEY,v INTERVAL);")[1] is None
        for row,(text,value) in enumerate((
            ("9223372036854775807 microseconds","2562047788:00:54.775807"),
            ("-9223372036854775808 microseconds","-2562047788:00:54.775808"),
            ("2147483647 months","178956970 years 7 mons"),
            ("-2147483648 months","-178956970 years -8 mons"),
            ("2147483647 days","2147483647 days"),
            ("-2147483648 days","-2147483648 days"),
            ("1.5 months","1 mon 15 days"),("0.5 years","6 mons"),
            ("1.5 weeks","10 days 12:00:00"),("1.5 days","1 day 12:00:00"),
            ("-1.5 months","-1 mons -15 days"),("1 yr 2 hrs","1 year 02:00:00"),
            ("1.25 seconds","00:00:01.25"),("2 milliseconds","00:00:00.002"),
            ("1 microsecond","00:00:00.000001"),("P1Y2M3DT4H5M6S","1 year 2 mons 3 days 04:05:06"),
            ("@ 1 day ago","-1 days"),
            ("0.9 years","11 mons"),("-0.9 years","-11 mons"),("1.9 years","1 year 11 mons"),
            ("0.1 months","3 days"),("1.9 months","1 mon 27 days"),("0.1 days","02:24:00"),
            ("9223372036854775807.4 microseconds","2562047788:00:54.775807"),
            ("9223372036854775807.5 microseconds","2562047788:00:54.775807"),
            ("-9223372036854775808.4 microseconds","-2562047788:00:54.775808"),
            ("-9223372036854775808.5 microseconds","-2562047788:00:54.775808"),
            ("1.5 microseconds","00:00:00.000001"),("1.0000015 seconds","00:00:01.000001"),
            ("1 us","00:00:00.000001"),("2 ms","00:00:00.002"),
            ("-1-2","-1 years -2 mons"),("-0-2","-2 mons"),
        ),1):
            result=query(f"INSERT INTO interval_storage_rows VALUES({row},'{text}');")
            assert result[1] is None,result
            result=query(f"SELECT v FROM interval_storage_rows WHERE id={row};")
            assert result[1] is None and result[0]==[[value]] and result[5]==[1186],(text,result)
        assert query("INSERT INTO interval_storage_rows VALUES(90,NULL);")[1] is None
        result=query("SELECT v FROM interval_storage_rows WHERE id=90;")
        assert result[1] is None and result[0]==[[None]] and result[5]==[1186],result
        assert query("UPDATE interval_storage_rows SET v='-9223372036854775808 microseconds' WHERE id=1;")[1] is None
        result=query("SELECT v FROM interval_storage_rows WHERE id=1;")
        assert result[1] is None and result[0]==[["-2562047788:00:54.775808"]],result
        print("[INTERVAL STORAGE INPUT "+("PG17.2 DIAGNOSTIC" if reference else "PROTOCOL E2E")+"] passed")
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=="__main__":
    main()
