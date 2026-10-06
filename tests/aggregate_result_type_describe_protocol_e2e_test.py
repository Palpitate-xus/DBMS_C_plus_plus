#!/usr/bin/env python3
"""Aggregate descriptors use declared overload types without executing inputs."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("aggregate_type_describe_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql))
    try:
        for sql in (
            "CREATE TABLE aggregate_types(s SMALLINT,i INT,b BIGINT,n NUMERIC,f REAL,d DOUBLE PRECISION,m MONEY,t INTERVAL);",
            "CREATE TABLE aggregate_type_effect(i INT);",
            "CREATE FUNCTION aggregate_type_writer(v REAL) RETURNS REAL LANGUAGE plpgsql AS $$BEGIN INSERT INTO aggregate_type_effect VALUES(1); RETURN v; END;$$;",
        ):
            assert query(sql)[1] is None, sql
        for index, (sql, oids) in enumerate((
            ("SELECT sum(s),sum(i),sum(b),sum(n) FROM aggregate_types;", [20, 20, 1700, 1700]),
            ("SELECT sum(f),sum(d) FROM aggregate_types;", [700, 701]),
            ("SELECT sum(m),sum(t),avg(t) FROM aggregate_types;", [790, 1186, 1186]),
            ("SELECT avg(f),avg(i) FROM aggregate_types;", [701, 1700]),
            ("SELECT sum(f)+CAST(0 AS REAL),sum(d)+CAST(0 AS DOUBLE PRECISION) FROM aggregate_types;", [700, 701]),
            ("SELECT sum(CAST(NULL AS REAL)),sum(CAST(NULL AS MONEY)),avg(CAST(NULL AS INTERVAL)) FROM aggregate_types;", [700, 790, 1186]),
            ("SELECT sum(aggregate_type_writer(f)) FROM aggregate_types;", [700]),
        )):
            if index == 6:
                assert query("INSERT INTO aggregate_types VALUES(NULL,NULL,NULL,NULL,1,NULL,NULL,NULL);")[1] is None
            name = ("aggregate_shape_" + str(index)).encode()
            server["sock"].sendall(client.typed(b"P", name+b"\0"+sql.encode()+b"\0\0\0") +
                                   client.typed(b"D", b"S"+name+b"\0") + client.typed(b"S"))
            messages = client.read_until_ready(server["sock"])
            assert not any(kind in (b"E", b"D") for kind, _ in messages), (sql, messages)
            fields = client.row_description_fields(messages)
            assert [field[3] for field in fields] == oids, (sql, fields, oids)
            assert [field[2] for field in fields] == [0] * len(oids), (sql, fields)
        effects = query("SELECT i FROM aggregate_type_effect;")
        assert effects[1] is None and effects[0] == [], effects
        print("[AGGREGATE RESULT TYPE DESCRIBE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
