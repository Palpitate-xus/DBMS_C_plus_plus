#!/usr/bin/env python3
"""Table projection CAST typmods survive Statement Describe without Execute."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("character_describe_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ('CREATE TABLE character_describe_rows(i INT);',
                    'INSERT INTO character_describe_rows VALUES(42);'):
            result = runner.decode_wire_result(client.simple_query(server["sock"], sql))
            assert result[1] is None, (sql, result)
        for index, (sql, oids, modifiers) in enumerate((
            ('SELECT CAST(i AS VARCHAR(4)) FROM character_describe_rows;', [1043], [8]),
            ('SELECT CAST(i AS CHAR(3)) FROM character_describe_rows;', [1042], [7]),
            ('SELECT i::VARCHAR(5) FROM character_describe_rows;', [1043], [9]),
            ('SELECT CAST(i AS CHAR),CAST(i AS VARCHAR) FROM character_describe_rows;', [1042, 1043], [5, -1]),
            ('SELECT i,CAST(i AS VARCHAR(4)),CAST(i AS CHAR(3)) FROM character_describe_rows;', [23, 1043, 1042], [-1, 8, 7]),
            ('SELECT *,CAST(i AS VARCHAR(4)) FROM character_describe_rows;', [23, 1043], [-1, 8]),
            ('SELECT CAST(i AS VARCHAR(4)) FROM character_describe_rows WHERE false;', [1043], [8]),
            ('SELECT CAST(42 AS VARCHAR(4));', [1043], [8]),
        )):
            name = ("character_shape_" + str(index)).encode()
            server["sock"].sendall(client.typed(b"P", name+b"\0"+sql.encode()+b"\0\0\0") +
                                   client.typed(b"D", b"S"+name+b"\0") + client.typed(b"S"))
            messages = client.read_until_ready(server["sock"])
            assert not any(kind in (b"E", b"D") for kind, _ in messages), (sql, messages)
            fields = client.row_description_fields(messages)
            assert [field[3] for field in fields] == oids, (sql, fields)
            assert [field[5] for field in fields] == modifiers, (sql, fields)
        print("[TABLE CHARACTER CAST DESCRIBE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
