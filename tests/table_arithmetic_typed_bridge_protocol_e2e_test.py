#!/usr/bin/env python3
"""Ordinary table arithmetic uses typed operands and explicit SQL NULL."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("table_arithmetic_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    def error(sql, state):
        result = query(sql)
        assert result[1] == state, (sql, result)

    try:
        for sql in ("CREATE TABLE ab(id INT,f REAL,g REAL,d DOUBLE PRECISION,e DOUBLE PRECISION,i INT,s SMALLINT,b BIGINT,t TEXT);", "INSERT INTO ab VALUES(1,16777216,1,9007199254740992,1,2147483647,32767,9223372036854775807,''),(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);", "CREATE TABLE ab_effect(id INT);"):
            assert query(sql)[1] is None, sql
        result = query("SELECT f+g,d+e,b+0,t||'' FROM ab WHERE id=1;")
        assert result[1] is None and result[5] == [700,701,20,25] and float(result[0][0][0]) == 16777216 and float(result[0][0][1]) == 9007199254740992 and result[0][0][2:] == ["9223372036854775807",""], result
        expect("SELECT f+g,d+e,b+0,t||'' FROM ab WHERE id=2;", [[None,None,None,None]], [700,701,20,25])
        error("SELECT i+1 FROM ab WHERE id=1;", "22003")
        error("SELECT s+s FROM ab WHERE id=1;", "22003")
        error("SELECT i/0 FROM ab WHERE id=1;", "22012")
        expect("SELECT s+g FROM ab WHERE id=1;", [["32768"]], [701])
        expect("SELECT i+CAST(1 AS BIGINT) FROM ab WHERE id=1;", [["2147483648"]], [20])
        assert query('CREATE TABLE ab_quote("F" BIGINT,f INT);')[1] is None
        assert query('INSERT INTO ab_quote VALUES(2,1),(NULL,2);')[1] is None
        expect('SELECT "F"+f FROM ab_quote;', [["3"],[None]], [20])
        assert query("CREATE FUNCTION ab_writer(v INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO ab_effect VALUES(v); RETURN v; END;$$;")[1] is None
        # Describe is static preparation, not a way to run a volatile target.
        sql = "SELECT ab_writer(id)+1 FROM ab WHERE id=1;"
        statement = b"arith_describe_only"
        server["sock"].sendall(client.typed(b"P", statement+b"\0"+sql.encode()+b"\0\0\0") +
                               client.typed(b"D", b"S"+statement+b"\0") + client.typed(b"S"))
        messages = client.read_until_ready(server["sock"])
        assert not any(kind in (b"E", b"D") for kind, _ in messages), messages
        assert [field[3] for field in client.row_description_fields(messages)] == [23], messages
        expect("SELECT id FROM ab_effect;", [], [23])
        expect("SELECT ab_writer(id)+1 FROM ab WHERE id=1;", [["2"]], [23])
        expect("SELECT id FROM ab_effect;", [["1"]], [23])
        assert query("BEGIN;")[1] is None
        error("SELECT ab_writer(id)+i+1 FROM ab WHERE id=1;", "22003")
        assert query("ROLLBACK;")[1] is None
        expect("SELECT id FROM ab_effect;", [["1"]], [23])
        print("[TABLE ARITHMETIC TYPED BRIDGE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
