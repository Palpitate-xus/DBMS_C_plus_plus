"""Prepared integer DML keeps typed predicates, NULLs and RETURNING images."""
from collections import Counter
import importlib.util
from pathlib import Path
import struct


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dml_prefilter_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    wire = runner.load_protocol_client()
    server = runner.start_ours(wire)
    sock = server["sock"]
    controls = 0
    prefix = "WITH marker AS (SELECT 1) "

    def check(messages, expected=None, state=None, ready=b"I"):
        nonlocal controls
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, result
        assert messages[-1] == (b"Z", ready), messages[-1]
        if expected is not None:
            assert Counter(map(tuple, result[0])) == Counter(map(tuple, expected)), (result, expected)
        controls += 1
        return result[0]

    def query(sql, expected=None, ready=b"I"):
        return check(wire.simple_query(sock, sql), expected, ready=ready)

    def prepare(name, sql, oid):
        body = name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)
        sock.sendall(wire.typed(b"P", body) + wire.typed(b"S"))
        check(wire.read_until_ready(sock), [])

    def execute(name, value, expected=None, state=None, ready=b"I", binary=False, width=4):
        raw = None if value is None else (int(value).to_bytes(width, "big", signed=True)
                                         if binary else str(value).encode())
        body = b"\0" + name.encode() + b"\0" + struct.pack("!HHH", 1, int(binary), 1)
        body += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"") + struct.pack("!H", 0)
        sock.sendall(wire.typed(b"B", body) + wire.typed(b"D", b"P\0") +
                     wire.typed(b"E", b"\0" + struct.pack("!I", 0)) + wire.typed(b"S"))
        return check(wire.read_until_ready(sock), expected, state, ready)

    try:
        query("CREATE TABLE dml_prefilter(id INTEGER PRIMARY KEY,k INTEGER,v TEXT)")
        query("INSERT INTO dml_prefilter VALUES(-1,1,NULL),(0,NULL,''),(1,1,'NULL'),(2,2,'x'),(3,2,'y'),(100,3,'z')")
        query(prefix + "UPDATE dml_prefilter SET v='changed' WHERE id=2 RETURNING id,k,v", [["2", "2", "changed"]])
        query(prefix + "UPDATE dml_prefilter SET v=NULL WHERE k=1 RETURNING id,v", [["-1", None], ["1", None]])
        query(prefix + "UPDATE dml_prefilter AS r SET k=7 WHERE 1=r.id RETURNING r.id,r.k", [["1", "7"]])
        query(prefix + "UPDATE dml_prefilter SET v='negative' WHERE id=-1 RETURNING id", [["-1"]])
        query(prefix + "UPDATE dml_prefilter SET v='never' WHERE k=NULL RETURNING id", [])
        query("CREATE SEQUENCE dml_set_calls")
        query(prefix + "UPDATE dml_prefilter SET k=nextval('dml_set_calls') WHERE id=1 RETURNING k", [["1"]])
        query("SELECT currval('dml_set_calls')", [["1"]])
        query("CREATE SEQUENCE dml_predicate_calls")
        query(prefix + "UPDATE dml_prefilter SET v='never' WHERE id=0*nextval('dml_predicate_calls')+999 RETURNING id", [])
        query("SELECT currval('dml_predicate_calls')", [["6"]])

        prepare("update_id", prefix + "UPDATE dml_prefilter SET v='prepared' WHERE id=$1 RETURNING id,v", 23)
        for binary in (False, True):
            execute("update_id", 2, [["2", "prepared"]], binary=binary)
            execute("update_id", None, [], binary=binary)
            execute("update_id", 999, [], binary=binary)
        execute("update_id", "bad", [], state="22P02")
        execute("update_id", 1, [["1", "prepared"]])
        prepare("wide", prefix + "UPDATE dml_prefilter SET v='wide' WHERE id=$1 RETURNING id", 20)
        execute("wide", 2147483648, [])
        execute("wide", 2, [["2"]])
        query("BEGIN", ready=b"T")
        query(prefix + "UPDATE dml_prefilter SET v='own' WHERE id=1 RETURNING id,v", [["1", "own"]], b"T")
        execute("update_id", 1, [["1", "prepared"]], ready=b"T")
        query("COMMIT")
        query("BEGIN", ready=b"T")
        query(prefix + "DELETE FROM dml_prefilter WHERE id=3 RETURNING id", [["3"]], b"T")
        query("ROLLBACK")
        query("SELECT id FROM dml_prefilter WHERE id=3", [["3"]])
        query(prefix + "UPDATE dml_prefilter SET id=200 WHERE id=2 RETURNING id,v", [["200", "wide"]])
        query("SELECT id FROM dml_prefilter WHERE id=2", [])
        query(prefix + "DELETE FROM dml_prefilter WHERE id=200 RETURNING id,k,v", [["200", "2", "wide"]])

        query("CREATE TABLE dml_prefilter_big(id BIGINT PRIMARY KEY,v INTEGER)")
        query("INSERT INTO dml_prefilter_big VALUES(9007199254740992,0),(9007199254740993,0),(-9223372036854775808,0)")
        query(prefix + "UPDATE dml_prefilter_big SET v=1 WHERE id=9007199254740993 RETURNING id", [["9007199254740993"]])
        query("SELECT id,v FROM dml_prefilter_big", [["9007199254740992", "0"], ["9007199254740993", "1"], ["-9223372036854775808", "0"]])
        prepare("big", prefix + "DELETE FROM dml_prefilter_big WHERE id=$1 RETURNING id", 20)
        execute("big", -9223372036854775808, [["-9223372036854775808"]], binary=True, width=8)
        query("CREATE TABLE dml_prefilter_composite(a INTEGER,b INTEGER,PRIMARY KEY(a,b))")
        query("INSERT INTO dml_prefilter_composite VALUES(1,10),(1,20),(2,30)")
        query(prefix + "DELETE FROM dml_prefilter_composite WHERE a=1 RETURNING b", [["10"], ["20"]])
        query("SELECT a,b FROM dml_prefilter_composite", [["2", "30"]])
        query('CREATE TABLE dml_prefilter_quoted("key value" INTEGER,v TEXT)')
        query("INSERT INTO dml_prefilter_quoted VALUES(1,'x'),(2,'y')")
        query(prefix + 'UPDATE dml_prefilter_quoted SET v=\'kept\' WHERE "key value"=1 RETURNING v', [["kept"]])
        print("[DML INTEGER PREFILTER]", controls, "controls passed", flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
