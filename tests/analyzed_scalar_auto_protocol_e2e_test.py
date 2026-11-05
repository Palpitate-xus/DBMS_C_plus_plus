#!/usr/bin/env python3
"""Automatic ANALYZE observes committed INSERT/UPDATE/DELETE, never a subabort."""
import importlib.util
import json
import os
from pathlib import Path
import struct


def integer_snapshot(path, population):
    data = path.read_bytes()
    magic = b"\0DBMS_ANALYZED_STATS\0"
    assert data.startswith(magic)
    position = len(magic)
    version, size = struct.unpack_from("<IQ", data, position)
    assert version == 1 and len(data) == position + 12 + size + 8
    checksum = 14695981039346656037
    for value in data[:-8]:
        checksum = ((checksum ^ value) * 1099511628211) & ((1 << 64) - 1)
    assert checksum == int.from_bytes(data[-8:], "little")
    position += 12
    generation, count = struct.unpack_from("<QI", data, position)
    assert count == 1, ("real automatic column statistics required", count)
    position += 12
    relation, attribute, inherited, physical, schema = struct.unpack_from(
        "<IhBQQ", data, position)
    assert relation > 0 and attribute == 1 and inherited == 0 and physical > 0
    position += 23
    actual, codec, width, modifier, kind, enum, binary, element, fields = struct.unpack_from(
        "<IIhiBBBBI", data, position)
    assert (actual, codec, width, modifier, kind, enum, element, fields) == (23, 23, 4, -1, 0, 0, 0, 0)
    position += 22
    collation, target, sample, nulls, total, fraction, average, distinct = struct.unpack_from(
        "<IiQQdfif", data, position)
    assert target == 100 and sample == population and total == population
    assert nulls == 0 and fraction == 0 and average == 4 and distinct == -1
    return generation


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "analyzed_scalar_auto_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    snapshot = Path(server["dir"]) / "info/.analyzed_statistics"
    commands = 0
    evidence = Path(os.environ["DBMS_TEST_EVIDENCE_DIR"]) if os.environ.get("DBMS_TEST_EVIDENCE_DIR") else None
    trace = {"commands": [], "server_pid": server["process"].pid, "complete": False}

    def inspect(population, label):
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / (label + ".bin")).write_bytes(snapshot.read_bytes())
        return integer_snapshot(snapshot, population)

    def query(sql):
        nonlocal commands
        commands += 1
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        trace["commands"].append({"sql": sql, "state": state, "message": message,
                                  "rows": rows, "tag": tag,
                                  "raw": [{"kind": k.decode(), "body_hex": b.hex()} for k, b in messages]})
        assert state is None, (sql, state, message)
        if sql.startswith("ANALYZE"):
            assert tag == "ANALYZE", (sql, tag)
        return rows

    def insert(first, last):
        query("INSERT INTO automatic_scalar VALUES " + ",".join(
            "(" + str(value) + ")" for value in range(first, last + 1)))

    def population(expected):
        actual = sorted(int(row[0]) for row in query("SELECT i FROM automatic_scalar"))
        assert actual == expected, (actual, expected)

    def planner_population(expected):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON) SELECT i FROM automatic_scalar")))
        assert document["totalRows"] == expected, document

    try:
        query("CREATE TABLE automatic_scalar(i integer)")
        insert(1, 55)
        generation = inspect(55, "after-automatic-insert55")
        assert generation == 1
        initial = snapshot.read_bytes()
        population(list(range(1, 56)))
        planner_population(55)

        query("BEGIN")
        insert(56, 110)
        assert snapshot.read_bytes() == initial, "uncommitted write published statistics"
        query("ROLLBACK")
        population(list(range(1, 56)))
        assert snapshot.read_bytes() == initial, "rollback published statistics"

        query("BEGIN")
        insert(56, 110)
        query("COMMIT")
        population(list(range(1, 111)))
        next_generation = inspect(110, "after-explicit-commit110")
        assert next_generation > generation
        generation = next_generation
        planner_population(110)

        query("UPDATE automatic_scalar SET i=i+1000 WHERE i>50")
        population(list(range(1, 51)) + list(range(1051, 1111)))
        next_generation = inspect(110, "after-automatic-update110")
        assert next_generation > generation
        generation = next_generation
        planner_population(110)

        query("DELETE FROM automatic_scalar WHERE i>=1000")
        population(list(range(1, 51)))
        next_generation = inspect(50, "after-automatic-delete50")
        assert next_generation > generation
        planner_population(50)

        query("ALTER SYSTEM SET auto_analyze=off")
        assert query("SELECT pg_reload_conf()") == [["t"]]
        disabled = snapshot.read_bytes()
        insert(1001, 1055)
        population(list(range(1, 51)) + list(range(1001, 1056)))
        assert snapshot.read_bytes() == disabled, "disabled automatic analysis ran"
        query("ANALYZE automatic_scalar")
        assert inspect(105, "disabled-then-explicit105") > next_generation
        planner_population(105)
        trace["complete"] = True
        print("ANALYZED_SCALAR_AUTO_PROTOCOL commands=" + str(commands) +
              " committed_insert_update_delete=1 rollback_unchanged=1 disabled_explicit=1")
    finally:
        runner.stop_ours(server)
        trace["server_exit"] = server["process"].returncode
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / "trace-v1.json").write_text(json.dumps(trace, indent=2) + "\n")
        assert server["process"].returncode == 0, "ordinary server failed to shut down normally"


if __name__ == "__main__":
    main()
