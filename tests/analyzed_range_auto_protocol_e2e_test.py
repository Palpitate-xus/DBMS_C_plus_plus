#!/usr/bin/env python3
"""Real range snapshots after committed writes, rollback and target changes."""
import importlib.util
import json
import os
from pathlib import Path


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def main():
    root = Path(__file__).resolve().parent.parent
    frames = module("range_stats_frames", root / "tests/analyzed_array_auto_protocol_e2e_test.py")
    runner = module("range_stats_runner", root / "tests/compat/pg_diff_runner.py")
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    path = Path(server["dir"]) / "info/.analyzed_statistics"
    evidence = Path(os.environ["DBMS_TEST_EVIDENCE_DIR"]) if os.environ.get("DBMS_TEST_EVIDENCE_DIR") else None
    trace = {"server_pid": server["process"].pid, "commands": [], "snapshots": [], "complete": False}
    original_ranges = ["[1,4)", "[1,2.5)", '["2000-01-01 00:00:00","2000-01-01 00:00:01")']
    changed_ranges = ["[2,8)", "[0,4)", '["2000-01-01 00:00:00","2000-01-01 00:00:02")']

    def query(sql):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        trace["commands"].append({"sql": sql, "state": state, "message": message, "rows": rows, "tag": tag,
                                  "raw": [{"kind": k.decode(), "body_hex": b.hex()} for k, b in messages]})
        assert state is None, (sql, state, message)
        return rows

    def values(i, changed=False):
        return changed_ranges if changed else [None] * 3 if i % 13 == 0 else original_ranges if i % 2 else ["empty"] * 3

    def insert(first, last):
        rows = []
        for i in range(first, last + 1):
            rows.append("(" + str(i) + "," + ",".join("NULL" if v is None else "'" + v + "'" for v in values(i)) + ")")
        query("INSERT INTO automatic_range VALUES " + ",".join(rows))

    def population(ids, changed=False):
        rows = query("SELECT id,r,n,t FROM automatic_range")
        actual = {int(row[0]): row[1:] for row in rows}
        assert len(rows) == len(ids) and actual == {i: values(i, changed) for i in ids}

    def inspect(ids, label, changed=False, disabled=False, target=100):
        generation, columns = frames.snapshot(path)
        actual = {column["attribute"]: column for column in columns}
        assert len(columns) == len(actual) == (3 if disabled else 4)
        assert set(actual) == ({1, 3, 4} if disabled else {1, 2, 3, 4})
        nonnull = len(ids) if changed else sum(i % 13 != 0 for i in ids)
        nonempty = len(ids) if changed else sum(i % 13 != 0 and i % 2 for i in ids)
        for attribute, oid, index, length in [(2, 3904, 0, 6 if changed else 3),
                                             (3, 3906, 1, 4 if changed else 1.5),
                                             (4, 3908, 2, 2 if changed else 1)]:
            if attribute == 2 and disabled:
                continue
            column = actual[attribute]
            assert column["relation"] > 0 and column["physical"] > 0 and not column["inherited"]
            assert column["type"]["actual"] == column["type"]["codec"] == oid
            assert column["sample"] == column["population"] == len(ids)
            assert column["nulls"] == len(ids) - nonnull
            assert column["null_fraction"] == frames.f32((len(ids) - nonnull) / len(ids))
            assert column["distinct"] == frames.f32(-(1 - column["null_fraction"]))
            assert column["target"] == (target if attribute == 2 else 100)
            assert set(column["slots"]) == {6, 7}
            bounds, lengths = column["slots"][7], column["slots"][6]
            assert bounds["operator"] == bounds["collation"] == 0 and bounds["numbers"] is None
            assert bounds["values"]["type"]["actual"] == oid
            count = nonempty if nonempty <= column["target"] else column["target"] + 1
            assert len(bounds["values"]["cells"]) == count
            for value in bounds["values"]["cells"]:
                assert not value["null"] and value["text"] == (changed_ranges if changed else original_ranges)[index]
            assert lengths["operator"] == 672 and lengths["collation"] == 0
            assert lengths["numbers"] == [frames.f32((nonnull - nonempty) / nonnull)]
            assert lengths["values"]["type"]["actual"] == 701
            assert len(lengths["values"]["cells"]) == count
            assert all(not v["null"] and float(v["text"]) == length for v in lengths["values"]["cells"])
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / (label + ".bin")).write_bytes(path.read_bytes())
        trace["snapshots"].append({"label": label, "generation": generation, "columns": columns})
        return generation

    try:
        query("CREATE TABLE automatic_range(id integer,r int4range,n numrange,t tsrange)")
        insert(1, 55)
        generation = inspect(range(1, 56), "automatic55")
        population(range(1, 56))
        initial = path.read_bytes()
        query("BEGIN")
        insert(56, 110)
        assert path.read_bytes() == initial
        query("ROLLBACK")
        assert path.read_bytes() == initial
        population(range(1, 56))
        query("BEGIN")
        insert(56, 110)
        query("COMMIT")
        next_generation = inspect(range(1, 111), "committed110")
        assert next_generation > generation
        generation = next_generation
        population(range(1, 111))
        query("UPDATE automatic_range SET r='[2,8)',n='[0,4)',t='" + changed_ranges[2] + "'")
        next_generation = inspect(range(1, 111), "updated110", changed=True)
        assert next_generation > generation
        generation = next_generation
        population(range(1, 111), changed=True)
        query("DELETE FROM automatic_range WHERE id>60")
        next_generation = inspect(range(1, 61), "deleted60", changed=True)
        assert next_generation > generation
        population(range(1, 61), changed=True)
        query("ALTER TABLE automatic_range ALTER COLUMN r SET STATISTICS 0")
        query("ANALYZE automatic_range")
        generation = inspect(range(1, 61), "disabled-column", changed=True, disabled=True)
        query("ALTER TABLE automatic_range ALTER COLUMN r SET STATISTICS 1")
        query("ANALYZE automatic_range")
        assert inspect(range(1, 61), "restored-target1", changed=True, target=1) > generation
        trace["complete"] = True
        print("ANALYZED_RANGE_AUTO_PROTOCOL commands=" + str(len(trace["commands"])) + " snapshots=6 rollback_unchanged=1 targets=0,1")
    finally:
        runner.stop_ours(server)
        trace["server_exit"] = server["process"].returncode
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / "trace-v1.json").write_text(json.dumps(trace, indent=2) + "\n")
        assert server["process"].returncode == 0


if __name__ == "__main__":
    main()
