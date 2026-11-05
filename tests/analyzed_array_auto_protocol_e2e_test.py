#!/usr/bin/env python3
"""Real array ANALYZE snapshots after committed writes and target changes."""
import importlib.util
import json
import os
from pathlib import Path
import struct


class SnapshotReader:
    def __init__(self, raw):
        self.raw, self.position = raw, 0

    def take(self, size):
        assert 0 <= size <= len(self.raw) - self.position
        start = self.position
        self.position += size
        return self.raw[start:self.position]

    def integer(self, size, signed=False):
        return int.from_bytes(self.take(size), "little", signed=signed)

    def flag(self):
        value = self.integer(1)
        assert value in (0, 1)
        return bool(value)

    def count(self):
        value = self.integer(4)
        assert value <= 1000000
        return value

    def text(self):
        return self.take(self.integer(4)).decode()

    def number(self, size):
        return struct.unpack("<f" if size == 4 else "<d", self.take(size))[0]

    def declaration(self, depth=0):
        assert depth <= 128
        value = {"actual": self.integer(4), "codec": self.integer(4),
                 "width": self.integer(2, True), "modifier": self.integer(4, True),
                 "kind": self.integer(1), "enum": self.flag(), "binary": self.flag()}
        value["element"] = self.declaration(depth + 1) if self.flag() else None
        value["fields"] = [self.declaration(depth + 1) for _ in range(self.count())]
        return value

    def value(self, depth=0):
        assert depth <= 128
        value = {"type": self.text(), "text": self.text(), "collation": self.text(), "null": self.flag()}
        value["oid"] = self.integer(4) if self.flag() else None
        value["element_oids"] = [self.integer(4) if self.flag() else None for _ in range(self.count())]
        value["fields"] = [self.value(depth + 1) for _ in range(self.count())] if self.flag() else None
        value["compound"] = None
        if self.flag():
            declaration = self.declaration(depth + 1) if self.flag() else None
            dimensions = [(self.integer(4, True), self.integer(4, True)) for _ in range(self.count())]
            bitmap = self.integer(1)
            assert bitmap <= 2
            elements = [self.value(depth + 1) for _ in range(self.count())] if self.flag() else None
            value["compound"] = {"declaration": declaration, "dimensions": dimensions,
                                  "bitmap": bitmap, "elements": elements}
        return value

    def column(self):
        value = {"relation": self.integer(4), "attribute": self.integer(2, True),
                 "inherited": self.flag(), "physical": self.integer(8), "schema": self.integer(8),
                 "type": self.declaration(), "collation": self.integer(4), "target": self.integer(4, True),
                 "sample": self.integer(8), "nulls": self.integer(8), "population": self.number(8),
                 "null_fraction": self.number(4), "width": self.integer(4, True), "distinct": self.number(4)}
        slots = []
        for _ in range(self.count()):
            slot = {"kind": self.integer(2), "operator": self.integer(4), "collation": self.integer(4)}
            slot["numbers"] = [self.number(4) for _ in range(self.count())] if self.flag() else None
            slot["values"] = {"type": self.declaration(), "cells": [self.value() for _ in range(self.count())]} if self.flag() else None
            slots.append(slot)
        value["slots"] = {slot["kind"]: slot for slot in slots}
        assert len(value["slots"]) == len(slots) <= 5
        return value


def snapshot(path):
    raw = path.read_bytes()
    assert len(raw) <= 128 * 1024 * 1024
    checksum = 14695981039346656037
    for byte in raw[:-8]:
        checksum = ((checksum ^ byte) * 1099511628211) & ((1 << 64) - 1)
    assert checksum == int.from_bytes(raw[-8:], "little")
    reader = SnapshotReader(raw)
    assert reader.take(21) == b"\0DBMS_ANALYZED_STATS\0"
    assert reader.integer(4) == 1
    assert reader.integer(8) == len(raw) - reader.position - 8
    generation = reader.integer(8)
    columns = [reader.column() for _ in range(reader.count())]
    assert reader.position == len(raw) - 8
    return generation, columns


def f32(number):
    return struct.unpack("<f", struct.pack("<f", number))[0]


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("array_stats_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    path = Path(server["dir"]) / "info/.analyzed_statistics"
    evidence = Path(os.environ["DBMS_TEST_EVIDENCE_DIR"]) if os.environ.get("DBMS_TEST_EVIDENCE_DIR") else None
    trace = {"server_pid": server["process"].pid, "commands": [], "snapshots": [], "complete": False}
    text = '{"","NULL","a b","a,b",NULL}'

    def query(sql):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        trace["commands"].append({"sql": sql, "state": state, "message": message, "rows": rows, "tag": tag,
                                  "raw": [{"kind": k.decode(), "body_hex": b.hex()} for k, b in messages]})
        assert state is None, (sql, state, message)
        return rows

    def insert(first, last, changed=False):
        values = []
        for i in range(first, last + 1):
            array = "{4,4,NULL}" if changed else "{1,1,2,NULL}" if i % 2 else "{2,3}"
            values.append("(" + str(i) + ",'" + array + "','" + text + "')")
        query("INSERT INTO automatic_array VALUES " + ",".join(values))

    def population(ids, changed=False):
        rows = query("SELECT id,i,t FROM automatic_array")
        actual = {int(row[0]): row[1:] for row in rows}
        expected = {i: ["{4,4,NULL}" if changed else "{1,1,2,NULL}" if i % 2 else "{2,3}", text] for i in ids}
        assert len(rows) == len(expected) and actual == expected, (actual, expected)

    def inspect(ids, label, changed=False, disabled=False, target=100):
        generation, columns = snapshot(path)
        assert len(columns) == (2 if disabled else 3)
        attributes = {column["attribute"]: column for column in columns}
        assert set(attributes) == ({1, 3} if disabled else {1, 2, 3})
        for column in columns:
            assert column["relation"] > 0 and column["physical"] > 0 and not column["inherited"]
            assert column["sample"] == column["population"] == len(ids)
            assert column["nulls"] == column["null_fraction"] == 0
        for attribute, oid, element in [(2, 1007, 23), (3, 1009, 25)]:
            if attribute == 2 and disabled:
                continue
            column = attributes[attribute]
            assert column["type"]["actual"] == oid and column["type"]["element"]["actual"] == element
            common = column["slots"][4]
            assert common["values"]["type"]["actual"] == element
            values = [value["text"] for value in common["values"]["cells"]]
            assert all(not value["null"] for value in common["values"]["cells"])
            if attribute == 2:
                assert column["target"] == target
                if changed:
                    assert values == ["4"] and common["numbers"] == [1, 1, 1, 1]
                else:
                    odd = sum(i % 2 for i in ids)
                    frequencies = [f32(odd / len(ids)), 1, f32((len(ids) - odd) / len(ids))]
                    assert values == ["1", "2", "3"]
                    assert common["numbers"] == frequencies + [min(frequencies), max(frequencies), frequencies[0]]
            else:
                assert set(values) == {"", "NULL", "a b", "a,b"}
                assert common["numbers"] == [1, 1, 1, 1, 1, 1, 1]
                assert column["slots"][5]["numbers"] == [4] * 101
            for kind in (1, 2):
                if kind in column["slots"]:
                    for value in column["slots"][kind]["values"]["cells"]:
                        assert value["compound"]["declaration"]["actual"] == oid
                        assert value["compound"]["elements"] is not None
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / (label + ".bin")).write_bytes(path.read_bytes())
        trace["snapshots"].append({"label": label, "generation": generation, "columns": columns})
        return generation

    try:
        query("CREATE TABLE automatic_array(id integer,i integer[],t text[])")
        insert(1, 55)
        generation = inspect(range(1, 56), "automatic55")
        population(range(1, 56))
        original = path.read_bytes()
        query("BEGIN")
        insert(56, 110)
        assert path.read_bytes() == original
        query("ROLLBACK")
        assert path.read_bytes() == original
        population(range(1, 56))
        query("BEGIN")
        insert(56, 110)
        query("COMMIT")
        next_generation = inspect(range(1, 111), "committed110")
        assert next_generation > generation
        generation = next_generation
        population(range(1, 111))
        query("UPDATE automatic_array SET i='{4,4,NULL}'")
        next_generation = inspect(range(1, 111), "updated110", changed=True)
        assert next_generation > generation
        generation = next_generation
        population(range(1, 111), changed=True)
        query("DELETE FROM automatic_array WHERE id>60")
        next_generation = inspect(range(1, 61), "deleted60", changed=True)
        assert next_generation > generation
        population(range(1, 61), changed=True)
        query("ALTER TABLE automatic_array ALTER COLUMN i SET STATISTICS 0")
        query("ANALYZE automatic_array")
        generation = inspect(range(1, 61), "disabled-column", changed=True, disabled=True)
        query("ALTER TABLE automatic_array ALTER COLUMN i SET STATISTICS 1")
        query("ANALYZE automatic_array")
        assert inspect(range(1, 61), "restored-target1", changed=True, target=1) > generation
        trace["complete"] = True
        print("ANALYZED_ARRAY_AUTO_PROTOCOL commands=" + str(len(trace["commands"])) + " snapshots=6 rollback_unchanged=1 targets=0,1")
    finally:
        runner.stop_ours(server)
        trace["server_exit"] = server["process"].returncode
        if evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / "trace-v1.json").write_text(json.dumps(trace, indent=2) + "\n")
        assert server["process"].returncode == 0


if __name__ == "__main__":
    main()
