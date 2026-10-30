#!/usr/bin/env python3
"""Validated operation benchmarks on a private cluster; never opens user data.

Mutation, transaction and maintenance latency includes durable completion.
Fixture setup, per-operation verification and cleanup are outside each timer.
SELECT fixtures and mutation results are checked against expected values.
All operations must complete without a protocol error; the read-only transaction
checks completion, and the sequence case checks its resulting currval.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import statistics
import sys
import tempfile
import time
import traceback

import benchmark_queries as query_bench
from postgres_protocol_test import read_message, read_until_ready, typed


class Operations:
    def __init__(self, client, pid, args, report):
        self.client, self.pid, self.args, self.report = client, pid, args, report
        self.cases = {}
        self.counts = Counter()

    def query(self, sql):
        return self.client.query(sql)

    def expect(self, sql, rows):
        actual = self.query(sql)
        assert Counter(map(tuple, actual)) == Counter(map(tuple, rows)), (sql, actual[:5], rows[:5])

    def add(self, name, action, verify, setup=lambda i: {}, cleanup=lambda c: None, units=1):
        self.cases[name] = (setup, action, verify, cleanup, units)

    def copy(self, sql, data=None):
        sock = self.client.sock
        sock.sendall(typed(b"Q", sql.encode() + b"\0"))
        kind, body = read_message(sock)
        assert kind == (b"H" if data is None else b"G"), (sql, kind, body)
        if data is not None:
            sock.sendall(typed(b"d", data) + typed(b"c"))
        messages = read_until_ready(sock)
        query_bench.checked_rows(messages)
        return b"".join(body for kind, body in messages if kind == b"d")

    def populate(self, table, rows):
        for start in range(1, rows + 1, 100):
            values = ",".join(f"({i},{i % 100},{i % 1000},'row-{i}')"
                              for i in range(start, min(rows + 1, start + 100)))
            self.query(f"INSERT INTO {table} VALUES {values}")

    def initialize(self):
        n, batch = self.args.rows, self.args.batch
        query_bench.load(self.client, n)
        self.client.prepare()
        specifications = query_bench.cases(n)
        for name, specification in specifications.items():
            self.add("select_" + name,
                     lambda c, name=name, specification=specification:
                         query_bench.execute_case(self.client, name, specification, c["i"]),
                     lambda c: None, setup=lambda i: {"i": i})
        schema = "(id INTEGER PRIMARY KEY,grp INTEGER,amount INTEGER,payload VARCHAR(64))"
        for table in ("op_insert", "op_bulk", "op_transaction", "op_rollback", "op_update", "op_indexed", "op_delete"):
            self.query("CREATE TABLE " + table + schema)
        for table in ("op_update", "op_indexed", "op_delete"):
            self.populate(table, n)
        self.query("CREATE INDEX op_secondary ON op_indexed(grp)")
        self.query("CREATE TABLE op_copy(id INTEGER PRIMARY KEY,payload TEXT)")
        self.query("CREATE TABLE op_toast(id INTEGER PRIMARY KEY,payload VARCHAR(12000))")
        self.query("CREATE SEQUENCE op_sequence")
        self.query("CHECKPOINT")

        def insert_setup(i, count=1):
            start = 1 + i * count
            return {"start": start, "count": count,
                    "values": ",".join(f"({k},{k % 100},{k % 1000},'row-{k}')"
                                        for k in range(start, start + count))}

        def inserted(table, context):
            start, count = context["start"], context["count"]
            self.expect(f"SELECT id,grp,amount,payload FROM {table} WHERE id BETWEEN {start} AND {start+count-1}",
                        [[str(k), str(k % 100), str(k % 1000), f"row-{k}"] for k in range(start, start + count)])

        self.add("insert_autocommit", lambda c: self.query("INSERT INTO op_insert VALUES " + c["values"]),
                 lambda c: inserted("op_insert", c), setup=insert_setup)
        self.add("insert_multi_values", lambda c: self.query("INSERT INTO op_bulk VALUES " + c["values"]),
                 lambda c: inserted("op_bulk", c), setup=lambda i: insert_setup(i, batch), units=batch)

        def transaction_insert(c):
            self.query("BEGIN")
            for k in range(c["start"], c["start"] + c["count"]):
                self.query(f"INSERT INTO op_transaction VALUES({k},{k%100},{k%1000},'row-{k}')")
            self.query("COMMIT")
        self.add("insert_explicit_transaction", transaction_insert,
                 lambda c: inserted("op_transaction", c), setup=lambda i: insert_setup(i, batch), units=batch)

        def update_setup(i):
            return {"id": i % n + 1, "value": 10000 + i}
        self.add("update_primary_key", lambda c: self.query(f"UPDATE op_update SET amount={c['value']} WHERE id={c['id']}"),
                 lambda c: self.expect(f"SELECT amount FROM op_update WHERE id={c['id']}", [[str(c["value"])]]), setup=update_setup)
        self.add("update_secondary_index", lambda c: self.query(f"UPDATE op_indexed SET grp={c['value']} WHERE id={c['id']}"),
                 lambda c: self.expect(f"SELECT id FROM op_indexed WHERE grp={c['value']}", [[str(c["id"])]]), setup=update_setup)
        self.add("update_cte_returning", lambda c: c.update(actual=self.query(
                     f"WITH marker AS (SELECT 1) UPDATE op_update SET amount={c['value']} WHERE id={c['id']} RETURNING id,amount")),
                 lambda c: self.check_returning_update(c), setup=update_setup)
        self.add("update_range", lambda c: self.query(f"UPDATE op_update SET amount={c['value']} WHERE id BETWEEN {c['start']} AND {c['start']+batch-1}"),
                 lambda c: self.expect(f"SELECT id,amount FROM op_update WHERE id BETWEEN {c['start']} AND {c['start']+batch-1}",
                                       [[str(k), str(c["value"])] for k in range(c["start"], c["start"] + batch)]),
                 setup=lambda i: {"start": n // 2, "value": 20000 + i}, units=batch)
        self.add("delete_primary_key", lambda c: self.query(f"DELETE FROM op_delete WHERE id={c['id']}"),
                 lambda c: self.expect(f"SELECT id FROM op_delete WHERE id={c['id']}", []), setup=lambda i: {"id": i + 1})
        self.add("delete_cte_returning", lambda c: c.update(actual=self.query(
                     f"WITH marker AS (SELECT 1) DELETE FROM op_delete WHERE id={c['id']} RETURNING id")),
                 lambda c: self.check_returning_delete(c), setup=lambda i: {"id": n-i})
        self.add("delete_range", lambda c: self.query(f"DELETE FROM op_delete WHERE id BETWEEN {c['start']} AND {c['start']+batch-1}"),
                 lambda c: self.expect("SELECT count(*) FROM op_delete", [[str(n - self.counts['delete_primary_key'] - self.counts['delete_range'] * batch - self.counts['delete_cte_returning'])]]),
                 setup=lambda i: {"start": n // 2 + i * batch}, units=batch)

        def transaction(c):
            for sql in c["statements"]:
                self.query(sql)
        self.add("transaction_read_commit", transaction, lambda c: None,
                 setup=lambda i: {"statements": ["BEGIN", "SELECT id FROM bench_rows WHERE id=1", "COMMIT"]})
        self.add("transaction_write_rollback", transaction,
                 lambda c: self.expect("SELECT count(*) FROM op_rollback", [["0"]]),
                 setup=lambda i: {"statements": ["BEGIN", f"INSERT INTO op_rollback VALUES({i+1},1,2,'rollback')", "ROLLBACK"]})
        self.add("transaction_savepoint", transaction,
                 lambda c: self.expect(f"SELECT amount FROM op_update WHERE id={n}", [["333"]]),
                 setup=lambda i: {"statements": ["BEGIN", f"UPDATE op_update SET amount=333 WHERE id={n}",
                     "SAVEPOINT op_sp", f"UPDATE op_update SET amount=444 WHERE id={n}",
                     "ROLLBACK TO SAVEPOINT op_sp", "RELEASE SAVEPOINT op_sp", "COMMIT"]})

        self.add("sequence_nextval", lambda c: c.update(actual=self.query("SELECT nextval('op_sequence')")),
                 lambda c: self.expect("SELECT currval('op_sequence')", [[str(c["i"] + 1)]]), setup=lambda i: {"i": i})
        self.add("copy_from", lambda c: self.copy("COPY op_copy FROM STDIN", c["data"]),
                 lambda c: self.expect(f"SELECT id,payload FROM op_copy WHERE id BETWEEN {c['start']} AND {c['start']+batch-1}",
                                      [[str(k), f"row-{k}"] for k in range(c["start"], c["start"] + batch)]),
                 setup=lambda i: {"start": 1 + i * batch, "data": "".join(f"{k}\trow-{k}\n" for k in range(1+i*batch,1+(i+1)*batch)).encode()}, units=batch)
        self.add("copy_to", lambda c: c.update(actual=self.copy("COPY bench_rows TO STDOUT")),
                 lambda c: self.check_copy(c["actual"], n), units=n)
        payload = "0123456789abcdef" * 384
        self.add("toast_insert_6k", lambda c: self.query(f"INSERT INTO op_toast VALUES({c['id']},'{payload}')"),
                 lambda c: self.expect(f"SELECT payload FROM op_toast WHERE id={c['id']}", [[payload]]), setup=lambda i: {"id": i + 1})

        def ddl_setup(i, stem, populated=False):
            table = f"{stem}_{i}"
            self.query(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY)")
            if populated:
                self.query(f"INSERT INTO {table} VALUES " + ",".join(f"({k})" for k in range(1, self.args.ddl_rows + 1)))
            return {"table": table}
        self.add("ddl_create_table", lambda c: self.query(f"CREATE TABLE {c['table']}(id INTEGER PRIMARY KEY)"),
                 lambda c: self.expect(f"SELECT count(*) FROM {c['table']}", [["0"]]),
                 setup=lambda i: {"table": f"op_create_{i}"}, cleanup=lambda c: self.query(f"DROP TABLE {c['table']}"))
        self.add("ddl_alter_add_column", lambda c: self.query(f"ALTER TABLE {c['table']} ADD COLUMN value INTEGER DEFAULT 5"),
                 lambda c: self.expect(f"SELECT id,value FROM {c['table']}", [[str(k), "5"] for k in range(1, self.args.ddl_rows + 1)]),
                 setup=lambda i: ddl_setup(i, "op_alter", True), cleanup=lambda c: self.query(f"DROP TABLE {c['table']}"))
        self.add("ddl_truncate", lambda c: self.query(f"TRUNCATE TABLE {c['table']}"),
                 lambda c: self.expect(f"SELECT count(*) FROM {c['table']}", [["0"]]),
                 setup=lambda i: ddl_setup(i, "op_truncate", True), cleanup=lambda c: self.query(f"DROP TABLE {c['table']}"))
        self.add("ddl_drop_table", lambda c: self.query(f"DROP TABLE {c['table']}"),
                 lambda c: self.expect(f"SELECT relname FROM pg_catalog.pg_class WHERE relname='{c['table']}'", []),
                 setup=lambda i: ddl_setup(i, "op_drop", True))
        for method in ("btree", "hash"):
            self.add("index_create_" + method,
                     lambda c, method=method: self.query(f"CREATE INDEX {c['index']} ON bench_rows USING {method}(grp)"),
                     lambda c: self.expect("SELECT id FROM bench_rows WHERE grp=42", [[str(k)] for k in range(1, n + 1) if k % 100 == 42]),
                     setup=lambda i, method=method: {"index": f"op_build_{method}_{i}"},
                     cleanup=lambda c: self.query(f"DROP INDEX {c['index']}"), units=n)
        self.add("maintenance_analyze", lambda c: self.query("ANALYZE bench_rows"),
                 lambda c: self.expect("SELECT count(*) FROM bench_rows", [[str(n)]]), units=n)
        self.add("maintenance_vacuum", lambda c: self.query("VACUUM op_delete"),
                 lambda c: self.expect("SELECT count(*) FROM op_delete", [[str(n-self.counts['delete_primary_key']-self.counts['delete_range']*batch-self.counts['delete_cte_returning'])]]))
        self.add("maintenance_reindex", lambda c: self.query("REINDEX TABLE op_indexed"),
                 lambda c: self.expect("SELECT count(*) FROM op_indexed", [[str(n)]]), units=n)
        self.add("maintenance_checkpoint", lambda c: self.query("CHECKPOINT"),
                 lambda c: self.expect("SELECT count(*) FROM bench_rows", [[str(n)]]))

    def check_copy(self, data, n):
        expected = [f"{k}\t{k%100}\t{k%1000}\trow-{k}" for k in range(1, n+1)]
        assert Counter(data.decode().splitlines()) == Counter(expected), "COPY output mismatch"

    def check_returning_update(self, context):
        assert context["actual"] == [[str(context["id"]), str(context["value"])]], context
        self.expect(f"SELECT amount FROM op_update WHERE id={context['id']}", [[str(context["value"])]])

    def check_returning_delete(self, context):
        assert context["actual"] == [[str(context["id"])]], context
        self.expect(f"SELECT id FROM op_delete WHERE id={context['id']}", [])

    def run(self, save):
        names = self.args.only or list(self.cases)
        assert all(name in self.cases for name in names), names
        for round_id in range(self.args.rounds):
            order = names[:]
            random.Random(3719 + round_id).shuffle(order)
            for name in order:
                setup, action, verify, cleanup, units = self.cases[name]
                samples = []
                for _ in range(self.args.samples):
                    index = self.counts[name]
                    context = setup(index)
                    before = query_bench.resources(self.pid)
                    start = time.perf_counter()
                    try:
                        action(context)
                    except BaseException:
                        self.report["failed_attempt"] = {"name": name, "round": round_id+1,
                            "sample": index+1, "elapsed_seconds": time.perf_counter()-start,
                            "before": before, "after": query_bench.resources(self.pid)}
                        save()
                        raise
                    elapsed = time.perf_counter() - start
                    after = query_bench.resources(self.pid)
                    self.counts[name] += 1
                    verify(context)
                    cleanup(context)
                    measured = query_bench.summarize([elapsed * 1000], elapsed, before, after)
                    measured.update(name=name, round=round_id+1, sample=index+1, units=units,
                                    units_per_second=units/elapsed)
                    self.report["measurements"].append(measured)
                    samples.append(elapsed * 1000)
                    save()
                print(f"OP round={round_id+1} {name} n={len(samples)} p50={statistics.median(samples):.3f}ms", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--ddl-rows", type=int, default=5,
                        help="rows in each ALTER/TRUNCATE/DROP fixture, independent of DML batch size")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--only", nargs="+")
    args = parser.parse_args()
    if min(args.rows, args.batch, args.ddl_rows, args.rounds, args.samples) < 1 or args.rows < 100:
        parser.error("rows >= 100, positive batch/rounds/samples required")
    if args.batch * args.rounds * args.samples > args.rows // 2:
        parser.error("batch*rounds*samples must fit in half the fixture for distinct DELETE ranges")
    delete_samples = args.rounds * args.samples
    if (delete_samples >= args.rows // 2 or
            args.rows // 2 + args.batch * delete_samples - 1 >= args.rows - delete_samples + 1):
        parser.error("DELETE point, range and CTE fixtures must be disjoint; reduce batch/rounds/samples or increase rows")
    binary, output = args.binary.resolve(), args.output.resolve()
    if (output / "operations.json").exists():
        parser.error("output already contains operations.json")
    output.mkdir(parents=True, exist_ok=True)
    data = Path(tempfile.mkdtemp(prefix="data-operations-", dir=output))
    (data / "info").mkdir(); (data / "info/tlist.lst").touch()
    query_bench.write_auth_catalog(str(data), "alice", "secret")
    (data / "pg_hba.conf").write_text("host all alice 127.0.0.1/32 scram-sha-256\n")
    report = {"created_utc": datetime.now(timezone.utc).isoformat(), "binary": str(binary),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "data_directory": str(data),
              "arguments": {k: str(v) if isinstance(v, Path) else v for k,v in vars(args).items()},
              "settings": "server defaults, persistence unchanged; loopback reused connection; no TLS",
              "measurement": "setup, mutation verification and cleanup excluded; query validation included; all successful samples verified; mutation and commit included",
              "measurements": [], "complete": False}
    def save():
        (output / "operations.json").write_text(json.dumps(report, indent=2))
    save()
    try:
        with query_bench.server(binary, data, output / "server.log") as (client, pid, port):
            operations = Operations(client, pid, args, report)
            print("FIXTURE_LOADING", flush=True)
            operations.initialize()
            report["available_cases"] = list(operations.cases)
            print("FIXTURE_READY", len(operations.cases), "cases", flush=True)
            operations.run(save)
        report["complete"] = True
        save()
        print("OPERATIONS_COMPLETE all measured results validated", flush=True)
    except BaseException as error:
        report["failure"] = repr(error)
        (output / "failure.txt").write_text(traceback.format_exc())
        save()
        raise


if __name__ == "__main__":
    main()
