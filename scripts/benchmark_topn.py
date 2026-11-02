#!/usr/bin/env python3
"""Validated old/new/old sort latency comparison on independent fixture copies."""

import argparse
import hashlib
import json
from pathlib import Path
import random
import shutil
import statistics
import time

import benchmark_queries as query


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_hashes(root):
    return {str(p.relative_to(root)): digest(p)
            for p in sorted(root.rglob("*")) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--fixture", type=Path,
                        help="Closed benchmark_queries fixture; must match --rows")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--samples", type=int, default=4)
    args = parser.parse_args()
    if min(args.rows, args.rounds, args.samples) < 1:
        parser.error("rows, rounds and samples must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    before, after = args.before.resolve(), args.after.resolve()
    fixture = args.fixture.resolve() if args.fixture else output / "fixture"
    result = {"complete": False, "rows": args.rows,
              "rounds_per_run": args.rounds,
              "samples_per_case_per_round": args.samples, "runs": [],
              "method": "Independent copies of one closed fixture; old/new/old; "
                        "fixed shuffled case order per round; three checked warmups "
                        "per case; socket timeout 15s; default durability. "
                        "Latency includes full result validation."}
    result_file = output / "comparison.json"
    try:
        if args.fixture is None:
            fixture.mkdir()
            (fixture / "info").mkdir()
            (fixture / "info/tlist.lst").touch()
            query.write_auth_catalog(fixture, "alice", "secret")
            (fixture / "pg_hba.conf").write_text("host all alice 127.0.0.1/32 scram-sha-256\n")
            with query.server(before, fixture, output / "fixture.log") as (client, _, _):
                client.sock.settimeout(15)
                query.load(client, args.rows)
        original = tree_hashes(fixture)
        result["fixture_hashes"] = original
        specs = {name: query.cases(args.rows)[name]
                 for name in ("sort_limit", "pk_lookup")}
        ordered = sorted(range(1, args.rows + 1), key=lambda i: (-(i % 1000), i))
        specs["sort_offset"] = (
            lambda _: "SELECT id,amount FROM bench_rows ORDER BY amount DESC,id ASC LIMIT 20 OFFSET 500",
            lambda _: [[str(i), str(i % 1000)] for i in ordered[500:520]])
        specs["sort_full"] = (
            lambda _: "SELECT id,amount FROM bench_rows ORDER BY amount DESC,id ASC",
            lambda _: [[str(i), str(i % 1000)] for i in ordered])
        for label, binary in (("old-first", before), ("new", after), ("old-last", before)):
            data = output / label
            shutil.copytree(fixture, data)
            run = {"label": label, "binary_sha256": digest(binary), "samples": []}
            result["runs"].append(run)
            with query.server(binary, data, output / (label + ".log")) as (client, _, _):
                client.sock.settimeout(15)
                for name, spec in specs.items():
                    for iteration in range(3):
                        query.execute_case(client, name, spec, iteration)
                for round_index in range(args.rounds):
                    order = list(specs)
                    random.Random(20261102 + round_index).shuffle(order)
                    for name in order:
                        for index in range(args.samples):
                            start = time.perf_counter()
                            query.execute_case(client, name, specs[name],
                                               round_index * args.samples + index)
                            run["samples"].append({"name": name, "round": round_index + 1,
                                                   "latency_ms": (time.perf_counter() - start) * 1000,
                                                   "validated": True})
            run["summaries"] = [{"name": name, "samples": args.rounds * args.samples,
                                 "p50_ms": statistics.median(s["latency_ms"] for s in run["samples"]
                                                             if s["name"] == name)}
                                for name in specs]
            print(label, run["summaries"], flush=True)
            result_file.write_text(json.dumps(result, indent=2) + "\n")
        result["source_fixture_unchanged"] = tree_hashes(fixture) == original
        if not result["source_fixture_unchanged"]:
            raise RuntimeError("source fixture changed")
        result["complete"] = True
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        result_file.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
