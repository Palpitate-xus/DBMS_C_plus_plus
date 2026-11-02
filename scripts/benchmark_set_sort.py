"""Validated real UNION ALL sort latency; private copies, fixed old/new/old order."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import shutil
import statistics
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
import benchmark_queries as query


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree(root):
    return {str(f.relative_to(root)): sha(f) for f in sorted(root.rglob("*")) if f.is_file()}


def specifications(size):
    ids = list(range(1, size + 1))
    pairs = [(i, branch) for branch in (1, 2) for i in ids]
    amount_order = sorted(pairs, key=lambda row: (-(row[0] % 1000), row[0], row[1]))
    text_order = sorted(pairs, key=lambda row: ("row-" + str(row[0])).encode(), reverse=True)
    peer_order = sorted(pairs, key=lambda row: -(row[0] % 100))
    union = lambda projection, order: ("SELECT " + projection + ",1 AS branch FROM bench_rows UNION ALL "
        "SELECT " + projection + ",2 AS branch FROM bench_rows ORDER BY " + order)
    rows = lambda ordered, value: [[str(i), value(i), str(branch)] for i, branch in ordered]
    integer_projection = "id,amount AS sortkey"
    amount = lambda i: str(i % 1000)
    largest_group = max(i % 100 for i in ids)
    fixed = {
        "union_text_full": (union('id,payload COLLATE "C" AS sortkey', "sortkey DESC,id,branch"),
                            rows(text_order, lambda i: "row-" + str(i))),
        "union_numeric_full": (union("id,amount::numeric AS sortkey", "sortkey DESC,id,branch"),
                               rows(amount_order, amount)),
        "union_float_full": (union("id,amount::double precision AS sortkey", "sortkey DESC,id,branch"),
                             rows(amount_order, amount)),
        "union_real_full": (union("id,amount::real AS sortkey", "sortkey DESC,id,branch"),
                            rows(amount_order, amount)),
        "union_integer_full": (union(integer_projection, "sortkey DESC,id,branch"), rows(amount_order, amount)),
        "union_stable_peers": (union("id,grp AS sortkey", "sortkey DESC"),
                               rows(peer_order, lambda i: str(i % 100))),
        "union_limit": (union(integer_projection, "sortkey DESC,id,branch") + " LIMIT 20",
                        rows(amount_order[:20], amount)),
        "union_offset": (union(integer_projection, "sortkey DESC,id,branch") + " LIMIT 20 OFFSET 500",
                         rows(amount_order[500:520], amount)),
        "union_ties": (union("id,grp AS sortkey", "sortkey DESC") + " FETCH FIRST 1 ROW WITH TIES",
                       rows([row for row in peer_order if row[0] % 100 == largest_group], lambda i: str(i % 100))),
    }
    result = {name: (lambda _, sql=sql: sql, lambda _, wanted=wanted: wanted)
              for name, (sql, wanted) in fixed.items()}
    result["integer_topn_control"] = query.cases(size)["sort_limit"]
    result["pk_lookup_control"] = query.cases(size)["pk_lookup"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--work-mem", required=True, choices=["64kB", "4MB", "16MB"])
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--samples", type=int, default=4)
    args = parser.parse_args()
    if min(args.rows, args.rounds, args.samples) < 1:
        parser.error("positive rows, rounds and samples required")
    output = args.output.resolve(); output.mkdir(parents=True)
    if any(output.iterdir()):
        parser.error("output must be empty")
    fixture = args.fixture.resolve(); original = tree(fixture)
    specs = specifications(args.rows)
    result = {"complete": False, "rows": args.rows, "union_rows_before_sort": 2 * args.rows, "work_mem": args.work_mem,
              "rounds": args.rounds, "samples_per_case_per_round": args.samples,
              "driver_sha256": sha(__file__), "fixture_hashes": original, "runs": [],
              "method": "Same closed fixture, independent old/new/old copies, three full-value warmups per case, fixed shuffled order per round; session work_mem explicit; default durability, socket15s/TCP_NODELAY; timer includes full validation. UNION ALL has two real branches and 2*rows typed input cells; stable-peer expected order belongs only to the fixed fixture. Integer TopN and point controls retained; no claim cache improves these integer controls."}
    dest = output / "comparison.json"
    try:
        for label, binary in [("old-first", args.before.resolve()), ("new", args.after.resolve()),
                              ("old-last", args.before.resolve())]:
            data = output / label; shutil.copytree(fixture, data)
            run = {"label": label, "binary_sha256": sha(binary), "samples": []}; result["runs"].append(run)
            with query.server(binary, data, output / (label + ".log")) as (client, _, _):
                client.sock.settimeout(15)
                assert client.query("SET work_mem='" + args.work_mem + "'") == []
                assert client.query("SHOW work_mem") == [[args.work_mem]]
                for name, spec in specs.items():
                    for iteration in range(3):
                        query.execute_case(client, name, spec, iteration)
                for round_index in range(args.rounds):
                    order = list(specs); random.Random(20261102 + round_index).shuffle(order)
                    for name in order:
                        for index in range(args.samples):
                            start = time.perf_counter()
                            query.execute_case(client, name, specs[name], round_index * args.samples + index)
                            run["samples"].append({"name": name, "round": round_index + 1,
                                                   "latency_ms": (time.perf_counter() - start) * 1000,
                                                   "validated": True})
            run["summaries"] = [{"name": name, "samples": args.rounds * args.samples,
                                 "p50_ms": statistics.median(s["latency_ms"] for s in run["samples"] if s["name"] == name)}
                                for name in specs]
            print(label, run["summaries"], flush=True)
            dest.write_text(json.dumps(result, indent=2) + "\n")
        result["source_fixture_unchanged"] = tree(fixture) == original
        assert result["source_fixture_unchanged"]
        result["complete"] = True
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        dest.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
