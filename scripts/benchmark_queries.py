#!/usr/bin/env python3
"""Measure validated SQL queries against a private, disposable DBMS cluster.

Example:
  python3 scripts/benchmark_queries.py --binary /absolute/path/dbms_main \
      --output build/query-benchmark --rows 1000 10000 --profile

The binary is never rebuilt by this driver. Every run creates its own data
directory, keeps default server settings, and reports client round-trip latency
separately from server CPU and /proc I/O counters. Connection authentication,
fixture loading, warmups and EXPLAIN are outside measured intervals. Profiles
run separately, so their instrumentation does not contaminate baseline times.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))
from postgres_protocol_test import (data_row_values, diagnostic_fields,
                                    read_until_ready, simple_query, startup,
                                    typed, write_auth_catalog)


def checked_rows(messages):
    for kind, body in messages:
        if kind == b"E":
            fields = diagnostic_fields(body)
            raise RuntimeError({k.decode(): v.decode(errors="replace")
                                for k, v in fields.items()})
    if not messages or messages[-1][0] != b"Z":
        raise RuntimeError("query did not complete with ReadyForQuery")
    return [[None if cell is None else cell.decode("utf-8") for cell in row]
            for row in data_row_values(messages)]


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        startup(self.sock, "alice", "info", validate_dbms_status=False,
                application_name="query-performance-benchmark")

    def query(self, sql):
        return checked_rows(simple_query(self.sock, sql))

    def prepare(self):
        sql = "SELECT id,grp,amount,payload FROM bench_rows WHERE id=$1"
        body = b"bench_pk\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 23)
        self.sock.sendall(typed(b"P", body) + typed(b"S"))
        messages = read_until_ready(self.sock)
        checked_rows(messages)
        if not any(kind == b"1" for kind, _ in messages):
            raise RuntimeError("missing ParseComplete")

    def prepared_point(self, key):
        body = (b"\0bench_pk\0" + struct.pack("!HHH", 1, 1, 1)
                + struct.pack("!iiH", 4, key, 0))
        self.sock.sendall(typed(b"B", body) + typed(b"D", b"P\0")
                          + typed(b"E", b"\0" + struct.pack("!I", 0))
                          + typed(b"S"))
        return checked_rows(read_until_ready(self.sock))

    def close(self):
        try:
            self.sock.sendall(typed(b"X"))
        except OSError:
            pass
        self.sock.close()


def target_pid(process, binary):
    pending = [process.pid]
    while pending:
        pid = pending.pop()
        try:
            command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if command[0] == str(binary).encode() and b"--server" in command:
                return pid
            children = Path(f"/proc/{pid}/task/{pid}/children").read_text()
            pending.extend(map(int, children.split()))
        except (FileNotFoundError, ProcessLookupError):
            pass
    raise RuntimeError("cannot identify the benchmark server process")


@contextmanager
def server(binary, data, log_path, experiment=None, command_prefix=()):
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    command = [str(binary), "--data-dir", str(data), "--server", str(port),
               "--insecure"]
    if experiment:
        # Clock sampling avoids the collector's I/O interposition hooks.
        # Some gprofng/glibc combinations cannot trace llseek/OFD locking;
        # trace system calls in a separate strace run when needed.
        command = ["gprofng", "collect", "app", "-p", "1",
                   "-o", str(experiment)] + command
    command = list(command_prefix) + command
    environment = {k: v for k, v in os.environ.items()
                   if not k.startswith("DBMS_")}
    environment["DBMS_DATA_DIR"] = str(data)
    with log_path.open("w") as log:
        process = subprocess.Popen(command, cwd=data, env=environment,
                                   stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        client = None
        pid = None
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("server exited at startup: "
                                       + log_path.read_text()[-3000:])
                try:
                    client = Client(port)
                    break
                except (ConnectionRefusedError, ConnectionResetError):
                    time.sleep(0.05)
            if client is None:
                raise RuntimeError("server startup deadline exceeded")
            pid = target_pid(process, binary)
            yield client, pid, port
        finally:
            if client:
                client.close()
            if process.poll() is None:
                if pid:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                else:
                    os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise RuntimeError("benchmark server required forced shutdown")
            if process.returncode:
                raise RuntimeError(f"server exited with {process.returncode}: "
                                   + log_path.read_text()[-2000:])


def resources(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    hz = os.sysconf("SC_CLK_TCK")
    result = {"user_seconds": int(fields[11]) / hz,
              "system_seconds": int(fields[12]) / hz}
    for line in Path(f"/proc/{pid}/io").read_text().splitlines():
        name, value = line.split(":")
        result[name] = int(value)
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith(("VmRSS:", "VmHWM:", "Threads:")):
            name, value, *_ = line.split()
            result[name.rstrip(":")] = int(value)
    return result


def summarize(latencies, elapsed, before, after):
    ordered = sorted(latencies)
    count = len(ordered)
    if not count:
        raise RuntimeError("empty measurement")
    delta = {key: after[key] - before[key] for key in before
             if key not in {"VmRSS", "VmHWM", "Threads"}}
    cpu = delta["user_seconds"] + delta["system_seconds"]
    return {"queries": count, "errors": 0, "elapsed_seconds": elapsed,
            "qps": count / elapsed, "mean_ms": statistics.mean(ordered),
            "p50_ms": statistics.median(ordered),
            "p95_ms": ordered[math.ceil(count * 0.95) - 1],
            "max_ms": ordered[-1], "server_cpu_ms_per_query": cpu * 1000 / count,
            "server_cpu_cores": cpu / elapsed, "server_io_delta": delta,
            "server_rss_kib": after["VmRSS"], "latencies_ms": latencies}


def cases(size):
    def key(i):
        return 1 + ((i * 7919 + size // 2) % size)

    def row(k):
        return [str(k), str(k % 100), str(k % 1000), f"row-{k}"]

    low, high = max(1, size // 2), min(size, size // 2 + 99)
    totals = [[str(group), str(sum(i % 100 == group for i in range(1, size + 1))),
               str(sum(i % 1000 for i in range(1, size + 1) if i % 100 == group))]
              for group in range(100)]
    top = [[str(i), str(i % 1000)] for i in
           sorted(range(1, size + 1), key=lambda i: (-(i % 1000), i))[:20]]
    return {
        "constant": (lambda i: "SELECT 1 AS v", lambda i: [["1"]]),
        "pk_lookup": (lambda i: "SELECT id,grp,amount,payload FROM bench_rows WHERE id="
                      + str(key(i)), lambda i: [row(key(i))]),
        "pk_prepared": (lambda i: key(i), lambda i: [row(key(i))]),
        "unindexed_lookup": (lambda i: "SELECT id,grp,amount,payload FROM bench_rows WHERE payload='row-"
                             + str(key(i)) + "'", lambda i: [row(key(i))]),
        "range_sum": (lambda i: "SELECT sum(amount) FROM bench_rows WHERE id BETWEEN "
                      + str(low) + " AND " + str(high),
                      lambda i: [[str(sum(k % 1000 for k in range(low, high + 1)))]]),
        "group_aggregate": (lambda i: "SELECT grp,count(*),sum(amount) FROM bench_rows GROUP BY grp ORDER BY grp",
                            lambda i: totals),
        "sort_limit": (lambda i: "SELECT id,amount FROM bench_rows ORDER BY amount DESC,id ASC LIMIT 20",
                       lambda i: top),
        "join_count": (lambda i: "SELECT count(*) FROM bench_rows r JOIN bench_groups g ON r.grp=g.id",
                       lambda i: [[str(size)]]),
    }


def execute_case(client, name, specification, iteration):
    sql, expected = specification
    actual = (client.prepared_point(sql(iteration)) if name == "pk_prepared"
              else client.query(sql(iteration)))
    wanted = expected(iteration)
    if actual != wanted:
        raise AssertionError((name, sql(iteration), "actual", actual[:5],
                              "expected", wanted[:5], "counts", len(actual), len(wanted)))


def measure(client, pid, name, specification, seconds, maximum):
    # Warm the statement, relation buffers and connection before measuring.
    for i in range(2):
        execute_case(client, name, specification, i)
    before = resources(pid)
    begin = time.perf_counter()
    latencies = []
    while len(latencies) < maximum:
        start = time.perf_counter()
        execute_case(client, name, specification, len(latencies))
        latencies.append((time.perf_counter() - start) * 1000)
        if len(latencies) >= 3 and time.perf_counter() - begin >= seconds:
            break
    elapsed = time.perf_counter() - begin
    return summarize(latencies, elapsed, before, resources(pid))


def load(client, size):
    client.query("CREATE TABLE bench_rows(id INTEGER PRIMARY KEY,grp INTEGER,amount INTEGER,payload VARCHAR(64))")
    client.query("CREATE TABLE bench_groups(id INTEGER PRIMARY KEY,label VARCHAR(32))")
    client.query("BEGIN")
    for start in range(1, size + 1, 200):
        values = ",".join(f"({i},{i % 100},{i % 1000},'row-{i}')"
                          for i in range(start, min(size + 1, start + 200)))
        client.query("INSERT INTO bench_rows VALUES " + values)
    client.query("INSERT INTO bench_groups VALUES " + ",".join(
        f"({i},'group-{i}')" for i in range(100)))
    client.query("COMMIT")
    client.query("ANALYZE bench_rows")
    client.query("ANALYZE bench_groups")
    if client.query("SELECT count(*) FROM bench_rows") != [[str(size)]]:
        raise AssertionError("fixture row count mismatch")
    client.query("CHECKPOINT")


def concurrency(pid, port, size, clients, seconds):
    ready = threading.Barrier(clients + 1)
    deadline = [None]
    spec = cases(size)["pk_lookup"]

    def worker(worker_id):
        client = Client(port)
        try:
            execute_case(client, "pk_lookup", spec, worker_id)
            ready.wait(timeout=70)
            values = []
            while time.perf_counter() < deadline[0]:
                start = time.perf_counter()
                execute_case(client, "pk_lookup", spec,
                             len(values) * clients + worker_id)
                values.append((time.perf_counter() - start) * 1000)
            return values
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [pool.submit(worker, i) for i in range(clients)]
        # Wait for authentication/warmup before taking the resource baseline.
        while ready.n_waiting != clients:
            if any(f.done() and f.exception() for f in futures):
                raise next(f.exception() for f in futures if f.done() and f.exception())
            time.sleep(0.01)
        before = resources(pid)
        begin = time.perf_counter()
        deadline[0] = begin + seconds
        ready.wait(timeout=70)
        latencies = [v for f in futures for v in f.result()]
        result = summarize(latencies, time.perf_counter() - begin,
                           before, resources(pid))
        result["clients"] = clients
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rows", nargs="+", type=int, default=[100, 1000])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seconds", type=float, default=1.0)
    parser.add_argument("--max-queries", type=int, default=1000)
    parser.add_argument("--concurrency-seconds", type=float, default=12,
                        help="minimum measured interval for each concurrency block")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--profile-from", type=Path,
                        help="profile a prior run's private cluster without repeating its baseline")
    parser.add_argument("--fixture-from", type=Path,
                        help="copy a prior benchmark's private fixtures for a controlled comparison")
    parser.add_argument("--profile-seconds", type=float, default=8)
    parser.add_argument("--profile-cases", nargs="+",
                        default=["pk_lookup", "group_aggregate", "sort_limit"])
    args = parser.parse_args()
    if (min(args.rows) < 100 or args.rounds < 1 or args.seconds <= 0
            or args.concurrency_seconds <= 0 or args.profile_seconds <= 0
            or args.max_queries < 3):
        parser.error("rows >= 100, rounds >= 1, seconds > 0 and max-queries >= 3 required")
    binary, output = args.binary.resolve(), args.output.resolve()
    if not binary.is_file():
        parser.error("binary does not exist")
    if args.profile_from and args.fixture_from:
        parser.error("profile-from and fixture-from are mutually exclusive")
    output.mkdir(parents=True, exist_ok=True)
    report_file = output / "baseline.json"
    if report_file.exists():
        parser.error("output already contains baseline.json; use a new output directory")
    report = {"created_utc": datetime.now(timezone.utc).isoformat(),
              "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "platform": platform.platform(), "python": sys.version,
              "settings": "server defaults; C++17/O2 build identity belongs to supplied binary",
              "measurement": "warm cache, reused loopback TCP connection, SQL result validation included; no TLS",
              "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              "cases": [], "concurrency": [], "profiles": [], "data_directories": {}}
    fixtures = {}
    if args.fixture_from:
        fixture_report = args.fixture_from.resolve()
        prior = json.loads(fixture_report.read_text())
        for size in args.rows:
            source = Path(prior["data_directories"][str(size)]).resolve()
            if (not source.is_relative_to(fixture_report.parent)
                    or not source.name.startswith(f"data-{size}-") or not source.is_dir()):
                parser.error("fixture source is not a benchmark-owned cluster")
            fixtures[size] = source
        report["fixture_source"] = str(fixture_report)
        report["fixture_mode"] = "private copy; source cluster is never opened by the supplied binary"
    if args.profile_from:
        prior_path = args.profile_from.resolve()
        prior = json.loads(prior_path.read_text())
        if prior["binary_sha256"] != report["binary_sha256"]:
            parser.error("prior baseline belongs to a different binary")
        for size, directory in prior["data_directories"].items():
            data = Path(directory).resolve()
            if (not data.is_relative_to(prior_path.parent)
                    or not data.name.startswith(f"data-{size}-")
                    or not data.is_dir()):
                parser.error("prior data directory is not a benchmark-owned cluster")
        report["data_directories"] = prior["data_directories"]
        report["baseline_source"] = str(prior_path)
        report["baseline_source_complete"] = prior.get("complete", False)
        report["baseline_source_failure"] = prior.get("failure")
        report["baseline_source_case_blocks"] = len(prior["cases"])
        report["baseline_source_concurrency_blocks"] = len(prior["concurrency"])
        args.rows = sorted(map(int, prior["data_directories"]))
        args.profile = True

    def save():
        report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    def progress(text):
        print(text, flush=True)
        save()

    save()
    try:
        for size in ([] if args.profile_from else args.rows):
            data = Path(tempfile.mkdtemp(prefix=f"data-{size}-", dir=output))
            if size in fixtures:
                shutil.copytree(fixtures[size], data, dirs_exist_ok=True)
            else:
                (data / "info").mkdir()
                (data / "info/tlist.lst").touch()
                write_auth_catalog(str(data), "alice", "secret")
                (data / "pg_hba.conf").write_text("host all alice 127.0.0.1/32 scram-sha-256\n")
            report["data_directories"][str(size)] = str(data)
            with server(binary, data, output / f"server-{size}.log") as (client, pid, port):
                progress(f"LOADING rows={size}")
                started = time.perf_counter()
                if size in fixtures:
                    if client.query("SELECT count(*) FROM bench_rows") != [[str(size)]]:
                        raise AssertionError("copied fixture row count mismatch")
                    client.query("CHECKPOINT")
                else:
                    load(client, size)
                report.setdefault("fixture_seconds", {})[str(size)] = time.perf_counter() - started
                client.prepare()
                specifications = cases(size)
                plans = {}
                for name, (sql, _) in specifications.items():
                    if name != "pk_prepared":
                        plans[name] = client.query("EXPLAIN " + sql(0))
                (output / f"plans-{size}.json").write_text(json.dumps(plans, indent=2))
                progress(f"LOADED rows={size}")
                for round_id in range(args.rounds):
                    names = list(specifications)
                    random.Random(1729 + round_id).shuffle(names)
                    for name in names:
                        measured = measure(client, pid, name, specifications[name],
                                           args.seconds, args.max_queries)
                        measured.update({"name": name, "rows": size, "round": round_id + 1,
                                         "example_input": specifications[name][0](0)})
                        report["cases"].append(measured)
                        progress(f"CASE rows={size} round={round_id + 1} {name} "
                                 f"n={measured['queries']} p50={measured['p50_ms']:.3f}ms "
                                 f"p95={measured['p95_ms']:.3f}ms qps={measured['qps']:.1f}")
                if size == max(args.rows):
                    for clients in (1, 4):
                        for round_id in range(args.rounds):
                            measured = concurrency(pid, port, size, clients,
                                                   args.concurrency_seconds)
                            measured.update({"round": round_id + 1, "rows": size, "name": "pk_lookup"})
                            report["concurrency"].append(measured)
                            progress(f"CONCURRENCY clients={clients} round={round_id + 1} "
                                     f"n={measured['queries']} p50={measured['p50_ms']:.3f}ms "
                                     f"p95={measured['p95_ms']:.3f}ms qps={measured['qps']:.1f}")
        if args.profile:
            if not shutil.which("gprofng"):
                raise RuntimeError("--profile requires gprofng")
            size = max(args.rows)
            data = Path(report["data_directories"][str(size)])
            specifications = cases(size)
            for name in args.profile_cases:
                if name not in specifications:
                    raise ValueError("unknown profile case " + name)
                experiment = output / f"profile-{name}.er"
                with server(binary, data, output / f"profile-{name}.log", experiment) as (client, pid, port):
                    client.prepare()
                    measured = measure(client, pid, name, specifications[name],
                                       args.profile_seconds, 100000)
                measured.update({"name": name, "rows": size, "experiment": str(experiment),
                                 "instrumented": True})
                report["profiles"].append(measured)
                command = ["gprofng", "display", "text", "-limit", "50", "-header", "-functions", str(experiment)]
                with (output / f"profile-{name}-functions.txt").open("w") as stream:
                    subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=90)
                progress(f"PROFILE_COMPLETE {name} queries={measured['queries']}")
        report["complete"] = True
        progress("BENCHMARK_COMPLETE all measured SQL results validated; errors=0")
    except BaseException as error:
        report["complete"] = False
        report["failure"] = repr(error)
        save()
        raise


if __name__ == "__main__":
    main()
