#!/usr/bin/env python3
"""Compare validated operations on two private, native database instances.

This small-fixture pilot reuses benchmark_operations.py, including its existing
oracles and limitations. It is not pgbench, a saturation test or a PG acceptance
gate. PostgreSQL must be 18.6; neither existing database is opened or modified.
"""
import argparse
from collections import Counter
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import statistics
import subprocess
import time
import traceback

import benchmark_queries as queries
from benchmark_operations import Operations
from postgres_protocol_test import startup_reference


class ReferenceClient(queries.Client):
    def __init__(self, port, database="info"):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            startup_reference(self.sock, "alice", database, password="secret")
        except BaseException:
            self.sock.close()
            raise


@contextmanager
def postgres(args, output):
    data = output / "postgres-data"
    data.mkdir()
    password = output / "postgres-password"
    password.touch(mode=0o600)
    password.write_text("secret\n")
    environment = {k: v for k, v in os.environ.items() if not k.startswith("DBMS_")}
    if args.pg_library_dir:
        environment["LD_LIBRARY_PATH"] = str(args.pg_library_dir.resolve()) + (
            ":" + environment["LD_LIBRARY_PATH"] if environment.get("LD_LIBRARY_PATH") else "")
    command = [str(args.pg_bin / "initdb"), "-D", str(data), "--no-locale",
               "--encoding=UTF8", "--username=alice", "--pwfile=" + str(password),
               "--auth-host=scram-sha-256", "--auth-local=trust"]
    try:
        with (output / "postgres-initdb.log").open("w") as log:
            subprocess.run(command, env=environment, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=60)
    finally:
        password.unlink()
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    with (output / "postgres-server.log").open("w") as log:
        process = subprocess.Popen([str(args.pg_bin / "postgres"), "-D", str(data),
                                    "-h", "127.0.0.1", "-p", str(port), "-k", ""],
                                   env=environment, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        client = None
        try:
            deadline = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError("private PostgreSQL exited; inspect postgres-server.log")
                # A listening socket precedes authentication readiness during
                # recovery/startup. Probe PostgreSQL's own readiness boundary
                # rather than passing a transient 57P03 into the wire oracle.
                ready = subprocess.run(
                    [str(args.pg_bin / "pg_isready"), "-h", "127.0.0.1",
                     "-p", str(port), "-t", "1"], env=environment,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=2)
                if ready.returncode:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("private PostgreSQL startup deadline exceeded")
                    time.sleep(0.05)
                    continue
                try:
                    client = ReferenceClient(port, "postgres")
                    break
                except (ConnectionRefusedError, ConnectionResetError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError("private PostgreSQL startup deadline exceeded")
                    time.sleep(0.05)
            assert client.query("SHOW server_version_num") == [["180006"]]
            assert client.query("SHOW data_directory") == [[str(data)]]
            client.query("CREATE DATABASE info")
            client.close()
            client = ReferenceClient(port)
            yield client, data
        finally:
            if client:
                client.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGINT)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                        raise RuntimeError("private PostgreSQL required forced shutdown")
            if process.returncode:
                raise RuntimeError(f"private PostgreSQL exited with {process.returncode}")


def operation(suite, name):
    setup, action, verify, cleanup, _ = suite.cases[name]
    context = setup(suite.counts[name])
    begin = time.perf_counter()
    action(context)
    elapsed = (time.perf_counter() - begin) * 1000
    suite.counts[name] += 1
    verify(context)
    cleanup(context)
    return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--pg-library-dir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--batch", type=int, default=20)
    parser.add_argument("--ddl-rows", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--only", nargs="+")
    args = parser.parse_args()
    if min(args.rows, args.batch, args.ddl_rows, args.rounds, args.samples) <= 0:
        parser.error("sizes and sample counts must be positive")
    if (args.rounds * args.samples + 1) * (args.batch + 2) > args.rows:
        parser.error("rows too small for disjoint DELETE samples including warmup")
    args.binary, args.pg_bin = args.binary.resolve(), args.pg_bin.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {"created_utc": datetime.now(timezone.utc).isoformat(), "complete": False,
              "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "source_hashes": {name: hashlib.sha256((queries.REPO / name).read_bytes()).hexdigest()
                                for name in ["scripts/benchmark_postgresql.py",
                                             "scripts/benchmark_operations.py", "scripts/benchmark_queries.py"]},
              "measurements": [], "warmups": [], "summaries": [],
              "method": "Two native processes, same filesystem, loopback TCP_NODELAY, one reused connection each; "
                        "one checked warmup/case/engine; case order shuffled by fixed seed; paired engine order alternates by round. "
                        "Action/response latency; setup, mutation oracles and cleanup outside timer; "
                        "SELECT oracles inside action. No cross-engine CPU/I/O comparison or saturation claim."}

    def save():
        (output / "comparison.json").write_text(json.dumps(report, indent=2))

    save()
    try:
        with postgres(args, output) as (reference, pg_data):
            settings = {}
            for key in ["server_version_num", "fsync", "synchronous_commit", "full_page_writes",
                        "shared_buffers", "work_mem", "checkpoint_timeout", "data_checksums", "jit"]:
                settings[key] = reference.query("SHOW " + key)[0][0]
            assert settings["fsync"] == settings["synchronous_commit"] == settings["full_page_writes"] == "on"
            report["postgres_settings"] = settings
            data = output / "dbms-data"
            (data / "info").mkdir(parents=True)
            (data / "info/tlist.lst").touch()
            queries.write_auth_catalog(str(data), "alice", "secret")
            (data / "pg_hba.conf").write_text("host all alice 127.0.0.1/32 scram-sha-256\n")
            assert os.stat(data).st_dev == os.stat(pg_data).st_dev
            report["filesystem_device"] = os.stat(data).st_dev
            with queries.server(args.binary, data, output / "dbms-server.log") as (client, pid, _):
                client.sock.settimeout(15)
                suites = {"dbms": Operations(client, pid, args, report),
                          "postgresql": Operations(reference, None, args, report)}
                for engine, suite in suites.items():
                    print("INITIALIZE", engine, flush=True)
                    suite.initialize()
                names = args.only or list(suites["dbms"].cases)
                assert all(name in suite.cases for suite in suites.values() for name in names)
                report["cases"] = names
                report["plans"] = {}
                for engine, suite in suites.items():
                    report["plans"][engine] = {name: suite.client.query("EXPLAIN " + queries.cases(args.rows)[name][0](0))
                                               for name in ["pk_lookup", "range_sum", "join_count"]}
                    for name in names:
                        operation(suite, name)
                        report["warmups"].append({"engine": engine, "name": name, "validated": True})
                    print("WARMED", engine, len(names), flush=True)
                    save()
                for round_id in range(args.rounds):
                    order = names[:]
                    random.Random(3719 + round_id).shuffle(order)
                    engines = ["dbms", "postgresql"] if round_id % 2 == 0 else ["postgresql", "dbms"]
                    for name in order:
                        for engine in engines:
                            for _ in range(args.samples):
                                elapsed = operation(suites[engine], name)
                                report["measurements"].append({"engine": engine, "name": name,
                                    "round": round_id + 1, "sample": suites[engine].counts[name] - 1,
                                    "latency_ms": elapsed, "validated": True})
                        save()
                        print("PAIRED", round_id + 1, name, flush=True)
                for name in names:
                    values = {engine: sorted(m["latency_ms"] for m in report["measurements"]
                                             if m["engine"] == engine and m["name"] == name)
                              for engine in suites}
                    medians = {engine: statistics.median(v) for engine, v in values.items()}
                    report["summaries"].append({"name": name, "samples_per_engine": len(values["dbms"]),
                        "dbms_p50_ms": medians["dbms"], "postgresql_p50_ms": medians["postgresql"],
                        "dbms_p95_ms": values["dbms"][math.ceil(len(values["dbms"]) * .95) - 1],
                        "postgresql_p95_ms": values["postgresql"][math.ceil(len(values["postgresql"]) * .95) - 1],
                        "latency_ratio_dbms_over_postgresql": medians["dbms"] / medians["postgresql"]})
                report["final_counts"] = {engine: dict(suite.counts) for engine, suite in suites.items()}
        report["complete"] = True
        save()
        with (output / "comparison.csv").open("w") as file:
            writer = csv.DictWriter(file, fieldnames=list(report["summaries"][0]))
            writer.writeheader()
            writer.writerows(report["summaries"])
        print("COMPLETE", len(report["measurements"]), flush=True)
    except BaseException:
        report["failure"] = traceback.format_exc()
        save()
        raise


if __name__ == "__main__":
    main()
