#!/usr/bin/env python3
"""Explicit data-directory and cluster-identity startup regression."""

import importlib.util
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time


DBMS_MAIN = os.path.abspath(os.environ.get(
    "DBMS_MAIN", os.path.join(os.path.dirname(__file__), "..", "dbms_main")))
SOCKET_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_TEST_TIMEOUT", "10"))
STARTUP_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_STARTUP_TIMEOUT", "15"))

_spec = importlib.util.spec_from_file_location(
    "pg_protocol_helpers",
    os.path.join(os.path.dirname(__file__), "postgres_protocol_test.py"))
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)


def connect(port):
    sock = socket.socket()
    sock.settimeout(SOCKET_TIMEOUT)
    deadline = time.time() + STARTUP_TIMEOUT
    while True:
        try:
            sock.connect(("127.0.0.1", port))
            return sock
        except OSError:
            if time.time() >= deadline:
                raise
            time.sleep(0.05)


def free_port():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")

    with tempfile.TemporaryDirectory(prefix="dbms-datadir-") as root:
        root = Path(root)
        launch = root / "launch"
        cluster = root / "cluster"
        missing = root / "implicit"
        pg_cluster = root / "postgres"
        corrupt = root / "corrupt"
        legacy = root / "legacy"
        future = root / "future"
        for directory in (launch, cluster, missing, pg_cluster, corrupt,
                          legacy, future):
            directory.mkdir()

        # Version discovery must not require or initialize a data directory.
        version = subprocess.run(
            [DBMS_MAIN, "--version"], cwd=missing,
            capture_output=True, text=True, timeout=5, check=False)
        assert version.returncode == 0 and version.stdout.startswith("dbms "), version
        assert list(missing.iterdir()) == [], list(missing.iterdir())

        implicit = subprocess.run(
            [DBMS_MAIN, "--server", "0", "--insecure"], cwd=missing,
            capture_output=True, text=True, timeout=5, check=False)
        assert implicit.returncode == 1, implicit
        assert "explicit data directory is required" in implicit.stderr, implicit
        assert list(missing.iterdir()) == [], list(missing.iterdir())

        (pg_cluster / "PG_VERSION").write_text("18\n", encoding="utf-8")
        wrong_product = subprocess.run(
            [DBMS_MAIN, "-D", str(pg_cluster), "--server", "0", "--insecure"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert wrong_product.returncode == 1, wrong_product
        assert "PostgreSQL cluster" in wrong_product.stderr, wrong_product
        assert not (pg_cluster / "DBMS_CONTROL").exists()

        (corrupt / "DBMS_CONTROL").write_text(
            "NOT_A_DBMS_CLUSTER\n", encoding="utf-8")
        bad_control = subprocess.run(
            [DBMS_MAIN, "--data-dir", str(corrupt),
             "--server", "0", "--insecure"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert bad_control.returncode == 1, bad_control
        assert "invalid DBMS_CONTROL" in bad_control.stderr, bad_control

        (cluster / "info").mkdir()
        (cluster / "info" / "tlist.lst").touch()
        _helpers.write_auth_catalog(str(cluster), "alice", "secret")
        (cluster / "pg_hba.conf").write_text(
            "host all alice 127.0.0.1/32 scram-sha-256\n", encoding="utf-8")

        port = free_port()
        process = subprocess.Popen(
            [DBMS_MAIN, "--data-dir", str(cluster),
             "--server", str(port), "--insecure"],
            cwd=launch, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            sock = connect(port)
            _helpers.startup(sock, "alice", "info")
            messages = _helpers.simple_query(sock, "SHOW compatibility_mode")
            assert _helpers.data_row_values(messages) == [[b"postgresql18"]], messages
            sock.close()
        finally:
            process.terminate()
            process.wait(timeout=30)

        control = (cluster / "DBMS_CONTROL").read_text(encoding="utf-8")
        assert control.startswith(
            "DBMS_CPP_CLUSTER_CONTROL_V2\n"
            "control_format_version=2\n"
            "catalog_format_version=1\n"
            "heap_format_version=2\n"
            "block_size=8192\n"
            "byte_order="), control
        match = re.search(r"^system_identifier=([0-9a-f]{16})$", control, re.M)
        assert match and int(match.group(1), 16) != 0, control
        assert list(launch.iterdir()) == [], list(launch.iterdir())

        checked = subprocess.run(
            [DBMS_MAIN, "-D", str(cluster), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert checked.returncode == 0, checked
        assert "data directory is compatible" in checked.stdout, checked
        assert f"system_identifier={match.group(1)}" in checked.stdout, checked

        legacy_identifier = "0123456789abcdef"
        (legacy / "DBMS_CONTROL").write_text(
            "DBMS_CPP_CLUSTER_CONTROL_V1\nformat_version=1\n"
            f"system_identifier={legacy_identifier}\n", encoding="utf-8")
        legacy_start = subprocess.run(
            [DBMS_MAIN, "-D", str(legacy), "--server", "0", "--insecure"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert legacy_start.returncode == 1, legacy_start
        assert "requires offline upgrade" in legacy_start.stderr, legacy_start
        assert "CONTROL_V1" in (legacy / "DBMS_CONTROL").read_text(), legacy_start

        upgraded = subprocess.run(
            [DBMS_MAIN, "-D", str(legacy), "--upgrade-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert upgraded.returncode == 0, upgraded
        upgraded_control = (legacy / "DBMS_CONTROL").read_text(encoding="utf-8")
        assert upgraded_control.startswith("DBMS_CPP_CLUSTER_CONTROL_V2\n"), upgraded_control
        assert f"system_identifier={legacy_identifier}" in upgraded_control, upgraded_control

        (future / "DBMS_CONTROL").write_text(
            "DBMS_CPP_CLUSTER_CONTROL_V99\n", encoding="utf-8")
        future_before = (future / "DBMS_CONTROL").read_bytes()
        rejected_future = subprocess.run(
            [DBMS_MAIN, "-D", str(future), "--upgrade-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_future.returncode == 1, rejected_future
        assert "unsupported DBMS_CONTROL" in rejected_future.stderr, rejected_future
        assert (future / "DBMS_CONTROL").read_bytes() == future_before

        # The environment form selects the same cluster from another CWD and
        # preserves its system identifier rather than silently initializing.
        port = free_port()
        process = subprocess.Popen(
            [DBMS_MAIN, "--server", str(port), "--insecure"], cwd=launch,
            env=dict(os.environ, DBMS_DATA_DIR=str(cluster)),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            sock = connect(port)
            _helpers.startup(sock, "alice", "info")
            sock.close()
        finally:
            process.terminate()
            process.wait(timeout=30)
        assert (cluster / "DBMS_CONTROL").read_text(encoding="utf-8") == control
        assert list(launch.iterdir()) == [], list(launch.iterdir())

        conflict = subprocess.run(
            [DBMS_MAIN, "-D", str(cluster), "--server", "0", "--insecure"],
            cwd=launch, env=dict(os.environ, DBMS_DATA_DIR=str(pg_cluster)),
            capture_output=True, text=True, timeout=5, check=False)
        assert conflict.returncode == 1 and "conflicts" in conflict.stderr, conflict

    print("[DATA DIRECTORY] explicit root and cluster identity OK")


if __name__ == "__main__":
    main()
