#!/usr/bin/env python3
"""Explicit data-directory and cluster-identity startup regression."""

import importlib.util
import os
from pathlib import Path
import re
import socket
import struct
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


def crc32c(data):
    value = 0xffffffff
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def replace_control_field(contents, key, value):
    lines = contents.splitlines()
    matches = [index for index, line in enumerate(lines)
               if line.startswith(f"{key}=")]
    assert len(matches) == 1, contents
    lines[matches[0]] = f"{key}={value}"
    prefix = "\n".join(lines[:-1]) + "\n"
    return prefix + f"control_checksum={crc32c(prefix.encode()):08x}\n"


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")
    byte_order = "little" if struct.pack("=H", 1)[0] == 1 else "big"

    with tempfile.TemporaryDirectory(prefix="dbms-datadir-") as root:
        root = Path(root)
        launch = root / "launch"
        cluster = root / "cluster"
        missing = root / "implicit"
        pg_cluster = root / "postgres"
        corrupt = root / "corrupt"
        tampered = root / "tampered"
        incompatible = root / "incompatible"
        framing = root / "framing"
        linked = root / "linked"
        oversized = root / "oversized"
        special = root / "special"
        legacy = root / "legacy"
        version2 = root / "version2"
        future = root / "future"
        for directory in (launch, cluster, missing, pg_cluster, corrupt,
                          tampered, incompatible, framing, linked, oversized, special,
                          legacy, version2,
                          future):
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

            second = subprocess.run(
                [DBMS_MAIN, "-D", str(cluster), "--server", str(free_port()),
                 "--insecure"], cwd=launch, capture_output=True,
                text=True, timeout=5, check=False)
            assert second.returncode == 1, second
            assert "data directory is already in use" in second.stderr, second
            assert process.poll() is None

            checked_live = subprocess.run(
                [DBMS_MAIN, "-D", str(cluster), "--check-data-directory"],
                cwd=launch, capture_output=True, text=True,
                timeout=5, check=False)
            assert checked_live.returncode == 0, checked_live
            for utility in ("--verify-data-checksums",
                            "--upgrade-data-directory"):
                offline = subprocess.run(
                    [DBMS_MAIN, "-D", str(cluster), utility], cwd=launch,
                    capture_output=True, text=True, timeout=5, check=False)
                assert offline.returncode == 1, offline
                assert "data directory is already in use" in offline.stderr, offline
        finally:
            process.terminate()
            process.wait(timeout=30)

        control = (cluster / "DBMS_CONTROL").read_text(encoding="utf-8")
        assert control.startswith(
            "DBMS_CPP_CLUSTER_CONTROL_V3\n"
            "control_format_version=3\n"
            "catalog_format_version=1\n"
            "heap_format_version=2\n"
            "block_size=8192\n"
            "wal_segment_size=16777216\n"
            f"byte_order={byte_order}\nfeature_flags=00000000\n"), control
        match = re.search(r"^system_identifier=([0-9a-f]{16})$", control, re.M)
        assert match and int(match.group(1), 16) != 0, control
        assert re.search(r"^control_checksum=[0-9a-f]{8}$", control, re.M), control
        assert list(launch.iterdir()) == [], list(launch.iterdir())

        # A different but syntactically valid system identifier must not make
        # a damaged control file appear to identify another cluster.
        tampered_identifier = (
            "1111111111111111" if match.group(1) != "1111111111111111"
            else "2222222222222222")
        tampered_control = control.replace(
            f"system_identifier={match.group(1)}",
            f"system_identifier={tampered_identifier}")
        (tampered / "DBMS_CONTROL").write_text(tampered_control, encoding="utf-8")
        rejected_tampering = subprocess.run(
            [DBMS_MAIN, "-D", str(tampered), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_tampering.returncode == 1, rejected_tampering
        assert "checksum" in rejected_tampering.stderr.lower(), rejected_tampering

        (framing / "DBMS_CONTROL").write_text(
            control[:-1], encoding="utf-8")
        rejected_framing = subprocess.run(
            [DBMS_MAIN, "-D", str(framing), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_framing.returncode == 1, rejected_framing
        assert "final LF" in rejected_framing.stderr, rejected_framing

        wrong_byte_order = "big" if byte_order == "little" else "little"
        unsupported_fields = (
            ("feature_flags", "00000001"),
            ("catalog_format_version", "2"),
            ("heap_format_version", "99"),
            ("block_size", "4096"),
            ("wal_segment_size", "8192"),
            ("byte_order", wrong_byte_order),
        )
        for key, value in unsupported_fields:
            (incompatible / "DBMS_CONTROL").write_text(
                replace_control_field(control, key, value), encoding="utf-8")
            rejected_field = subprocess.run(
                [DBMS_MAIN, "-D", str(incompatible), "--check-data-directory"],
                cwd=launch, capture_output=True, text=True,
                timeout=5, check=False)
            assert rejected_field.returncode == 1, (key, rejected_field)
            assert "incompatible" in rejected_field.stderr.lower(), (key, rejected_field)

        # Identity files are bounded regular files, not arbitrary symlink
        # targets supplied from outside the data directory.
        (linked / "DBMS_CONTROL").symlink_to(cluster / "DBMS_CONTROL")
        rejected_symlink = subprocess.run(
            [DBMS_MAIN, "-D", str(linked), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_symlink.returncode == 1, rejected_symlink
        (oversized / "DBMS_CONTROL").write_bytes(b"x" * 1025)
        rejected_oversized = subprocess.run(
            [DBMS_MAIN, "-D", str(oversized), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_oversized.returncode == 1, rejected_oversized
        assert "maximum supported size" in rejected_oversized.stderr, rejected_oversized
        os.mkfifo(special / "DBMS_CONTROL")
        rejected_fifo = subprocess.run(
            [DBMS_MAIN, "-D", str(special), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert rejected_fifo.returncode == 1, rejected_fifo
        assert "not a regular file" in rejected_fifo.stderr, rejected_fifo

        checked = subprocess.run(
            [DBMS_MAIN, "-D", str(cluster), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert checked.returncode == 0, checked
        assert "data directory is compatible" in checked.stdout, checked
        assert "control_format_version=3" in checked.stdout, checked
        assert "wal_segment_size=16777216" in checked.stdout, checked
        assert f"system_identifier={match.group(1)}" in checked.stdout, checked

        # Relative -D paths are resolved against the launch directory before
        # bootstrap chdir; restart must select the same existing cluster, not
        # create a sibling relative to the changed process CWD.
        relative_cluster = os.path.relpath(cluster, launch)
        port = free_port()
        process = subprocess.Popen(
            [DBMS_MAIN, "-D", relative_cluster, "--server", str(port),
             "--insecure"], cwd=launch, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        try:
            sock = connect(port)
            _helpers.startup(sock, "alice", "info")
            sock.close()
        finally:
            process.terminate()
            process.wait(timeout=30)
        assert (cluster / "DBMS_CONTROL").read_text(encoding="utf-8") == control
        assert list(launch.iterdir()) == [], list(launch.iterdir())

        legacy_identifier = "0123456789abcdef"
        (legacy / "DBMS_CONTROL").write_text(
            "DBMS_CPP_CLUSTER_CONTROL_V1\nformat_version=1\n"
            f"system_identifier={legacy_identifier}\n", encoding="utf-8")
        legacy_start = subprocess.run(
            [DBMS_MAIN, "-D", str(legacy), "--server", "0", "--insecure"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert legacy_start.returncode == 1, legacy_start
        assert "offline upgrade is required" in legacy_start.stderr, legacy_start
        assert "CONTROL_V1" in (legacy / "DBMS_CONTROL").read_text(), legacy_start

        upgraded = subprocess.run(
            [DBMS_MAIN, "-D", str(legacy), "--upgrade-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert upgraded.returncode == 0, upgraded
        upgraded_control = (legacy / "DBMS_CONTROL").read_text(encoding="utf-8")
        assert upgraded_control.startswith("DBMS_CPP_CLUSTER_CONTROL_V3\n"), upgraded_control
        assert f"system_identifier={legacy_identifier}" in upgraded_control, upgraded_control

        version2_identifier = "fedcba9876543210"
        (version2 / "DBMS_CONTROL").write_text(
            "DBMS_CPP_CLUSTER_CONTROL_V2\n"
            "control_format_version=2\n"
            "catalog_format_version=1\n"
            "heap_format_version=2\n"
            "block_size=8192\n"
            f"byte_order={byte_order}\n"
            f"system_identifier={version2_identifier}\n", encoding="utf-8")
        version2_start = subprocess.run(
            [DBMS_MAIN, "-D", str(version2), "--server", "0", "--insecure"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert version2_start.returncode == 1, version2_start
        assert "control version 2" in version2_start.stderr, version2_start
        version2_check = subprocess.run(
            [DBMS_MAIN, "-D", str(version2), "--check-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert version2_check.returncode == 1, version2_check
        assert "offline upgrade is required" in version2_check.stderr, version2_check
        version2_upgrade = subprocess.run(
            [DBMS_MAIN, "-D", str(version2), "--upgrade-data-directory"],
            cwd=launch, capture_output=True, text=True, timeout=5, check=False)
        assert version2_upgrade.returncode == 0, version2_upgrade
        upgraded_v2 = (version2 / "DBMS_CONTROL").read_text(encoding="utf-8")
        assert upgraded_v2.startswith("DBMS_CPP_CLUSTER_CONTROL_V3\n"), upgraded_v2
        assert f"system_identifier={version2_identifier}" in upgraded_v2, upgraded_v2
        assert "from version 2 to version 3" in version2_upgrade.stdout, version2_upgrade

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
