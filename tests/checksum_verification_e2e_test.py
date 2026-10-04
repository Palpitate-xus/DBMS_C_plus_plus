#!/usr/bin/env python3
"""Offline heap-checksum verification and corruption-injection regression."""

import os
from pathlib import Path
import hashlib
import stat
import struct
import subprocess
import tempfile


DBMS_MAIN = os.path.abspath(os.environ.get(
    "DBMS_MAIN", os.path.join(os.path.dirname(__file__), "..", "dbms_main")))
PAGE_SIZE = 8192
DATA_FILE_MAGIC = 0x44415441
DATA_FILE_FORMAT_VERSION = 2


def fnv1a32(data):
    value = 2166136261
    for byte in data:
        value ^= byte
        value = (value * 16777619) & 0xffffffff
    return value


def fletcher16(data):
    sum1 = 0
    sum2 = 0
    for byte in data:
        sum1 = (sum1 + byte) % 255
        sum2 = (sum2 + sum1) % 255
    value = (sum2 << 8) | sum1
    return value if value else 0xffff


def crc32c(data):
    value = 0xffffffff
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def heap_page():
    page = bytearray(PAGE_SIZE)
    page_size_version = ((PAGE_SIZE // 512) << 8) | 4
    struct.pack_into(
        "=QHHHHHHI", page, 0,
        0, 0, 0, 24, PAGE_SIZE - 4, PAGE_SIZE - 4,
        page_size_version, 0)
    struct.pack_into("=H", page, 8, fletcher16(page))
    return bytes(page)


def write_heap(path, blocks=2):
    assert blocks >= 1
    header_without_checksum = struct.pack(
        "=IIIII", DATA_FILE_MAGIC, blocks, 0, 64,
        DATA_FILE_FORMAT_VERSION)
    header = header_without_checksum + struct.pack(
        "=I", fnv1a32(header_without_checksum))
    header_page = header + bytes(PAGE_SIZE - len(header))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(header_page + heap_page() * (blocks - 1))


def write_encrypted_heap(path, key):
    write_heap(path, 2)
    contents = bytearray(path.read_bytes())
    plaintext = contents[PAGE_SIZE:]
    nonce = bytes(range(16))
    stream = bytearray()
    counter = 0
    while len(stream) < len(plaintext):
        stream.extend(hashlib.sha256(
            key + struct.pack("=I", 1) + nonce +
            struct.pack("=I", counter)).digest())
        counter += 1
    ciphertext = bytes(left ^ right for left, right in
                       zip(plaintext, stream))
    contents[PAGE_SIZE:] = ciphertext
    path.write_bytes(contents)
    mac = hashlib.sha256(
        nonce + ciphertext + key + struct.pack("=I", 1)).digest()
    Path(str(path) + ".tde").write_bytes(bytes(48) + nonce + mac)


def write_control(cluster):
    prefix = (
        "DBMS_CPP_CLUSTER_CONTROL_V3\n"
        "control_format_version=3\n"
        "catalog_format_version=1\n"
        "heap_format_version=2\n"
        "block_size=8192\n"
        "wal_segment_size=16777216\n"
        f"byte_order={'little' if struct.pack('=H', 1)[0] == 1 else 'big'}\n"
        "feature_flags=00000000\n"
        "system_identifier=0123456789abcdef\n")
    cluster.joinpath("DBMS_CONTROL").write_text(
        prefix + f"control_checksum={crc32c(prefix.encode()):08x}\n",
        encoding="utf-8")


def snapshot_tree(root):
    result = {}
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        key = str(path.relative_to(root))
        if path.is_file():
            result[key] = (
                path.read_bytes(), stat.S_IMODE(metadata.st_mode),
                metadata.st_mtime_ns)
        else:
            result[key] = (None, stat.S_IMODE(metadata.st_mode),
                           metadata.st_mtime_ns)
    return result


def run_verify(cluster, launch, *extra):
    return subprocess.run(
        [DBMS_MAIN, "-D", str(cluster), "--verify-data-checksums", *extra],
        cwd=launch, capture_output=True, text=True, timeout=10, check=False)


def flip_byte(path, offset):
    data = bytearray(path.read_bytes())
    data[offset] ^= 1
    path.write_bytes(data)


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")

    with tempfile.TemporaryDirectory(prefix="dbms-checksums-") as temporary:
        base = Path(temporary)
        launch = base / "launch"
        cluster = base / "cluster"
        external = base / "tablespace"
        launch.mkdir()
        cluster.mkdir()
        external.mkdir()
        write_control(cluster)

        database = cluster / "app"
        database.mkdir()
        main_heap = database / "accounts.dt"
        init_heap = database / "scratch.dt.init"
        write_heap(main_heap, 2)
        write_heap(init_heap, 1)
        key = bytes.fromhex("11" * 32)
        cluster.joinpath("tde.key").write_text("11" * 32 + "\n",
                                               encoding="ascii")
        cluster.joinpath("dbms.conf").write_text(
            "tde_keyring=tde.key\n", encoding="utf-8")
        encrypted_heap = database / "secrets.dt"
        write_encrypted_heap(encrypted_heap, key)

        marker_directory = database / "pg_tblspc"
        marker_directory.mkdir()
        marker_directory.joinpath("fast.path").write_text(
            str(external) + "\n", encoding="utf-8")
        external_heap = external / "app" / "ledger.toast.dt"
        write_heap(external_heap, 2)

        before_cluster = snapshot_tree(cluster)
        before_external = snapshot_tree(external)
        clean = run_verify(cluster, launch)
        assert clean.returncode == 0, clean
        assert "heap checksum verification passed" in clean.stdout, clean
        assert "files=4" in clean.stdout and "blocks=7" in clean.stdout, clean
        assert snapshot_tree(cluster) == before_cluster
        assert snapshot_tree(external) == before_external
        assert list(launch.iterdir()) == []

        original_main = main_heap.read_bytes()
        flip_byte(main_heap, PAGE_SIZE + 100)
        corrupt_page = run_verify(cluster, launch)
        assert corrupt_page.returncode == 1, corrupt_page
        assert "app/accounts.dt: block 1" in corrupt_page.stderr, corrupt_page
        assert "checksum or layout" in corrupt_page.stderr, corrupt_page
        main_heap.write_bytes(original_main)

        original_external = external_heap.read_bytes()
        flip_byte(external_heap, PAGE_SIZE + 200)
        corrupt_tablespace = run_verify(cluster, launch)
        assert corrupt_tablespace.returncode == 1, corrupt_tablespace
        assert str(external_heap) in corrupt_tablespace.stderr, corrupt_tablespace
        assert "block 1" in corrupt_tablespace.stderr, corrupt_tablespace
        external_heap.write_bytes(original_external)

        encrypted_contents = encrypted_heap.read_bytes()
        flip_byte(encrypted_heap, PAGE_SIZE + 300)
        bad_envelope = run_verify(cluster, launch)
        assert bad_envelope.returncode == 1, bad_envelope
        assert "app/secrets.dt: block 1" in bad_envelope.stderr, bad_envelope
        assert "envelope authentication failed" in bad_envelope.stderr, bad_envelope
        encrypted_heap.write_bytes(encrypted_contents)

        keyring = cluster / "tde.key"
        keyring.write_text("22" * 32 + "\n", encoding="ascii")
        wrong_key = run_verify(cluster, launch)
        assert wrong_key.returncode == 1, wrong_key
        assert "envelope authentication failed" in wrong_key.stderr, wrong_key
        keyring.write_text("11" * 32 + "\n", encoding="ascii")

        flip_byte(main_heap, 20)
        corrupt_header = run_verify(cluster, launch)
        assert corrupt_header.returncode == 1, corrupt_header
        assert "app/accounts.dt: block 0" in corrupt_header.stderr, corrupt_header
        main_heap.write_bytes(original_main)

        main_heap.write_bytes(original_main[:-1])
        truncated = run_verify(cluster, launch)
        assert truncated.returncode == 1, truncated
        assert "truncated or misaligned" in truncated.stderr, truncated
        main_heap.write_bytes(original_main)

        sidecar = Path(str(main_heap) + ".tde")
        sidecar.write_bytes(b"partial")
        bad_sidecar = run_verify(cluster, launch)
        assert bad_sidecar.returncode == 1, bad_sidecar
        assert "TDE sidecar" in bad_sidecar.stderr, bad_sidecar
        sidecar.unlink()

        pending = Path(str(main_heap) + ".extent_pending")
        pending.write_text("incomplete", encoding="utf-8")
        pending_result = run_verify(cluster, launch)
        assert pending_result.returncode == 1, pending_result
        assert "requires recovery" in pending_result.stderr, pending_result
        pending.unlink()

        conflict = run_verify(cluster, launch, "--check-data-directory")
        assert conflict.returncode == 1, conflict
        assert "conflicting data-directory utility" in conflict.stderr, conflict

        final = run_verify(cluster, launch)
        assert final.returncode == 0, final

    print("[CHECKSUM VERIFY] offline heap scan and corruption diagnostics OK")


if __name__ == "__main__":
    main()
