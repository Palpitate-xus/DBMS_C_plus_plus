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
BTREE_PAGE_SIZE = 4096
BTREE_CHECKSUM_OFFSET = BTREE_PAGE_SIZE - 4
BTREE_CONTENT_CHECKSUM_FORMAT = 0xC551
BTREE_PAGE_BOUND_CHECKSUM_FORMAT = 0xC552


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


def btree_page_checksum(page, page_id=None):
    checksum_input = bytearray(page)
    checksum_input[BTREE_CHECKSUM_OFFSET:] = bytes(4)
    if page_id is not None:
        checksum_input.extend(struct.pack("<I", page_id))
    value = crc32c(checksum_input)
    return value if value else 0xffffffff


def btree_page(key, value, next_leaf=0):
    page = bytearray(BTREE_PAGE_SIZE)
    page[0] = 1
    struct.pack_into("=H", page, 1, 1)
    encoded_key = key.encode("ascii")
    assert len(encoded_key) <= 20
    page[3:23] = encoded_key.ljust(20, b"\0")
    struct.pack_into("=qI", page, 23, value, next_leaf)
    return page


def write_btree(path, format_marker=BTREE_PAGE_BOUND_CHECKSUM_FORMAT):
    """Write a small valid four-page B+ tree fixture."""
    pages = [bytearray(BTREE_PAGE_SIZE) for _ in range(4)]
    struct.pack_into("=IIHH", pages[0], 0, 3, 4, 2, format_marker)
    pages[1] = btree_page("a", 1, 2)
    pages[2] = btree_page("z", 2, 0)
    pages[3][0] = 0
    struct.pack_into("=H", pages[3], 1, 1)
    pages[3][3:23] = b"z".ljust(20, b"\0")
    struct.pack_into("=II", pages[3], 23, 1, 2)

    if format_marker in (BTREE_CONTENT_CHECKSUM_FORMAT,
                         BTREE_PAGE_BOUND_CHECKSUM_FORMAT):
        for page_id, page in enumerate(pages):
            checksum_page_id = (
                page_id if format_marker == BTREE_PAGE_BOUND_CHECKSUM_FORMAT
                else None)
            struct.pack_into(
                "=I", page, BTREE_CHECKSUM_OFFSET,
                btree_page_checksum(page, checksum_page_id))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(pages))


def write_hash_index(path, version=2):
    assert version in (1, 2)
    contents = bytearray(struct.pack("=IIQ", 0x48494458, version, 1))
    key = b"key"
    contents.extend(struct.pack("=Q", len(key)))
    contents.extend(key)
    contents.extend(struct.pack("=Qq", 1, 42))
    if version == 2:
        contents.extend(struct.pack("=I", crc32c(contents)))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)


def write_bloom_index(path, version=2):
    assert version in (1, 2)
    magic = 0x324D4C42 if version == 2 else 0x314D4C42
    contents = bytearray(struct.pack("=IIII", magic, 64, 7, 1))
    key = b"key"
    contents.extend(struct.pack("=I", len(key)))
    contents.extend(key)
    contents.extend(struct.pack("=IQ", 1, 42))
    if version == 2:
        contents.extend(struct.pack("=I", crc32c(contents)))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)


def write_gin_index(path, version=2):
    if version == 1:
        contents = b"hello 42\n"
    else:
        assert version == 2
        contents = bytearray(b"\x00DBMSG\x002" + struct.pack(
            "<IQ", 2, 2))
        for key, rids in ((b"hello world", (42, 43)), (b"other", (44,))):
            contents.extend(struct.pack("<Q", len(key)))
            contents.extend(key)
            contents.extend(struct.pack("<Q", len(rids)))
            for rid in rids:
                contents.extend(struct.pack("<Q", rid))
        contents.extend(struct.pack("<I", crc32c(contents)))
        contents = bytes(contents)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)


def heap_page(page_id, layout_version=5):
    page = bytearray(PAGE_SIZE)
    page_id_bound = layout_version == 5
    page_size_version = ((PAGE_SIZE // 512) << 8) | layout_version
    struct.pack_into(
        "=QHHHHHHI", page, 0,
        0, 0, 0x0008 if page_id_bound else 0,
        24, PAGE_SIZE - 4, PAGE_SIZE - 4,
        page_size_version, page_id if page_id_bound else 0)
    struct.pack_into("=H", page, 8, fletcher16(page))
    return bytes(page)


def write_heap(path, blocks=2, layout_version=5):
    assert blocks >= 1
    header_without_checksum = struct.pack(
        "=IIIII", DATA_FILE_MAGIC, blocks, 0, 64,
        DATA_FILE_FORMAT_VERSION)
    header = header_without_checksum + struct.pack(
        "=I", fnv1a32(header_without_checksum))
    header_page = header + bytes(PAGE_SIZE - len(header))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(header_page + b"".join(
        heap_page(page_id, layout_version)
        for page_id in range(1, blocks)))


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
        btree_index = database / "accounts.idx"
        write_btree(btree_index)
        composite_btree_index = database / "accounts.idx_comp"
        write_btree(composite_btree_index)
        legacy_btree_index = database / "legacy.idx"
        write_btree(legacy_btree_index, BTREE_CONTENT_CHECKSUM_FORMAT)
        unchecked_btree_index = database / "old.idx"
        write_btree(unchecked_btree_index, 0)
        hash_index = database / "accounts_name.hidx"
        write_hash_index(hash_index)
        bloom_index = database / "accounts_tags.bidx"
        write_bloom_index(bloom_index)
        gin_index = database / "accounts_doc.gin"
        write_gin_index(gin_index)
        legacy_heap = database / "legacy.dt"
        write_heap(legacy_heap, 2, layout_version=4)
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
        external_index = external / "app" / "ledger.idx"
        write_btree(external_index)
        external_legacy_hash = external / "app" / "ledger_id.hidx"
        write_hash_index(external_legacy_hash, 1)
        external_legacy_bloom = external / "app" / "ledger_tags.bidx"
        write_bloom_index(external_legacy_bloom, 1)
        external_legacy_gin = external / "app" / "ledger_doc.gin"
        write_gin_index(external_legacy_gin, 1)

        before_cluster = snapshot_tree(cluster)
        before_external = snapshot_tree(external)
        clean = run_verify(cluster, launch)
        assert clean.returncode == 0, clean
        assert "heap checksum verification passed" in clean.stdout, clean
        assert "B+ tree index scan completed" in clean.stdout, clean
        assert "hash index scan completed" in clean.stdout, clean
        assert "Bloom index scan completed" in clean.stdout, clean
        assert "GIN index scan completed" in clean.stdout, clean
        assert "files=5" in clean.stdout and "blocks=9" in clean.stdout, clean
        assert "identity-bound-blocks=3" in clean.stdout, clean
        assert "legacy-identity-unbound-blocks=1" in clean.stdout, clean
        assert "index-files=5" in clean.stdout, clean
        assert "index-pages=20" in clean.stdout, clean
        assert "page-bound-index-pages=12" in clean.stdout, clean
        assert "content-only-index-pages=4" in clean.stdout, clean
        assert "unchecked-index-pages=4" in clean.stdout, clean
        assert "hash-index-files=2" in clean.stdout, clean
        assert "checksummed-hash-index-files=1" in clean.stdout, clean
        assert "unchecked-legacy-hash-index-files=1" in clean.stdout, clean
        assert "bloom-index-files=2" in clean.stdout, clean
        assert "checksummed-bloom-index-files=1" in clean.stdout, clean
        assert "unchecked-legacy-bloom-index-files=1" in clean.stdout, clean
        assert "gin-index-files=2" in clean.stdout, clean
        assert "checksummed-gin-index-files=1" in clean.stdout, clean
        assert "unchecked-legacy-gin-index-files=1" in clean.stdout, clean
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

        original_index = btree_index.read_bytes()
        flip_byte(btree_index, BTREE_PAGE_SIZE + 3)
        corrupt_index = run_verify(cluster, launch)
        assert corrupt_index.returncode == 1, corrupt_index
        assert "app/accounts.idx: block 1" in corrupt_index.stderr, corrupt_index
        assert "B+ tree page checksum mismatch" in corrupt_index.stderr, corrupt_index
        btree_index.write_bytes(original_index)

        swapped_index = bytearray(original_index)
        first_leaf = swapped_index[BTREE_PAGE_SIZE:2 * BTREE_PAGE_SIZE]
        second_leaf = swapped_index[2 * BTREE_PAGE_SIZE:3 * BTREE_PAGE_SIZE]
        swapped_index[BTREE_PAGE_SIZE:2 * BTREE_PAGE_SIZE] = second_leaf
        swapped_index[2 * BTREE_PAGE_SIZE:3 * BTREE_PAGE_SIZE] = first_leaf
        btree_index.write_bytes(swapped_index)
        swapped_result = run_verify(cluster, launch)
        assert swapped_result.returncode == 1, swapped_result
        assert "app/accounts.idx: block " in swapped_result.stderr, swapped_result
        assert "B+ tree page checksum mismatch" in swapped_result.stderr, swapped_result
        btree_index.write_bytes(original_index)

        original_hash = hash_index.read_bytes()
        flip_byte(hash_index, len(original_hash) - 5)
        corrupt_hash = run_verify(cluster, launch)
        assert corrupt_hash.returncode == 1, corrupt_hash
        assert "app/accounts_name.hidx" in corrupt_hash.stderr, corrupt_hash
        assert "hash index checksum mismatch" in corrupt_hash.stderr, corrupt_hash
        hash_index.write_bytes(original_hash)

        original_bloom = bloom_index.read_bytes()
        flip_byte(bloom_index, len(original_bloom) - 5)
        corrupt_bloom = run_verify(cluster, launch)
        assert corrupt_bloom.returncode == 1, corrupt_bloom
        assert "app/accounts_tags.bidx" in corrupt_bloom.stderr, corrupt_bloom
        assert "Bloom index checksum mismatch" in corrupt_bloom.stderr, corrupt_bloom
        bloom_index.write_bytes(original_bloom)

        original_gin = gin_index.read_bytes()
        flip_byte(gin_index, len(original_gin) - 5)
        corrupt_gin = run_verify(cluster, launch)
        assert corrupt_gin.returncode == 1, corrupt_gin
        assert "app/accounts_doc.gin" in corrupt_gin.stderr, corrupt_gin
        assert "GIN index checksum mismatch" in corrupt_gin.stderr, corrupt_gin
        gin_index.write_bytes(original_gin)

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

    print("[CHECKSUM VERIFY] offline heap/index scan and corruption diagnostics OK")


if __name__ == "__main__":
    main()
