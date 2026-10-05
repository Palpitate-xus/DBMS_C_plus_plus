# WAL-06: checksum coverage remains partial

## Finding and fix

Heap blocks already carry a Fletcher-16 checksum and physical page identity,
and `--verify-data-checksums` scans heap, TOAST, init-fork, tablespace, and TDE
envelope files. B+ tree index pages, however, had no checksum. Their online
reader only checked node shape and key ordering, so a bit flip in a key could
preserve a valid-looking tree while silently changing a lookup result.

New B+ tree files use format `0xC552`: CRC32C in the final four bytes of each
4 KiB page covers both the page bytes and the physical block number (encoded
little-endian), including block zero. The buffer pool validates pages when
they are first loaded, before `BPTree` parses them. Mutations recompute the
page-bound checksum for the header, new pages, and rewritten nodes. This also
rejects swapping two individually intact pages, which the previous
content-only checksum could not detect.

Existing `0xC551` files remain readable with their original content-only
checksum; they are not upgraded in place, so REINDEX is needed for page-swap
detection. Older zero-marker files remain readable without being falsely
treated as checksummed. REINDEX-built replacements use the new `0xC552`
format.

## Verification

- `bash scripts/build.sh` — production build passed.
- `scripts/build_one_test.sh bptree_topology_corruption_test` — passed:
  key-byte corruption and swapped sibling pages are rejected; a synthesized
  `0xC551` file remains readable.
- `scripts/build_one_test.sh bptree_concurrency_test` — passed with concurrent
  readers and a writer.
- `bash tests/crash_matrix_test.sh` — `PASS=12 FAIL=0`.
- `git diff --check` — passed before the source/test commit.

The full registered suite and a PostgreSQL 18.6 runtime differential were not
run for this change.

## Remaining gaps

WAL-06 remains partial. Existing `0xC551` B+ tree files are not page-bound
until rewritten; zero-marker legacy trees remain unchecked. Hash, Bloom, GIN,
GiST, SP-GiST, and BRIN files still lack this format; catalog/schema, FSM/VM,
and other metadata do not have unified checksums. The offline verifier still
scans heap-family files only, and there is no `pg_checksums`-style
enable/disable/rewrite/progress workflow or online whole-cluster verification.
The custom checksums are not PostgreSQL page checksums.

Source/test commits: `93addab7`, `2685454b` (not pushed).
