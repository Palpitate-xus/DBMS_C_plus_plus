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

Hash sidecars now use format V2 for new writes: CRC32C covers the complete
serialized `.hidx` payload and a fixed-width checksum trailer is appended.
Loading verifies the checksum before trusting entry counts, bounds parsing to
the checksummed payload, and rejects unknown versions. Existing V1 sidecars
remain readable but have no checksum and are reported as unchecked.

The offline `--verify-data-checksums` utility discovers B+ tree files named
`.idx`/`.idx_*` and hash sidecars named `.hidx` below the cluster and resolved
tablespace roots. It opens them with read-only, no-follow descriptors; checks
file/header bounds and checksums; and reports page-bound, content-only,
unchecked B+ tree page counts, and checksummed/unchecked hash-file counts
separately. A zero-marker B+ tree or V1 hash file is scanned but explicitly
reported as unchecked. Encrypted B+ tree sidecars use the existing
`PageCrypto` read path; this addition is not a TDE security review.

## Verification

- `bash scripts/build.sh` — production build passed.
- `scripts/build_one_test.sh bptree_topology_corruption_test` — passed:
  key-byte corruption and swapped sibling pages are rejected; a synthesized
  `0xC551` file remains readable.
- `scripts/build_one_test.sh bptree_concurrency_test` — passed with concurrent
  readers and a writer.
- `bash tests/crash_matrix_test.sh` — `PASS=12 FAIL=0`.
- Direct compile/run of `tests/hash_index_checksum_test.cpp` with
  `src/access/HashIndex.cpp` — passed V2 write/reload, structurally valid RID
  bit-flip rejection and V1 compatibility.
- `python3 tests/checksum_verification_e2e_test.py` — passed read-only
  snapshots, in-cluster/composite/tablespace B+ tree discovery, page-bit and
  whole-page-swap rejection, `.hidx` corruption rejection, V1 compatibility
  and unchecked legacy counts.
- `git diff --check` — passed.

The full registered suite and a PostgreSQL 18.6 runtime differential were not
run. The encrypted-index sidecar path was not independently exercised.

## Remaining gaps

WAL-06 remains partial. Existing `0xC551` B+ tree files are not page-bound
until rewritten; zero-marker legacy trees and V1 hash files remain unchecked.
Bloom, GIN, GiST, SP-GiST, and BRIN files still lack checksums; catalog/schema,
FSM/VM, and other metadata do not have unified checksums. The offline verifier
checks B+ tree page checksums and V2 hash sidecar CRCs, but does not parse tree
topology or validate the other index AMs. There is no `pg_checksums`-style
enable/disable/rewrite/progress workflow or online whole-cluster verification.
The custom checksums are not PostgreSQL page checksums.

Source/test commits: `93addab7`, `2685454b`, `3b595189`, `8e92ebd9` (not pushed).
