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

Bloom sidecars now use the `BLM2` magic for new writes and append CRC32C over
the complete little-endian payload. Loading validates the trailer before
parsing the serialized entry count; old `BLM1` files remain readable and are
reported as unchecked. Bloom publication also uses the shared atomic writer
instead of a fixed `.tmp` name and non-synced stream/rename sequence, so the
sidecar file and its parent directory receive the same durability treatment
as the other index sidecars.

GIN sidecars now use a binary V2 format with length-delimited keys and postings
plus CRC32C over the complete payload. This also fixes a query correctness bug:
the old whitespace-delimited V1 format could not represent JSON/array keys
containing spaces (for example, `"hello world"`). V1 text sidecars remain
searchable where representable and are reported as unchecked.

BRIN sidecars now use a portable little-endian V2 encoding and CRC32C trailer.
The online reader validates the checksum before parsing range summaries; V1
native-endian files remain readable and are explicitly unchecked.

The offline `--verify-data-checksums` utility discovers B+ tree files named
`.idx`/`.idx_*`, hash sidecars named `.hidx`, Bloom sidecars named `.bidx`, and
GIN/BRIN sidecars named `.gin`/`.brin` below the cluster and resolved tablespace roots. It opens them with read-only,
no-follow descriptors; checks file/header bounds and checksums; and reports
page-bound, content-only, unchecked B+ tree page counts, and checked/unchecked
hash, Bloom, GIN, and BRIN file counts separately. A zero-marker B+ tree, V1 hash
file, `BLM1` Bloom file, V1 GIN text file, or V1 BRIN file is scanned but explicitly
reported as unchecked. GIN V2 offline validation checks the signature, version,
entry-count bound and CRC32C, not posting-list topology; BRIN V2 checks the
header, range-count bound and CRC32C, not range-summary semantics. Encrypted
B+ tree sidecars use the existing `PageCrypto` read path; this addition is not
a TDE security review.

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
- Direct compile/run of `tests/bloom_index_checksum_test.cpp` with
  `src/access/BloomIndex.cpp` — passed BLM2 write/reload, RID bit-flip
  rejection, BLM1 compatibility, and directory-fsync failure/retry dirty-state
  checks.
- `scripts/build_one_test.sh bloom_index_test` — passed existing Bloom unit,
  growth, malformed-file and StorageEngine lifecycle/DML regression coverage.
- `scripts/build_one_test.sh gin_brin_index_test` — passed GIN whitespace-key
  lookup, GIN/BRIN V2 checksum-corruption rejection, V1 compatibility and BRIN
  range lookup coverage.
- `python3 tests/checksum_verification_e2e_test.py` — passed read-only
  snapshots, in-cluster/composite/tablespace B+ tree discovery, page-bit and
  whole-page-swap rejection, `.hidx`, `.bidx`, `.gin`, and `.brin` corruption
  rejection, V1 compatibility and unchecked legacy counts.
- `git diff --check` — passed.

The full registered suite and a PostgreSQL 18.6 runtime differential were not
run. The encrypted-index sidecar path was not independently exercised.

## Remaining gaps

WAL-06 remains partial. Existing `0xC551` B+ tree files are not page-bound
until rewritten; zero-marker legacy trees, V1 hash files, `BLM1` Bloom files,
V1 GIN text files, and V1 BRIN files remain unchecked. GiST and SP-GiST files
still lack checksums; catalog/schema, FSM/VM, and other metadata do not have
unified checksums. The offline verifier checks B+ tree page checksums and V2
Hash/Bloom/GIN/BRIN sidecar CRCs, but does not parse tree topology, GIN postings,
or BRIN range semantics.
There is no `pg_checksums`-style
enable/disable/rewrite/progress workflow or online whole-cluster verification.
The custom checksums are not PostgreSQL page checksums.

Source/test commits: `93addab7`, `2685454b`, `3b595189`, `8e92ebd9`,
`b32ab185`, `f76809b6`, `16b72b0a` (not pushed).
