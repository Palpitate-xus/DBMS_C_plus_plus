# WAL-06: checksum coverage remains partial

## Finding and fix

Heap blocks already carry a Fletcher-16 checksum and physical page identity,
and `--verify-data-checksums` scans heap, TOAST, init-fork, tablespace, and TDE
envelope files. B+ tree index pages, however, had no checksum. Their online
reader only checked node shape and key ordering, so a bit flip in a key could
preserve a valid-looking tree while silently changing a lookup result.

New B+ tree files now mark their page format and store CRC32C in the final four
bytes of each 4 KiB page. The buffer pool validates a page when it is first
loaded, before `BPTree` parses it. Page zero is checked explicitly while
reading the file header; mutations recompute checksums for the header, new
pages, and rewritten nodes. Existing legacy files with the old zero marker
remain readable and are not falsely treated as checksummed; a REINDEX-built
replacement uses the checksummed format.

## Verification

- `bash scripts/build.sh` — production build passed.
- `bptree_topology_corruption_test` — passed after direct compilation; a
  key-byte flip in a structurally valid leaf was rejected, while legacy
  topology fixtures remained readable.
- `bptree_concurrency_test` — passed after direct compilation with concurrent
  readers and a writer.
- `bash tests/crash_matrix_test.sh` — `PASS=12 FAIL=0` after the index-page
  format change.
- `git diff --check` — passed before the source/test commit.

The full registered suite and a PostgreSQL 18.6 runtime differential were not
run for this change.

## Remaining gaps

WAL-06 remains partial. Existing legacy B+ tree files are not checksummed until
rewritten. Hash, Bloom, GIN, GiST, SP-GiST, and BRIN files still lack this
format; catalog/schema, FSM/VM, and other metadata do not have unified
checksums. The offline verifier still scans heap-family files only, and there
is no `pg_checksums`-style enable/disable/rewrite/progress workflow or online
whole-cluster verification. The custom checksums are not PostgreSQL page
checksums.

Source/test commit: `93addab7` (not pushed).
