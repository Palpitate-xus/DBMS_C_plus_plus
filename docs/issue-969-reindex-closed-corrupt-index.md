# Issue 969: REINDEX recovery from a closed corrupt cache

Date: 2026-10-03
Source/test commit: `06c483a7`
Status: locally committed; not pushed. `IDX-03` and `IDX-14` remain partial.

The existing corruption regression deliberately damages primary and
secondary B-tree files, verifies indexed reads fail closed, then attempts an
UPDATE. The heap row remained unchanged, the transaction ended, and table
locks were released; the engine correctly returned `IO_ERROR` and logged
that index compensation was incomplete because the pre-existing trees were
unusable. However, the explicit recovery path also failed: `REINDEX` tried to
flush every cached tree even when a failed open had left that cache closed.
There can be no resident dirty pages in that closed cache, so this prevented a
valid rebuild from the healthy heap.

`REINDEX` now flushes an existing cached tree only while it is open. A closed
generation is left on disk as the rollback generation and can be replaced by
the fully built tree through the existing durable swap protocol. The
corruption regression now verifies that REINDEX restores both primary and
secondary indexed reads and that a subsequent UPDATE succeeds.

Verification:

- `bash scripts/build.sh`: passed.
- `index_scan_corruption_error_test`: passed; the expected `rollback
  incomplete` diagnostic still appears for the initially corrupt trees.
- `reindex_atomicity_test`: passed, including failed-build preservation,
  successful swap, transaction rollback, and internal DDL transaction paths.
- `python3 tests/index_corruption_protocol_e2e_test.py`: passed.
- The full test script was not rerun after this focused change. The previous
  issue-968 full run had one server-start connection abort; its isolated test
  passed 11 times, but that does not count as a full-suite pass.
- `git diff --check`: passed before commit.

This makes explicit REINDEX a working repair path for closed/corrupt B-tree
caches; it does not make DML silently succeed on corrupted indexes, recover
corrupt heap/WAL data, or complete the broader B-tree persistence and crash
guarantees. The index checklist families remain partial.
