# WAL-02: insertion and segment handling remain partial

## Findings

The local WAL implementation has several working building blocks: records use
CRC32C and 8-byte alignment; `appendBytes()` splits a record across 16 MiB
segment files; flush serializes waiters and fsyncs the segment range covering
the requested LSN; `switchWal()` pads the current segment; archive and
truncation paths retain the live segment and reject unarchived removal.
Checkpoint page-image logging is also present.

This is not PostgreSQL's WAL page/record format. A record crossing a segment
is written as contiguous bytes across files, without PostgreSQL WAL page
headers or continuation records. WAL record compression is absent, and the
page LSN/FPI behavior remains a simplified implementation rather than the full
PostgreSQL insertion and concurrent-writer protocol. These are format and
recovery-compatibility gaps, not a newly demonstrated corruption finding in
this audit.

## Evidence and verification

Focused tests passed:

- `wal_basic_test` — record CRC/chain, heap/index records, flush, and two
  `WALManager` instances appending to the same stream.
- `redo_crash_recovery_test` — committed and uncommitted transaction recovery.
- `wal_full_page_write_test` — checkpoint boundary and page image.
- `wal_truncate_test` — archive-gated removal and retained-prefix discovery.
- `wal_timeline_archive_test` — timeline switching, archive workflow, and
  rejected malformed/locked switches.

No code changed in this audit. The full registered suite and PostgreSQL 18.6
runtime differential were not run.

## Status

WAL-02 remains partial. PostgreSQL-compatible WAL page headers and continuation
records, record compression, page LSN/FPI details, segment recycling rules,
and broader concurrent-writer/fault-injection coverage remain open.
