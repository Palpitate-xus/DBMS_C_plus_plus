# Terminal WAL belongs to the transaction's physical owner

The unchanged `begin_transaction_database_drop_race_test` failed at line 56:
an exclusive transaction holder renamed its owned directory and replaced its
public name with a symlink to that same directory. Its COMMIT returned I/O
failure (`wal=0`), while the waiting BEGIN correctly had to reject the name.
`getWAL()` applied public-name validity before retaining the holder's physical
owner. BEGIN did not necessarily warm any WAL manager either.

The initial cache-only candidate passed an explicitly warmed test, but still
failed the unchanged original. That failure is retained; it was not completion.

Real physical BEGIN and deferred-to-physical promotion now pin a shared WAL
owner under the database ownership lock. The pin is backend/transaction local,
not a public-name cache entry. Terminal I/O may use that existing owner only
while the same open physical directory remains current. A different regular
directory or unrelated symlink cannot redirect that xid into another WAL.
New BEGIN still performs its original post-lock namespace check. Memory-only
deferred BEGIN does not acquire a physical WAL owner or perform new WAL I/O.

Controlled cache closure under this backend's exclusive ownership explicitly
releases the pin before DDL/snapshot replacement, and the replacement getter
repins the real new generation. A background pruner cannot release another
backend's owner. Commit, rollback, prepare and prepared-completion cleanup
release their transaction-local ownership after terminal work.

## Exact scoped proof

Artifacts are under `/tmp/dbms-owned-wal-publication.JKfgarcp/`.

- Original unchanged failure: the identity donor's
  `/tmp/dbms-heap-wal-identity.dcGDltHu/completion-baseline/begin_transaction_database_drop_race_test.log`, exit 134.
- New explicit-owner baseline: `baseline/test.log`, handle 19312, exit 134.
  It used the exact immutable `2e0545e0` 58-source/header/flag basis.
- Cache-only V1: `candidate.log`, handle 15937, exit 1. Seven adjacent controls
  passed but the unchanged original still failed with `wal=0`.
- Public TransactionContext layout changed. All 58 units and fresh stubs were
  rebuilt, not mixed with the old context ABI. `build-v2.log`, handle 1235,
  exit 0; all before-final source/header audits passed. Binary SHA-256:
  `3174cd1db9308e1ee1e54569924b159c640d234e3832339d703ca38a742f0f96`.
- Final `native-v3.log`, handle 76254, exit 0: 17/17 native executables passed.
  They include the unchanged original, same-inode/unrelated-symlink/regular
  replacement owner controls, deferred ownership and snapshot preservation,
  prepared transactions, shared heap/B+Tree owner/generation/rows/index/crash,
  missing-live-file failure, physical backup/restore, database rename, six SSI
  controls and the external xid/snapshot control.
- The intermediate V2 test run retained one fixture compile error: it called
  the private `closeAllWALs()` method. That invalid added call was removed,
  with every public API assertion retained, then the entire 17-test group was
  rerun. It is not counted as a database red or a passing group.
- `sanitized.log`, handle 95965, exit 0: four scoped executables passed with
  fresh ASan/UBSan TableManage, stubs and drivers (owner, unchanged original,
  deferred and prepared controls). The other 56 native dependency objects
  are matching, uninstrumented `-O0`; leak detection was disabled. Not a full
  storage sanitizer claim.
- Source/header/flag and object/test before-final receipts are retained in
  `build-v2/`, `native-v3/` and `sanitized/`.

No original assertion or timeout is weakened. This closes the terminal-owner
defect, not all cross-process storage ownership, cache-generation or recovery
boundaries. The separate clean retired tablespace-cache backup defect remains
independently implemented/verified. These are native API tests, not SQL
PostgreSQL differential evidence. ROOT must rebuild its combined public ABI.
