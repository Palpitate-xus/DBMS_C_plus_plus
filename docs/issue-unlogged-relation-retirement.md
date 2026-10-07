# Retire a generation even after SET UNLOGGED

The unchanged `alter_table_only_test` exposed a recovery regression after its
physical-backup assertion was separately repaired. It creates a LOGGED table,
inserts row 9, changes it to UNLOGGED, then drops it. Its old LOGGED heap images
still contain physical relation identity 2. DROP returned success without
retirement records because it tested the table's current `isUnlogged` flag.
Startup then correctly refused the missing, apparently live identity.

DROP now publishes the same real intent/completion pair for every nonzero
physical identity, regardless of current persistence. Completion still requires
durable unlink/catalog publication and the actual owning transaction's commit.
Neither intent alone nor an uncommitted completion authorizes skipping images.
CREATE and heap/index writes retain their UNLOGGED suppression. The new records
contain lifecycle identity/name metadata, never UNLOGGED row contents.

The permanent native control keeps six scenarios in a single warmed parent:
LOGGED-to-UNLOGGED deletion, name reuse with a fresh identity, committed reuse,
aborted snapshot restoration of the original LOGGED row 9, initially UNLOGGED
deletion, and an incomplete intent/catalog-publication fault that fails closed.
Crash paths use actual fork/exec writers and `_exit`, with row 101 left
uncommitted where applicable; it must never become row 100's replacement.

## Evidence and limits

Artifacts: `/tmp/dbms-unlogged-retirement.2ZTu0yg8/`.

- `baseline.log`, handle 31243, exit 1, and
  `baseline/unlogged_relation_retirement_test.log`: a successful public DROP
  had `intent=0 completion=0`, assertion failure. The exact immutable
  `18af7579` 58-unit completion donor's production sources/headers/flags and
  object fingerprints were checked; fresh stubs and the new driver were used.
- The original later startup failure is retained in
  `/tmp/dbms-clean-retired-cache.zcS6anO4/native/alter_table_only_test.log`.
  It is not replaced by the new metadata-count assertion.
- `build.log`, handle 58560, exit 0: fresh TableManage, the other 57 exact
  donor sources/objects, unchanged public headers and identical flags. This
  is matched incremental `-O0`, not a new production all-`-O2` claim.
- The first candidate normal 19-unit group (47898) exited 1: all 18 original
  adjacent controls passed; only the added initially UNLOGGED fixture failed.
  The corresponding scoped sanitizer group (99649) exited 1 for the same
  fixture, while both original UNLOGGED and retirement-completion controls
  passed. Both failed logs are retained, not counted as a passing group.
- That added fixture incorrectly required total WAL length zero after an
  implicitly transactional INSERT. Its actual record was RM_XACT/COMMIT,
  not heap/index values. The corrected fixture retains total zero after
  CREATE, and asserts that INSERT/DROP contain exactly one COMMIT and the
  intent/completion pair, with no HEAP/INDEX or relation-birth record. The
  original independent CREATE/full-text-index zero-WAL assertion is unchanged.

- Final corrected `native-final.log`, handle 67907, exit 0: all 19 executables
  passed, including six new scenarios, the unchanged original UNLOGGED
  zero-WAL/startup controls, nine-phase retirement completion faults, original
  generation/crash/identity guard/codec, FK rename, shared heap/B+Tree/TOAST,
  physical backup/restore, checkpoint, six SSI and xid process/snapshot controls.
- Final corrected `sanitized-final.log`, 29641, exit 0: the new six-scenario
  control, original UNLOGGED control and retirement completion control passed.
  The unchanged freshly instrumented TableManage object from 99649 was reused
  only after matching production source/header final receipts and sanitizer
  flags; stubs/drivers were rebuilt. Other 56 native dependencies are matched
  uninstrumented `-O0`; leak detection was disabled. Not full-engine ASan.
- Before/final source/header, object and test audits passed in `native-final/`
  and `sanitized-final/`. Matching binary SHA-256:
  `bf34e9590fdc7a1524d65bc0c7cf67587a64ac93c8512735f4c8b12d47c8d3d0`.

The independent clean-retired-cache fix is required to pass the entire original
tablespace/backup test; this item does not claim that cache defect as repaired.
These are native API/WAL contracts, not PostgreSQL WAL-format differential
tests. Broader WAL recycling, legacy identities, physical rewrite epochs and
complete DROP crash atomicity remain outside this scoped repair.
