# WAL-07: no-replace schema marker publication

## Finding and fix

`CREATE SCHEMA` publishes its marker with a same-directory hard link so a
concurrent marker cannot be overwritten. The old ordering removed the
temporary hard link before syncing the parent directory. If that directory
`fsync` failed, the operation returned `IO_ERROR` after the schema marker had
already become visible, so a retry could report that the failed schema
already existed.

No-replace publication now syncs the directory while both hard links still
exist. If this sync fails, it removes the destination only if it still names
the temporary inode, removes the temporary name, and attempts to sync the
rollback. After the first sync succeeds, the destination is durable; removing
the hidden temporary alias is then cleanup and cannot turn a durable schema
creation into a reported failure.

`CREATE DATABASE` had a related missing parent-directory barrier. It synced
the initial files and entries inside the new database directory but did not
sync the cluster directory containing that database directory. It now syncs
the parent after all initial files are durable; a failed parent sync removes
the just-created database tree and attempts to persist that rollback before
returning `IO_ERROR`.

`DROP DATABASE` likewise removed the database tree without syncing the
cluster directory. It now syncs that parent before reporting success. If this
final sync fails, the tree has already been removed and the call returns
`IO_ERROR`; as with other filesystem durability errors, the caller must treat
the result as indeterminate and re-check database existence before retrying.

`ALTER DATABASE ... RENAME` moved the database directory (and optional WAL
archive sibling) without syncing the cluster directory. It now syncs the
shared parent after both moves and attempts to move both names back if that
sync fails, then syncs the rollback.

`DROP SCHEMA` previously unlinked its marker without syncing the containing
database directory. It now uses durable removal; if unlink succeeded but the
directory sync fails, it attempts a no-replace restoration of the empty marker
and reports `IO_ERROR` so a failed statement does not silently leave the
namespace removed.

`RENAME SEQUENCE` previously failed to sync its rollback rename after a
publication-sync error. `DROP SEQUENCE` could return `IO_ERROR` after unlink
without restoring the sequence file. Rename now syncs its rollback; drop
retains the original bytes and restores them with no-replace publication when
the parent sync fails.

`tlist.lst` was also updated with direct append or truncate-and-rewrite
streams. A crash during DROP/RENAME/startup cleanup could leave a partial
fixed-width record or truncate unrelated table names. CREATE, DROP, RENAME,
failed-CREATE cleanup, and stale-temp startup cleanup now serialize a complete
fixed-record image and publish it with `writeAtomically`. If the atomic helper
reports a parent-directory sync error after publishing, the wrapper verifies
that the destination is exactly the intended byte image and retries that
barrier. CREATE rollback restores the exact pre-publication name snapshot.
DROP/RENAME return `IO_ERROR` when the list generation cannot be durably
confirmed after their physical work; callers must re-check the relation state.
Startup aborts rather than serving a database when stale-temp cleanup cannot
durably update the list or remove the leftover files.

## Verification

- `bash scripts/build_one_test.sh schema_marker_publish_guard_test` — passed.
  The script rebuilt the production object cache, linked the focused test, and
  exercised injected directory-`fsync` failure, rollback of the schema marker
  and temporary alias, successful same-name retry, symlink collision, and
  concurrent creators.
- `bash scripts/build_one_test.sh database_lifecycle_test` — passed. The
  parent-directory `fsync` was failed after the four initial child-file
  publications; `CREATE DATABASE` returned `IO_ERROR`, removed the new tree,
  and a same-name retry then succeeded. A separate injected parent-sync error
  after `DROP DATABASE` removed a fresh database returned `IO_ERROR`; the
  database path was absent, confirming that callers must re-check after this
  post-delete failure. The rename case injected the parent-sync error after
  moving a fresh database; it restored the source name, left the destination
  absent, and a retry succeeded. `schema_marker_publish_guard_test` injected
  failure after marker unlink and verified the schema marker was restored,
  duplicate CREATE was reported, and a subsequent DROP succeeded.
- `bash scripts/build_one_test.sh sequence_namespace_durability_test` —
  passed. Injected directory-sync failures after sequence rename and deletion
  restored the original name/content; retries then succeeded.
- `bash scripts/build_one_test.sh table_list_atomicity_test` — passed. The
  CREATE case lets the heap allocation-marker barrier succeed, injects `EIO`
  at the `tlist.lst` publication barrier, and verifies the exact published
  fixed-record generation is re-synced before success. The DROP case injects
  the same post-rename failure and verifies surviving names remain intact and
  the file length remains a multiple of the fixed record width.
- `bash scripts/build_one_test.sh table_comment_storage_test` — passed before
  the final startup fail-closed adjustment; it covers normal table rename and
  drop behavior. `bash scripts/build_one_test.sh database_lifecycle_test` —
  passed after the final source change, including construction of a fresh
  engine and database create/drop/recreate paths.
- `git diff --check` — passed before commit.

No full registered suite, standalone production executable build, or
PostgreSQL 18.6 differential was run for this change.

## Remaining gaps

WAL-07 remains partial. The one-shot failure injection validates the live
rollback path, not real power loss or the persistence behavior of ext4/XFS.
This change does not establish disk-full, short/partial-write, torn-write,
cross-device tablespace publication, or multi-root crash guarantees. Table
file/catalog changes are not one WAL-atomic DDL generation: a persistent
`tlist.lst` publication failure after DROP/RENAME file changes still returns
an indeterminate `IO_ERROR` and requires operator/client re-check. A temporary
hard-link alias may remain if cleanup itself fails, but the schema target was
synced before that cleanup and the alias is not interpreted as a schema
marker.

Source/test commits: `e8a6d1ee` (schema marker create), `cc129973` (schema
marker drop), `f8c02416` (database creation), `b707afd3` (database drop),
`ab72c8fe` (database rename), and `3db539a9` (sequence rename/drop), all not
pushed.
