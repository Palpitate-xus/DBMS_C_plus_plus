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

## Verification

- `bash scripts/build_one_test.sh schema_marker_publish_guard_test` — passed.
  The script rebuilt the production object cache, linked the focused test, and
  exercised injected directory-`fsync` failure, rollback of the schema marker
  and temporary alias, successful same-name retry, symlink collision, and
  concurrent creators.
- `bash scripts/build_one_test.sh database_lifecycle_test` — passed. The
  parent-directory `fsync` was failed after the four initial child-file
  publications; `CREATE DATABASE` returned `IO_ERROR`, removed the new tree,
  and a same-name retry then succeeded.
- `git diff --check` — passed before commit.

No full registered suite, standalone production executable build, or
PostgreSQL 18.6 differential was run for this change.

## Remaining gaps

WAL-07 remains partial. The one-shot failure injection validates the live
rollback path, not real power loss or the persistence behavior of ext4/XFS.
This change does not establish disk-full, short/partial-write, torn-write,
cross-device tablespace publication, or multi-root crash guarantees. A
temporary hard-link alias may remain if cleanup itself fails, but the schema
target was synced before that cleanup and the alias is not interpreted as a
schema marker.

Source/test commits: `e8a6d1ee` (schema marker) and `f8c02416` (database
directory publication), both not pushed.
