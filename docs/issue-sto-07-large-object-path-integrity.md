# STO-07: large-object path integrity and remaining support

## Finding and fix

Large objects are stored as `lo_<id>.dat` files. The manager previously used
path-based streams and `resize_file` for object access, which followed a
symbolic link substituted for an object file. A concrete write through such a
link can modify a file outside the database directory; reads and exports can
also consume the link target.

Object-file access now opens with `O_NOFOLLOW`, verifies the opened descriptor
is a regular file, and performs reads, writes, and truncation through that
descriptor. Import validates the existing object before atomic replacement;
export reads from a validated descriptor. Initial directory creation rejects
symlink components, and startup scans only regular, non-symlink object files.
Writes and truncations also `fsync` the opened file before reporting success.

The fix is intentionally bounded: the manager does not pin `.lobjects` with a
directory descriptor for its full lifetime, so a concurrent replacement of a
parent directory after initialization is not covered by this check.

## Verification

- Before the fix, `large_object_symlink_guard_test` failed at the assertion
  that writing to a symlink-backed object must be rejected.
- `bash scripts/build_one_test.sh large_object_symlink_guard_test` passed after
  the fix. It verifies that an object-file symlink cannot modify or disclose
  its external target, that failed export leaves its destination untouched,
  and that an initially symlinked `.lobjects` directory is rejected without
  creating an object in the external directory.
- The following focused regressions passed after the source change:
  `large_object_create_durability_test`, `large_object_reopen_test`,
  `large_object_empty_write_test`, `large_object_import_replace_test`,
  `large_object_drop_failure_test`, `large_object_truncate_failure_test`,
  `large_object_export_alias_test`, `large_object_export_missing_test`, and
  `large_object_missing_import_test`.
- `git diff --check` passed before the source commit.

No full registered suite, standalone production executable build, or
PostgreSQL 18.6 differential was run for this change.

## Remaining gaps

STO-07 remains partial. PostgreSQL large-object catalog identities and ACLs,
transaction/WAL integration, complete 64-bit-offset semantics, the full
`lo_*` API, protocol/libpq behavior, and vacuum lifecycle remain unimplemented
or unverified. The parent-directory replacement race noted above is also
outside this fix. File-content fsync does not make multi-step object writes
transactional or provide rollback after an I/O error.

Source/test commit: `258456ad`, not pushed.
