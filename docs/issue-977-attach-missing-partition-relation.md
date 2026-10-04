# CAT-11: reject ATTACH PARTITION when the relation is missing

## Finding

`ALTER TABLE ... ATTACH PARTITION` must attach an existing relation. PostgreSQL
18.6 returns SQLSTATE `42P01` for a missing child and leaves the parent
unchanged. The old local DDL path called the storage metadata operation without
checking the child relation; that operation could create a parent partition
fork for a name that had no table, yielding a phantom partition.

The local PostgreSQL 18.6 oracle (`server_version_num=180006`) confirmed the
missing-relation error and SQLSTATE `42P01`.

## Change

Before changing partition metadata or creating a fork, the DDL executor now
resolves the requested child name and verifies that it is a table in the
current database. A missing relation throws `DbError("42P01", ...)`; the
parent's range metadata is not changed. The low-level `StorageEngine` API still
accepts synthetic partition names that do not name standalone relations; a
later safety guard rejects existing relation attachments until their storage
can be shared correctly (see issue #978).

## Verification and remaining scope

At commit `40b72024`, `tests/alter_table_only_test.cpp` covered the missing-child
SQLSTATE, asserted that no range partition was published, and confirmed an
empty child could attach. Issue #978 later showed the empty-child success was
also unsafe: future writes through the child use a separate relation file.
The current regression therefore expects `42P01` for a missing child and
`0A000` for an existing relation, with no parent metadata change. At the time of
this commit, the focused test and production build passed; the full registered
suite was not rerun after #977. The immediately preceding step #976 had a full
registered-suite pass.

This prevents the phantom partition created by an ATTACH request for a missing
child. The original empty-child success assertion was superseded by issue #978.
Child column compatibility, storage sharing/migration, constraint proofs,
concurrent detach/finalize, partition indexes, and cross-partition uniqueness
remain unresolved. CAT-11 stays partial.
Source/test commit: `40b72024`.
