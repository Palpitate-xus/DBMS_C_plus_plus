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
parent's range metadata is not changed. The low-level `StorageEngine` API is
unchanged because it is also used to construct synthetic partition metadata in
storage tests.

## Verification and remaining scope

`tests/alter_table_only_test.cpp` covers the missing-child SQLSTATE and asserts
that no range partition was published; it then creates a valid empty child and
confirms ATTACH still succeeds. `bash scripts/build_one_test.sh
alter_table_only_test` passed, and `bash scripts/build.sh` produced
`dbms_main` successfully. The full registered suite was not rerun after this
small follow-up change; the immediately preceding CAT-11 step `#976` had a full
registered-suite pass.

This only prevents missing-child attachments. It does not yet validate child
column compatibility or move/use rows already stored in a standalone child;
constraint proofs, concurrent detach/finalize, partition indexes, and
cross-partition uniqueness also remain unresolved. CAT-11 stays partial.
Source/test commit: `40b72024`.
