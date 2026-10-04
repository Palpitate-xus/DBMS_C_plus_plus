# CAT-11: validate DEFAULT rows when attaching a RANGE partition

## Finding

When a new RANGE partition is attached, the DEFAULT partition's existing rows
must continue to satisfy its updated constraint. PostgreSQL 18.6 rejects an
attach if any DEFAULT row falls inside the proposed half-open range, returning
SQLSTATE `23514`; the data and partition metadata remain unchanged. If there is
no conflicting row, the attach succeeds and later rows in the range belong to
the new partition.

The local PostgreSQL 18.6 oracle (`server_version_num=180006`) confirmed both
paths: attaching `[10,20)` with a DEFAULT row at `15` failed with `23514` and
preserved all DEFAULT rows; after removing that row the same attach succeeded,
`15` routed to the attached partition, and `25` remained in DEFAULT.

## Change

`StorageEngine::attachPartition` now scans only the DEFAULT leaf under the
parent's metadata lock and compares each non-NULL partition-key value against
the proposed typed `[lower, upper)` bounds. Missing partition-key metadata
fails closed as corrupted data; a failed scan does not publish new metadata.
Conflicts return `DBStatus::CHECK_VIOLATION` before changing range bounds or
creating the new partition fork. The DDL executor translates that status into
PostgreSQL SQLSTATE `23514`, allowing its DDL transaction to roll back.

## Verification and remaining scope

`tests/partition_test.cpp` covers conflict rejection, preserved rows and schema,
the DDL executor's `23514` mapping/rollback, and successful attach when the
DEFAULT rows are disjoint. The focused test `bash scripts/build_one_test.sh
partition_test` passed. The full registered suite
`DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120
DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh` exited 0 with
`[test-build] All tests passed`, including all C++ tests, the PostgreSQL
protocol test, and registered E2E tests. Source/test commit: `c83e67ac`.

This does not move rows out of DEFAULT, prove constraints without a scan,
implement concurrent detach/finalize, partition indexes, or cross-partition
uniqueness. CAT-11 remains partial. Parent-table INSERT currently has a
separate transaction limitation; this verification seeds DEFAULT data through
the storage API and does not claim to fix that path.
