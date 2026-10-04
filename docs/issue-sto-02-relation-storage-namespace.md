# STO-02: relation storage identity and physical-name collision containment

## Scope

The PostgreSQL 18 audit asks for relation locators and fork/segment layout,
database and tablespace OIDs, and backend-scoped temporary relation naming.
This repository still uses table names as physical identity; this note records
one concrete integrity failure and a fail-closed containment fix, not completion
of that storage architecture.

## Reproduced failure

`StorageEngine::partitionDataPath()` maps a partition to
`<table>#<partition>.dt`, while `dataPath()` maps a standalone table to
`<table>.dt`. `#` is allowed in stored identifiers. Therefore the partition
heap for `orders` partition `p1` aliases the main heap of standalone table
`orders#p1`. The pre-fix regression reproduced cross-table visibility, then
showed that dropping the standalone name removed the partition heap.

The same prefix ownership predicate also makes names separated by `_` or `.`
ambiguous for other physical forks and access-method files. The risk is data
loss, not merely a nonstandard filename.

## Containment implemented

`createTable()` now rejects a relation name whose main-heap filename overlaps
another relation's name-based physical namespace in the same relation
directory. It also refuses to reuse any already-present file recognized as
belonging to the prospective relation. `alterTableRenameTable()` applies the
same registered-name collision check before publishing a new schema. `dropTable()`
fails closed when it detects a legacy overlapping relation, before changing
metadata or unlinking files.

`tests/partition_test.cpp` covers both CREATE orders for the partition/main-heap
collision, rename into a colliding namespace, a legacy overlapping catalog
fixture rejected by DROP in either direction, and acceptance of the distinct
name `orders2`. It also preserves an orphaned/corrupt heap on a rejected
CREATE rather than deleting it as failed-CREATE cleanup. The focused partition
test, `create_table_like_test`, and the revised `temp_restart_cleanup_test`
passed after the fix.

The full `scripts/build_tests.sh` run reached all registered C++ and E2E tests,
but exited 1 because its already-compiled `temp_restart_cleanup_test` still
expected CREATE to delete the pre-existing corrupt heap. That assertion was
updated and the test was rebuilt/run separately; the suite was not rerun from
the beginning after that test-only update. The TLS statement-timeout E2E was
skipped because this build uses the TLS stub.

## Remaining STO-02 work

Physical heaps and forks are still named from SQL identifiers rather than a
`RelFileLocator`/relation OID. Forks remain extension-based, relation files are
not split into fixed-size segments, and database/tablespace paths are still
name-based. Temporary relations still use a generated backend prefix rather
than a locator carrying backend identity. Other prefix-scanning backup,
tablespace-move, and maintenance paths still need a systematic audit for
legacy ambiguous catalogs. No PostgreSQL 18.6 differential was run. STO-02
therefore remains **partial**; no format migration or broad architecture change
is claimed here.
