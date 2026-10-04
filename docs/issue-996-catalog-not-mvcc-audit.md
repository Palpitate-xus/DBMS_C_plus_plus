# Issue 996: system-catalog persistence architecture audit

## Finding

`CatalogManager` is a per-database in-memory cache of vectors and hash indexes.
Its persistence path serializes catalog rows into one `pg_<name>.cat` file per
catalog, loads those files when the manager is constructed, and saves them
through explicit `persistAll()` calls and the destructor. `OidGenerator` keeps
its allocation counter/free list separately. The catalog writer uses
temporary-file replacement, `fsync`, and rename, which improves durability of
individual files but does not provide a transaction log or an atomic commit
across a set of catalog files.

The SQL-visible catalog support is partly virtual, and low-level heap storage
can be created without registering a `pg_class` row. The `catalog_service_test`
explicitly verifies that storage-only metadata is not imported into the
catalog. The `catalog_snapshot_test` covers selected storage schema/list
snapshots; it is not MVCC for `CatalogManager` rows and must not be treated as
evidence that catalog DDL is transactionally versioned.

## Evidence and verification

Source inspection:

- `src/catalog/catalog.h` documents an in-memory cache and one CSV file per
  catalog.
- `src/catalog/catalog.cpp` implements `pg_<name>.cat` serialization and
  loads it from the `CatalogManager` constructor; the destructor calls
  `persistAll()`.
- `src/catalog/CatalogService.h` describes the cache as current-format-only
  and persisted at shutdown or explicit calls.
- `src/catalog/oid.cpp` stores the OID counter and free list separately.
- DDL code contains many explicit `persistAll()` calls rather than using a
  catalog WAL/MVCC transaction relation.

Existing isolated catalog regression binaries passed:

- `build/catalog_service_test` — bootstrap/cache, storage-only metadata
  boundary, checkpoint persistence/reload, and legacy current-format prefix.
- `build/catalog_persistence_failure_test` — persistence I/O failure is
  surfaced for the tested replacement failure.
- `build/catalog_resolve_test` — qualified-name/search-path relation and
  attribute resolution.

`build/catalog_snapshot_test` also exited 0 in an isolated temporary directory,
but emitted a heap-allocator cleanup warning and tests storage-engine snapshot
behavior rather than catalog MVCC; it is not counted as CAT-01 completion
evidence. No code changed in this audit, no full registered test suite ran, and
no PostgreSQL 18.6 runtime oracle/differential is claimed.

## Status

CAT-01 is partial, not complete. Atomic single-file replacement, cache
locking, checkpoint persistence, and current-format loading exist. The
required catalog heap relations, WAL redo/undo, MVCC tuple visibility,
transactional multi-catalog commit/rollback, crash recovery, and upgrade
migration are still absent. This audit records the evidence and updates the
ledger classification; it does not claim to repair the architecture.
