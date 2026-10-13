# R18: owned catalog row values for runtime consumers

## Cause and compatibility boundary

Base: `b54cc7f0`, with independent R16 complete-read commit `cb9c25d2` and
cold optional-directory compatibility followup `35f844ed`.
The old `find*` and `resolve*` inspection APIs return pointers into mutable row
vectors after releasing `CatalogManager::mutex_`. A subsequent row creation,
drop, or full reload can invalidate those pointers before a caller copies or
reads the row. This is the source-level lifetime defect from review R18; no
original crash or sanitizer failure is claimed here.

The compatible repair preserves the old inspection APIs and their actual
`const_cast`-based catalog-edit fixture behavior. Their header contract now
requires external serialization of the entire pointer use; any mutation or
reload invalidates the borrowed result. They are not concurrent APIs.

## Repair

- New `getNamespace`, `getClass`, `getAttribute`, `getType`, `getProc`, their
  name/temporary variants, and `getProcsByName` copy while holding the catalog
  mutex. Missing rows return `std::nullopt` or an empty owned vector.
- `resolveRelationSnapshot` resolves and copies under the same lock;
  `resolveAttributeSnapshot` resolves the relation and copies its attribute
  within one critical section, without borrowing a public getter result.
- Ordinary production consumers in CatalogService, DdlExecutor, DdlTransaction,
  DmlExecutor, TableManage, Main type lookup, and Network custom-OID validation
  use owned results. Existing schema/type identity and row fields are copied,
  not inferred from a name or SQL value.
- There are no thread-local row substitutes, permanent retained allocations,
  modified SQL text, or changes to authentication/permission/TDE modules.

The returned row value survives catalog mutation, reload, and destruction, and
survives eviction after the lookup completes. The lifetime of a borrowed
`CatalogService::get` manager handle itself still requires caller coordination;
this change does not authorize calling a destroyed manager concurrently with
eviction or turn a sequence of separate DDL operations into an atomic transaction.

## Verification

The permanent `catalog_value_snapshot_test` covers every new API's identity
and missing result, quoted/search-path relation resolution, temporary namespace,
600 normal creations in each vector family, update/rename and full reload,
catalog destruction after values escape, 2,000 ordinary writer/reader operations
with copied-field coherence checks, and real StorageEngine/CatalogService
eviction/reload after namespace/class/attribute/type/procedure copies are taken.

Actual complete results:

| Full native fixture | Final result |
| --- | --- |
| `catalog_value_snapshot_test` (all APIs, real eviction and concurrent reads) | exit 0 |
| `catalog_complete_read_test` (all 691 original assertions) | exit 0 |
| `catalog_optional_directory_test` (cold absence and warm directory loss) | exit 0 |
| `catalog_clean_publication_test` (including original direct row edit) | exit 0 |
| `catalog_resolve_test` | exit 0 |
| `catalog_persistence_failure_test` | exit 0 |
| `type_catalog_descriptor_persistence_test` | exit 0 |
| `catalog_snapshot_test` (all three original sections) | exit 0 |

The original seven-gate run had six passes and one actual failure in
`catalog_snapshot_test`, caused by the initial R16 optional cold-directory
incompatibility. That terminal exit-1 log is preserved. After the independent
`35f844ed` correction, the complete eight-gate runner terminated with exit 0.
The earlier standalone first-stage gate also passed
growth/reload/destruction/concurrent copies, but deliberately did not select
the Engine eviction consumer and was not counted as a complete-fixture pass.

The public catalog header changes. Verification therefore requires a genuine
fresh 58-TU normal production build, not old-header object reuse. The private
runner checks all 58 current source/header/compiler receipts and the whole
build signature, then links 57 current non-Main production objects with freshly
compiled drivers/stubs. The genuine fresh58 production build terminated with
exit 0. The source-only compatibility correction then rebuilt catalog normally
and proved the other 57 byte/signature-matched objects from that same new-header
epoch; the second production build also terminated with exit 0. The build and
runner do not modify Root caches.

Artifacts are under `/tmp/dbms-review-catalog-oct13.ZlX4CjTC/`; the full build is
`r18-fresh58-normal.log`, `r18-after-compat-normal.log`, the retained initial red
`r18-catalog-gates.log`, and final `r18-catalog-gates-after-compat.log`.
The gate runner is `run_r18_catalog_gates.sh`. Final production SHA-256:
`9a3511e166dea0bb47d59876d4fbd9b7c4c1fe440aada6683feaa6c021d9d20f`.
This issue is independent of the still-open owner publication healthy-retry
failure; it does not claim that persistence issue has been fixed.
