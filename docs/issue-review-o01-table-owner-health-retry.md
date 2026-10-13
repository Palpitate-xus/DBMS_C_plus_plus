# O01: table-owner health retry and a clean peer catalog image

## Confirmed failure

Base `2886e4fe` includes R16's complete class/attribute readers, the cold-directory
compatibility followup, and R18's owned catalog-row APIs. Its unchanged original
`table_owner_atomicity_test` completes both expected publication-error checks,
then fails the healthy retry at original line 105. A second run with an added
status diagnostic reports `DBStatus::IO_ERROR` (5), not an unknown-role or lock
error. The original schema/catalog/reload/retry assertions are retained.

An independent normal-API regression, `table_owner_peer_retry_test`, also fails
on the base: a second `CatalogManager` publishes a description, then an otherwise
healthy owner change through the first engine returns an error.

The old ordering was:

1. Read a class from the potentially stale, but clean, catalog cache.
2. Publish the new `.stc` owner and change the cached class owner.
3. Call `persistAll()`.

The peer-generation guard now sees both a newer durable catalog and a local
unsaved class edit. It correctly rejects this dirty-cache conflict; restoring the
class/schema merely makes the unnecessary owner failure visible to the caller.
A temporary diagnostic on the original fixture confirmed changed peer image
generations and `class` as the only locally changed image. That diagnostic was
removed before the candidate production build.

## Repair

`StorageEngine::alterTableOwner` calls the existing `persistAll()` before
capturing the original class and before writing `.stc`. An unchanged cache can
therefore adopt the real durable peer model while it is still clean. A genuine
unsaved-local/peer conflict still returns an error before writing the schema;
the catalog conflict policy is unchanged. This is not a cross-cache merge or
an assertion of atomic multi-file catalog publication.

The original test's incorrect comment claiming that class is written before
attribute is corrected: `persistAll()` preflights all image targets first.
The directory-backed attribute failure is a preflight error, not proof of a
class image having been published and subsequently reverted. Later actual
write errors may still partially publish a set, and the existing best-effort
rollback remains explicitly best-effort.

## Permanent coverage

- Original `table_owner_atomicity_test`, including schema and attribute target
  errors, restored original owners, service eviction, independent engine reload,
  healthy retry, and durable new-owner reload. Only the comment and failure
  diagnostic changed; no assertion was removed or weakened.
- New `table_owner_peer_retry_test`: normal peer description publication;
  successful owner change retaining that peer metadata; a real local-unsaved/
  peer conflict retaining both respective models and the original durable/schema
  owner; explicit reload and a subsequent successful retry; final eviction and
  durable owner checks.
- The complete eight R16/R18 native gates remain part of the adjacent suite:
  `catalog_value_snapshot_test`, `catalog_complete_read_test` (691 checks),
  `catalog_optional_directory_test`, `catalog_clean_publication_test`,
  `catalog_resolve_test`, `catalog_persistence_failure_test`,
  `type_catalog_descriptor_persistence_test`, and `catalog_snapshot_test`.

## Build/evidence scope

Private artifacts: `/tmp/dbms-review-owner-retry.d5Tbg6nE/`.

The frozen `2886e4fe` donor's entire `src`, `scripts`, and `cmake` inputs were
compared byte-for-byte. All headers, compiler/version, normal flags/includes,
TLS selection, manifest, all 58 original object receipts and copied object bytes
were checked; only the absolute-path spelling in the verified receipt hashes
was remapped for the new private tree. `baseline-donor-proof.log` ends PASS.
No Root cache or frozen donor input was changed.

The candidate changes only `TableManage.cpp`; this TU is rebuilt with the normal
production flags and the other 57 matched objects retain the same header ABI.
Every native run checks all current 58 object receipts and the full build
signature, then compiles a fresh test driver and stub.

Baseline logs `owner-original-baseline.log`,
`owner-original-status-baseline.log`, and `owner-peer-baseline.log` have actual
nonzero terminal results. `owner-diagnostic-run.log` records the finite causal
diagnostic. Candidate normal build and complete ten-gate results are recorded
in `owner-candidate-build.log` and `owner-candidate-complete10.log`.
Both have actual terminal exit 0; the native log ends `complete failures=0`
for all ten complete fixtures, including the original healthy-retry controls
and the new dirty-conflict controls.

Reproduction in this private tree:

```sh
bash scripts/build.sh
bash /tmp/dbms-review-owner-retry.d5Tbg6nE/run_catalog_gates.sh \
  table_owner_atomicity_test table_owner_peer_retry_test \
  catalog_value_snapshot_test catalog_complete_read_test \
  catalog_optional_directory_test catalog_clean_publication_test \
  catalog_resolve_test catalog_persistence_failure_test \
  type_catalog_descriptor_persistence_test catalog_snapshot_test
```

This issue changes ordinary owner metadata publication order only. It does not
change authentication/permission policy, safety/TDE modules, CTAS, or background
checkpoint behavior, and it does not establish correctness for the separate
R17 failed-DDL-rollback recovery path or a whole-project regression.
