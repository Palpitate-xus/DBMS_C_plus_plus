# R16 followup: preserve legitimate cold optional metadata reads

## Actual regression and receiver

The independent R16 commit `cb9c25d2` correctly rejects incomplete class and
attribute images, but initially rejected a legitimate cold read before a
database's `pg_catalog` directory had been bootstrapped. The full unchanged
`catalog_snapshot_test` aborted with `58030: cannot read catalog image class`.
The R18 seven-gate run recorded six passes and this failure; it is retained as
`r18-catalog-gates.log` (actual terminal exit 1), not reported as a green run.

`StorageEngine::getTableSchema` reads optional domain identity through
`CatalogService::domainColumns`; an uncached service performs a read-only
`CatalogManager` lookup. The read-only constructor intentionally neither creates
directories nor allocates OIDs. `catalogGeneration` cannot stat an absent parent
directory, so the initial R16 opener mistook absent optional metadata for an I/O
failure. The original native snapshot fixture had passed the unchanged full
regression before R16 (`full-regression.log:1926`).

## Narrow correction

An initial read-only lookup, with no previously observed image, may return empty
relation metadata when the catalog directory is genuinely absent. It still
does not bootstrap or publish any path. Inspection failures, non-directory
targets, incomplete existing records, and loss of a previously loaded mutable
catalog directory keep the R16 fail-closed behavior and whole-cache rollback.
The serialized relation format and all 48 strict truncation boundaries remain
unchanged. This is a source-only compatibility followup, not a public ABI change.

## Actual gates

- New `catalog_optional_directory_test`: initial R16 baseline exit 1 with two
  failed cold-read checks; candidate exit 0. It also checks that cold reads do
  not create the missing path, and that a disappearing warm directory raises
  `58030`, retains old rows, cannot be republished, and permits restoration/retry.
- The unchanged complete `catalog_snapshot_test` now passes all three original
  snapshot/rollback sections, actual exit 0.
- The original R16 `catalog_complete_read_test` retains all 691 assertions and
  passes, actual exit 0.

For the full Engine gate, every frozen donor source/header byte, current normal
compiler flags, manifest, build configuration, and all 58 original per-source
receipts were checked. The native link used 56 byte-matched non-Main donor
objects plus freshly compiled catalog, driver, and stubs; no Root cache was
modified. The donor was the read-only snapshot
`/tmp/dbms-review-fix-donor.N9sdas6f` and the private script was
`/tmp/dbms-review-catalog-oct13.ZlX4CjTC/run_r16_followup_engine_gate.sh`.

Preserved evidence under `/tmp/dbms-review-catalog-oct13.ZlX4CjTC/`:
`r16-cold-directory-baseline-native.log`, `r16-cold-directory-candidate-native.log`,
and `r16-followup-engine-gates.log` (three full gates, terminal exit 0).
No security/permission/encryption module, skipped CTAS/checkpoint branch, or
original fixture assertion was changed.
