# Native resolved array columns cast scalar elements to arrays

## Exact failure and cause

The original `tests/explain_typed_execution_test.cpp` setup is unchanged:
`resolveColumnType(arraySchema.cols[0], "integer", {}, true)`,
`createTable`, then `insertRow(..., {{"items", "{1,2}"}})` must succeed.
On the exact unmodified `6aecdb0453554b9d3f8bfdc1a1f7570312e6de0a`
production build, resolve/create succeed but this insert returns
`DBStatus::INVALID_VALUE` (12), and the original assertion aborts (134).
The new direct-API regression reproduces that same first failure.

`resolveColumnType` legitimately returns the public full spelling `integer[]`,
element width 4, and the separate array/variable-length flags. The original
type-registry test explicitly requires that public spelling. `validateColumn`
recognized the suffix but retained it in the physical column's element type.
`normalizeArray` consequently tried to cast each scalar `1` to `integer[]`,
raising 22P02 (`malformed array literal: 1`) rather than casting it to integer.
The existing native status boundary reports 12 and rolls back the insert.

The fix keeps the public resolve result and const caller metadata unchanged.
Physical validation records the element spelling plus the existing array flag.
The scalar array normalizer also strips the canonical suffix for previously
persisted full-spelling schemas. It does not change schema layout, relation
identity, transaction ownership, default origin, or the array value format.

## Permanent tests

Both new standalone tests are discovered by the existing canonical native-test
glob; neither replaces the original fixture or relaxes its setup/assertions.

- `native_resolved_array_storage_test.cpp`: exact original producer/seed;
  stored element metadata and unchanged caller/default metadata; genuine values,
  explicit lower bounds, 2D shape, element NULL, outer NULL versus empty array;
  overflow, invalid element/bounds/ragged input and unchanged rows; unique-index
  canonical duplicate detection; parent-transaction rollback; cold reopen and
  actual bound-plan output/NULL bits; int2/int8 widths and text NULL/empty/Unicode.
- `native_legacy_array_schema_test.cpp`: actual old public-API schema bytes,
  SHA256 `4a6cafda8173e17f943607c1e643a9287c5ccceb38b603b79ee356fc0cdfa6c6`,
  produced by the unchanged 6aec resolve/create API. The test installs only those
  bytes into its own newly created, isolated RID1 table after closing its owner.
  Cold loading preserves full `integer[]` spelling, RID, flags and width; the
  original insert, bounded/NULL array, rejected input, and typed query succeed.
  The same complete test aborts with status 12 against the old build.
- `type_registry_test.cpp` retains the full public resolve assertion and adds
  the corresponding physical-validation element-type assertion.

PostgreSQL 18.6 was verified as exactly `server_version_num=180006` before the
reference controls. Integer-array literals, lower bounds, dimensions, empty and
outer NULL, int2/int8 limits, and text element NULL/empty/Unicode round trips
were checked. An initially authored ambiguous `ORDER BY items` is preserved as
a 42702 negative control, followed by the correctly qualified positive control.
It is not counted as a production defect. PostgreSQL has no C++ resolve/create
API; the oracle verifies the corresponding SQL value semantics.
The unchanged complete explicit-bounds, ALTER-array-type envelope and array
element-typmod fixtures also pass against strict 180006, recorded in
`final-three-array-reference18-full.log` and their individual full logs.

## Actual validation and retained failures

Evidence directory: `/tmp/dbms-native-array-type-api.PhzM6j1B` (local, not shipped).

- `current6aec-all58-fresh-baseline.log`: all 58 production translation units
  freshly built with the official O2 flags; repeat build and all source/header/
  flags/object receipts checked. Original frozen binary SHA256
  `a20a8ca29259ebf71fbf39d8f56af4f7d80bb7f6882fba89f91010ea2cc064f1`.
- `current6aec-original-and-new-native-baseline-full.log`: original and new
  exact-seed native tests abort 134; original type-registry test succeeds.
  `current6aec-legacy-native-baseline-full.log` retains the independent old-schema
  direct-API abort 134. `old8694-array-api-observation.log` is an earlier
  read-only diagnostic, not a substitute for this matching 6aec proof.
- `candidate-two-cpp-normal-build.log`: only the two changed production CPPs
  rebuilt, with the other 56 matched to the fresh 58-unit epoch. Its reused
  wrapper's final `ALL58_FRESH...BASELINE=0` label is inaccurate for this second
  invocation; the actual compilation count is two. Candidate frozen SHA256
  `ae805beffed512eb1bd87dc6bbe842d7e1a16c6b9b7839efadcc78e80c6fd704`.
- `candidate-v1-original-and-new-native-full.log`: five complete native tests
  pass, including the untouched original EXPLAIN typed execution fixture.
  `candidate-v1-index-native-full.log` adds complete unique-index controls.
  `scoped2cpp-native-full.log`: four complete native tests pass with ASan+UBSan
  on TableManage/type_registry plus drivers/stubs, and 55 matching normal
  non-main units; leak detection is disabled. This is not an all-58 SAN run.
- `final16-whole-v1-serial.log`: 15 complete protocol scripts pass; one fails.
  Original SQL, row/OID/NULL/rollback assertions and default 15-second deadlines
  remain intact. Array type rewrite, bounds, physical descriptors, element mods,
  concat, ordinary/source DELETE and UPDATE, exception owner, DML EXPLAIN,
  Unicode patterns, native legacy-pattern consumers, and domain/default
  transaction controls pass.

The failing original `view_trigger_typed_values_protocol_e2e_test.py` correctly
returns 23514 for the late trigger-action constraint failure, then its next
SELECT times out at line 103. `view-trigger-exact6aec-old-whole.log` reproduces
the identical timeout with the original a20 frozen binary;
`view-trigger-reference18-whole.log` passes the entire strict PG18.6 fixture.
Both failed complete logs are retained. This existing frontend view-error path
timeout remains OPEN and its cause is not established here; 16 protocol tests
are not described as green.
The corresponding original native view-trigger parameter test does pass, but
does not close that frontend wire gap.

`final16-adjacent-native-full.log` also retains a wrapper filename mistake
(`physical_array_element_descriptor_test.cpp` does not exist): 15 real native
tests passed and that invocation failed to compile its nonexistent entry. The
three correctly named physical descriptor/type-identity fixtures subsequently
pass in `final-three-true-array-descriptor-native-full.log`; the corrected
complete 23-native group passes with authoritative exit 0 in
`final23-native-full.log`. `audit-final-committed.log` records the final 58-source,
all-relative-header, flags, object-receipt/byte and frozen-binary checks, with
only the two changed CPPs fresh and the other 56 matched to the 6aec epoch.

No claims are made about untested application-defined array types or all
collation/ARE families, and no unrelated view cleanup is included in this fix.
