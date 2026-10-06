# Prepared-query catalog array declarations

Status: the two array-descriptor root causes below are repaired and verified.
This is not a complete array/type-family claim.

The shared `StorageEngine::prepareBoundQuery` catalog path copied
`pg_type.typname` but lost `pg_attribute.attndims`. A table created by this
project's DDL can publish base type OID 23 (`int4`) with dimensions 1, whereas
the physical schema correctly carries `isArray`. The prepared range therefore
declared a scalar, and the strict typed execution carrier rejected its actual
array cell before evaluation.

Actual first red: frozen EXPLAIN v2 O2 binary SHA256
`65b3cf72b0e036a2d22e43de2add4f047b4c215b8aa44a55a6e12be58534334c`.
Its TEXT/JSON `array_projection` controls returned `XX000` rather than the
expected successful two-row analyzed query. Artifact:
`/tmp/dbms-explain-typed-execution.fGV8WYCQ/candidate-explain-v2.log`.

The first attempted marker repair still failed: v3's copied declaration was
`int4[]`, not `integer[]`. The scalar alias registry does not normalize an
already-suffixed array spelling. The stronger native control printed this
exact descriptor and aborted (1038 / 49824, exit 134), while the immutable v3
wire run (71613, exit 1) retained the same two array failures. All other v3
wire controls passed. Relevant artifacts are `native-explain-v3-final.log`,
`native-array-v3-diagnostic.log`, and `candidate-explain-v3.log` in the same
task directory.

The first element-first catalog repair (v4) corrected both catalog descriptor
shapes, but the physical cell still arrived as `int[]` while copied metadata
was `integer[]`. Its stronger native check aborted (47476, exit 134), and
TEXT/JSON still failed (42199, exit 1). Artifacts are
`native-explain-v4-final.log` and `candidate-explain-v4.log`. The shared
canonical type helper now removes array markers before normalizing the base
alias, then reattaches one array marker. It keeps the strict carrier check;
it does not make a scalar compatible with an array. A read-only PostgreSQL
17.2 control reports `integer[]` for both one- and two-dimensional integer
arrays (`pg17-array-type.log`); SQL dimensions do not create distinct types.

The final implementation reads either `attndims` or an actual array
`pg_type` category/element, canonicalizes the element first, then appends
`[]`. It preserves the copied name/order/source identity and never executes
a query to infer the descriptor. The carrier's strict source-cell type check
remains unchanged.

The dedicated native regression covers the real base-OID-plus-dimensions
shape and the array-OID/typelem shape (including dimensions zero). It checks
the bound column's declaration, evaluates typed array cells, and rejects a
scalar cell with `XX000`. It does not depend on the new EXPLAIN operator tree.
It also retains explicit alias/dimension and modifier controls for the shared
canonical helper.

Final matching production group: all 57 units were compiled against the new
header set; subsequent changed-source rebuilds, repeat up-to-date check,
57/57 signatures and binary stamp succeeded (final handle 51314, exit 0).
Frozen V6 binary:
`/tmp/dbms-explain-typed-execution.fGV8WYCQ/dbms_main.explain-v6.o2`, SHA256
`f39e8891871f141aedad08aa0e5bf4282d108490bb594d37ce2040d662d2140d`.
Native handle 32194 exited 0: the independent array test printed
`integer[]` for both descriptor shapes, and all 15 matching native controls
passed (`native-explain-v6-final.log`). Wire handle 78440 exited 0: both
TEXT/JSON array controls and the entire retained 46-case matrix passed
(`candidate-explain-v6.log`). This combined binary is named explicitly;
it is not presented as an array-only build or an old 56-unit ABI result.

This does not claim complete array operators, multidimensional behavior,
domain/composite coercion, malformed catalog recovery, or every static type
error. Those remain separate review items.
