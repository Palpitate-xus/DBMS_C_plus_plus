# Enum scalar comparison and sort bindings

## Actual defect

For `rank_type AS ENUM ('zeta','','alpha','aa','NULL','it''s','longlonglong')`,
physical WHERE/ORDER consumers already use declaration rank, but a SELECT-list
`r < 'alpha'` used lexical text ordering. On the original normal Source2 frozen
binary, the permanent seven-operator matrix fails its first exact row assertion
on both default disk and the separately scoped `TMPDIR=/dev/shm` run. Strict real
PostgreSQL 18.6 (`server_version_num=180006`) gives declaration-order results for
the identical labels, operators, NULLs and ordinary SQL controls.

Binding a native AST alone was insufficient: V1's native test passed, but its
actual SQL wire projection still returned the original wrong results. The CLI/
protocol scalar dispatcher discarded the retained typed AST for these ordinary
projections. V1's complete failed wire log and frozen binary are preserved.

## Change and scope

- Copy enum labels into the catalog's existing single-lock metadata snapshot.
  Resolve the actual source type OID first, following genuine domain ancestry;
  declaration spellings use the session's real schema path only when no physical
  OID is available. Copy identity and ordered labels into the bound operator.
- Bind ordinary scalar enum comparisons and simple CASE equality to that exact
  type. Validate genuine UNKNOWN string literals at binding (including empty/
  SQL-NULL-looking/escaped labels and zero execution demand); never run a
  function, parameter, row or child query to discover its type.
- Keep actual enum result identity through casts, CASE, scalar child descriptors,
  CTE projection descriptors and execution-owned expression copies. CASE common
  type uses the resolved enum identity, not differing qualified/bare spellings.
- Bind ordinary sort keys to copied enum operators, including CASE results,
  output aliases and ordinals. Prepared sorting uses declaration rank, ASC/DESC
  and the existing actual NULL placement rather than builtin TEXT inference.
- The ordinary SQL dispatcher performs its existing supported-shape check, then
  whole metadata binding, and only retains this new scalar receiver for actual
  enum projection/ORDER expression bindings. WHERE-only enum predicates retain
  their existing physical schema/index consumer after pure validation. Existing
  CASE/pattern/scalar ownership is unchanged; errors are not swallowed to retry
  an alternate parser. Builtin CASE does not allocate unused enum binding slots.

Custom protocol type-OID publication, unsafe use/rollback of ALTER-added labels,
general aggregate consumers and the prior broader namespace/owner work are not
claimed fixed. PostgreSQL's custom OID need not numerically equal this engine's
OID: the new native assertions check real type identity, and wire comparisons
assert actual BOOL/INT outputs and data semantics, not false raw-OID equality or
a fabricated default TEXT type.

## Permanent controls and prefix boundary

`tests/enum_comparison_binding_test.cpp` covers two genuine databases with the
same enum basename and opposite declaration ranks, actual copied identities,
typed NULLs, simple CASE, execution copies and constant CASE planning, real
sorted physical rows, aliases/ordinals, cold metadata and preparation-time invalid
literals. `tests/enum_comparison_binding_protocol_e2e_test.py` is registered in
the real production runner and keeps the full seven-operator/label/NULL matrix,
WHERE/ORDER consistency, prepared rank/CASE sorting, simple/searched/result CASE,
CTEs, no-FROM casts, same-basename schemas, scalar children, index and actual cold
process controls, exact 22P02/42883/42846 errors, plus the quoted TYPE controls.
Reference writes are confined to unique owned schemas in a rolled-back PG txn.

The full wrapper deliberately retains a separate existing quoted-TYPE creation
identity failure: parser `parseCreateType` passes raw quoted tokens to catalog/
sidecar creation while later SQL type lookup decodes identifiers. V2 passes the
initial ordinary rank/NULL/CASE/sort controls, then the original quoted
`"MixedRank"` CASE control reports 42883 (a direct cast comparison also wrongly
returns `f`, not PostgreSQL's `t`). Those statements/assertions are not removed,
weakened or replaced with lowercase creation to manufacture whole-suite success.
This projection prefix is not a READY whole-matrix result; a separate ordinary
TYPE-identity commit must make the final combined original wrapper pass.

## Preserved production evidence

Private base is `4724583a2ce3a31b91b1bb4aefffc73f4b715e04`, not newest ROOT's
separate BIT/empty-hash-NULL/startup composition. Public AST/binding/catalog headers
changed: both V1 and V2 normal O2 builds actually freshly compile all 58 production
translation units, without old-header donor substitution. V2 is frozen at
SHA-256 `e06b4e86c52c569b63bc61248728fe0de9779b1670b892b91f46fa5c9d8c72cc`.
V2 build/repeat/current58 receipts/header/compiler/stamp/freeze audit are terminal
0; its complete default-disk new native driver is terminal 0 and its full default
wire matrix is terminal 1 at the quoted TYPE dependency. V3 refines three CPPs
only, preserving those same current public headers and unchanged other 55 normal
production inputs; final receipts and scope-specific regression logs accompany
the prefix commit rather than claiming it was a fresh-all-58 invocation itself.

V3 normal build and repeat, all-current58 receipt/header/source/compiler/stamp/
frozen audits: terminal 0. Its frozen SHA-256 is
`1e6abb2c55562d85ad25a7ff4748f2c33de4c5fe0a75e810ac1b8efba52c8aac`.
The complete new native driver on default disk is terminal 0; the full original
18 native drivers plus the new driver and ten original CASE/query/cursor/
quantified/pattern/distinct neighbours (29 complete drivers) are terminal 0 with
`TMPDIR=/dev/shm`, without changing assertions or normal O2 compile/link flags.
The complete original non-enum simple-CASE wire driver and original HASH enum
WHERE/index/rollback/cold-process driver are both terminal 0 on default disk.
V3's full registered projection wrapper remains terminal 1 at the same retained
quoted TYPE identity control. This remains a prefix dependency, not a whole green
claim. The actual original 18.6 full reference matrices including quoted TYPE,
qualified schema/CAST, CASE identity/errors and sort controls are terminal 0.

External artifacts are under `/tmp/dbms-enum-projection.Wt62NZeV/`: all frozen
binaries, `projection-fresh58-v{1,2}-build.log`, repeat/audit logs, source input
seal and frozen production patch, default-disk and tmpfs original baseline logs,
V1/V2 candidate failed wire logs, native drivers and strict-18 reference logs.
The initial default docker reference was actually 17.2; the unchanged strict
180006 gate rejected it, and that failed log remains separate from real 18.6
reference runs. No default-disk red result is replaced by a tmpfs green result.
