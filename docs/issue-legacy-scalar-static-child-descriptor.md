# Static output types for legacy scalar SQL children

This is a separate root cause from structured cardinality error creation
(`8770ba41`) and explicit scalar-host ownership (`c5e94063`).

## Actual failure

The strengthened whole scalar wire matrix retained OID **23** for an integer
child. Actual PostgreSQL 180006 returned [23,23], while both the previous
candidate and the cardinality-fixed candidate returned [23,25]. All four data
rows were otherwise correct. Empty children likewise require their declared
integer type rather than a type guessed from a first row.

The legacy hasScalar metadata publisher passed opaque `(SELECT ...)` text to
the scalar-expression type helper. That helper's TEXT fallback was then
published as the child type; `getUDF("subquery")` cannot supply a SQL child's
descriptor. The query already has a complete metadata-only binding API.

## Independent metadata repair

The legacy scalar-child projection publisher now prepares the **whole original
query** with its actual database owner, consumes output types by actual
projection ordinal, and checks descriptor width. Preparation happens before
the legacy executor or a sibling writer is evaluated. It does not execute a
child, infer schema from returned rows, rewrite TEMP SQL names, or use NULL/value
spelling to select a type. Existing scalar expressions without the internal
SQL-child role keep their prior consumer in this narrow change.

The private candidate is `690da81eb14a9e7160176dc64424734a40fc90e7a709c548f640509b5d572214`,
based on `8770ba41`. Complete 58-source/header/flags matching was audited before
importing the immutable parent group; only changed main.cpp was freshly
compiled (session 52408 actually exit 0, repeat/all58 signatures/stamp 0).
No public header changes or new ABI are introduced by this metadata repair.
These are O0 semantic proofs, not ROOT normal-O2 production validation.

Actual strengthened reference matrix passed against XML-enabled PostgreSQL
**180006**. Candidate V1 repaired the original integer/empty/NULL OID assertions
and the subsequent BIGINT (20), integer-array (1007), and BOOL (16) checks.
The full expanded candidate matrix then **failed** on a distinct TEMP child
with an inner projection alias (42P01 from the old standalone legacy child
parser). The unchanged alias, quoted-source, descriptor, pre-effect, and
cardinality expectations remain in the same unsplit matrix.

The matrix is deliberately **not registered as a passing gate** at this stage.
The next independent runtime repair must consume the retained prepared AST
through the shared typed source/cursor carrier; a metadata preflight alone does
not repair that legacy TEMP/alias execution path. No complete matrix, scalar
family, or original-full-protocol pass is claimed here.

Artifacts: `/tmp/dbms-scalar-static-descriptor.8kDQWlUr/`:
`import-baseline.log`, `build-v1.log`,
`reference-expanded-unsplit-scalar-18.log`, `wire-expanded-unsplit-v1.log`.
All original failing assertions/logs remain. Task-scoped TMPDIR `/dev/shm`
semantic gates retain deadlines and are not disk-performance evidence.

Separately, the actual original full-protocol run on immutable cardinality-only
parent `170e9a...` (session 40930, exit 1) passed the old scalar 21000 assertion
and stopped later at line 2764, `UPDATE jt_view SET val='v2' WHERE bid=10`.
That joined-view mutation failure remains open and is not bypassed here.
