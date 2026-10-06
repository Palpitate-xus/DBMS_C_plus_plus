# Genuine prepared ANY / ALL / SOME execution

## ROOT integration contract

ROOT merges this change with the independently committed genuine source-context
and ordinary CASE consumers. The conflict resolution keeps the strong static
projection labels, owned view queries, typed logical occurrences, lazy JOIN
reader and row-independent predicate guard, together with this change's paired
child cursor, actual ProjectSet/EXPLAIN graph and runtime-error descriptor.
Root CTE definitions are installed once; lowering validates used readers and
all writers without executing an unused reader to discover its descriptor.

Correlated logical sources advertise restart only when their owner supplies a
real rebinder. It closes the previous source once and purely constructs a fresh
provider/cache for the new typed outer row. JOIN close traverses both real
children and preserves the first error. FROM-less input is an actual empty-row
source with the current outer context in the execution state, not a closure
capturing the first invocation. Captured CTE frames still require a separate
restart contract. The native source-context test verifies real changed cells,
pure rebind, close-once and preservation of close errors. These integration
changes require ROOT's new all58 fresh optimized build and original strong
source/correlated-demand gates; private proof below is not that new combination.

This is a scoped implementation and regression proof. It does not close the
SQL preparation/type, subquery, array, SRF, operator or EXPLAIN families, and
does not claim a full default protocol/full registered suite pass.

## Original defect and retained expectations

The original full protocol stopped at its quantified EXPLAIN assertion: the
query `id > ANY (SELECT ...)` worked in the legacy spaced path, but EXPLAIN
reported `42703 column select does not exist`. Compact ANY/ALL also reported
that error. `original-baseline-tmpfs.log` retains twelve explicit controls:
two spaced queries passed; compact queries and eight TEXT/JSON plan controls
failed. Earlier startup errno103 and setup read-timeout attempts remain in
`original-baseline.log` and `original-baseline-repeat.log`; these are not SQL
semantic failures or proof of a disk performance repair.

The original thirty version-checked demand expectations were kept. They were
expanded, never replaced or lowered: the final matrix has 84 cases, an ordinary
three-row receiver control and twelve plan publication/demand checks. The
reference is explicitly PostgreSQL 18.6 (`180006`) on the isolated TCP endpoint.
Older PostgreSQL 17.2 diagnostics remain labeled 17.2.

## Preparation and runtime contracts

`QuantifiedComparisonExpr` is a genuine AST role, not an ANY function-name
heuristic. It owns the left/right expressions and quantifier; SOME canonicalizes
to ANY. Parser/binder/type/clone/collation/identity/routine/constant-CASE walkers
traverse the role explicitly. Raw source positions, typed ParameterExpr cells,
canonical names, original child sites and source occurrence identities survive.

Whole metadata-only preparation finishes before opening a child or writing
CTE. SQL children require exactly one output column (`42601`); SQL unknown
outputs finalize to text before operator resolution. An array RHS uses its
actual element descriptor, including NULL, and a scalar RHS reports `42809`.
The resolved builtin operator retains actual left/right operand signatures,
strictness, collation and implementation identity/capability. Raw `=` alone is
not permission to hash. Recognized unsupported types/operators do not execute
a routine to infer their signature. The builtin resolver is intentionally not
a complete PostgreSQL operator/cast catalog.

SQL children use `PreparedQueryCursor`: pure construction/descriptor access,
lazy actual graph opening, typed ordinal cells and explicit close-once. They
borrow the calling xid/CID/read view and never advance a PL SPI command merely
to read a child. There is no appended SQL LIMIT, OFFSET rerun, eager full-row
compatibility query or display-string reconstruction in this receiver.

Scan mode retains an execution-owned incremental spool. ANY stops on true,
ALL on false; later needed rows resume the same cursor. It evaluates its left
test for each demanded RHS tuple, including cached tuples, but not for empty
SQL input. Separate genuine child sites have separate state. Hash-capable
uncorrelated equality ANY/SOME instead reads the RHS once before evaluating
the left once; empty input still skips the left. Typed comparator rechecks
remain authoritative, including a linear compatibility recheck of the spool;
this is not a hash performance claim. NULL/empty/three-valued outcomes match
the retained oracle. Array construction evaluates the whole actual array first,
and array empty input still evaluates the left once.

Correlated physical child plans explicitly close/rebind/restart with current
typed outer cells. A captured correlated CTE frame is not falsely reset by
changing a projection row. Such a provider still requires a genuine restart
contract. Primary SQL/C++ errors win over secondary close errors; destruction
does not throw, partial cells are cleared and interrupts stay active.

The source provider supports the tested physical, derived VALUES and read/write
CTE shapes with actual AST/descriptor ownership. Successful writing CTEs finish
once even under LIMIT 0; preparation errors run no writer. A naked fromless
`unnest(array)` uses an actual typed ProjectSet node. Its complete array argument,
NULL/empty/multidimensional output and demand are tested, not a scalar stub.
More complex SRF, relational JOIN/view, recursive/set/window/aggregate, record,
array-valued operand, custom operator/type and correlated CTE restart shapes
remain separate work. Legacy standalone VALUES/rewrite paths without a query
owner explicitly reject this role instead of silently classifying a new node.

TEXT/JSON EXPLAIN consumes the same actual graph, including actual child plans.
Plain EXPLAIN performs preparation only. ANALYZE runs demand once; telemetry
belongs to those operators, and publication waits for statement/constraint/
commit success. Failed ANALYZE publishes no partial plan. Runtime query errors
retain the already-exposed typed descriptor without success rows/tag, while
preparation errors expose no descriptor. New DmlResult metadata and evaluator/
AST/factory fields require a completely fresh 58-TU combined build.

The primary SQL NULL/empty rules are in PostgreSQL's
[subquery comparisons](https://www.postgresql.org/docs/18/functions-subquery.html)
and [array comparisons](https://www.postgresql.org/docs/18/functions-comparisons.html).
Actual demand was checked against the versioned
[planner](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/plan/subselect.c)
and [executor](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeSubplan.c),
not inferred from the SQL spelling alone.

## Failure history and independent dependencies

Artifact directory: `/tmp/dbms-quantified-comparison.oUtteDhP`.

- V1/V2 retained real alias, unknown-query type and runtime RowDescription
  mismatches, followed by correct metadata/source/descriptor fixes.
- V2 native `with_logical_source_plan` failed its original callback assertion.
  An explicit full-row owner now remains in charge unless it supplies the paired
  logical cursor. The default physical cursor is not silently substituted.
- Operator probes retained genuine BIGINT/float4/float8/operator/character/
  collation differences; the V3 correction passed those fourteen controls.
  Its three remaining finite interval differences were separately repaired.
- V4/V5 retained sequence rollback counter failures; expectations were not reset.
  Four independently verified sequence commits are dependencies, not a quantifier
  sequence workaround: `2d454ec9`, `4610f2d5`, `fbbe35b3`, `487b7caf` (private maps
  `29d27839`, `8875bd0b`, `83dff6ef`, `d89c0bba`). ROOT integrates its own maps.
  V4's brief overlap with a peer server remains recorded, not final serial proof.
- After sequence integration, V5's 75-case matrix passed, but eight adjacent
  scripts had three actual failures. The default physical child cursor passed
  SQL-quoted qualified names to storage APIs that require stored identifiers.
  It now consumes canonical prepared public/custom/temp identities. The original
  scalar empty/cardinality controls and a new real native three-namespace gate
  pass; none was removed or relabeled.
- NAME numeric-looking comparisons and then implicit-C ordering exposed actual
  mismatches. Those narrow shared repairs are independently committed and
  documented, as is finite interval value comparison.
- Native fixture compile errors (wrong result fields and missing helper arguments)
  and an invalid temporary table setup remain retained. They were corrected
  without changing the semantic assertions. Four legacy switch warnings were
  addressed by explicit traversal/unsupported contracts, not ignored enum roles.

## Final verification

The private source parent is ROOT `e16844aa` (production `07803aef`) plus the
four independent sequence dependencies. No ROOT source/cache was edited.
The official manifest has 58 TUs. Private development flags are the official
flags with `-O2` replaced by `-O0`; this is not an optimized ROOT build claim.
Full fresh group 56587 exited 0. Later changed-CPP rebuilds/relinks retained the
same headers and audited every source/header/flag signature, repeat and binary
stamp. The final exact source/binary/frozen artifact audit is recorded below.

`quantified_query_execution_test.cpp` tests pure metadata, actual cursor identity,
stream/hash/spool/reuse, NULL/typed parameters, numeric precision/NaN/signed zero,
primary-error priority, native public/custom/temp physical child plans and real
scalar cardinality. The native cursor/EXPLAIN tests also retain caller NULL-row,
xid/CID/read-view, limits, partial failure and cleanup controls.

Final scoped gates comprise twelve separate native executables; the full 84-case
strict-18/candidate matrix plus ordinary receiver and twelve TEXT/JSON demand/
publication controls; twelve original spaced/compact queries/plans; unchanged
`quantified_subquery` and `any_all_null` strict-18 differential files; and eight
serial neighboring protocol scripts. Every owned server is stopped in finally.
TMPDIR=/dev/shm was selected for semantic isolation, with original SQL/deadlines
unchanged. Disk I/O, durability under power loss and full-suite completion are
not inferred from these scoped results.

Final build 41140, serial protocol/differential group 39981, twelve-native group
91901 and freeze/audit 70008 all actually exited 0. Logs are respectively
`build-v8-name-c.log`, `final-v8-serial.log`, `native-v8-final.log` and
`freeze-v8-final.log`; the strict oracle log is
`reference-demand84-explain-18.log`. Earlier groups/failures were not overwritten.
The freeze includes all 58 production objects/signatures, complete source and
header manifests, test manifests, flags, stubs and the binary:

`/tmp/dbms-quantified-comparison.oUtteDhP/final-immutable.Y3P6iPUg`

Final binary SHA-256:
`950495e8ffb22122f70fa999dbad2750603416c4115719670f71bd5be5321be1`.
The copied stubs hash equals the final native group's stubs hash. Source/header/
test content audits passed; an initial manifest command used the wrong working
directory and is retained separately from the corrected successful audit.

ROOT integration must retain the separate NAME and INTERVAL comparator commits,
all four READY sequence dependencies (its own maps), CASE implicit/equality/
constant-planning metadata, ARRAY constructor/concat metadata and RETURNING
copies, typed ParameterExpr/RowContext ownership, checked `throwIfFailed` and
deferred transaction ownership. No new production TU is added, but changed
AST/evaluator/query callback/DmlResult/factory headers require all 58 fresh
normal-O2 objects; the private O0 donor objects are not an ABI shortcut.
