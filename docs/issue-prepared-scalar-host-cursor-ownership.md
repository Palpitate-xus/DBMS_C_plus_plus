# Explicit scalar hosts beside quantified cursors

## Actual regression and root cause

The quantified integration `4f23f997` (documentation HEAD `6178ea7c`) introduced
a genuine native execution-contract regression. The existing
`ordinary_scalar_where_plan_test.cpp:63` and
`ordinary_scalar_order_plan_test.cpp:51` install an engine-level ordinary query
host and assert its exact invocation counts/results. Both aborted at their
unchanged assertions against the matching normal-O2, fresh-58-TU baseline.
These are not obsolete fixtures.

The default prepared plan had a physical child cursor. Its scalar child consumer
unconditionally preferred that cursor, even when the actual StorageEngine owner
had an explicitly installed `PlpgsqlQueryExecutor`. Checking only a borrowed
plan's explicit `PreparedChildExecutor` was insufficient: the engine-level host
is another supported scalar execution contract.

Baseline binary SHA:
`a7a2c010135595ab8ce176e73c8aba2e37d039018ade470857d5168da776451e`.
Every donor production object/source/header/flag signature was checked before
copying into task-owned immutable baseline objects. Original assertions were
re-run independently: both exit 134. Logs are retained, not relabelled green.

## Ownership contract

StorageEngine now exposes a pure `hasPlpgsqlQueryExecutor()` observer. It neither
prepares nor executes SQL; callback installation still precedes backend threads.
The carrier's `setChildCursorFactory(factory, ownsScalarChildren)` and the
borrowed prepared-select factory explicitly distinguish a paired execution
owner from a default physical fallback. Existing explicitly supplied paired
cursors keep their default scalar ownership.

| Execution owner | Scalar child | Quantified SQL child |
| --- | --- | --- |
| Explicit paired cursor | That cursor | That cursor |
| Explicit full-row reader, without paired cursor | That reader | Genuine default cursor |
| Installed engine ordinary-query host, without paired cursor/reader | That host | Genuine default cursor |
| No explicit reader or engine host | Physical fallback cursor | Genuine default cursor |

The distinction is declared by the producer, not inferred from SQL text, values,
function names, row counts, or an evaluated side effect. Default SELECT and VALUES
child graphs preserve the fallback role recursively. Quantified evaluation still
requires its original typed lazy cursor and never switches to an eager scalar
host or SQL LIMIT rewrite. Scalar memo remains keyed by the genuine original
expression site, including nested/correlated children. Typed parameter/ancestor
adapters and OrdinarySubquery/maxRows=2 remain the existing scalar boundary.

An empty/cleared engine host restores the physical scalar fallback. A host change
before an already prepared plan begins execution is observed without pre-running
that host during metadata preparation.

## Actual new-candidate validation

The private candidate was compiled from **all 58 fresh production TUs**, with
official flags except O2 replaced by O0. It is not a normal-O2 production claim.
Build session 58279 actually exited 0; normal repeat, all source/header/flags
object signatures, and binary configuration stamp also matched.
Final candidate SHA:
`073d48cb039f3dca652bd4994824aad9e2f355ccbc19b246ca634430d893b28b`.

Session 87086 actually exited 0 for all **15 native tests**:

- Both unchanged original scalar WHERE/ORDER tests.
- New `prepared_scalar_host_cursor_contract_test`: mixed scalar/ANY/ALL,
  scalar inside a quantified graph, VALUES, separate genuine sites,
  correlation, SQL NULL, zero rows, exact 21000/P0002/22P02 and no partial rows,
  explicit row-reader versus paired-cursor priority, pure preparation, and host
  clearing before execution.
- The prior twelve quantified/adjacent native controls, including the genuine
  logical-source callback, cursor descriptor/demand/cleanup, actual owner,
  comparison codecs, sequence rollback/migration, and typed EXPLAIN.

Session 3921 actually exited 0 for **11 serial protocol invocations**:
84 original quantified-demand controls plus the ordinary receiver and 12
TEXT/JSON EXPLAIN/effect checks; both unchanged `quantified_subquery` and
`any_all_null` strict-reference differential cases; and eight adjacent suites
(ordinary scalar WHERE/ORDER, scalar sort slots, WITH scalar, typed EXPLAIN,
PL query binding, INTO demand, and stored-function atomicity).
The unchanged 84-control/EXPLAIN reference separately passed against actual
XML-enabled PostgreSQL **180006**, with the owned en_US/libc database.

Protocol semantic gates used task-scoped TMPDIR `/dev/shm`, preserving the
original 15-second deadlines. This is not disk-performance or durability
evidence. All owned test-server processes were cleaned up and all handles
terminated. No full original protocol or full registered suite was run for this
private candidate; ROOT's separate matching normal-O2 integration is required.

Artifacts and all historical failures:
`/tmp/dbms-scalar-host-cursor-contract.ygxQundx/` (`baseline-inputs.log`,
`baseline-native.log`, `build-v1.log`, `native-v1.log`,
`reference84-explain-18.log`, `wire-v1-serial.log`).

## Integration boundary

The carrier gains a private boolean and three public headers change. ROOT must
freshly compile **all 58 TUs plus native stubs/tests**; old-layout objects may not
be combined. Preserve ROOT's prepared source-context/view ownership, later pure
constant-planning fields, real typed parameter cells, and checked-result
`throwIfFailed` contract. This fix changes callback precedence, not the quantified
comparison capabilities or any unsupported-shape boundary. It does not fix the
separate CTE heap-cache flush lifecycle or establish a cause for old ADD/DROP
timeouts.
