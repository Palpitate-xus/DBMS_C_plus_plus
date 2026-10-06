# ARRAY constructors and concatenation: typed execution

Scope: the original `array_funcs` array `||` RowDescription failures and the
constructor/operator paths needed to resolve and execute them correctly.
This is not completion of every array, cast, function or prepared-cache feature.

## Actual baseline

Source parent is `fd183ec3`. The frozen production baseline in
`/tmp/dbms-canonical-input-cursor.4U3gkfUf/repo/dbms_main` returned OID 25 for
integer/text array concatenation; PostgreSQL 18.6 returned 1007/1009 with the
same rows and tags. Locale does not determine these OIDs.

The explicit version-checked oracle used the isolated TCP PostgreSQL 18.6
instance (`server_version_num=180006`), not the older `pgref` container.
`/tmp/dbms-array-concat.zIUClAoz/reference18.log` retains the first 26 controls;
`checked-reference18-v6.log` retains the expanded oracle. The historical
baseline and intermediate failures are not relabelled as passes.

Baseline mechanisms:

* ARRAY grammar became an internal function returning TEXT, not an ArrayExpr.
* `||` selected array concatenation from brace-looking values. Consequently
  ordinary TEXT `'{1,2}' || '{3,4}'` was also changed into an array value.
* The binary result inferencer declared TEXT; SQL NULL arrays were treated as
  strict text concatenation, and typed scalar prepend/append was missing.
* A cast inside ARRAY consumed its closing `]` as part of the type label.

## Implementation contract

ARRAY now retains genuine expression children, fixed element metadata and a
static nested/scalar mode. Whole-query binding and the scalar helper resolve
common element types and `||` overloads before evaluating children. Operand
datums do not choose a type or a namespace. Unknown Const inputs use only the
primitive input codec during preparation; routines, sequences, source rows and
queries are never executed to infer metadata.

The typed operator preserves NULL array identity, NULL scalar elements, numeric
common types and compatible multidimensional concatenation. TEXT brace
lookalikes remain TEXT. Execution-owned AST copies preserve `elementType`,
`nestedElements` and `BinaryOpExpr::arrayConcat`; sort identities include array
metadata. The PL scalar host binds names first, prepares ARRAY types with the
actual routine owner, and only then lowers routine names to private callbacks.

Supported single-source ordinary queries reuse the existing prepared typed
executor and its real statement owner, receiver demand and structured errors.
Database-independent scalar queries retain their established no-database
consumer. The existing builtin result inferencer now declares fixed INTEGER
results for array inspection functions; polymorphic array results retain their
canonical declared array type at the evaluator boundary.

The original `array_expr_test` SQL `ARRAY[]` remains tested as 42P18, with a
typed-empty positive added. The original mixed scalar/nested-array SQL remains
tested as 42804, with a rectangular containment positive added. These are
PostgreSQL-confirmed invalid old expectations, not removed controls.

## Evidence and boundaries

All artifacts are under `/tmp/dbms-array-concat.zIUClAoz/`.

* `checked-baseline-fd.log`: original integer-array OID assertion fails.
* `baseline-fd.log`: full exploratory baseline, including wrong NULL/operator
  results and side-effect observations.
* `candidate-v1.log` / `native-v1.log`: real closing-bracket cast failure.
* `candidate-v2.log`: missing NEXTVAL result metadata caused a TEXT array.
* `native-v2.log`: observer used the preceding command snapshot; the strengthened
  actual-write check starts a new SQL command before observing writes.
* `native-v3.log`: original invalid empty-array fixture actually aborts.
* `native-v4.log`: genuine ARRAY was not consumed by the PL scalar host.
* `native-v5.log`: eight actual native passes followed by a test-group filename
  typo; this failed group is not described as an all-pass run.
* `array-funcs-diff-v5.log`: four real inspection-result OID regressions after
  typed routing, then `array-funcs-diff-v6.log`: the unchanged original 13 SQL
  statements pass strict PostgreSQL 18.6 comparison.
* `native-v6.log`: 14 actual native passes, including actual-owner writer
  preparation with zero writes, execution/rollback, stored array return,
  canonical typed cursor consumption, scalar resolver and array inspection.

Final matching-source verification, after registration and the last helper-only
rebuild, is terminal: `build-v7-final.log` / `freeze-final.log` retain the
up-to-date repeat, all 58 source/header/flag signatures and binary stamp. The
58-object private O0 group was freshly built at V5; later changed CPP objects
were rebuilt and relinked against that unchanged header group. It is not
described as 58 new compilations at V7. Final executable SHA-256 is
`f05dd307ae01a62567a18f285138fd84545c371c21f28eeb9539209b73d36b89`.
The immutable executable, all objects, flags, source/header hashes and manifest
are in `final-immutable.9ZLJVvkL/`.

* `checked-reference18-final.log`: all 45 final controls pass against actual
  180006 (40 value/type/error cases, two empty-input negatives, three table-array
  cases), unchanged sequence-effect assertions and transaction rollback.
* `wire-final-repeat.log`: the same 45 controls plus seven adjacent scripts
  pass, including binder, scalar WHERE/ORDER, atomicity, SubLink sort slots,
  typed EXPLAIN and the original 50ms/15sec lock/recovery gate.
* `wire-logical-source-final.log`: both literal-source RowDescription and
  extended/deferred transaction-demand controls pass; physical/routine owners
  remain required and writer rollback/recovery expectations are unchanged.
* `array-funcs-diff-final.log`: the original 13 `array_funcs` statements pass
  strict row/type/tag/error comparison with PostgreSQL 18.6 on the final binary.
* `native-final.log`: the three final ARRAY native tests and the literal owner
  native pass before a nonexistent requested filename aborts that group.
  `native-logical-source-final.log` reruns both actual owner/deferred native
  fixtures successfully. `native-adjacent-final.log` passes all 11 remaining
  adjacent natives on final source-matching objects. Thus all 14 array/adjacent
  native gates and two logical-source gates are covered, not that the failed
  filename group passed.
* `wire-final.log`: the first final wrapper failed argument parsing before SQL;
  that log is retained separately from the successful repeat.
* `array-positions-baseline-fd.log` and `array-positions-final.log`: both retain
  the independent 42883 unsupported-function failure, with PostgreSQL's
  original `{1,3}` / OID 1007 expectation unmodified.

The source fix and the PostgreSQL-confirmed old-fixture corrections are separate
commits; both are required for the reported 14 native gates. No old-ABI objects
are borrowed from ROOT. A later ROOT integration must preserve the three array
metadata fields in its newer RETURNING AST clone as well as CASE's
`CastExpr::implicit`, and freshly build all 58 objects against the combined
headers. This private parent predates that CASE/RETURNING ABI, so it does not
claim to verify their future combination. Test-owned
protocol processes are always stopped by their fixture. Tiny wire controls
use explicitly recorded `/dev/shm` test directories with unchanged deadlines;
this is not a durability or full disk-I/O validation.

Open: `array_positions` has no registered callback in the baseline. The original
PostgreSQL expectation is retained in `tests/array_positions_known_gap.py`, not
registered as a green test. Full support requires typed array datums with real
dimensions/lower bounds and element equality, not a raw string-position scan.
Bound-prefixed literals, every cast/typmod/collation combination, multidimensional
indexing, broader query shapes and schema/routine cache invalidation remain
separate work. The quantified-query AST/cursor task is still open.

Primary semantics: PostgreSQL's [array operators](https://www.postgresql.org/docs/18/functions-array.html)
specify common element coercion and non-strict NULL/empty array concatenation;
the [constructor rules](https://www.postgresql.org/docs/18/sql-expressions.html#SQL-SYNTAX-ARRAY-CONSTRUCTORS)
require an explicit type for an empty constructor and compatible element types.
