# Set-query descriptors use the actual common input type

The pure binder used only its left SELECT's output type. The matching old
native test reports 14 failed assertions and exits 134: BIGINT/array width,
all-NULL TEXT and contextually typed NULL/string outputs are wrong, while
bad UNKNOWN input and incompatible known input types incorrectly prepare.
These are failed controls for one binding root, not 14 independent bugs.

The binder now transforms both actual operands, checks width, selects the
builtin common type by output ordinal, and applies analysis-phase conversion
only to genuinely UNKNOWN value sites. It does not execute functions, query
children, arithmetic or known-type casts to discover a result. A replaced
value root updates the same live `projectionBindings`/leaf metadata and keeps
raw source spans, parameters and output labels intact.

`PreparedQuery::setOperationInputs` is a new public, execution-independent
map keyed by actual `SelectStmt*`. It retains both branch/body descriptors:
known INTEGER input can remain INTEGER even when the set output is BIGINT.
The inline-left and composed-wrapper AST shapes remain unchanged. No SQL is
rendered into an invented left child. A future typed set operator must consume
these exact body descriptors and perform genuine per-row common-type coercion.
The new map changes public layout; all 58 production objects and stubs must
be rebuilt together. There is no new TU.

## Evidence and boundaries

Private tree `/tmp/dbms-bound-dml-cursor.rTuMF7gk/repo`, from `be66e5a6`.
Immutable artifacts are in its parent directory.

- `baseline-set-binding/native.log`: old matching native 14 failed assertions,
  exit 134. Original queries and all strong expectations remain checked in.
- `candidate-set-binding-build.log`: wholly fresh all-58 O0 production/stubs,
  source/header/flags audits and eight distinct native tests pass. SHA256
  `562bb8d30bc8f50779b1fd478372f64aba66cad09e158c29c78c35ca7934598c`.
- `set-binding-reference18.log`: the full 18-query Parse/Describe diagnostic
  passes strict PostgreSQL 18.6 / 180006. No Execute is sent, the VOLATILE
  writer's sequence remains uncalled, and set outputs have no physical origin.
- `candidate-set-binding-adjacents.log`: eight complete serial scripts pass
  (ordinary Q DML, WITH physical restart, primary/multisource DML, Q demand,
  PL binding, CASE and array concat); owned servers are finally stopped.
- Native additional assertions cover execution-free function metadata,
  retained INTEGER parameter slots, new UNKNOWN CAST/projection-root identity,
  original byte spans, labels, and both levels of a three-branch AST.

`set_operation_type_metadata_known_gap.py` is a checked-in, unregistered
required diagnostic, not a downgraded green mode. Its matching old and new
whole protocol logs both remain exit 1: ordinary Parse/Describe still lacks
the consumer, including original 22P02/42804/42601/42883 expectations. This
metadata foundation does not claim that protocol gate or typed set execution
is fixed. The complete 37-query bound-DML diagnostic still retains its UNION
ALL execution and local-Var ANY planning failures.

The helper currently implements the existing builtin common-type contract;
catalog domain/user-cast rules, set ORDER/LIMIT ownership and complete
UNION/INTERSECT/EXCEPT execution remain independent open work. ROOT's pending
OID-aware descriptor extension is not in this private header epoch. On merge,
changing a common type must clear a stale left/domain OID or obtain the actual
selected type OID; a guessed OID must never accompany a changed type spelling.
