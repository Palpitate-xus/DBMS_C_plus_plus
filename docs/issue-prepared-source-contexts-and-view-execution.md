# Prepared source contexts and view execution

Ordinary CASE needs a typed consumer of the complete prepared query, including
its actual FROM occurrences. A physical single-table descriptor cannot stand
in for a view, a logical CTE or a JOIN. The source correction preserves typed
cells and NULLs by source ordinal, owns independently prepared view queries,
and reads ordinary SELECT sources according to row demand.

## Observed failures

The reference is isolated PostgreSQL 18.6 with a strict `180006` version gate.
The permanent ordinary protocol fixture retains the original CASE assertions
and adds source, width, NULL, qualification and sequence controls.

| Query shape | Observed candidate failure | Required result |
| --- | --- | --- |
| CASE over a simple view | `0A000` in the initial typed consumer | Correct ordinary view rows |
| CASE over supported pg_settings columns | `0A000` | TEXT values and genuine unit NULL |
| CASE over a CTE whose read query joins repeated ranges | `XX000` | Independently bound quoted wide and narrow cells |
| Volatile JOIN ON with LIMIT 1 | Sequence advanced four times | One pair evaluation |
| View with bare NULL and empty string outputs | `XX000` declared output mismatch | TEXT NULL and empty TEXT, both OID 25 |

The original legacy candidate returned the simple view result correctly,
so the initial view rejection was a development regression. Its pg_settings
CASE and CTE read JOIN already failed with `0A000` and `42703`, respectively;
these two were not falsely counted as previously passing regressions.

The initial source expansion failed 23 assertions, including consequences of
missing result metadata. After genuine source lowering, the strengthened
JOIN demand control independently exposed the four-versus-one sequence error.
The UNKNOWN view control then exposed a separate output-boundary mismatch.
All failures and the intermediate successful narrower matrices are retained.

## Source and execution contract

`Operator.lastPreparedContext` transports complete bound row contexts through
filter and sort. `PreparedSourceContextsOp` reads real occurrences; projection
and ORDER consume the same cells rather than a synthetic flattened schema.
Stars follow actual range visibility, including USING merged columns and
qualified original columns. Parameters and correlated outer cells remain in
the same context.

`SourceRange.viewQuery` owns a separately prepared view AST and its namespace.
View preparation does not inherit caller PL datums, CTEs or sibling ranges.
UNKNOWN view outputs acquire genuine implicit TEXT casts at the relation
boundary, not a guessed type from the first row. View output values are then
bound to the caller's actual view occurrence.

The existing source provider now reads leaf cursors lazily and advances JOIN
pairs lazily. FULL JOIN matched flags identify row occurrences, including
duplicates. A false row-independent WHERE does not open a volatile JOIN
source. DML can explicitly demand the full provider before mutation; ordinary
SELECT and LIMIT do not implicitly request all rows. Closing a source closes
its owned cursor and preserves the primary exception.

pg_settings reuses the existing typed generator for its three supported
columns. This does not claim support for PostgreSQL's other fourteen columns.
No source row or routine is executed to discover a descriptor.

## Verification and artifacts

The private artifact root is `/tmp/dbms-simple-case-binding.KtyWx31T`.

- `build.sources.log` and `sources/obj` contain a fresh build of all 58
  production sources and fresh stubs after the SourceRange and Operator
  header changes. Every source, header and compiler flag was audited.
- `ordinary.sources.baseline.log` retains the 23 source assertions.
  `ordinary.sources.candidate.demand.baseline.log` retains the actual four
  sequence calls. `ordinary.sources.candidate.v2.log` and
  `ordinary.sources.candidate.final.log` retain the UNKNOWN view mismatch.
- `ordinary.reference18.sources.final.log` and
  `ordinary.sources.candidate.v3.log` are terminal successes for the original
  CASE controls plus the complete source expansion and row-demand controls.
- `sources.native.log` contains twelve successful fresh O2 native tests.
  `build.source-final-v3.log` contains six matching final native tests after
  the UNKNOWN view casts, including the dedicated source-context test.
- `sources.asan.log` contains four successful native tests with ASan and
  UBSan instrumentation in query binding, prepared execution and execution
  plans. The other production objects are matching uninstrumented objects;
  this is a scoped sanitizer check, not an all-58 sanitizer build.
- `source.adjacent.protocol.log` contains eleven successful matching protocol
  scripts: common CASE, equality, constant demand, VALUES types, labels,
  multisource DML, its demand boundaries, primary WITH DML, WITH transitions,
  typed arrays and PL SELECT INTO. Each server is independently cleaned up.

`source-demand` replaces only main after lazy JOIN demand, and
`source-final-v3` replaces only query binding after the view boundary casts.
All remaining source/header/flag signatures match the immutable all-58 donor.
No old public layout or unrelated QuantifiedComparison layout is linked.
The source contract and ordinary activation are two logical commits; this
full matrix verifies their combination. The source-context native control
uses the public contract without main's ordinary dispatch hooks. Activation
is required for the new ordinary CASE protocol matrix.

## Remaining boundaries

The separate ordinary CASE activation uses this contract. CASE column labels
inherited from a strong ELSE column or function have a dedicated actual
follow-up; this correction does not silently declare every metadata shape
complete. Aggregate, window, set-returning, custom operator/domain and broader
virtual catalog planning remain separate issues. Existing simple CASE literal
width, geometric comparison and INTERVAL comparison diagnostics remain open.
View definer permissions and saved creation-time search-path semantics are not
claimed complete by this read-source correction.
