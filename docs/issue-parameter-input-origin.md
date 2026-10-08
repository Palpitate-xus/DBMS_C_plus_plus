# Parameter cell origin and BETWEEN demand

## Finite root cause

The demand rewrite in `d14a008a` tested the current NULL flag of every
`ParameterExpr` as though it were a frozen statement Bind input. The aggregate
executor also represents real input columns and computed aggregate results by
parameter slots. A NULL row or NULL SUM is not a statement constant: PostgreSQL
still demands the other operand of each strict comparison. The incorrect
shortcut omitted 26 actual writer effects in the unchanged 32-control aggregate
probe. Values, NULL bits and result descriptors alone did not expose the error.

This commit distinguishes three actual producers rather than inferring their
role from a slot, value, name, source position or SQL spelling:

| Origin | Actual ownership | NULL may prune strict peer demand |
| --- | --- | --- |
| `StatementInput` | Frozen query/interpreter input datum | Yes |
| `RuntimeCell` | Row, aggregate result, array element or comparison cell | No |
| `MetadataPlaceholder` | Pure analysis/Describe datum without a runtime input | No |

Raw `ParameterExpr` construction defaults conservatively to `RuntimeCell`.
The real SQL `$n` parser explicitly assigns `StatementInput`. Existing
seven-field `QueryBindingDatum` API initializers retain their statement-input
contract; runtime and metadata producers explicitly override that default.
The binder copies the datum origin into the actual executable node.

No public enum/window/CTE/aggregate/signed-FETCH fields, source coordinates,
parameter values, SQL NULL bits, source ordinals or catalog identities are
removed. The one existing full-node parameter copy in
`prepared_query_execution.cpp` preserves the new field automatically. Moving
nodes into casts and rebasing source coordinates does not recreate their origin.

## Complete producer and consumer audit

The production audit found 14 fresh parameter-node constructor sites and one
full-copy site. Each fresh producer is explicit:

| Producer | Sites | Assigned origin |
| --- | ---: | --- |
| SQL parser `$n` | 1 | `StatementInput` |
| Binder from a real `QueryBindingDatum` | 1 | Copy the actual datum origin |
| Set append conversion, set sort comparator, set ties comparator | 5 | `RuntimeCell` |
| Array input normalization and ALTER array-element conversion | 2 | `RuntimeCell` |
| Aggregate binary operands, aggregate conversion, actual input column and aggregate-result lowering | 5 | `RuntimeCell` |

The actual datum producers are also explicit: interpreter scope/frame/variable
snapshots are statement inputs; domain VALUE validation and protocol metadata
probes are metadata placeholders; OLD/NEW runtime-row bindings retain runtime
origin. The last assignment changes provenance only, not trigger execution or
dispatch. No filtered trigger/view/temp/WAL/EXPLAIN issue is implemented here.

The BETWEEN `nullInput` consumer permits the NULL shortcut only for statement
inputs. Its transparent primitive cast recursion and literal-NULL handling
remain unchanged. Two row-independent predicate admission consumers likewise
accept only statement-input parameters. Scalar/sort identity now includes the
explicit origin, so identical slot/type with different demand semantics cannot
share an interchangeable scalar key. Existing constant-planning predicates
which conservatively reject all parameter nodes remain conservative; result
type inference still uses the declared type, not the origin or row datum.

Preparation never executes a function, parameter callback, scalar child,
volatile writer, aggregate or source reader merely to discover origin or type.
The original range-left repetition, pair-by-pair coercion, lazy AND/OR demand,
three-valued logic and genuine fourth left-expression copy remain intact.

## Permanent strong controls

`parameter_input_origin_test.cpp` contains 127 assertions: public defaults,
actual SQL parsing, seven-field datum compatibility, role-sensitive scalar
identity, genuine full copy, 60 compiled owner/NULL/CAST/operator/demand cells,
and default raw-runtime AST behavior. Its real UDF INSERT effects distinguish
statement NULL pruning from runtime/metadata NULL peer demand.

`parameter_input_origin_protocol_e2e_test.py` keeps all 112 controls:
64 real Parse/statement Describe/Bind/portal Describe/Execute cases, with actual
1560/1562 parameter OIDs, NULL and non-NULL values, two operators and four
positions; plus 48 real row and computed-aggregate NULL/empty/non-NULL cases.
Every case checks values, NULLs, Boolean OID/width/typmod, tag and writer effects.
Parse/Describe checks that no writer has run. Reference connections verify
`server_version_num = 180006` and use only uniquely owned schemas, removed in
`finally`. Default 15-second wire and 20-second startup deadlines are unchanged.

The separate unchanged 32-control parent probe is retained in full. Its two
old COUNT(*) FILTER BETWEEN `0A000` cases remain unsupported and are not omitted
or reclassified. The other 30 cases pass after this finite origin repair.

## Separate correlated fallback issue remains OPEN in this commit

`parameter_origin_correlated_child_test.cpp` retains all 24 actual cursor and
legacy-fallback cases plus its reach assertion. The genuine direct public
execution owner, without a fake provider/executor, takes a scalar child path
which renders correlated runtime cells into typed SQL literals. That old
adapter loses the origin before the next consumer binds/evaluates the query.
For BIT/INTEGER runtime-NULL left operands and both operators, the d14 snapshot
and this origin-label snapshot still omit both writer demands: four failures.
The pre-demand Root104 snapshot passes all 25 assertions. These are four new
d14 regressions, not old unsupported behavior and not fixed by this commit.

The separate 12-case protocol fixture passes with strict PG18.6, Root104, d14
and this candidate because Main owns a genuine typed child cursor for those
queries. That fact does not cover the real native legacy-fallback owner.
A following independent root-cause commit must carry actual typed metadata and
`QueryBindingDatum` origin across that consumer boundary; a rendered literal,
string marker or datum-value heuristic is not a repair.

## Exact private proof

Base: immutable Root104+d14 snapshot `86a8e6e6` in
`/tmp/dbms-root-between-demand-current.LbMkJgQW/repo`.
Candidate: `/tmp/dbms-parameter-origin-fix.ZzkhuGfN/repo`.
Both appended public fields require a genuinely fresh normal all-58-TU build;
no object from an old ABI is borrowed.

The first candidate normal build, session 7382, completed with exit 0 and
exactly 58 fresh compilation lines in `parameter-origin-normal-fresh58.log`.
The repeat/final native collection, session 92720, verifies every current
source/header/flag receipt and the all-58 build stamp before linking tests
against the candidate's own 57 normal O2 project objects. It exits 1 only for
the explicitly retained four correlated fallback failures; the new native127,
original native587 and aggregate/sort neighbors each exit 0.

Frozen candidate binary:
`/tmp/dbms-parameter-origin-fix.ZzkhuGfN/dbms_main.parameter-origin-v1.frozen`,
SHA256 `c04394c31780f8eeeb37e9e680794360deeda133f25da7a75b88848f9bf2e9e6`.

| Complete unchanged control | Actual result | Evidence in the candidate artifact directory |
| --- | --- | --- |
| New112 strict PG180006 | 0 | `parameter-origin-new112-strict.log` |
| New112 old d14 baseline | 1, 32 actual effect failures | `parameter-origin-new112-baseline-d14.log` |
| New112 candidate | 0 | `parameter-origin-new112-candidate-v1.log` |
| Parent original32 candidate | 1, only two old COUNT FILTER cases | `parameter-origin-original32-candidate-v1.log` |
| Correlated24/25 Root104 native baseline | 0 | `parameter-origin-correlated24-baseline-root104-real-data.log` |
| Correlated24/25 old d14 native baseline | 1, four new fallback failures | `parameter-origin-correlated24-baseline-d14-real-data.log` |
| Correlated24/25 candidate native | 1, same four failures | `parameter-origin-normal-native-v1.log` |
| Correlated12 strict/Main candidate | 0 / 0 | `parameter-origin-correlated12-strict.log`, `parameter-origin-correlated12-candidate-v1.log` |
| Original190/20 full demand protocol | 0 | `parameter-origin-original190-20-v1.log` |

The initial correlated baseline draft supplied cells in physical rather than
the actual binder descriptor order and correctly failed 12 cases on both
baselines. Those draft failure logs remain retained, not claimed as evidence
of a production error. The final unchanged 24-case input supplies real typed
cells by the actual source descriptor and exposes exactly the four omissions.

The complete Root94 native collection, session 67712, exits 0: all 94 original
fixtures, including the actual typed-NULL68 test, run on isolated default-disk
directories and current normal O2 objects. The first full78 wire collection,
session 38460, exits 1 for two original fixture timeouts (prepared assignment
at its first BEGIN and ordinary CASE at a late CREATE TEMP SEQUENCE). Session
23106 runs both whole timeout fixtures against the unchanged 86a8 baseline,
both exit 0, then repeats all 78 candidate fixtures without changing deadlines:
that full collection exits 1 for a different CREATE TABLE timeout in the
original1192 BIT BETWEEN input fixture. The original two fixtures pass on that
candidate repeat. All failed logs remain retained. The test's failed finally
DROP skipped server cleanup; only its exact owned orphan process is terminated,
with its isolated data directory retained. These runs are not full-wire PASS.

The two new whole fixtures are permanently registered; native discovery picks
up both new C++ fixtures automatically. A subsequent unchanged full78 run is
recorded separately and must reach an actual terminal before approval. Original
INTEGER full9649/shared-parameter70 requires the three independent INTEGER
commits and is not covered by this old dependency base. The original TYPE-11
273-case family, catalog/type-owner gaps, BIT(0)/VARBIT(0), and old COUNT FILTER
support are not declared complete. No Root/master import or push occurs here.
