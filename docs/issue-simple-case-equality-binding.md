# Simple CASE equality binding and separate execution gaps

Simple CASE must resolve an equality operator for every WHEN before executing
the statement. Its comparison operand types are not the common type of its
THEN and ELSE results. The scoped binding correction rejects incompatible
builtin operands before row demand or volatile execution, retains actual
cross-type operand signatures, and evaluates the switch once. Ordinary CASE
dispatch and constant NULL planning demand remain separate issues below.

## PostgreSQL reference and original failures

The reference is the isolated PostgreSQL 18.6 instance on port 15486, checked
by `verify_reference_version` against `server_version_num=180006`. Historical
PostgreSQL 17 results are not used as this issue's oracle.

PostgreSQL resolves operators by exact signatures, implicit compatibility,
exact argument count and same-category preferred types. It does not simply
promote both operands to CASE's result common type. Simple CASE evaluates its
switch once and tests each WHEN for equality.
[Operator type resolution](https://www.postgresql.org/docs/18/typeconv-oper.html),
[Conditional expressions](https://www.postgresql.org/docs/18/functions-conditional.html).

| Original expression | PostgreSQL 18.6 | Original candidate |
| --- | --- | --- |
| `CASE 1 WHEN true THEN 1 ELSE 2 END` | `42883` | Returned `2`; empty INSERT SELECT succeeded |
| `CASE '1' WHEN 1 THEN 1 ELSE 2 END` | `42883` | Returned `1`; VALUES inserted a row |
| Integer switch and explicitly typed TEXT WHEN | `42883` | Compared numeric-looking text to integer |
| DATE switch and INTERVAL WHEN | `42883` | Returned a result instead of preparing an error |
| Typed NULL INTEGER switch and BOOLEAN WHEN | `42883` | NULL hid the missing operator |
| Invalid comparison followed by `CAST('bad' AS INT)` | `42883` first | `22P02` first |
| BIGINT `9007199254740993` versus DOUBLE `9007199254740992` | Match | No match |
| REAL `1e20` versus DOUBLE `1e20` | No match | Match |

The permanent initial native fixture failed 11 assertions and its prepared
INSERT SELECT and VALUES protocol fixture failed 32 assertions. Invalid
operators were also accepted for zero rows. These are observed failures,
not only deductions from source inspection.

The baseline is ROOT `24e3a5fd`. Its parser and query binder were freshly
compiled; the other 56 production sources, all public headers and compiler
flags matched the immutable CASE donor individually. The baseline is not
an object mixture involving old CastExpr or newer CaseExpr layouts.

## Binding and typed execution correction

`equality_type.h` supplies pure builtin equality signature selection.
`CaseExpr.simpleComparisonTypes` retains both declared operand types for each
WHEN. The binder resolves each comparison before transforming that WHEN's
result, preserving source-order error priority. An unknown switch becomes
TEXT; an unknown WHEN acquires the selected operand input type.

The evaluator caches the switch datum once and coerces that datum separately
for each selected operator signature. Integer cross-width operators remain
integer operators. REAL values round-trip through float4 before promotion to
float8, including subnormals and NaN. Implicit CHAR and BIT casts do not apply
the explicit default length of one. SQL NULL never becomes textual NULL.

Both execution-owned AST copies preserve comparison metadata. Expression
identity also includes the retained types. No routine or source row is
executed to resolve an equality signature.

The verified source includes the independently validated typed ARRAY and
WITH multi-source dependencies. The legacy RETURNING ARRAY activation and
versioned UPDATE fixes introduced later on ROOT are not silently counted as
part of this private component proof.

## Exact retained artifacts

All paths below are under `/tmp/dbms-simple-case-binding.KtyWx31T`.

| Evidence | Artifact | Terminal result |
| --- | --- | --- |
| Diagnostic strict 18 reference | `reference18.log` | 0 |
| Earlier matching CASE donor diagnostic | `prepared-e0-baseline.log`, `native.prepared.baseline.log` | Original wrong results printed |
| Older ROOT RETURNING frozen diagnostic | `root-b4-baseline.log` | Original wrong results and sequence effects printed |
| ROOT 24 matching baseline build | `build.baseline.log`, `baseline/*audit.txt` | 0 |
| Original permanent native red | `baseline.native.log` | 134, 11 failed assertions |
| Original permanent prepared protocol red | `baseline.prepared.wire.log` | 1, 32 failed assertions |
| Full new-layout 58-source build | `build.core.log` | 0 |
| Final native with runtime NULL source | `core.native.final.log` | 0 |
| Exact strict 18 prepared matrix | `prepared.reference18.runtime-null.log` | 0 |
| Exact prepared protocol matrix | `core.prepared.wire.runtime-null.log` | 0 |
| Twelve adjacent native executions | `core.adjacent.native.log` | 0 |
| Five adjacent protocol scripts | `core.adjacent.wire.log` | 0 |
| Three scoped ASan and UBSan natives | `core.asan.log` | 0 |

The twelve native fixtures cover equality binding, CASE common types, ARRAY,
prepared execution, SQL namespaces, both RETURNING consumers, WITH multi-source
mutations, the full expression evaluator and arithmetic width/inference.
The five protocols cover CASE common types, typed ARRAY, original WITH
multi-source mutations, its boundary controls and RETURNING namespaces.

`core/sources.final.sha256`, `headers.final.sha256`, `flags.txt`, and source
and header audits retain the precise compiled inputs. Scoped sanitizers
instrument parser, query binder, evaluator, helper, prepared execution and
DML cloning plus fresh stubs; the other 51 non-main objects are regular
matching development objects. Leak detection is disabled. This is not a
claim that all storage code was instrumented.

## Ordinary CASE dispatch remains independent

Ordinary SELECT still has paths that turn CASE into `case_when` pseudo-tokens
or reparse a row expression without retaining bound comparison metadata.
Adding metadata to the binder does not activate those consumers. The old
writing CTE diagnostic executed a sequence before its invalid CASE failed;
PostgreSQL rejects the operator before any such execution. Typed ordinary
dispatch and whole-query preparation must fix that separate cause.

## Constant NULL and runtime NULL require different demand

The added literal-NULL probe originally assumed two WHEN effects. Strict
PostgreSQL 18.6 disproved that assumption: it plans away strict comparisons
with a constant NULL, and `currval` remains `55000`. The preserved original
SQL now lives in `simple_case_null_constant_demand_protocol_e2e_test.py`
with the correct expectation. `constant-null.reference18.log` exits 0;
`core.constant-null.baseline.log` exits 1 with an actual sequence call.
The earlier failed reference is retained in `prepared.reference18.final.log`.

A runtime NULL source column is different: the two demanded WHEN expressions
both execute, yielding `currval=2`. That separately checked contrast remains
in the passing equality fixture. A blanket runtime NULL short-circuit would
change valid SQL effects and is not a solution to planning demand.

## Scope still open

Full custom type, domain, user-defined operator, collation and general planner
resolution are not closed. Literal descriptor widths and unsupported query
shapes also retain their independent preparation limitations. The verified
operator correction does not claim that every raw expression consumer,
aggregate, window, join or procedural expression has acquired full CASE
preparation semantics. The total database audit remains active.
