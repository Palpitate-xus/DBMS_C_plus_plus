# CASE common types and prepared INSERT consumers

CASE preparation now chooses a common builtin result type and validates
contextual unknown inputs before rows or selected branches can hide an
error. INSERT VALUES and the existing supported INSERT SELECT shapes now
execute the retained prepared expressions, including their common type
coercions. These are two independent causes with separate commits;
the complete retained matrix passes only after both are applied.

PostgreSQL resolves CASE results using its common type rules, with ELSE as
the first result candidate. Unknown only results become text.
[PostgreSQL 18 common type rules](https://www.postgresql.org/docs/18/typeconv-union-case.html).
The identical expanded protocol matrix ran against an actual PostgreSQL
18.6 server with strict `server_version_num = 180006` verification. Older
PostgreSQL 17.2 runs remain historical diagnostics, not an 18.6 oracle.

## Independent production changes

`8e2c2076` introduces the pure builtin common type resolver and CASE
analysis. WHEN and THEN expressions transform in source order, followed by
ELSE. Common result selection and contextual input conversions use ELSE
first. Searched CASE conditions require boolean input. Unknown literals
receive pure input conversion; functions, arithmetic and known numeric
narrowing casts are not executed during metadata preparation.

The binder inserts real coercion nodes. The new public CastExpr implicit
flag distinguishes these nodes from explicit CHAR and BIT casts whose
default width is one. An unconstrained implicit conversion preserves the
complete VARCHAR or VARBIT datum. The prepared expression copier retains
the flag, and pure parsed inference uses the same common type resolver.

`bb447a38` fixes the consumers independently. Direct VALUES invokes whole
preparation, and both VALUES and supported INSERT SELECT retain and execute
the resulting AST through PreparedQueryExecution. Source cells use the
actual prepared source occurrence, descriptor ordinal, declared type and
NULL bit. No raw expression reparse, case folded value lookup or first row
type inference substitutes for those bindings. The legacy RETURNING
execution copy also preserves the new implicit cast flag.

The existing selected input, typed literal, unknown name, WHERE and row
width priorities remain checked by the original interval and strict VALUES
fixtures. DEFAULT VALUES retains its existing dedicated path.

## Preserved failing matrices

All current evidence is under
`/tmp/dbms-case-common-current.0TnLTCfj/`.

The immutable optimized production baseline `84e988d2` ran the complete
expanded fixture in `62587`, `case.root84.baseline.log`, terminal 1 with
36 failed assertions. It incorrectly accepted several CASE results for an
INTERVAL target, including UNKNOWN only cases, incompatible categories,
nonboolean conditions and invalid unselected input. Some VALUES cases
inserted a row instead of reporting an error. A sequence projection ran
before an unselected invalid input should have failed. Error priority also
incorrectly reported the later missing function instead of an earlier
typed input failure, and the converse source order control differed.

The separate wide positive controls exposed a runtime consumer defect.
Both a quoted SMALLINT source column selected by a CASE whose other result
is BIGINT, and the corresponding VALUES expression, incorrectly raised
`22003` when adding `2147483647`. PostgreSQL returns `2147483648`; the
source NULL row stays NULL and the result column is BIGINT OID 20.

A matching common type only carrier retains the distinction between the
two causes. Its pure CASE native control `26539` is terminal 0,
`case.core-only.native.log`. The same unchanged full protocol matrix
`76462` remains terminal 1 with 20 failures,
`case.core-only.matrix.log`: sixteen VALUES error and no write assertions,
plus both `22003` runtime failures and their two result row assertions.
Direct SELECT metadata errors are corrected, but VALUES still bypasses
whole preparation and both runtime consumers discard prepared coercions.
That failing intermediate matrix is not reported as passing.

The common type only DML unit was compiled from the immutable prior DML
source against the new CastExpr public headers. Its other 57 objects and
fresh stubs come from the exact new layout build, with matching source and
header audits. It does not reuse an old public layout object.

Earlier development evidence remains under
`/tmp/dbms-case-common.WlA5glbz/`. In particular, its original 24 native
run exposed missing EXCLUDED and RETURNING dependencies, which were fixed
in independent commits rather than bypassed. Its PostgreSQL 17.2 reference
is not relabeled as the current PostgreSQL 18.6 run.

## Final combined verification

The final immutable tree is
`/tmp/dbms-case-common-current.0TnLTCfj/repo`, based on `d0cb36ca` and both
CASE commits. The public CastExpr layout changed, so all 58 production
objects and test stubs were freshly compiled in this tree. Shared build
flags followed by `-O0` form the production development carrier; native
test sources compile with `-O2`.

- Full fresh build `46617`: terminal 0, `build.full58.log`. The source and
  header before and after audits are terminal 0.
- Eight focused native tests and six complete protocol scripts `94314`:
  terminal 0, `case.native8.wire6.log`. The native set includes 22 CASE
  controls plus original interval assignment, input state, INSERT SELECT,
  width, parsed inference, arithmetic inference and prepared execution
  controls. The six protocol scripts retain the full CASE matrix, width,
  interval input, INSERT SELECT, frontend priority and strict VALUES tests.
- The complete original 24 native adjacent set `13586`: terminal 0,
  `case.original24.native.log`. Existing update atomicity, failure hooks,
  storage validation, RETURNING, procedural query and quoted identity
  assertions are unchanged.
- Five additional protocol scripts `8965`: terminal 0,
  `case.adjacent5.wire.log`. They cover the complete procedural query
  binder, typed UPDATE, original 47 WITH controls, the new 19 WITH
  transition controls and expanded legacy RETURNING.
- Exact expanded PostgreSQL 18.6 reference: terminal 0,
  `case.reference18.log`. Both wide source and VALUES positives, typed NULL
  and BIGINT OID assertions are retained.
- Fresh six production units, parser public layout, stubs and four native
  ASan and UBSan tests `21957`: terminal 0, `case.asan.log`. The units are
  DML, parser, binder, evaluator, expression helper and prepared execution.
  Other 51 non main production objects are matching uninstrumented
  development objects; leak detection is disabled.

The final 32 native executions across 30 distinct fixtures and 11 protocol
scripts are all terminal 0.
Source, header and post test and post commit audits are in `verified/`.
The final binary SHA256 is
`e0d8c10e5c5d3e478b57e9f3c5b8e0873cb85b57259f125693df247ef2dd1bab`.

## Remaining type compatibility work

This builtin resolver is not a complete operator or cast catalog. Known
simple CASE switch versus WHEN operator compatibility, full domain and
user defined cast resolution, quoted custom type identity, the binder
width of bare huge or bit literals, and complete static builtin results
remain separate work. Those source derived boundaries are not relabeled
as reproduced failures by this report. Unsupported INSERT SELECT shapes
and other callers that do not consume the prepared tree retain their
existing boundaries; this matrix does not close the entire SQL type family.

The impending ARRAY changes add public element type, nested element mode
and binary concatenation metadata. Their integration must preserve all
three fields in expression copies alongside CastExpr implicit, and use a
new matching full public layout build. They are absent from this CastExpr
only carrier and are not claimed as validated by the evidence above.
