# R21: bind and evaluate ON CONFLICT WHERE as BOOLEAN

## Defect and modification

The old conflict receiver guessed truth from strings and did not reject typed
integer/TEXT WHERE input before attempting INSERT. Unknown `'f'`/`'off'` could
update a conflicting row; non-conflicting or empty input could conceal an
analysis error. Scalar/correlated predicates also lost their prepared query host.

Whole INSERT binding now assigns the genuine BOOLEAN context, rejects typed
non-Boolean input with `42804` and invalid unknown Boolean strings with `22P02`.
The same execution-owned prepared carrier plans/prepares WHERE before mutation,
but evaluates its volatile value only for an actual conflicting row. Target
and EXCLUDED cells use their bound source/column ordinals and real NULL bits;
false/NULL means no update. Result publication and statement rollback remain
with the existing atomic DML owner. Parse/Describe perform pure metadata binding
with typed placeholders, without evaluating a source or sequence.

## Actual verification

Private worktree `/tmp/dbms-review-root-conflict.Kdz38nRU/repo`, originally
based on `b844f7c5`, plus the independent R35 parser fix (`9b6341f3`).

| Driver | Actual original baseline | Candidate / reference |
| --- | --- | --- |
| Native actual storage/API fixture, 119 checks | 44 failed, exit 1 | all passed, exit 0 |
| Protocol fixture, 75 controls | 40 failed, exit 1 | candidate and PostgreSQL 18.6 all passed, exit 0 |
| Whole conflict binding, unique-index upsert, RETURNING-subquery and query-binding native neighbors | not counted as red reproductions | all four exit 0 |
| Whole RETURNING-subquery, INSERT clause boundary, HAVING/identifier, global aggregate and PostgreSQL protocol neighbors | not counted as red reproductions | all five exit 0 |

Controls distinguish conflict/no-conflict/empty SELECT analysis, compare exact
SQLSTATE and Parse failure phase, exercise FALSE/NULL/unknown Boolean input,
target/EXCLUDED correlations and scalar queries, check values/OIDs/count tags,
and retain earlier caller transaction/savepoint work. A sequence is not invoked
for a non-conflicting row and is invoked exactly once for a conflicting row.
The native fixture checks no leaked implicit transaction and no failed-result
publication in addition to stored rows. Public PG fixtures use unique names and
remove only their own objects.

The first candidate still had five empty SELECT failures from R35's source
boundary bug. `candidate-protocol.log` is retained; it is not counted as a
passing run or made green by omitting those assertions. All 75 assertions passed
after the independent R35 correction. Accepted baseline runs use the actual
R35 Main/objects without this Boolean patch, so syntax failures are not credited
as reproductions of the Boolean defect.

The initial build verified all 58 donor receipts/headers/configuration and
genuinely compiled DmlExecutor, NetworkServer and query binding. R35 then
genuinely recompiled parser.cpp. No public header changed or mixed-epoch objects
were used. The final native gates checked all 58 current production receipts and
unchanged final input signature. Logs in the private parent include
`r21-{native-baseline,candidate-native,protocol-baseline-r35}.log`,
`r21-r35-{candidate-build,candidate-protocol}.log`, `reference-protocol.log` and
`r21-neighbor-*.log`.

This fix does not migrate all INSERT SELECT conflict/complex-source execution
shapes; those original full assertions remain open under R39. It also does not
claim universal SQL PREPARE template-analysis or complete PostgreSQL compatibility.
