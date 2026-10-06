# Preserve numeric aggregate ambiguity during scalar preparation

The shared scalar metadata binder introduced in `7353cd9d` classified every
missing callback as `42883`. This intercepted the evaluator's preexisting
SUM/AVG overload-ambiguity diagnostic. The unchanged `constraint_expr_test`
failed at line 70 on both that baseline and the owner candidate. ROOT also
confirmed its earlier combined `5411a7aa` native test had passed; the repeated
later red does not establish that this was always broken.

The dedicated owner-baseline probe (`77849`, exit 134) printed
`sum('abc'): function does not exist: sum (SQLSTATE 42883)` while retaining the
original expected `42725`. PostgreSQL 17.2 (`server_version_num = 170002`, not
18.6), tested inside an isolated transaction with diagnostic savepoints and
ROLLBACK, reported `42725` for `sum('abc')`, `avg(NULL)`,
`pg_catalog.sum('abc')`, and an unused `coalesce(1,sum('abc'))` arm. An explicit
TEXT argument reported `42883`. The version-18
[function resolution rules](https://www.postgresql.org/docs/18/typeconv-func.html)
explain why unknown arguments do not determine a unique numeric overload.

The repair retains this recognized ambiguity in metadata preparation, before
any routine is evaluated. Canonical identifiers distinguish quoted uppercase
`"SUM"`, explicit non-catalog schemas, and actual public stored routines.
The private SQL/PL scalar binder also preserves the shared typed preparation
error rather than relabeling it as a missing callback.

The new native regression keeps the original SQLSTATE expectations, checks
SQL and PL bodies, typed TEXT and wrong-schema negatives, quoted/public stored
routine positives, and a volatile writer in an earlier COALESCE arm. Preparing
the latter fails with `42725` while leaving its table empty and no active
transaction.

The two changed translation units were rebuilt with the owner candidate's
matching headers and flags plus `-O0`, and linked with its unchanged 53 matching
objects and fresh stubs. Compile handles `68321` and `43823` completed with
exit 0. Handle `57452` passed the new regression, unchanged `constraint_expr`,
scalar resolver, and independent-engine owner regressions. Artifacts are
retained together with the passing WHERE/atomicity protocol recheck `27330`;
final source hashes match, and the binary SHA is recorded. Artifacts are
retained under `/tmp/dbms-scalar-aggregate-ambiguity.S8kmnY`, with the original
red executable under `/tmp/dbms-function-engine-owner.nBNBgt`.

This restores a narrow existing overload diagnostic; it is not a general
`pg_proc` overload/type binder or support for aggregate execution in scalar
contexts. Complete statement preparation and the seven previously retained
clause-execution failures remain separate work. No claim is made about a
ROOT optimized combination or the full protocol gate.
