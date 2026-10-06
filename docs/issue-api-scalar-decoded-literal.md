# Native scalar predicates preserve decoded literal identity

The compact native query API supplies decoded right-hand data. A scalar
left operand such as `UPPER(email)` nevertheless rebuilt that datum as SQL,
so `SAME@EXAMPLE.TEST` was bound as a column and raised 42703. This was an
actual failure of the full `partial_index_test`, not a protocol-only issue.

`Condition` now records whether its right operand is a proven decoded API
literal. Only such data is quoted when bridging to a scalar expression.
Actual SQL expressions remain untagged, so missing columns still fail with
42703. Arithmetic-looking compact data uses the declared left-hand result
type, inferred without executing routines: text `1+2` stays text, whereas
numeric `ABS(id)` can compare to an arithmetic expression. Column-to-column
resolution no longer overrides an explicitly tagged API datum.

The new native test covers email, apostrophes, empty text, text NULL versus
SQL NULL, arithmetic-looking text, numeric arithmetic, explicit CAST and
concatenation, a literal matching a column name, strict SQL binding, and
lock cleanup after errors. The existing partial-index test retains its
index counters, rollback/savepoint, INCLUDE and expression-index assertions.

Artifacts remain under `/tmp/dbms-insert-omitted-null.iHZNTrqu`:

- Actual baseline session 19506 exited 134 on the first email predicate;
  `api-scalar-literal-baseline-v2.log` preserves it. The earlier Bash-loader
  harness failure is separately retained, not counted as a production bug.
- The changed `Condition` layout required a fresh complete 56-source normal
  O2 build (session 21997, exit 0), followed by matching source/header object
  signatures and binary-stamp checks. Later source-only refinements were
  rebuilt without mixing incompatible headers.
- Candidate v1 kept a genuine arithmetic-looking-text failure; v2 exposed
  a native error lock leak. The latter was fixed by the independent query
  resource-guard commit `983b38a6` (private mapping `55fde510`), not by
  weakening the new lock assertion.
- Final v3 native session 37922 exited 0: all 25 freshly compiled, linked
  and isolated natives passed. Adjacent wire session 23874 exited 0 for
  compact RHS SQL boundaries, table literal boundaries, quoted predicates,
  WHERE function scope, read-owner index snapshots and single index residual
  execution.

The exact frozen v3 server SHA256 is
`8a84a96e98820bf7a5fe5956238997fbd4290e5cca76a014a23971394d3fe25e`;
the immutable native/wire logs are `api-literal-native-v3.log` and
`api-literal-wire-v3.log`. ROOT must rebuild every production translation
unit against the new public header after integration. These private passes
do not establish combined ROOT or complete index/query-family correctness.
