# Stored scalar routines in ORDER BY

This repair addresses direct scalar ORDER BY execution, typed key comparison,
and sharing a bound key with the same SELECT target. It does not complete the
stored-routine review families or the overall checklist.

## Cause and repair

The single-source SELECT adapter treated an unlisted stored routine as an
unresolved physical sort column. It consequently never called the routine,
did not propagate its error, and could sort without its side effects.
Legacy display sorting also discarded the structured SQLSTATE returned by
the expression bridge. The scalar-projection comparator guessed numeric types
from text, making text keys such as `10` and `2` sort numerically.

Direct scalar keys now use the shared AST routine binder and execute against
the original physical row with its NULL bitmap and actual StorageEngine owner.
All keys are prepared before qualification, including zero-row inputs and
unused CASE branches; preparation never executes a routine. Structured
`ExprEvalResult.sqlState` retains the actual DbError code. The scalar comparator
uses declared/source types and NULL flags instead of inspecting textual data.
CASE remains a genuine AST in ORDER BY. A typed full predicate avoids repeating
the sort writer in overlapping legacy OR scans.

Two metadata-only accessors retain the resolved routine's return type and
identity, including after private callback rebinding. A named callback holds
its actual owner/database and declaration metadata; replacing that callback
does not leave stale result-type metadata. `ExprHelper::inferResultType` accepts
the database and owner to infer a stored expression without calling it.

`ExprHelper::scalarExpressionIdentity` uses the consumer's validated source
occurrence and descriptor ordinal, source type/width/collation, canonical
routine identity/arity/declared types/owner, and recursive operand AST identity.
It does not compare runtime values, raw SQL strings, or temporary callback
names. The single-source consumer therefore shares `f(id)` with `f(d.id)` and
`public.f(d.id)`. Different CAST modifiers and quoted COLLATE labels remain
distinct. The identity explicitly distinguishes SQL NULL from non-NULL text.
Preparation rejects unsupported query-scope/aggregate/window identity shapes;
this is not a general join, subquery or parameter binding implementation.

## Retained evidence

Worktree: `/tmp/dbms-stored-function-atomicity.D0JmfO/repo`.
The unchanged nine-control clause diagnostic is checked in separately and
keeps its original PostgreSQL SQLSTATE, row, and surviving-write expectations.
The prior WHERE repair left seven real failures; this ORDER repair leaves five.

- Initial fresh 55-object development build and stubs are retained in
  `/tmp/dbms-function-order-api2.jXUqwH` (build `53897`, exit 0). The initial
  ORDER suite (`98453`) and native metadata test (`25142`) passed. The unchanged
  diagnostic (`29186`) still exited 1 with five failures.
- Adding qualified source-equivalence controls exposed a real candidate
  failure (`97570`, exit 1): `f(id)` in the target and `f(d.id)` in ORDER BY
  executed twice and failed its writing function. Raw-token matching was
  replaced by the structural identity above. The independently frozen new
  header set rebuilt all 55 translation units and stubs (`15425`, exit 0)
  under `/tmp/dbms-function-order-canonical.OHLPmF`, with normal configured
  zlib/ICU/TLS selection and normal flags plus `-O0`.
- That canonical-key candidate passed the expanded ORDER wire test (`42402`),
  native metadata test (`8771`), eight adjacent natives (`61176`), and nine
  focused wire scripts (`29365`). The unchanged diagnostic (`63224`) still
  exited 1 with exactly five real failures. This is development evidence,
  not an integrated ROOT optimized/full-gate result.
- A read-only review identified a NULL identity collision. Actual native
  baseline `88359` exited 134 because `f(NULL)` and `f('null')` had the same
  key. Wire baseline `1341` exited 1: the writing function was called once
  with NULL instead of twice with NULL and non-NULL `null`. The original
  pre-fix binary and helper object are preserved as `dbms_main.pre-null-identity`
  and `expr_helper.pre-null-identity.cpp.o`. The uppercase text `'NULL'` does
  not collide with the parser's lowercase NULL token; its probe `32377` passed
  and is not presented as a red baseline.
- The literal-kind repair rebuilt only the changed helper with unchanged
  headers and relinked (`24064`, exit 0). Final source hashes, unchanged header
  audits, and binary hashes are recorded by the artifact's `.final` files.
  The initial native-link attempt `79178` had an empty object array because
  the harness named the wrong shared-build variable; it failed to link and
  is not runtime evidence. The corrected harness checks all 54 non-main objects.
- Final metadata native `37767` and expanded ORDER wire `41003` passed,
  including both NULL-key inequalities and the exact two-call NULL/text wire
  assertion. Eight freshly relinked adjacent natives passed in `66768`:
  scalar resolver, engine owner, aggregate ambiguity, unchanged constraint
  expression, function atomicity, PL query host, quoted scalar binding, and
  function/procedure. Together with the metadata test these are nine distinct
  native tests. The final unchanged clause diagnostic `31659` still exited 1
  with exactly five failures. Final binary SHA-256:
  `ade48f8c939f8f02395641a95a3e9df03d39e0cb18242082a528c9d2bcc4c700`.
- Nine final focused protocol scripts passed in `26049`: stored-function
  WHERE execution, WHERE function scope, function atomicity, PL SELECT INTO,
  quoted scalar binding, table CASE AST, lexical table predicates, constant
  Boolean predicates, and DISTINCT source queries. Source/header audits match
  the final object set: the 55 fresh same-header objects from `15425`, with
  the helper replaced by its successful final-source compile `24064`.
  `15425` alone is not described as a full build of the later literal-kind
  source. All final validation handles reached terminal status.

Isolated PostgreSQL diagnostics used the accessible PostgreSQL **17.2** server,
not 18.6. Transactions were rolled back. They verified three calls total for
qualified/unqualified equivalent target/sort and repeated-key expressions,
including public qualification, and two calls with separate NULL/non-NULL
values for the literal control. The documented SQL expression and ordering
contracts are available in the official PostgreSQL 18
[expression rules](https://www.postgresql.org/docs/18/sql-expressions.html) and
[SELECT reference](https://www.postgresql.org/docs/18/sql-select.html).

## Still unresolved in the original diagnostic

Scalar-subquery errors in WHERE and ORDER BY, actual EXPLAIN ANALYZE routine
execution for FROM-less and table-backed queries, and arithmetic UPDATE
assignment with a typed predicate remain separate real failures. They have
not been removed, assigned weaker expectations, or counted as passing.
Broader query scopes and routine compatibility remain partial.
