# PL/pgSQL scalar identity and native expression diagnostics

Date: 2026-10-06. Independent root commits:

- `b6683521`: preserve native query expression SQLSTATE (isolated `2c668954`).
- `8cd860e7`: bind scalar variables by canonical positional identity
  (isolated `d572a120`).

Both follow up [the complete SELECT INTO boundary](issue-plpgsql-select-into-typed-query.md).
FUNC-05/FUNC-06 and SQL-04 remain partial; this report closes neither a complete
function language nor the general SQL binder.

## Native SQLSTATE loss

The actual native engine reproduced seven failures: SELECT/WHERE/ORDER BY
division by zero, invalid integer input, integer overflow and a stored native
SELECT INTO division. All returned XX000 instead of the expression's specific
SQLSTATE. The baseline process exited 1; a failure was not inferred merely from
source inspection.

The fallback's generic exception handler now passes the diagnostic through the
existing checked scalar-error mapper. Only a valid terminal `(SQLSTATE XXXXX)`
marker is recognized. Explicit DbError handling and unsupported-shape states
are unchanged; arbitrary text in a datum is not interpreted as an error code.

| Actual expression failure | Expected preserved state |
|---|---|
| Integer division by zero in projection, predicate or ordering | `22012` |
| Invalid integer input | `22P02` |
| Out-of-range integer input | `22003` |

The registered `plpgsql_native_query_sqlstate_test.cpp` checks all seven cases
and a following successful row containing 7, empty TEXT and SQL NULL. Thus error
recovery is checked alongside diagnostics, not just matching an error string.

## Canonical scalar-variable identity

Procedural names are already canonical after parsing, whereas RowContext folds
keys to lowercase. Sending variables directly to that context merged `"X"`
and `x`. The same collapse affected values, NULL bits and type metadata.
Independent native and real-protocol baselines reproduced the defect.

| Variables and expression | Baseline | Correct result |
|---|---|---|
| `"X"=1`, `x=2`, `"X"+x` | 4 | 3 |
| `"X"` NULL, `x=2`, `coalesce("X",9)+x` | 4 | 11 |
| `"X"` TEXT `00123`, `x=2`, text concatenation | Wrong identity/type | `00123:2` |

Parse the scalar expression once, retain its AST and bind each actual canonical
ColumnRef to a private positional key. Put values/types/NULL bits under those
keys, then evaluate the AST directly. Do not serialize CASE/array/parenthesis
trees back to a lossy SQL string, rewrite function/type names or derive types
from numeric-looking TEXT. Actual declared BIGINT width is preserved rather
than inheriting the older row helper's integer-family narrowing.

The recursive binding walk handles scalar value children, including CASE,
casts, predicates, function/named argument values and the parser's array forms.
EXTRACT's field is syntax rather than a variable. Session user/date/timestamp
values retain their prior contextual behavior. Complete explicit NEW.id trigger
bindings remain available. Unknown variables/private-looking names do not become
NULL; missing qualifiers do not fall back to bare variables. Function/reference
errors are validated even in an unevaluated coalesce arm.

Remaining scope includes arbitrary row/record fields, complete function/type
resolution and broader query shapes. Array element type metadata already had a
separate TEXT-width limitation: the new array regression uses explicit integer
casts and does not pretend that limitation was repaired here. The independent
stored-query variable-versus-source-column ambiguity and function transaction
issues are not solved by positional scalar binding.
Actual query-level ambiguity, qualification and lexical-role probes are recorded
in [the open preparation report](issue-plpgsql-query-binding-preflight.md),
including a sequence control that distinguishes pre-execution rejection from
rollback after execution.

## Retained failures and verification

The isolated worktree was `/tmp/dbms-plpgsql-followups.WEf8rM`, based on
`7bc76204`. Native SQLSTATE baseline session 76162 exited 1 with the seven
incorrect XX000 results. Quoted native baseline 77823 exited 1 with wrong values,
NULL/types and unresolved references accepted; protocol baseline 41521 returned
4 where 3 was required. The first quoted native fixture compilation (43768)
lacked its Session header and failed; that test include was corrected rather
than recorded as a passing run.

After the independent fixes, three freshly linked matching development native
tests passed: native SQLSTATE, quoted scalar binding and the adjacent native
query-host suite. The dedicated quoted protocol and original complete SELECT
INTO protocol also passed on the isolated development binary. Native tests cover
case/NULL/types/BIGINT, CASE, parentheses, escaped/spaced names, explicit array
casts, private-key collisions, session values, EXTRACT/date, quoted parameters,
trigger bindings, unknown references/functions and post-error recovery. The
wire test additionally verifies integer/BIGINT/text OIDs and no rows on errors.

The root optimized follow-up build recompiled the changed TableManage object
under the shared production O2 configuration, reused only the other 54 objects
with matching unchanged header/source signatures, linked normally, then passed
repeat up-to-date build, all 55 object signatures and the binary build stamp.
No public API/header or StorageEngine layout changed in these two follow-ups.
Six freshly linked matching root optimized native tests passed: native SQLSTATE,
quoted scalar binding, query host, SELECT INTO interpreter, corrected existing
PL/pgSQL and function/procedure metadata/execution. Seven root optimized protocol
entry points passed: quoted scalar binding, SELECT INTO, function result, stored
CTE namespace, DML CTE, self-JOIN range identity and the four-case cold-restart
regression. These are focused/adjacent checks on the final two-follow-up
combination, not the full registered suite. Six ledger unit tests and three
documentation/version/compatibility checks are verified separately in the ledger.

No fresh full default protocol was run for these two follow-ups. The immediately
preceding combined `615f2c54`/`7bc76204` run failed on INSERT kw_joined with a
socket timeout; earlier complete failures and unresolved statement-image I/O
amplification remain recorded in the parent and cold-start reports. Full
registered suite, TLS runtime and PostgreSQL 18.6 differential remain unrun.
No push, Actions enablement or user-deferred security/TDE work was performed.
