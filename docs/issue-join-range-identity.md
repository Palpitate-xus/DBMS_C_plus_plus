# Independent range identity for repeated JOIN sources

Status: specific source/test fix committed as `d3fcfc0b`; scoped optimized
regressions verified, complete default-protocol runs failed. SQL-04 and
QRY-01/02/03/05 remain partial. No push; Actions stay disabled; user-deferred
security/TDE work stays deferred.

## Reproduction

The preceding optimized binary crossed a four-row table with itself and
returned 16 duplicated diagonal pairs instead of all 16 independent pairs.
`WHERE a.id=1 AND b.id=2` returned no row instead of `(1,2)`. A repeated CTE
source exposed the same defect. The new protocol regression failed on its
first ordered-pair assertion before the fix.

The dispatcher normalized both SQL aliases to the same physical qualified
column key. Storage JOIN maps also reused that key, overwriting the left
entry with the right entry. Changing projection offsets alone would leave
WHERE, residual ON and aggregate input binding incorrect.

## Change

The five native JOIN methods accept optional range names independent of
physical storage names. SQL execution supplies distinct left/right ranges,
including each stage of a multi-source join. Locks, scans and NULL bitmap
lookups still use the actual physical table. Native callers that omit names
retain the existing table-qualified interface.

The SQL-side binder maps visible qualifiers to range keys in a single pass
over the original predicate. It decodes quoted identifiers, preserves quoted
values/comments/dollar literals and does not rescan inserted keys: a user's
alias can equal an internal range name. Column names use injective byte-hex
keys when requested, so spaces, quotes, dots and encoded-looking column
names cannot become legacy condition delimiters or collide with one another.
SQL visibility validation runs against original names; private keys do not
become publicly addressable aliases. Duplicate source aliases reject 42712.

ON conditions, WHERE, selected columns, aggregate type hints and projection
positions share the bound range keys. Qualified stars expand the chosen
source schema in target-list order, preserving headers, types and actual
NULLs. Multi-source residual ON and composite USING conditions pass their
stage-specific keys into storage.

A private condition carrier preserves literal versus column identity and
SQL NULL before the existing decoder removes quotes. Text equal to a range
key remains text; SQL NULL comparison stays unknown. The public native
Condition layout is unchanged. Signed numeric literal controls guard against
mistaking a value for a same-spelled delimited column.

## Candidate failures retained

The first candidate fixed independent pairs but failed `SELECT b.*,a.id`
with 42703; qualified-star expansion was then added. An adjacent JOIN test
exposed an introduced BETWEEN regression: internal leading underscores did
not pass the legacy identifier check, so all rows survived instead of the
requested range. The check now uses SQL identifier continuation rules.

An isolated text probe returned no row for a datum equal to an internal key;
storage had interpreted the decoded literal as a column reference. The
private literal/NULL carrier fixes that path; both native and protocol
regressions cover it. These failed candidate runs are not counted as passes.

## Verification

The latest development binary passed the focused protocol script, covering
ordered pairs, independent WHERE/ON, aggregate inputs, reordered projection
and stars, LEFT/RIGHT/FULL unmatched rows, NULL/empty/text-NULL values,
duplicate-key bags, repeated CTE sources, three-source joins, LATERAL,
quoted aliases and delimited columns, literal collisions, BETWEEN/NOT LIKE,
negative literals and exact negative binding SQLSTATEs.

Fresh matching development links of `join_range_identity_test` and the
complete existing `join_null_key_test` passed. The former covers explicit
native ranges, all five JOIN variants, non-equality INNER fallback,
structured NULL bits, duplicate bags, literal identity and encoded delimited
columns. Twelve adjacent development entry points also passed: JOIN types,
multijoin, materialized alias scope, CTE relation scope, lexical predicates,
duplicate CTE names, DML CTE, derived types, LATERAL scope, quoted JOIN
projection, stored-view CTE isolation and UPDATE/DELETE FROM. Thus the
development scope is 13 distinct protocol/E2E entry points and two native
tests.

All 55 production objects were rebuilt with the official shared optimized
configuration, using independent compiler workers and the ordinary object
signature/publication functions. The normal build then validated and linked
successfully; a repeat normal build reported up to date. All 55 object
signatures were checked against current source/configuration. This is
TLS-stub/plain-TCP with zlib and ICU, not TLS verification. Both native tests
were freshly linked with the matching optimized objects and fresh stubs,
run in isolated directories and passed. All 13 focused/adjacent protocol
entry points listed above also passed on that optimized binary.

The first complete default-protocol run failed with a socket TimeoutError
at `ALTER TABLE t ADD COLUMN prepared_note TEXT` inside
`prepared_transaction_error_boundaries` (line 1197), reached from main at
line 3431. This run overlapped the optimized adjacent batch. No production,
load or fixture cause is established; the failure is retained and does not
count as a complete protocol pass. An isolated boundary check and a fresh
serial complete protocol run are being used to investigate, not to erase
the failed run.
The isolated complete prepared boundary passed with the unchanged default
10-second socket timeout; the previously timed-out ALTER completed in
0.787 seconds in that fresh, smaller fixture. This does not explain the
failure in the large default-protocol fixture.

The subsequent serial complete default-protocol run also exited 1, this time
with a socket TimeoutError at `INSERT INTO delete_rows VALUES (8)` after
creating a temporary table with ON COMMIT DELETE ROWS (main line 3458).
Both complete runs are retained failures; neither is attributed to a proven
production, load or timeout-fixture cause. The final verified optimized scope
is 13 distinct focused/adjacent protocol entry points and two matching native
tests, plus the isolated boundary diagnostic, not a passing complete default
protocol or full registered suite. Six ledger unit tests and three
documentation/version/compatibility-contract checks passed.
A fresh isolated `BEGIN; CREATE TEMP TABLE ... ON COMMIT DELETE ROWS;
INSERT ...; COMMIT` check also passed with the same 10-second timeout,
including an empty result after commit; INSERT took 0.405 seconds. Neither
small isolated diagnostic establishes why the large-fixture runs timed out.

## Remaining scope

This closes a concrete repeated-source identity defect, not the complete
SQL binder, arbitrary join trees, correlated analysis or all JOIN expression,
USING/NATURAL, aggregate/window and planning requirements. PL/pgSQL SELECT
INTO partitioning and typed NULL assignment remain independently broken.
Fresh isolated callback probes retain the malformed SELECT/target partition
failure and show a no-row callback overwrites a previously non-NULL variable
with text `null` while leaving `returnIsNull=false`. These are follow-up
reproductions, not fixed by this range-identity commit. Conversely, with an
explicit `DECLARE n INT := NULL`, a successful callback writes 99 but
`RETURN n` still reports `returnIsNull=true`. Both assignment directions need
typed NULL synchronization, not just target-list partition repair.
Follow-up `7bc76204` independently repairs the reproduced SELECT INTO partition
and nullable-assignment defects; see
[the typed procedural-query report](issue-plpgsql-select-into-typed-query.md).
This does not change the range-identity commit's historical verification scope
or close the broader procedural-language family. Quoted compound-variable
identity and autocommit function-write atomicity were subsequently reproduced
and need separate fixes. Cold restart `615f2c54` and the distinct I/O investigation
are recorded in
[the lock-registry report](issue-cold-start-transaction-lock-registry.md);
neither proves that the two complete-protocol timeouts above are solved.
Full registered-suite and PostgreSQL 18.6 differential gates remain open.
The total ledger is still 273: 22 complete / 166 partial / 70 unverified /
15 deferred_by_user.
