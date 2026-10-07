# Typed OLD/NEW values for view SQL-action triggers

Status: the original joined-view UPDATE failure and this SQL-action value
transport issue are fixed and verified. The full default protocol is still not
passing: its next reached, independent physical UPDATE predicate failure is
recorded below.

## Original failure and repair

The unchanged full protocol at line 2764 updates `jt_view`, a JOIN view, with
`SET val = 'v2'`. On exact a7 source it failed with 42703, `column v2 does not
exist`, and the base row stayed `v1`. The legacy SET parser stripped literal
quotes and inserted the resulting datum bytes into trigger SQL. Its view-row
collector also split printed rows on whitespace, losing NULL and value types.

The new UPDATE/DELETE trigger boundary reads real structured source rows and
their NULL bitmap from the prepared view source graph. OLD/NEW carry declared
`ExprValue` cells. Every UPDATE SET expression is evaluated once against the
same OLD row through the retained prepared AST, with target assignment casts.
SQL-action trigger bodies are whole-statement bound with qualified typed
OLD/NEW parameter slots before the existing typed SQL adapter dispatches them.
Text containing `OLD.x` or `NEW.x` remains literal data, not a replacement site.

Source errors propagate. A pure unsupported graph-lowering result can select
the existing full dispatcher before opening a producer, but an executed query
is never retried. The old output-tokenizing collector is removed. Existing
function/utility trigger actions retain their old raw argument interface; their
wider argument/NULL family is not claimed fixed by this SQL-action repair.

## Matching evidence

Artifacts are retained in `/tmp/dbms-joined-view-trigger.Cbhks6Kd`.
The private parent is exact `a7d460c7`, plus the separately verified physical
array descriptor fix `dc7469ea` (private mapping `bbdfc523`). That independent
issue was exposed by the view test: array values were correct but the physical
result descriptor reported integer OID 23 instead of array OID 1007. It was not
hidden by removing the OID assertion.

The final normal O0 immutable binary is
`final-immutable.rBH7oVIB/dbms_main.frozen`, SHA256
`7134302d3ddeae9b9cfe04a0e59535fe3a937b8c53f16bf02b64688567c8c5a1`.
It is not a current ROOT O2 proof. All 58 source/header/flags signatures and the
binary stamp match: exact immutable a7 objects were audited before reuse and
the changed main/NetworkServer/systables CPPs were freshly compiled. This view
commit adds no public header, ABI field, or translation unit.

| Check | Actual result / artifact |
| --- | --- |
| Original joined-view SQL on a7 | exit 1, 42703 and unchanged base row; `original-baseline-a7.log` |
| Original legitimate PostgreSQL trigger-function oracle | strict runtime 180006, exit 0; `original-reference-18.log` |
| Full typed-value matrix | exit 0, `typed-values-candidate-v5-array.log` |
| Added explicit owner/savepoint rollback guard | exit 0, `typed-values-candidate-v5-explicit-owner.log` |
| Strict XML PostgreSQL 18.6/en_US full matrix with owner guard | exit 0, `typed-values-reference-18-explicit-owner-fixed-wrapper.log` |
| Eighteen matching native controls | exit 0, `native-v5-final-18-official-host.log` |
| Eight complete serial wire scripts | exit 0, `adjacent-v5-fixture-corrected-final-8.log` |
| Official repeat / all 58 signatures and stamp / final freeze | exit 0, `build-final-repeat-audit.log`, `freeze-final.log` |

The permanent whole wire matrix retains quoted column names and apostrophes,
commas, spaces and record-looking text; empty string, text `NULL` and actual SQL
NULL; INT[] cells and OID 1007; BIGINT maximum; CASE and simultaneous SET;
multirow per-row sequence calls; zero qualifying rows; unknown SET column
pre-effect failure; CHECK 23514 after the first base write; both writes rolled
back with irreversible sequence counter 4; SELECT 1 recovery; explicit caller
BEGIN/savepoint/rollback; and DELETE OLD-image comparisons including SQL NULL.
Default 15-second socket deadlines are unchanged.

The new reference fixture initially assumed unspecified physical row update
order; its failed log is retained. Its own view definition now orders by bid,
and the complete strict reference passes. The original full-protocol view SQL
has not changed. When the explicit parent-savepoint control was added, the
oracle's generic per-query savepoint wrapper released the parent test savepoint
as well; that 3B001 wrapper failure is retained. Transaction-control reference
queries no longer receive an interfering automatic savepoint. All production
SQL and strong row/state assertions remain intact.

Two native wrapper mistakes are also retained: a mistyped source filename, and
linking common test stubs into the existing `before_trigger` fixture that owns
its own host globals. The final eighteen-test repeat follows the official
test-local-stub rule and terminates 0 without editing these fixtures.

## Unchanged complete protocol and remaining boundary

The original full file has SHA256
`4d0affcb70ba817a30f474e9f7439cf935b4f97d55cb675641651c0be4f4a14b`.
Only an external observation wrapper captured server output and wire error
fields; original SQL, assertions, and 10-second deadlines were unchanged.
`original-full-candidate-v5.log` terminates 1 after 379 simple-query responses:

- Original joined-view UPDATE returns `UPDATE 1`, changes the base row to `v2`,
  and its DELETE returns `DELETE 1` with no remaining row.
- Subsequent writable-view INSERT and its original RETURNING assertions pass.
- The next assertion at line 2867 fails: physical
  `UPDATE pred_t SET name = 'x' WHERE name NOT LIKE 'a%'` returns 42804,
  `argument of WHERE must be type boolean`, instead of the expected rows 2,3.
  That independent static operator typing issue is not changed by this commit.

The first eight-script adjacent attempt had one old RETURNING fixture expecting
0A000 for a hidden OLD range, while both exact a7 and the candidate returned
42P01. Both failure logs are kept. The independently reference-verified
test-only fix `d04c4ae4` (private mapping `a51d13f0`, already ROOT `ab7972eb`)
retains the complete RETURNING matrix and corrects that old expectation.
The entire eight-script group then passed on the same unchanged production
binary. This is not a narrowed known-gap mode.

No full-default PASS, all-view-trigger family closure, UPDATE FROM/RETURNING
view completeness, disk-I/O performance repair, or binary array codec support
is claimed. `/dev/shm` is used for isolated semantic runs, not as evidence that
the separately retained disk timeouts are fixed.
