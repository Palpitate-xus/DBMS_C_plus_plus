# UNION ALL children consume a genuine typed Append

The complete 37-query bound-DML diagnostic previously failed at its UNION ALL
ANY child with 0A000. Two required writer calls were consequently missing and
all later cumulative sequence sentinels remained wrong. No sentinel was reset
or assertion removed. The actual typed Append candidate now passes that whole
matrix, including all original errors, rows, descriptors, rollback and effects.

## Execution contract

`QueryPlanner::buildPreparedUnionAllPlan` accepts actual retained branch/body
identities and a genuine branch graph factory. The inline-left AST remains
intact: its executable body uses `setOperationInputs.left`, not a rendered SQL
fragment, temporary mutation of `setOp`, or a fabricated SELECT statement.
Nested wrappers preserve their true branch/output ordinals.

Branches borrow a live `PreparedExecutionProvider` slot. One carrier owns the
whole set execution's planned copies, typed parameters, actual original child
sites and memo. The root plans constants before sources or routines execute;
body plans consume those same compiled roots. A body closing at EOF does not
close its sibling's cursor. The owning Append closes the whole carrier once,
and reopening/rebinding its actual graph creates a fresh carrier. Distinct
genuine scalar SELECT sites are not merged by SQL text or value fingerprints.

Append opens the right graph only after the left graph is exhausted. Its
`next()` consumes structured typed datums and performs actual implicit
common-type casts; it does not re-label a rendered row. SQL NULL, text `NULL`,
empty text, arrays, widths and collation-bearing cells retain their identities.
The displayed/instrumented tree is the same real Append/branch graph. Normal
and error cleanup retain the original dynamic SQL error over secondary closes.

Public `ExecutionPlan.h` changes append compatible defaults to the borrowed
SELECT and prepared-child builders and add the provider/branch-factory API.
All 58 production TUs and test stubs were rebuilt fresh. No TU was added.
Existing non-set builders keep their own carrier/default planning behavior;
only the actual producer root opts into whole-query planning.

## API/body-runtime proof, separate from ordinary entry routing

Private artifacts: `/tmp/dbms-bound-dml-cursor.rTuMF7gk/candidate-union-v1`.
All-58 O0 build/native handle 10066 and serial protocol handle 53515 both
terminated 0. Source/header/object hashes and post-gate audits pass. Binary
SHA256 is `eb0a6fcc055d381d9072cfb2959c6c8223387623ef021b845a49d34e629bc778`.

Nine matching native tests pass, including real Append rows/types, arrays,
NULL, lazy unopened RHS, same-tree restart, two separate scalar sites and
pure planning before a left writer. The unchanged whole-37 diagnostic passes,
as do eight complete adjacent scripts: qualification planning, ordinary Q DML,
physical-child restart, primary/multisource WITH DML, Q demand, PL binding and
typed EXPLAIN.

The checked-in 21-query UNION matrix passes strict PostgreSQL 18.6/180006. An
initial harness incorrectly compared column names to OIDs; that failed log is
retained, and correcting only the tuple index leaves every OID expectation
unchanged. The old matching b3 binary's full baseline remains red.

The API candidate's full 21-query matrix still fails: two ordinary top-level
array/CASE UNION SELECTs stay on the legacy route, and a legacy left writer
executes before the right branch's planning error. All original assertions,
including that cumulative one-call discrepancy, remain intact. The next
ordinary-entry consumer is a separate required repair; this API proof is not
a claim that the full 21-query gate passes.

UNION DISTINCT, INTERSECT/EXCEPT, complete set ORDER/LIMIT ownership, domain/
user-cast descriptors, general SRF branches and correlated captured logical
producer lifetimes remain independently open. Parse/Describe's ordinary set
descriptor consumer also retains its original full required diagnostic.
