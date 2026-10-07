# EXPLAIN ANALYZE primary exception cleanup

The real frontend `publishExplainPlan` consumed one plan in a streaming loop,
but its error cleanup called `close()` before a bare rethrow. A cleanup exception
therefore replaced an already active execution exception. The independent
`executePlanChecked` contract and complete `checked_plan_sqlstate_test` already
require the original SQLSTATE, full message and exception subtype to survive
cleanup; the EXPLAIN frontend did not satisfy that contract.

The production change is only the catch body: capture `current_exception`, try
one close while suppressing only its secondary exception, then rethrow the
captured primary object. Successful execution still closes once outside that
catch. A close failure on that success path remains the primary exception and
prevents plan publication. The existing streaming row counter, single plan,
single execution, output deferral and renderer are unchanged. There is no
`executePlanChecked` row materialization or execution replay in this fix.

The permanent native driver includes the actual `src/main.cpp` in the same TU,
renaming only its CLI/server program entry, which is never invoked. It calls the
real static helper directly rather than copying its algorithm. Main's globals
and helper definitions are genuine; no test stubs are linked. Its mandatory
pre-global data-directory bootstrap runs with `DBMS_DATA_DIR=.` inside the fresh
isolated test cwd. It does not start a server or run an arbitrary program entry.

Run `bash scripts/test_explain_primary_error_cleanup.sh`; the canonical full
test runner also invokes it outside the normal standalone-C++ test loop. The
test-only same-TU driver explicitly overrides optimization to O0; production
main is independently compiled with official O2 flags. `--sanitize` instruments
the actual main/driver TU at O0 with ASan/UBSan and g1. The other57 production
TUs retain their checked normal O2 objects. Verification uses leak detection
disabled; this is scoped main-TU coverage, not all58 sanitizer coverage.

TEXT/JSON and immediate/pending output are checked for open, first-next and
late-next failures. Already constructed exception_ptr objects let the test
compare the exact exception object, subtype, SQLSTATE, full message and what(),
including misleading embedded SQLSTATE text. Cases include 22012, 21000,
StatementCommitError 23503 and std::logic_error, with and without secondary
58030. Every case checks the same plan pointer, exact ordered events, one close,
one destruction and no render or partial publication. Reported open/next errors
retain exact XX000 diagnostics; successful close failures retain their original
58030, StatementCommitError or logic_error object without retrying close.

Plain EXPLAIN does not execute a deliberately failing root. A successful genuine
prepared table/project/sort graph executes once, closes once and renders that
same child graph. A bounded-trace producer generates 8192 reusable 64KiB rows
(512MiB total); ANALYZE counts all rows with one open/8193 next calls/one close
while its maximum-RSS increase stays below 64MiB. No full row collection is
introduced in either production or that producer's trace.

Private evidence is in `/tmp/dbms-explain-primary-cleanup.CmMCE8p8`, based on
8694f31e29584fbb7cd0696c0b23bfd07314c7de. The original full same-TU driver has
152 failed checks/terminal1, all caused by replacing the primary exception. That
is not a claim of 152 distinct bugs. The identical driver input passes with the
production fix, both normal and scoped sanitizer, including permanent-script
replays. Initial authored compile errors and missing data-directory setup are
retained separately, not classified as SQL runtime failures.

The exact ROOT8694 all58 fresh O2 foundation was checked per source, all relative
header names/bytes, flags, receipts and object bytes before explicit migration
of 57 nonmain objects. Production main was freshly compiled at O2 for both the
baseline and candidate. Baseline frozen SHA256 matches ROOT's
ed533a2975147579ea4848bb1b2c9811d84728a300984cb5c5daf4dc68b60995;
candidate frozen SHA256 is
ad97988608800f6fc634f9aecae628380aa461b79a9679cd401fb6aecb68dae5.
Final all58 inputs/object/receipt/driver/frozen audits are retained. No public
header/layout, storage, WAL, snapshot, retirement or generation code is changed.

The final strictly serial group passes all 12 original complete protocol/CLI
fixtures, including typed EXPLAIN, DML33x4, root constant planning, JSON actuals/
options/cache, publication, relation/timing, legacy ANALYZE, WITH multisource and
ordinary quantified DML. All test-owned processes are gone and default deadlines
are unchanged. First whole-run launch failures from an incorrectly nonexecutable
frozen mode444 are retained; correcting only its mode to555 left binary bytes
and every SQL/assertion unchanged, after which all12 pass. Strict PostgreSQL18.6
server_version_num180006 runs pass the complete root-planning and DML33x4 SQL
controls; the artificial C++ cleanup fault is not claimed to be PostgreSQL SQL.

The original adjacent native group is explicitly **9 pass, 1 fail**. Original
`explain_typed_execution_test` still fails at its unchanged array-source seed
`resolveColumnType(..., "integer", {}, true)` / createTable / insertRow("{1,2}")
control. A second full run linked to ROOT8694's own57 nonmain objects and fresh
stubs reproduces the same assertion/terminal134. Those tests do not link main,
so this cleanup fix cannot account for that failure. The original expectation
is not weakened; its separate native array root remains open for diagnosis.
