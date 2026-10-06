# Extended constant-query database ownership

This is independent of the earlier transaction-start SQLSTATE and simple-query
owner-demand fixes. It does not close all extended-protocol transaction demand,
all query preparation, or all lock/cancellation families.

## Retained actual baseline

The isolated phase diagnostic used the previous verified private `a93312f2`
production binary, not a binary falsely labelled as the later ROOT source:

- `/tmp/dbms-query-begin-lock-state.TNJZ551R/dbms_main.lock-state.v3.o2`
- SHA-256 `6738292c68369fac3a53970066a52692725c0c1a336bd3ffa96f937507a801d7`
- `/tmp/dbms-extended-literal-demand.zSV7mYvf/baseline-v3.log`
- handle `65220`, terminal exit 1, owned server stopped in `finally`.

With the original 50 ms lock timeout and 15 s socket timeout, another connection
held the database DDL snapshot lock. Describing an already prepared constant
query succeeded. A full Parse/Describe/Bind/Describe/Execute/Sync `SELECT 1`,
binding/executing a previously prepared `SELECT 1`, and `SELECT 1/0` each failed
with `55P03`. The first two require row `1`; the third requires `22012` instead
of a lock failure. These are three retained red controls, not three unrelated
root causes. The writer negative control remained blocked and never advanced
the private sequence or inserted a row.

The same constant phase packets passed against the existing isolated PostgreSQL
17.2 reference (`server_version_num=170002`), with a reference-owned TEMP table
and rollback/connection cleanup. Artifact `reference-pg17-v2.log` is terminal 0.
The reference division error occurs at Bind (`ParseComplete, ErrorResponse,
ReadyForQuery`) before BindComplete. The initial reference authentication
fixture failure is separately retained in `reference-pg17.log`; it is not SQL
evidence. No authentication or reference roles were modified. This is not a
PostgreSQL 18.6 differential-suite claim.

## Candidate contract

The private source starts at ROOT `fa63b0bed751389c1d1fec5c4b36bad0acc6761a`,
with only the verified earlier fixes mapped to `bc9de098` and `9f622fe8`.
Those mappings retain the EXPLAIN deferred-publication scope when merging the
simple-query proof into the outer dispatcher.

Parse/Bind may request deferred physical ownership only after the entire parsed
AST passes `SQLParser::isDatabaseIndependentQuery`. Name references, routines,
CTEs, SQL children, casts/catalog types, positional parameters, unknown nodes
and unsupported envelopes retain real ownership. No raw SQL keyword or routine
whitelist determines this proof. Existing explicit transactions are not reset.

`DatabaseIndependentBeginScope` still calls the normal BEGIN coordinator. The
engine owns a real logical transaction with a reserved, registered xid, active
database association, isolation/read-only characteristics, command IDs, first
snapshot and normal notification/advisory boundaries. Only the database mutex,
catalog persistence and CLOG handle are deferred. The global xid allocator is
still durable: this is not a claim of zero filesystem I/O or PostgreSQL's exact
virtual-xid representation.

Physical access promotes the same transaction before it obtains table/catalog,
page/index/WAL handles or modifies data. Promotion retains xid/CID and the
first RR snapshot, obtains the real database shared lock, rechecks generation
existence, persists catalog state, and attaches CLOG. Failure leaves the logical
transaction available to the ordinary abort/Sync boundary with the original
structured status. Cached native handle paths are fenced as well as cold paths.
A deferred owner cannot create a transaction backup before obtaining physical
ownership. Unknown non-query commands conservatively promote; transaction
ending/BEGIN/transaction-characteristic commands retain their established
logical coordinator.

A never-promoted transaction has no tuple/DDL undo or physical resources to
commit/abort. Its checked memory-only terminal path unregisters the xid,
removes SSI state and logical locks, clears transaction state, and preserves
backend-local isolation and sequence lastval. It does not write database
CLOG/WAL, flush database caches, or restore temporary storage. Existing TEMP
ON COMMIT delete/drop actions conservatively require real ownership at BEGIN.
Unexpected writes/undo/savepoints/checks cannot silently pass the pure cleanup
invariant.

Bind folding is permitted only for a fully proven primitive AST. It evaluates
literal/operator nodes through the expression evaluator, never executing SQL,
user routines, a CTE, or a child query to plan a constant statement. This permits
the division error to precede BindComplete without losing its SQLSTATE.

## Retained candidate failures

V1 full 57-TU official O2 build `73563`, repeated no-change build and all object
signatures/stamp are terminal 0. Frozen binary `dbms_main.extended.v1.o2` has
SHA-256 `19d8034ffe6bb4f64cbbb130df2315746241909426de36cabeed1a0ff1050a1e`.
The first native gate (`66007`) is terminal 0, including failed ownership
upgrade, cached WAL fencing and xid/CID/RR snapshot preservation.

The first protocol gate (`49430`) is terminal 1, not a full pass. Full constant
Parse/Bind/Describe/Execute, existing Bind, NULL and arithmetic controls passed.
The retained FROM string then hit the unrelated old raw-text physical-origin
helper during returned metadata; its unnecessary catalog ownership failure
escaped the description boundary and terminated the owned process (92819,
confirmed gone). `wire-v1.log` and the server log retain that failure. The
literal-origin defect has its own old-binary red gate and separate issue/fix.

V2 also fences the public physical ReadView getter: callers cannot copy a
null-CLOG deferred view into legacy visibility. Logical snapshot inspection
uses the existing serialized `exportSnapshot` interface. Promotion wait loops
check cooperative query interruption even when lock_timeout is zero, and
Describe propagates typed metadata errors through the normal protocol abort
boundary. These refinements require another complete matching header build.

PG17.2 lifecycle reference `reference-lifecycle-v2.log` is terminal 0 for
read-only tightening, rejected isolation change after Parse snapshot,
savepoint/rollback, advisory release and commit-only notification publication.
Its first run retained one incorrect expected RFQ byte: a failed BEGIN
promotion from an implicit Parse block returns I, whereas an already explicit
failed block returns E. The fixture was corrected from that actual reference,
without relaxing SQLSTATE 25001; `reference-lifecycle.log` remains terminal 1.

V2 full 57-TU new-header official O2 build `56049`, repeat and 57 signature/stamp
audit are terminal 0. Frozen binary `dbms_main.extended.v2.o2` has SHA-256
`2d14edf651e448c893ddb229b6f39518e875ee8c691156bdcffa98f8fca28329`.
Eight matching native gates (`95857`) and the dedicated literal-origin gate are
terminal 0. Its extended gate (`65322`) is nevertheless terminal 1: constant
and non-pure promotion controls passed, then extended user BEGIN left a deferred
explicit transaction and Sync's ParameterStatus observer attempted to read the
role catalog while the other backend retained the DDL lock. That 55P03 escaped
the observer and terminated the process. `wire-v2.log` remains a failure.

An isolated instrumented NetworkServer copy (`85938`, diagnostic mixed O0/O2,
not the production binary) reproduced the same failure at `45447`:
`wire-status-trace.log` and the retained status-trace server log identify the
ParameterStatus catalog observer throwing 55P03 while deferred. This is direct
cause evidence, not an inference from system load. No reference/production
authentication, permissions or roles were changed.

The protocol now reuses the connection's already reported `is_superuser` only
while physical ownership is deferred and the effective role is unchanged.
Actual role-changing commands still require physical ownership and fresh
observation. This avoids turning a status report into catalog execution; it
does not alter authorization or remove the engine's physical-access fences.

## Final matching gates

V3 changed only NetworkServer.cpp against the completed V2 new-header object
group. Official O2 build/relink `26624`, repeat and all 57 source/header/object
signatures plus binary stamp are terminal 0. Frozen binary
`/tmp/dbms-extended-literal-demand.zSV7mYvf/dbms_main.extended.v3.o2` has SHA-256
`cb981f4fffbde28c1c58bb4f41777a0e4073e5b4ea5c469f3d5f939bf3c6933a`.

New protocol group `96051` is terminal 0 for both the full extended demand gate
and the independent literal-origin gate. The original 50 ms lock timeout and
15 s socket timeout are unchanged. Controls include P/B/D/E/H/S, planning-only
Sync, expired portal 34000, Bind division 22012 and recovery, non-pure table,
routine, CAST and writing-CTE promotion failures, explicit BEGIN and E/ROLLBACK
recovery, writer execution exactly once, data-dependent 22P02 rolling back its
rows while retaining nontransactional sequence advancement, read-only and
isolation characteristics, savepoints, advisory-lock release, commit-only
notification delivery, and disconnect cleanup. Owned server PIDs 250005 and
252124 were stopped and are confirmed gone.

Matching native group `77451` is terminal 0: deferred ownership (including
57014 cancellation/timeout and 57P01 termination during promotion), primitive
whole-AST demand, snapshot characteristics, read-owner index snapshots, stored
function atomicity and actual engine owner, prepared-query execution and query
binding. The native deferred gate retains the same xid/CID/RR snapshot across
successful and failed promotion, public ReadView and cached WAL fences, and
checked memory-only commit/rollback.

Ten original adjacent protocol gates (`14181`) are terminal 0: original table
lock timeout/recovery, transaction-start SQLSTATE, snapshot characteristics,
extended error abort, prepared TEMP access (including ON COMMIT), stored
function atomicity, transaction-chain read-only, savepoint read-only, read-owner
index snapshot and extended quoted aliases. All owned PIDs are confirmed gone.

A later final repeat (`76031`) is retained as terminal 1: both extended and
descriptor fixtures timed out their first CREATE TABLE before the added
application_name status assertions. Neither the 15 s socket timeout nor source
was changed. Its original 100 ms / 15 s CLI statement_timeout recovery gate
passed. Read-only inspection while its second owned server was live observed
worker TID 262346 in D/submit_bio_wait; this is blocked-I/O evidence, not a
complete causal diagnosis or a fixed I/O issue. `wire-v3-final.log` and
`final-repeat-timeout-observation.md` retain the result. A bounded unchanged
repeat (`83612`) is terminal 0 for both full protocol gates, including the new
before/after-promotion application_name ParameterStatus assertions. Owned PIDs
266299/266822 were stopped and are confirmed gone. This successful repeat does
not erase `76031` or turn an observed I/O stall into a solved performance issue.

Broader database-generation reuse, parameterized constant queries, and conservative
real-owner shapes are not closed by these results. No complete family claim or
full default suite pass is made.
