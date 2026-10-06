# Pure-literal SELECT database transaction demand

Status: original recovery, matching native and final adjacent verification
passed. This is a
separate root cause from statement-start SQLSTATE mapping.

## Actual red

The original registered table-lock regression preserves `lock_timeout=50`
and both 15-second second-connection bounds. After another connection begins
and performs ALTER TABLE, physical SELECT must fail with `55P03`, but the
following SELECT 1 must succeed without touching the locked relation.

Old canonical production `0204a334...` failed earlier with XX000, hiding this
second failure. Private V1's structured-start repair did pass the two
physical SELECT SQLSTATE controls, then returned `55P03` for SELECT 1
(50804, exit 1; `v1-constant-red.log`). Exact frozen V1 binary is
`/tmp/dbms-query-begin-lock-state.TNJZ551R/dbms_main.lock-state.o2`, SHA256
`0375741794531be915fa08b8c0db7ca1932e6f63168555ef7e728d5c528728a9`;
its fresh 57-unit build/repeat/signature/stamp checks exited 0 (90130).
This is not relabelled as the current candidate or as a ROOT matching binary.

`requiresQuerySnapshot` recognizes all SELECT queries, including constants.
The new implicit owner treated that snapshot classification as an unconditional
request for StorageEngine::beginTransaction, whose database shared lock waits
behind DDL's exclusive rollback-image protection. A literal result needs no
database access, so this locking dependency is unnecessary.

## Repair contract

`SQLParser::isDatabaseIndependentQuery(const Stmt&)` is a conservative,
metadata-only proof over the entire parsed query, not a string search, a
routine-name whitelist, a query execution or a row-count observation. It
checks projection, WHERE, ORDER and DISTINCT ON; relations, CTEs, set
operations, VALUES envelopes, grouping/windows/locking and unhandled roles
retain ownership. Primitive literal leaves must be lexically genuine datums,
not SQL-child/array-bound/aggregate text disguised as LiteralExpr. Declared
or unknown literal types, parameter/column references, all routines, casts,
COLLATE and opaque children are conservative. Primitive direct operators
recurse through operands; no operand is evaluated by the proof.

CASE and the parser's BETWEEN intrinsic-call envelope also remain owned:
legacy execution may lower grammar to synthetic routines, so this change
does not assume that a syntactically constant expression proves an unknown
executor callback pure. Output-alias references similarly remain conservative.
This is not full constant-folding or complete side-effect-demand analysis.

Only an outer autocommit SELECT with this proof skips the database owner.
All writing routines and unknown expression shapes retain the same calling-
statement transaction and rollback boundary. `requiresQuerySnapshot` itself
is unchanged; existing user transactions keep command IDs, catalog/read
views, snapshot acquisition, locks, notifications and isolation-change rules.
The normal notification statement boundary for an owner-free statement is
also retained.

Extended-protocol preparation has its own explicit BEGIN path before this
dispatcher proof. The simple-query repair is not proof that an extended
pure-constant Parse/Bind/Execute can run while another connection holds DDL's
database lock. That path remains subject to a separate actual phase probe;
no virtual-transaction implementation or verified extended-constant pass is
claimed here. The tested extended physical SELECT still returns exact 55P03
and recovers through Sync I, followed by a successful simple SELECT 1.

The public parser declaration requires a new private fresh 57-unit group;
the prior V2 objects/binary cannot establish the final new-header proof.
The independent native regression tests positive primitive query shapes,
negative relation/routine/subquery/type/parameter roles and malformed/typed
LiteralExpr controls without executing any SQL. The original registered
wire regression retains its original assertions and adds primitive literal
results/error recovery, repeated timeouts with holder-T validation, extended
Sync recovery and a post-release actually committed writing function.

## Matching proof

Private parent is the separately verified structured-start commit `2f85de1f`
on source `912894e1`. New parser/native syntax check 85028 exited 0. New
public parser header group compiled all 57 production units from scratch
(57 compile entries), repeated up-to-date and matched 57/57 signatures and
binary stamp (36304, exit 0). Exact frozen
`/tmp/dbms-query-begin-lock-state.TNJZ551R/dbms_main.lock-state.v3.o2`, SHA256
`6738292c68369fac3a53970066a52692725c0c1a336bd3ffa96f937507a801d7`.

The original expanded registered lock regression passed at unchanged limits
(28346, exit 0; `candidate-literal-owner.log`, owned PID 4094232 stopped).
Both original physical SELECT checks and SELECT 1 recovery remain; the
extra pure primitive cases, 22012 recovery, seven repeated timeout commands,
extended physical-query recovery, holder-T protection and post-release
writer commit also passed. The matching native batch passed 8/8 (48242,
exit 0; `native-literal-owner-v3.log`): new literal-demand proof (20 positive,
26 conservative negative and three manual type/opaque guards), parser,
binder/carrier, function atomicity/actual owner/resolver and array metadata.
Every native was linked from this new header group's checked production
objects with freshly compiled stubs, not the old V2 object ABI.
The final serial adjacent batch passed 8/8 (61768, exit 0;
`adjacent-literal-owner-v3.log`): the 9-shape transaction-start error test,
transaction SELECT relation locks, DDL upgrade timeout, row timeout, DML
lock-error/savepoint, function atomicity, simple/extended snapshot rules,
and all 18 INTO + 4 ordinary + 20 row-demand boundaries. Final owned PID
4103402 was stopped and confirmed gone. All production sources/headers were
frozen throughout the build and matching runtime checks.
No full-suite, all-pure-query or transaction-family completion is claimed.
