# Index completeness must be decided before residual effects

Date: 2026-10-06. Follow-up to the narrow read-owner exception in `d184d8a1`.
IDX-03/TXN-01/02 and SQL-04 remain partial, not complete MVCC indexes or routines.

## Actual regression

The formal optimized ROOT `6460a246` combination used a post-candidate owner
check after evaluating residuals. A current isolated read owner became a writer
when its indexed residual called a volatile routine. Discarding that already
evaluated candidate set and scanning the heap repeated the routine's write.

The first fresh matching native probe, terminal `16308`, exited 134, printing
`actual calls=2` for one matching input row before its expected-one assertion.
The permanent native, compiled against frozen matching formal 646 objects,
also exited 134 in terminal `13868` at the unchanged one-call assertion.
Its log is `/tmp/dbms-index-recheck-effects.mkD1UgDL/permanent-native-baseline.log`.
The frozen formal binary SHA is `d6efa285dc82a5b06de534cdd6a1978e59532abb5fa6351a05403c45d42ce7b0`.

The new frontend protocol **already passed** on that old frozen binary
(`68767`, exit 0), because its typed WHERE route does not enter this same native
index-residual path. This is a useful frontend compatibility control, not a
manufactured wire baseline failure. The actual regression is the public native
query/index boundary.

## Repair

All index branches now select physical candidate RIDs without evaluating SQL
residuals. Required physical row bytes and their NULL bitmaps are copied before
the allocation/owner fence is checked. If the fence fails, the untouched query
can safely use its original-snapshot heap scan. Otherwise, residuals execute
once over the selected physical images, and cannot cause a second heap pass.

The selected index atom is skipped only when the physical lookup already proves
it; long fixed-width keys still recheck that atom. Other conjuncts are checked
centrally, including nullable predicates. Buffered bitmap binding has priority
over a later live-RID bitmap, while retaining the actual row engine/database
context for virtual/scalar values. This prevents mixing frozen row bytes with
unrelated later metadata. The captured-NULL strengthening came from an
independent source review; it is not mislabeled as a separately reproduced bug.

Actual index scan counters and existing SSI predicate recording are retained.
Checked index errors still fail closed before any fallback. The old raw
readRowByRid false result cannot distinguish absent rows from every heap I/O
failure; this preexisting limitation is not called fully repaired here.
The preexisting nontransactional index path gets no new general completeness
guarantee. Allocation races, physical index MVCC/vacuum behavior and full SSI
remain broader independent work.

## Matching artifacts and real validation

Worktree `/tmp/dbms-index-recheck-effects.mkD1UgDL/repo`, base `6460a246`.
No production header/API/layout/disk change. Before reuse, the other 54 source
files and every header were compared to the verified formal ROOT combination,
and each original object signature was checked. Those objects were frozen
privately before ROOT's newer header changes. Candidate TableManage was freshly
compiled with the shared formal O2 flags; tests/stubs were also freshly compiled
and each native ran in a separate fresh directory.

V1 build/native terminal `23178` passed 12 entry points, but predates the captured
bitmap strengthening. It is retained separately as `dbms_main.v1.frozen` and
`TableManage.v1.o`. V2 terminal `43857` completed compilation and 11 adjacent
passes, but its expanded native initially failed `0A000`: the standalone PL
bridge requires a full SQL host for UPDATE. This is not another repeated-effect
failure or a successful full-native-DML claim. Its failed log is retained.

The permanent native keeps that unsupported-UPDATE negative assertion rather
than pretending UPDATE ran. Its supported two-candidate INSERT residual still
must write exactly twice and preserve both original nullable images. The real
protocol independently performs the source UPDATE and checks both original
NULL rows, two effects and rollback restoration.

Final matching native terminal `58716` exited 0 for 12 distinct entry points:
index_residual_execution_once, read_owner_index_snapshot,
index_scan_full_value_recheck, integer_index_full_value_recheck,
legacy_index_prefix_recheck, update_index_failure, mvcc_update, runtime_stats,
planner_runtime_stats, stored_function_atomicity, stored_function_scalar_resolver
and null_safe_join. Match/reject/NULL each call the writer once, retaining real
index use without a false heap counter. Exact log: `native-final.log`.

Six additional freshly linked optimized native index-AM/checked-error entry
points passed in terminal `67016`: gin_brin_index, gist_range_search, gist_scan,
fulltext_search, hash_index_checksum and index_scan_corruption_error. These
retain their expected corruption diagnostics (including the deliberately
incomplete rollback case), not silent-success paths. Exact log:
`native-index-ams.log`. Together these are 18 distinct final native entry points.

Protocol terminal `17314` exited 0 for nine distinct scripts: the new residual
control, read_owner_index_snapshot, index_scan_full_value,
legacy_index_prefix_recheck, stored_function_where_execution,
stored_function_atomicity, null_safe_join, query_snapshot_characteristics and
commit_failure_recovery. Exact list and frozen-binary environment are in
`verify-protocol.sh`; exact log is `protocol-final.log`.

Final CPP SHA `637a5b255b35ddab0bf534b9fc0c255e494ae891feb5dc95944bd5eefe2afc6f`;
object SHA `6c0fe4fa657af15215cd92b56739112e8e71761edb7c91ef72f94ca8e6a28151`;
binary SHA `4f53646986f276609085c601357691546478248528c0cdb72bfb104fd387c409`.
All artifacts and scripts above remain in the parent private directory.
Two initial provenance checks used private absolute-path signatures where the
original ROOT path was required; both stopped before compilation. Corrected
checks verified actual ROOT signatures before copying. Initial probe fixture
compile/argument-order failures are retained separately from the true red.

The formal ROOT combination with subsequent owner, receiver and numeric fixes
was independently rebuilt in 96406 with all55 fresh formalO2 objects; repeat
build and55/55 signatures/stamp passed. Frozen a38da2abc3fb62270eedd988d67e66135a5a5f3a446f535c7e7ef92cb4a02b9c
then passed39 fresh matching native49782 and36 wire96112 entry points, including
this native/wire regression. See `docs/issue-engine-owner-receiver-numeric-followups.md`.
The unchanged known-gap diagnostic still fails7 cases. A new complete default
protocol80673 is running, not a completed passing gate. No
full registered suite, TLS runtime, PG18.6 differential or full-program
sanitizer claim. Earlier full-gate failures/I/O amplification remain recorded.
No push or Actions enablement; user-deferred security/TDE remains deferred.
