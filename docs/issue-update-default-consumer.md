# UPDATE DEFAULT uses its retained, execution-owned value AST

The ordinary target-only UPDATE entry no longer excludes DEFAULT from its typed
consumer. Whole binding resolves the current target definition, then the actual
PreparedQueryExecution plans structural constants in each lowered DEFAULT root
before row demand. A volatile default's constant argument can raise 22012 under
WHERE false without calling the routine. A genuinely dead CASE branch does not
raise or call it. Non-DEFAULT ordinary consumers retain their prior planning
entry; WITH execution already uses its genuine root planner and bound source
provider. The compiled value graph is consumed by the same carrier that runs
the mutation, not a discarded preflight or a second rendered query.

Predicates precede runtime SET evaluation. Defaults execute once per matched row;
false/zero rows execute none. Actual nullable row cells and OLD/NEW output channels
are retained. Storage applies real NOT NULL/CHECK constraints and statement
rollback; nextval remains nontransactional. No routine or source is executed to
discover a default or its descriptor. A default still present at BoundDmlExecution
after the engine supplier has run is an internal lowering error, not supported
fallback execution.

Two permanently registered, unfiltered whole scripts preserve ordinary/typed
UPDATE, current direct-domain versus explicit/NULL column sources, domain creation
snapshots, ALTER COLUMN SET/DROP, WITH primary/producer mutation, numeric implicit
conversion, OLD/NEW and qualified aliases, known errors before effects, volatile
row counts, savepoint/failed-block/full rollback and a new backend on committed
objects. Parse/Describe obtains the real descriptor without calls. Populated and
unfilled MV DEFAULT controls keep target 42809 and source 55000.

Actual DML EXPLAIN execution is still a separate frontend/planner root cause.
The native metadata-only prepare of EXPLAIN UPDATE proves no calls; it is not
a successful EXPLAIN/ANALYZE consumer or a fabricated UPDATE plan. Ordinary UPDATE
FROM, VIEW triggers, generated/identity defaults, cursor/ONLY edges, complete
routine/default identity freezing and all assignment types are not claimed
complete by these target-only controls.

Evidence: /tmp/dbms-domain-update-default.eBHK9e56 retains the all58 new-header
O0 build, base/priority/numeric/qualifier/MV failures and actual strict180006
references. The final candidate V4 SHA256 is
18d3742b292b7124831c13603650247b8191f98140f5a113d9f9abc65426c519.
The original full fourteen-script serial group 66870 exits0, including both new
scripts and twelve complete adjacent scripts (source71, MV48, old defaults,
typed UPDATE, duplicate targets, pattern priority, sequence DDL, ARRAY routine,
provider/search_path and domain ancestry/FK). SQL, assertions and deadlines are
unchanged. A mistaken initial MV fixture using a TEMP source was corrected only
after retained strict18 rejection; the regular-source strict18 whole then exits0.
The earlier successful V3 group is not treated as coverage for the added MV cases.

Twelve final natives 70610 exit0, including actual-engine cold binding, exact
qualified default persistence, matched/false demand and native MV 42809. Final
ASan/UBSan four production TUs (TableManage, query_binding, DdlExecutor and
DmlExecutor) plus five drivers 14900 exit0; the other 54 TUs remain normal O0.
Two additional complete array ALTER/value and physical array descriptor scripts
53145 also pass after the source-span change. The earlier three-TU sanitizer
93086 exits0 but is not presented as proof for the final DDL/MV source changes.
Final all58 sources/107 headers/object receipts and flags match the frozen V4
production bytes; there is no mixing with ROOT's later public headers. Initial
new-native sequence setup used a low-level API without a catalog row under an
active session (42P01); the retained fixture failure was corrected by real CREATE
SEQUENCE, without weakening its original 55000/no-call assertion.
This private O0/CPP-rebuild epoch does not substitute for ROOT's normal full58
build or repository-wide completion. All own build/native/sanitizer/wire handles
are terminal; no candidate SQL server remains.
