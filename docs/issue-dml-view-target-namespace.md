# DML target dispatch uses the resolved relation, not requested spelling

The original native bridge mistakes `public.session_view` for a nonexistent
table and lets an unrelated public view divert a real reporting/temporary
table with the same unqualified name. Actual corrected38-control baseline
against ROOT078's all58 normal optimized objects exits1 with14 wrong-route
checks in `/tmp/dbms-view-target-namespace.66ybpT2J/native-baseline-v2.log`.

Private resolveTable now also reports the resolved target's view role. It
recognizes actual CREATE VIEW metadata's `schema.name` spelling separately
from heap tables' `schema__name`, preserves quoted names, and lets temporary
and earlier search_path tables shadow later public views. All nine ordinary
INSERT/UPDATE/DELETE/MERGE target guards consume this resolved identity,
not another lookup using raw requested spelling. Read/source materialized
view55000 checks remain separate from target-role classification.

The permanent native fixture keeps32 view spelling/dispatch checks and6 real
table-shadow CRUD checks, caller-context restoration, no partial view results,
base-row preservation and empty final real targets/lock state. Already
unsupported view UPDATE FROM/DELETE USING retain their exact existing0A000
and no-effects route; this namespace correction does not implement that
remaining feature family or silently send unsupported shapes to unsafe
legacy mutation. The first draft's26 failures included mistaken expectations
for those existing unsupported routes; both that baseline and the24-failure
first candidate remain diagnostic logs, not claimed production bug counts.

Final normalO2 source proof freshly compiles only DmlExecutor CPP, with all58
ROOT078 donor signatures,57 identical other sources and every header audited.
Fresh stubs and7 distinct natives actually pass, handle19202/log
`native-candidate-v2.log`, including all38 namespace controls, original
materialized-view lifecycle, RETURNING, nullable sources, stored-function
atomicity and typed predicates. Run-specific native output/stub paths avoid
overwriting another run's executable.

Frozen candidate SHA256 is
`3b847fe74793af186dca8022e2dffea601d5f982654efc337aefffc29b5eb0c0`.
The6-wire group9721 exits1:five complete adjacent scripts pass and original
materialized-view setup times out under its unchanged deadline. That failed
log remains `view-namespace-wire.log`; no timing cause is claimed. An
unchanged original materialized-view script repeat4091 against the exact same
frozen binary/deadline exits0 in `matview-refresh-frozen-repeat.log`.
Thus all6 distinct scripts eventually actually pass, not one all-green group.

Actual wire diagnostics also expose a separate ordinary qualified
materialized-view INSERT erroneously accepted while strict PostgreSQL18.6
rejects42809; WITH targets report0A000 instead of42809. Readonly target
execution/error-priority is under a separate independent repair, not closed
by this namespace commit. Strict180006 also reports planning22012 for
`UPDATE mv SET id=1/0`, and retains unknown-input/name priority; no blanket
early target rejection is inferred. No full suite/family closure, push,
Actions activation or deferred security/TDE work.
