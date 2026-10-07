# Source DML RETURNING name oracle

The original `update_delete_from_protocol_e2e_test.py` expected successful
`RETURNING id, val` when both target and source expose both names. Its exact
SQL and original assertions fail on the local strictly checked PostgreSQL 18.6
reference with 42702 at UPDATE. The unchanged original fixture passes on the old
matching array-bounds frozen binary; the new DELETE consumer still accepts that
UPDATE and then correctly fails the original DELETE's RETURNING with 42702.
Per-SQL observation corrects the initial misreading of the original traceback
line: the candidate error was DELETE, not UPDATE. The old success is not
PostgreSQL evidence. Both results,
the reference failure, and the original 33-script candidate group (exit 1)
remain in `/tmp/dbms-delete-bound-integration.fkakvwdN`.

This independent test correction retains both original UPDATE and DELETE SQL
strings as 42702 negative controls. Each checks no command tag, no mutation,
exact rows, OIDs, and idle ReadyForQuery. Both duplicate column names are
qualified in subsequent positive statements (`dst.id, dst.val`), which execute
the original successful row/count/type expectations. No production source is
part of this oracle correction.

The corrected candidate fixture exposes a distinct remaining UPDATE FROM pure
binding gap: the current adapter incorrectly accepts the ambiguous original
UPDATE, so the new 42702/no-effect negative control really fails. Its complete
normal and scoped-sanitizer group failures remain recorded; they must not be
ignored or described as passed merely because the DELETE controls succeed.

The original UPDATE LEFT JOIN and cursor-positioned mutation SQL remain in the
full fixture. Their project 0A000 expectations are historical unsupported
boundaries, not PostgreSQL requirements fulfilled: PostgreSQL executes the
legal outer source and reports 34000 for the missing cursor. The strict
reference mode checks those real PostgreSQL outcomes separately, while the
project mode retains the existing fail-closed expectations. Full nullable
UPDATE-source execution and cursor-positioned mutation remain OPEN; a passing
dual-mode fixture must not be described as closing those capabilities.

No SQL, original control, or default wire deadline is deleted or relaxed. The
new connection mode checks `server_version_num = 180006` before trusting the
reference. Test-owned reference tables are cleaned up even on assertion failure.
