# Native DML binds to the passed session and restores its caller

Actual ROOT4cf fresh normal58 matching native matview fixture fails at the
original55000 assertion. Diagnostic repro records the structured error:
42P01, relation sink does not exist. The passed session has search_path
`reporting, public`, and reporting.sink exists; prepareBoundQuery instead
uses an absent/unrelated ambient session and searches public. Target/source
resolution and pure preparation therefore disagree within one native DML.
Original and diagnostic logs remain in
`/tmp/dbms-array-source-integration.XsuuhG42/array-source-combination-native.log`
and `/tmp/dbms-checked-native-fixtures.xmBivqmc/checked-fixtures.log`.

tryDmlBridge now establishes a stack-scoped currentSession equal to its
explicit Session argument after recognizing DML. It restores the prior
pointer on success, unsupported fallback and exception unwind. No new global
owner, public header/API, catalog mutation or string-rewritten binding is
introduced. Nested routines and preparation use the same passed namespace.

The original unpopulated-materialized-view55000 assertion remains. Added
controls supply a conflicting public-only caller, verify no target mutation
on the original failure, insert8 into the reporting target on success, and
prove caller restoration after the supported unqualified view fallback.
The separately observed qualified view fallback defect is not declared fixed;
its exact failed SQL is retained in issue-qualified-view-target-dispatch-open.md.

Actual matching optimized source verification: sole changed DmlExecutor CPP
freshly compiled with the normalO2 flags; all58 donor signatures,57 unchanged
source files and every public header match ROOT4cf. Fresh test stubs and7
distinct native fixtures all exit0, final handle18668/log
`/tmp/dbms-checked-native-fixtures.xmBivqmc/dml-session-final-v3.log`.
An earlier draft assert-macro compile failure and newly exposed qualified
view control failure remain in dml-session-candidate.log/final.log; neither
failed run is relabeled green.

Six complete adjacent protocol scripts actually exit0, handle2941/log
`/tmp/dbms-checked-native-fixtures.xmBivqmc/dml-session-wire.log`:
materialized-view refresh, stored-function atomicity, versioned UPDATE,
array RETURNING,47 multi-source controls and5 multi-source boundaries.
Frozen source candidate SHA256 is
`25d97b62b45575ee6a556d0f60951bf15073cbb155643eb368efad860b438bd2`.
Strict PostgreSQL18.6 verified180006 namespace/unpopulated55000/no-effects/
populated-reference controls exit0 in session-reference18.log; its isolated
schema/table/view changes are enclosed in BEGIN/ROLLBACK, not left behind.

This is a native session-context repair, not all view/routine/binder closure,
full-suite success, TLS or sanitizer evidence. Only local git commit; no push,
Actions enablement or deferred security/TDE work.
