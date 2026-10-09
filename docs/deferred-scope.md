# User-deferred scope and ordinary incomplete work

The original273-item ledger explicitly defers15 items: P0-13 (unified permission
boundaries), STO-08 (TDE), and SEC-01 through SEC-13. These remain
deferred_by_user, unchecked and outside the repair workflow. Do not implement or
claim completion of the deferred security, authentication, ACL/RLS or TDE work.

Ordinary SQL/type/declaration, query execution, storage correctness, recovery and
EXPLAIN requirements are not deferred merely because a test fails or a prior
diagnostic was not pursued. Historical reports calling every CREATE-error,
NAME/enum declaration, TEMP/recovery, trigger or EXPLAIN failure "user-filtered"
overstate the recorded user scope. Those reports preserve real failures, not new
deferred item states or authorization to abandon ordinary functionality.

For an overlapping failure, keep the security/permission aspect deferred and
continue the separable ordinary correctness work. For example, registering
builtin NAME as a routine parameter/result type does not change function ACLs,
SECURITY DEFINER/INVOKER, login roles or authentication. Its declaration error is
still ordinary TYPE/FUNC work, and strict positive tests must remain positive.

Do not delete or loosen failed tests, silently expand the deferred list, or mark
an entire original family complete using narrow repair evidence. The full ledger
remains273 items; existing partial/unverified items require further work.

This clarifies execution scope, not a new completion claim. No push or Actions
activation, and no restart of the15 user-deferred security/TDE requirements.
