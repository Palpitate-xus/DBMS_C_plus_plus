# User-deferred scope and ordinary incomplete work

On 2026-10-20 the user explicitly said "skip it" while R177 grouped ordinary
aggregate ORDER/DISTINCT/projection work was active. This investigation remains
unresolved and deferred; its observed failures and isolated unaccepted candidate
are preserved in [the R177 record](issue-review-r177-grouped-aggregate-order.md).
No R177 production fix or registered regression is accepted. A generic continuation
does not resume this module. Independent ordinary SQL correctness remains in
scope, R164 remains unresolved rather than deferred, and the original273 ledger
and its15 original deferred items are unchanged.

The user subsequently also asked to skip the CTAS / background checkpoint investigation. That specific investigation remains unresolved and deferred separately from the original 15 ledger entries; it does not defer all catalog, storage, recovery or ordinary query correctness work. Existing regression observations remain evidence, not a completion claim. Current review findings are recorded in [the review index](review-index.md).

The user also separately deferred R15 (shutdown wake-order). On 2026-10-14 the
user asked to skip the then-current R51 DROP DATABASE/cache-lock contention
investigation and move to the next issue. R51 remains unresolved and deferred;
its observed DATABASE_IN_USE failures are retained. An unfinished diagnostic
fixture is not part of the registered suite. Do not resume this investigation
merely on a generic "continue", and do not broaden it to all DROP, recovery or
cache correctness work. Existing unfiltered registered regression observations
may still be recorded; they do not authorize reopening the deferred investigation.

On 2026-10-14 the user also explicitly asked to skip the then-current R65
mutation execution graph / child-provider lifecycle investigation. It remains
unresolved. The five original diagnostic modes exited 134/134/134/134/139;
the partial implementation still failed its acceptance gate. Its five production
file changes and two new unaccepted native tests were removed from the candidate
tree after preservation under `/tmp/dbms-review-r46.ifu6SWLp/` as
`r65-user-deferred.patch` and `r65-user-deferred-tests/`. Earlier build, assertion,
and stack-trace logs remain there and in
`/tmp/dbms-review-orphan-controls.vQleNfjM/`. These local evidence paths are not
portable release artifacts. No R65 product fix is committed or accepted, and a
generic continuation does not resume this module. Ordinary view replacement and
other independent SQL correctness issues remain in scope. The original273-item
ledger and its15 deferred items are unchanged by this separate investigation.

On 2026-10-15 the user explicitly said "skip it, then continue" while R96's
builtin collation exit-lifetime investigation was active. R96 remains unresolved;
the pure component sanitizer failure is preserved in [its record](issue-review-r96-collation-exit-user-deferred.md).
There is no accepted product fix. Do not resume, rename or bypass this specific
investigation on a generic continuation, and do not broaden the skip to all
collations, types, or ordinary query correctness. The component evidence does
not prove the R95 warning has the same runtime stack. This additional deferral
does not change the original273 ledger or its15 original deferred entries.

On 2026-10-16 the user explicitly said "skip it" while R106's unselected explicit
CAST input-analysis investigation was active. R106 is unresolved and deferred;
its four original observations and full 612-control reference/old-program test
logs remain in [its record](issue-review-r106-coalesce-explicit-input-analysis.md).
There are no R106 product edits or accepted registered tests. The unregistered
test draft is preserved, not committed as an accepted fix. Do not reopen this
specific investigation on a generic continuation or broaden it to all address
types, expressions, input checks, or ordinary SQL correctness. This separate
deferral does not change the original273 ledger or its15 original deferred items.

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
