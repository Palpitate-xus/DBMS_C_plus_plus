# Genuine EXPLAIN analysis and protocol descriptors

The whole binder now describes EXPLAIN as QUERY PLAN, with intrinsic TEXT
OID 25 or JSON OID 114 (and XML OID 142 in the pure AST descriptor). The
frontend TEXT/JSON consumer binds the actual ExplainStmt and its entire inner
statement before publishing a named statement or executing analyzed effects.
Provided protocol parameter OIDs become real positioned binding datums; no
parameter values or substituted NULL SQL are used for description. Catalog
type ancestry is copied metadata, with cycle protection.

Parse/Describe does not plan constants, invoke defaults/routines or open row
providers. A malformed unknown input is an analysis error and does not leave
the failed named statement published; arithmetic default planning remains an
execution phase. The Simple Query descriptor is retained from before effects,
and JSON is one complete datum from the actual renderer, not several CLI
lines independently labeled JSON. Statement and portal descriptions share the
same producer.

The permanent unfiltered phase matrix includes both description phases,
TEXT/JSON labels and OIDs, JSON parsing, a failed Parse followed by reuse of
the same name, typed $1 execution, no descriptor-time sequence calls, and
changed volatile/constant/NULL/bad arithmetic defaults for a named EXPLAIN
ANALYZE. It is registered without a skip.

Evidence is under `/tmp/dbms-insert-default-plan.oBQfCVWC/candidate-v2`.
The prior compound-span fix's whole phase failures remain in its V4 logs:
Describe incorrectly returned NoData. The old matching native JSON descriptor
assertion fails with exit 134 in `descriptor-baseline.log` (21409). No original
assertion, SQL, effect counter or timeout has been relaxed. The first expanded
strict reference attempt reused a named portal and correctly got 42P03; the
fixture now uses a distinct named portal for each statement. The complete
strict PostgreSQL 18.6 reference (`180006`) is terminal 0 in
`default-phase.reference18.final.log`.

V6 uses the genuine new InsertStmt all-58 O0 epoch, with its three later
changed CPPs freshly compiled and every other source/header/flag/object kept
matching. It does not reuse ROOT's different AST epoch. Normal binary SHA-256:
`dec7778ad6ae036d2fa1b4980d681264427cb2426c4bac49709a4f48f6ff6a82`.
The complete fourteen-script serial group 1775 is terminal 0, including the
new phase whole, full 29-by-4 INSERT default matrix, 33-by-4 mutation EXPLAIN,
typed EXPLAIN, domain origin/ancestry/transaction, UPDATE DEFAULT, ordinary
WHERE/ORDER, PL binding, bound DML children, WITH sources and Q84.
All thirteen native tests 47520 are terminal 0, including the exact final
descriptor/span drivers, stored defaults, binder/planner/cursor, actual
mutation plans and ordinary WHERE/ORDER.

The six changed production CPPs and the native drivers/stubs are scoped
ASan/UBSan; the other 52 production objects, including main, remain matching
normal O0. Native descriptor/span tests 57584 and both entire phase/default
wire matrices 4686 are terminal 0. Leak detection is disabled. Sanitizer binary
SHA-256: `7e2f2056dae2f5d7610b7ff8ec305c09dfa5455516306c6d1040ddcfac984afe`.
This is not an all-58 sanitizer or O2 claim.

XML/YAML runtime renderers, WITH-final-DML EXPLAIN envelopes, all parameter
OID-zero inference, domains/user casts, exact PostgreSQL cost estimates and
all physical provider instrumentation remain independent open work. The
frontend explicitly declines unowned XML/YAML rendering instead of labeling
text as those types. This change does not claim the entire EXPLAIN family is
complete.
