# Correlated scalar fallback: retain the actual typed child

## Independent root cause and unchanged regression

The origin-label repair `b1a60a90` correctly fixes aggregate runtime NULL
parameters, but it does not repair the scalar child's legacy transport. That
transport renders a real correlated NULL into `CAST(NULL AS type)`, reparses it,
and therefore gives it literal-constant demand semantics. The unchanged native
24-case/25-assertion regression proves four new d14 writer omissions on the
actual public `PreparedQueryExecution` owner, without a fake reader/provider.
The pre-demand Root104 baseline passes all 25; the d14 and origin-only baseline
each fail exactly four. The existing 12-case Main protocol control passes on
all baselines and cannot be used to approve this different native owner.

## Finite transport repair

The new `executeScalarSubquery` overload receives the original, wholly bound
`PreparedQuery` shared owner, the genuine retained child `Stmt` identity and the
actual caller `RowContext`. These are explicit execution-owned metadata and
typed cells: catalog/source/namespace bindings, real `QueryBindingDatum` origins,
original parameter nodes and real correlated row values remain together.
No role is inferred from a slot, source name, SQL spelling, value or NULL flag.
No function, parameter callback, child or volatile effect is executed to infer
the graph's type or provenance. There is no new parameterized-SQL rebind, which
could otherwise lose an ancestor's original namespace/catalog owner.

`PlPgsqlQueryOptions` appends the retained owner, child pointer and copied caller
row. The shared owner keeps that pointer alive. Existing max-row and purpose
defaults, callback signatures, public Root fields and old string-only API remain
intact. Existing explicit host ownership is preserved. Its historical SQL text
is still supplied because permanent host-contract controls deliberately inspect
`CAST('12' AS integer) AS "id"`; those assertions are not weakened or rewritten.

The actual native consumer uses the retained child and caller row in a real
typed `QueryPlanner` cursor. It never executes the rendered correlated literal.
The actual Main host consumes supported source-free graphs from the same typed
carrier. Unsupported graph admission remains pure, before opening any source
or effect; the established full-dispatcher compatibility path remains for other
shapes. This finite issue does not declare every Main/embedding fallback shape
fixed, and arbitrary external callbacks may still choose their old string API.

Ordinary children retain their caller statement/read view. This overload does
not enter the stored-function SPI command-counter or snapshot-refresh wrapper.
The two-row scalar demand, empty typed NULL, exact result width, `21000`
cardinality error, primary error precedence, explicit close and per-execution
memo contracts remain checked. There is no execution retry after a source or
effect has opened.

## Actual private gates

Private base: `b1a60a90` (the independent role fix) over immutable Root104+d14
`86a8e6e6`. Worktree: `/tmp/dbms-correlated-typed-carrier.LXsrVHmQ/repo`.
Two more public headers change, so this tree compiles every normal production
TU afresh. No old-ABI object is copied or donated.

Session 36431 completes the normal/repeat/all-58 receipt and build-stamp audit
and ten complete native fixtures with exit 0. The log has exactly 58 fresh
compilation lines. These include unchanged correlated25 (now zero failures),
origin127, original demand587, original prepared execution, real scalar host
ownership, snapshot context, typed cursor, projection demand/cardinality,
primary-error cleanup and self-correlation controls.

Frozen binary:
`/tmp/dbms-correlated-typed-carrier.LXsrVHmQ/dbms_main.typed-carrier-v1.frozen`.
SHA256 `bcc66c5c6a769cc3bbdb9bd0b275d1c66b029f7180dc8bdc91255deb42c8ab36`.
Frozen source/test/script input hash:
`04961a2ec71755e010623698d83fe44edd51a2e19c376f98f063af1066d31347`.

All eight complete strong demand fixtures, session 59223, actually exit 0:
new112, correlated12, original190 and original20, each once with strict real
PG18.6 `180006` and once with the candidate. Default wire/startup deadlines and
all original SQL/assertions remain unchanged; each reference uses its own
isolated schema and removes it in `finally`.

The unchanged parent original32, session 68040, actually exits 1 for only the
same two old COUNT(*) FILTER BETWEEN `0A000` cases. All 26 introduced row/computed
NULL omissions and all other supported cases remain repaired. That old failure
is retained in `typed-carrier-original32-v1.log`, not relabeled as PASS.

Complete Root94 native session 42364 and Root78 wire session 56202 each actually
exit 0: all 94 native and all 78 whole fixtures, against the candidate's own
fresh normal O2 objects and frozen binary, with current source/header/flag
receipts and the all-58 stamp verified before and after each collection.
The exact logs are `typed-carrier-root94-native-v1.log` and
`typed-carrier-root78-wire-v1.log`. The original family273 is not closed.

## Explicit next independent boundary

A separate 12-control read-only API probe against the origin-only snapshot
demonstrates six dynamic runtime/metadata-parameter child memo differences:
after a real caller changes NULL to `00` and `01`, the uncorrelated child retains
its first NULL memo and one writer effect instead of three. True statement input
memo is correct. This boundary is not repaired by merely retaining a child
carrier; memo qualification and parameter-context ownership need a separate
origin-aware root-cause change and strong unchanged controls. No claim that all
runtime-parameter/correlated/embedding fallbacks are green is made here.

No Root/master write, push, Actions change or filtered trigger/view/temp/WAL/
EXPLAIN task is performed in this private issue.
