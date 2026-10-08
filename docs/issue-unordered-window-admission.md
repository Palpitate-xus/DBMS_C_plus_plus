# Admit builtin windows with an empty specification

Independent production repair based on Root `9603c8ab` / source `53865cec`.
The private Main parser incorrectly rejected every non-aggregate window
without PARTITION BY or ORDER BY. It then sent valid builtin calls to a scalar
or hypothetical aggregate receiver. Row-number/offset functions reported
42883; rank/dense_rank silently returned zero rows on populated input.

An empty specification is admitted only for the existing eleven builtin
window spellings. Arbitrary scalar names are not added to this admission.
Before consuming any newly admitted non-aggregate window, Main performs pure
binding of the actual complete query exactly once. The existing metadata
receiver validates real signatures, arguments and column/range identities,
including empty input, WHERE FALSE and LIMIT 0. No row/body is evaluated to
decide admission. The initialized default sort direction from `53865cec`
and all actual current Root CPP/header/consumer changes are retained.

This is not an all-window implementation: qualified/named/malformed window,
stored callee/search-path ownership, dynamic offset/value expressions,
additional frames and general zero-demand preparation remain separate scope.
The complete new fixture retains NTH_VALUE rather than hiding its independent
default-frame failure. No claim is made that this prefix passes all84.

## Complete evidence

Artifacts: `/tmp/dbms-root-unordered-window.C4g0LIT2`.

| Actual complete gate | Result |
| --- | --- |
| Owned strict PostgreSQL18.6, server_version_num180006 | 0; all84 |
| Unchanged Root108 binary0d on the same84 fixture | 15270=1; all84,37 failures |
| Sole fresh Main normal/repeat build | 87712=0; actual1 fresh CPP +57 individually source/header/flags/compiler/manifest/receipt/object-byte proved current Root normal donors |
| Current candidate full84 + five original whole-window wrappers | 62342=1; all6 finish,84 has exactly1 NTH_VALUE frame failure; all five original wrappers pass |
| Five complete fresh native drivers/stubs, default disk | 65923=0; full window/NULL144/GROUPS72/metadata/Volcano drivers |
| Separately registered actual Main frontend58 | 94577=0; true current Main, no test stubs, genuine isolated data directory |

Candidate frozen normal binary SHA256:
`00b55a90d33c1b1370c36522668ca9c66dab71bbb211ea70c80c7d14dd645cbe`.
All58 current path-sensitive receipts and link cache are independently checked;
the normal repeat compiles no CPP. This is not a fresh58 compilation.

The new protocol fixture compares real rows, SQL NULL masks, headers, command
tags and OIDs. NTILE remains INTEGER/OID23. Identical input values and exact
multisets avoid imposing an unspecified no-order row-to-ID assignment.
Six invalid calls are tested across populated/empty/FALSE/LIMIT0 inputs;
owned PostgreSQL savepoint recovery preserves their real SQLSTATE oracle.
All24 negative controls pass after the repair. Original window SQL,
assertions and default protocol deadlines are unchanged.

## Independent remaining frame error

NTH_VALUE(v,2) OVER() now enters its genuine existing legacy window consumer,
but that consumer still limits its default frame to the current row instead
of the complete unordered peer partition. This is also reproducible with
PARTITION BY alone, which the old admission already accepted. Its separate
strong80 fixture passes the owned PostgreSQL reference and fails the candidate
in four exact default-frame controls. Explicit ROWS-prefix and ordered frames
are retained as controls. This frame repair must be independently committed;
the complete84 failure is retained, not replaced by the passed subset.
The same80 on original Root108 completes with10 failures, including the two
already-admitted PARTITION-only frame controls. Strict80 actually exits0.

This admission-only prefix's Root publication was held until the separate
frame repair and final complete composition gates passed. Actual subsequent
publication is recorded in `issue-unordered-window-current-root-composition.md`.
No original full/sanitizer run, family or
273-item completion, push, Actions activation, skipped security/TDE work or
filtered-branch rerun is claimed.
