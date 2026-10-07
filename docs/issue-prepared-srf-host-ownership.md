# Typed SRF receiver takes functions it does not own

The ordinary ProjectSet receiver treated every bare FunctionCall SELECT as
a possible typed set-valued query, then requested whole binding before
checking an actual provider role. The existing backend-local
`pg_listening_channels()` has its own committed-subscription consumer, not
an ExprEvaluator scalar entry. The new probe therefore raised42883 and
removed its RowDescription, both with no subscription and after real LISTEN.

StorageEngine now exposes a pure `ownsPreparedSetReturningCall` probe, backed
by the same real provider-identity selector as QueryBindingMetadata's typed
set-returning callback. It respects actual scalar/stored resolution and
decoded routine namespace/identifier identity. It does not inspect result
values, execute argument routines, catch-and-ignore analysis errors, re-render
SQL or add a pg_listening_channels spelling/row whitelist.

The receiver asks for whole binding only if it owns that provider or the
statement already requires a genuine quantified carrier. Argument types and
errors remain at the whole-query boundary; the identity-only probe does not
resolve UNKNOWN by running a function. The original backend consumer retains
its real backend/database-local committed LISTEN state and transaction staging.

## Matching scoped evidence

Private basis ROOT `29609faa` in `/tmp/dbms-srf-host-owner.A4SsDz1H/repo`.
The public TableManage.h declaration requires a wholly matching epoch.

- `candidate-fresh58-O0.log`: all58 production objects are freshly compiled,
  with matching source/header/flags signatures, actual repeat and binary stamp
  checks. Frozen SHA256
  `ad8df7e85281cda34b293207852cef922ac4e6b9134bc46a5c459ec59e9018d6`.
- `candidate-native-seven-catalog-sequence.log`, session56552: seven fresh
  matching private O0 natives actually pass, including pure ownership,
  namespaces/quoted names, stored shadowing, nested unknown-function priority
  and a real SQL-registered sequence proving zero routine side effects.
- Eight complete serial adjacent scripts actually exit0 in session69473:
  original ProjectSet, unchanged UNNEST, quantified demand, ordinary Q-DML,
  OLD/NEW, PL binding, routine atomicity and plain EXPLAIN root planning.
  The authoritative output is in the tool record, not an invented log file.
- `baseline-e6-srf-host-owner.log`: the actual normal all58 e6 frozen binary
  reports42883 and loses descriptors for empty and committed channel sets.
- `reference-180006-srf-host-owner.log`: the entire permanent diagnostic
  passes PostgreSQL18.6/180006, including transaction staging/rollback,
  descriptors, LIMIT0, UNKNOWN signatures and no-effect/error controls.

The initial new native used a direct engine sequence without SQL catalog
registration and failed42P01 at its final sentinel. It is a harness failure,
retained in `candidate-native-seven.log`; the corrected native creates the
same real sequence through DDL and keeps the unchanged one-call sentinel.

## The full diagnostic and whole protocol are still required

`prepared_srf_host_ownership_known_gap.py` keeps every SQL/descriptor/side
effect assertion. Its candidate log still fails TWO separate cases:
`pg_listening_channels() LIMIT0` is independently preflighted by the legacy
fromless suppression path, and `unnest(NULL)` reports42883 instead of42725
because input inference uses a protocol-output TEXT fallback. Those controls
are not removed, reset or registered as a fake passing E2E gate.

The unchanged full original protocol moves past the former line2006 and
joined-view line2764, then actually fails the existing NOT LIKE UPDATE
assertion at line2867 (`candidate-original-full-protocol.log`, session39714).
The pattern/carrier work owns that independent failure. No full protocol,
ROOT normal O2 combination, TLS runtime or complete273-item claim is made.
No push, GitHub Actions activation or user-skipped security work is performed.
