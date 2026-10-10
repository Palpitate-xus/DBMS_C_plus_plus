# Lazy generate_series query-host execution

The strict bound-metadata positive committed in 3d9b1a61 fails with 42883:
`SELECT lower('ABC'),length('a'),abs(2),generate_series(1,2)`.
PostgreSQL 18.6 / server_version_num 180006 returns two rows with OIDs
25,23,23,23. The older command-line fallback also substitutes step 1 for
zero and eagerly expands the range. The unchanged baseline wire fixture
times out at the BIGINT upper boundary; its failure log remains preserved.

Register generate_series as an actual query-host provider, with execution-
owned, demand-driven cursors. Metadata never opens a provider or evaluates a
routine. Integers use checked next-value arithmetic; NUMERIC uses exact decimal
datums and retains display scale. Timestamp steps retain months, calendar days
and elapsed microseconds; explicit timezone day steps follow DST. Strict NULL
arguments produce no rows. Zero steps raise 22023 only when execution reaches
the provider, not behind LIMIT 0 or a false/NULL WHERE. Multiple target providers
zip rows with typed NULL padding; existing materialized readers remain usable.

The genuine reference fixture first exposed an authoring mistake: two SMALLINT
arguments have no exact generate_series overload and raise 42725. Keep that
negative control and a separate valid SMALLINT/INTEGER positive. Initial
candidate wire execution then exposed insignificant timestamp fractional zeros
and incorrect concrete-builtin namespace precedence. Correct these while
preserving the existing polymorphic unnest namespace behavior. The reference
fixture now checks 78 controls, including actual same-name scalar routines and
explicit pg_catalog lookup; all pass on verified PostgreSQL 18.6.

Own normal O2 compilation of all 57 non-Main units and Main completed; the
subsequent concrete namespace repair recompiles its own evaluator CPP. Early
matching native cursor, bound-metadata, backend-provider and namespace-provider
tests pass. No foreign object/binary copying. A normal build/repeat and all 58
own signatures/cache/source/frozen checks pass at publication start. Expanded
43-native/27-wire verification91737 ends1: all43native and25/27wire pass,
including the entire unchanged original default protocol. Only the independent
qualified-DROP dependency and pre-existing mixed pg_typeof neighbor fail. All58
own receipts/cache/source/frozen terminal checks pass. This is not a full-suite
pass; both failures remain recorded and the next parser repair is separate.

The namespace fixture additionally finds a separate DROP bug: the parser
splits `public.generate_series` into several objectNames and the DDL executor
rejects it as unsupported multiple targets. This blocks cleanup and subsequent
same-name TEXT-overload controls. An independent exact AST regression fails
134. Qualified routine-name parsing will be repaired separately, not hidden
by removing the cleanup or dependent controls. Nonempty DROP signatures remain
an ordinary unfinished requirement, not part of the deferred security scope.

The other failed neighbor is the unchanged fromless_structured fixture's mixed
SQL-value/pg_typeof projection (42883). The exact same original fixture fails
42883 on preceding frozen routine-result-oid-void-corrected, independently
verified before committing this repair. Preserve that existing ordinary gap;
do not weaken its OID2206 expectation or call the complete neighbor green.
The original unchanged BIT384 probe against verified180006 passes on this
exact8c3f frozen generation (83177 exits0).

Artifacts: /tmp/dbms-having-grammar.MPk64yc0.
Baseline: generate-series-original-baseline.log.
Reference: generate-series-reference18-final78.log.
Initial candidate: generate-series-candidate-initial-wire.log,
generate-series-candidate-initial-namespace.log.
Current candidate: generate-series-corrected-direct-wire.log,
generate-series-corrected-gates.log.
Frozen SHA256: 8c3f109e1f259ddeb7458d81009f4385037acf6a7526dabd358ec23172959c28.
Source seal: c2f11e59322a56d7b452a215a229be889dd4abe39260a125efb912fe5a166f1c.

Root full775native/Main/437wire driver stays on its own previous frozen source;
no private source is imported until its terminal. It has already reported
ordinary date_functions, enum_quoted_type_identity, plpgsql_quoted_scalar_binding
and reindex_atomicity failures, all retained for investigation. No broad skip.
This repair does not complete the entire SRF/QRY/TYPE/FUNC families: FROM and
relational lowering, ordering, nested set-returning expressions, domains and
broader temporal boundaries still require their own evidence and repairs.
All original273 requirements/statuses remain; only the15 explicit security/TDE
requirements are deferred. No push or Actions activation.
