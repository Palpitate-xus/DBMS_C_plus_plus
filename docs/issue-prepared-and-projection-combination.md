# Prepared namespaces and projection/ORDER follow-ups

Date: 2026-10-06. Six independent source/test fixes are committed locally.
The broader audit is still incomplete; mapped families remain partial.

| Actual defect | ROOT commit | Independent evidence and repair |
| --- | --- | --- |
| Reached PL/pgSQL queries substituted ambiguous local/column names and could execute a writing CTE/sequence before rejecting ambiguity | `272bc46f` | Pure copied catalog metadata, whole reached-query namespace preparation, owned AST/provenance and typed parameter cells; original seven defects and expanded 43 cases plus two nontransactional sequence controls verified privately. The dispatcher still uses a post-preparation legacy adapter. |
| Canonical quoted source aliases were folded again; valid qualifiers failed and invalid ones succeeded; qualified WHERE/HAVING lost rows or filters | `2a2a8d62` | Decode raw source/range identifiers once, retain exact AST components and physical column ordinals, validate before effects and lower only structurally bound physical keys/comparisons. Expanded private range wire, three natives and seven adjacent wire entries passed. |
| New scalar ORDER binding misclassified existing aggregate calls | `3983dccb` | The original registered typed-group gate passed on old formal b418 and failed on new formal f30 with COUNT 42883. Preserve aggregate-result descriptor sorting, bind argument/FILTER scalar calls without invoking them, and distinguish actual public stored COUNT. Original and dedicated gates, six natives and six adjacent scripts passed on matching private fresh-56 development objects. |
| Plain table EXTRACT consumed its grammar field as a column value and mishandled quoted/nullable source expressions | `49a3416c` | Retain original parser EXTRACT field/value roles and complete operand SQL; nullable typed actual-owner evaluation and structured error propagation. Dedicated native/wire, seven adjacent natives and eight adjacent wires passed privately. |
| Statement Describe lost CHAR/VARCHAR projection cast lengths | `2ec07854` | Derive character modifiers from the original live projection AST before the physical SELECT-star descriptor adapter; original VARCHAR(4) expected 8, actual -1 baseline retained. Independent index-only Network source passed all eight original Describe/no-Execute controls. |
| Statement Describe serialized canonical quoted ColumnRefs back to SQL, changing BIGINT/INTEGER identity and physical attribute numbers | `39844448` | Infer directly on the original live AST, including declared prepared-parameter types; canonical source/alias/ref and physical-column matching uses exact bytes. All original 14 no-Execute Describe controls passed privately with unchanged OIDs/attribute numbers/typmods, alongside nine fresh matching natives and 12 adjacent scripts. Pure metadata does not invoke the writing routine. |

## ROOT formal build checkpoints

ROOT `272bc46f` completed all **56** production translation units under the
normal optimized build configuration (`33133`, terminal 0), including the new
query_binding TU and all consumers of changed AST/RowContext headers.
Normal `scripts/build.sh` repeat reported up-to-date. All 56 object signatures
and the production binary stamp matched (`c65430`, terminal 0).
Frozen executable:
`/tmp/dbms-prepared-query-namespace-combination.FS4egSBy/dbms_main.binder.frozen`,
SHA-256 `a0a555ffbf40b554ccf6934a9cc4454bfed0f200624dd881fecb18fede9944e0`.
Registered binder wire `72137` then reached terminal exit 0 on this exact
frozen executable: all 43 cases and two real sequence pre-effect controls
passed (`binder-baseline-protocol.log`). This does not establish a native,
full-suite or broader-family pass.

The four following CPP-only fixes are integrated at production HEAD
`2ec07854`, with no new header/layout change. Matching optimized rebuild of
changed main, storage, expression helper and network TUs `25468` reached
terminal exit 0; its log contains precisely four recompilations. Normal
repeat reported up-to-date (`e1ff94`), and all 56 signatures/stamp matched
(`f01269`). Frozen production binary (docs-only HEAD `c921f292`):
`dbms_main.projection.frozen`, SHA-256
`36a4af231a055fe19a8a3385f6f269d60e4eac0e41026696addaa3649d48c572`.
Its serial 50 focused/adjacent wire entries `53287` reached terminal exit 0;
`verify.sh protocol-previous` explicitly excludes the newly added quoted
Describe fix which is absent from that frozen revision. All 50 passed on
that exact binary (`projection-protocol.log`); no native/full-suite pass
is inferred for that preceding revision.

The independent quoted Describe/public pure-AST API fix is now integrated at
`39844448`. Because the shared header changed, a new all-56 optimized rebuild
`93903` reached terminal exit 0; its log has 56 fresh compilation entries
(`parsed-ast-production-build.log`). Normal repeat `16229c` was up-to-date,
and `060ea1` verified all 56 signatures plus the binary stamp. New freeze
`dbms_main.parsed.frozen`, SHA-256
`0204a334c1f7b8e7534f8dd07a5e4dffb951ae9441ae211a7e012470865f2728`.
47 freshly compiled/linked matching natives `72750` and 51 focused/adjacent
wire entries `47695` reached terminal exit 0: every entry in their explicit
`verify.sh` arrays passed (`parsed-native.log`, `parsed-protocol.log`).
Full-default protocol with original deadlines/captured diagnostics `4056`
reached terminal exit 1, described below. Source/header inputs remain frozen.

Canonical complete registered runner `40539` is now live: `build_tests.sh`
covers all **501** root-level native test sources and **244** registered
protocol/E2E entries, not just the focused arrays. Its 55 no-main production
objects are reused from the source/header/compiler-signature-verified new
all-56 formal build; canonical flags/production includes are identical, and
stubs were freshly compiled (`7731`, terminal 0). This is matching formal
object reuse, not a second cold production build. Canonical runner still
compiles/links every native test itself and runs every registered script.
No terminal full-suite pass is claimed (`full-registered.log`).
Private O0/selected-TU O2 results and older ROOT green binaries are not used
as proof of this new revision.

## Retained incomplete work

The earlier f30 full-default protocol `56926` failed during initial connect,
before SQL; its cause remains undetermined because server output was discarded.
Earlier protocol timeouts and measured transaction-image I/O amplification are
not erased by any historical green rerun. Original clause diagnostic `13344`
reached terminal exit 1 against frozen 36a4, still exactly five genuine failures
(`projection-known-clause.log`): WHERE/ORDER scalar subquery error propagation,
two EXPLAIN ANALYZE execution errors, and typed computed-predicate UPDATE.
Original controls were not removed.

Quoted-column prepared Describe OID/identity/attribute failures have their
independent committed repair and private proof above; new ROOT combination
proof is still pending. Character typmods remain a separate root cause/commit,
not evidence of complete query preparation or protocol-family support.
Aggregate arithmetic empty-input singleton/NULL, all-NULL SUM, ignored WHERE
and casted-aggregate errors remain. Hidden/compound aggregate ORDER, complete
operator/type/coercion preparation, general record/polymorphic/LATERAL scopes,
and schema-qualified routine CREATE remain independently open. The optional
binder open-namespace diagnostic retains its 44th case failure; it is not
counted as a passing 44-case group.

Detailed evidence: `issue-plpgsql-query-binding-preflight.md`,
`issue-canonical-source-range-scope.md`, `issue-aggregate-order-role.md`, and
`issue-order-metadata-arithmetic-combination.md`. EXTRACT private optimized
changed-TU evidence uses `/tmp/dbms-null-safe-join.0MiJvD`, binary SHA-256
`f02ebf02ec427d49784506ecfb414f216aa6949c976b6963a31150df48f255ea`;
the rest of that independent development core was not all O2. Typmod/private
AST artifacts are under `/tmp/dbms-parsed-expression-type.La3lmoEv`;
the typmod commit itself does not depend on the independently committed
pure-AST helper API.

The unchanged f30 full-default protocol was rerun with a local observer that
only captures Popen server output and socket/process state; it preserves
the original 10-second socket and 15-second startup deadlines. `96855`
reached terminal exit 1 (`full-f30-captured.log`). Both startup phases connected after
seven attempts in approximately 0.302/0.303 seconds; early errno 111/103
occurred while these owned server processes briefly waited on
`jbd2_log_wait_commit`. This shows a transient errno 103 does not by itself
establish a crashed server. This rerun subsequently failed the original
`pg_settings` assertion at `postgres_protocol_test.py:3537`: statement_timeout
was not returned after SET 123. The original assertion and timeout were not
changed. The first observer did not capture that query's messages, so missing
rows versus an ErrorResponse are not yet distinguished. This is a separate
actual full-test failure, not a claimed startup fix or established semantic
regression. The new frozen-398 full run `4056` captures those messages too;
it does not execute extra observer SQL. Both f30 server processes exited 0
after fixture cleanup. The earlier startup failure is retained independently.

New `4056` actually failed the earlier startup-settings assertion at line
1465. Captured SELECT pg_settings response is exactly ErrorResponse/ReadyForQuery
with SQLSTATE `57014` and statement-timeout cancellation, after the original
startup options selected 321ms. This is now a verified timeout response, not
an unexplained missing row, while both owned server processes exited 0 during
normal fixture cleanup. It does not establish a newly introduced regression.

Bounded independent fresh-fixture timing/I/O probe `22323` reached terminal
exit 1 with nine pg_settings queries: timeout 0 passed 3/3 at 327.9–337.8ms;
timeout 123 failed 3/3 with 57014 at 169.1–340.5ms; timeout 321 passed 3/3
at 161.3–328.9ms. Each supposedly read-only query wrote approximately
264KB through eight write syscalls and 283–291KB of filesystem write_bytes,
even in the fresh fixture (`pg-settings-timeout-io.log`). This provides
concrete read-query write/I/O evidence to investigate alongside earlier
transaction-image amplification, not a repaired performance claim, universal
latency bound or proof of the original startup failure. No timeout or expected
settings value has been relaxed.

Totals remain 273: 22 complete, 166 partial, 70 unverified, 15 user-deferred.
No push, Actions remain disabled, skipped security/TDE work remains deferred.
