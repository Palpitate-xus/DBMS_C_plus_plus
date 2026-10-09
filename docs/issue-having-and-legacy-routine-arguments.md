# Grouped-column HAVING and legacy scalar routine arguments

| Local source/test commit | Repair |
| --- | --- |
| `f787fd83` | Consume the parsed canonical grouped-column identity in a column/literal HAVING comparison. Rendered identifier quotes and operator characters inside an identifier no longer make this predicate disappear. |
| `0a8cfbff` | Replace public-only direct stored-function dispatch and unevaluated argument text with ordinary namespace-aware whole-call binding against the real typed row, actual storage owner and NULL bitmap. |

These are two independent scoped repairs, bringing the count to174.
The actual inventory is764 automatic native tests +2 actual Main drivers,
427 registered protocol/E2E entries and58 production compilation units.
Neither the inventory nor these focused results prove original273 completion.

## Plan and actual evidence

1. Reproduce the original HAVING and recursive CTE failures on the previous
   frozen production binary, retaining SQL/rows/OIDs/effect counts.
2. Verify new HAVING cases and the original whole CTE fixture against the
   explicitly selected real PostgreSQL18.6 server (`180006`).
3. Repair and commit each cause independently, then verify the real current
   production objects, immutable binary, complete adjacent fixtures and BIT384.
4. Run the unchanged full764+2/427 standard driver. Continue original whole-run
   failures and the newly verified identifier edge cases after its source freeze.

Artifacts: `/tmp/dbms-root-catalog-followup.PAz6zkPp`.

| Invocation / artifact | Actual result |
| --- | --- |
| New HAVING protocol on previous frozen Root | Exit1;53 statement checks,51 failures. Source values/NULLs/descriptors are real, but qualifying HAVING filters disappear. |
| Actual updated HAVING PostgreSQL18.6 reference | Exit0;54 checks including BEGIN. All exact positive values, zero-row NULL comparisons and OIDs pass. |
| Original new native HAVING baseline35875 | Exit134 at the first expected group-count assertion. |
| HAVING storage17744 / proof50492 | Incremental own-path storage build0. Complete17 native all0;24 wire23PASS/1 original CTE failure. Original complete quoted-range/HAVING fixture now0. Current58 receipts/cache/repeat/source/frozen fences pass. |
| HAVING unchanged BIT38475290 | Exit0 against actual180006;384 cases,zero differences. |
| First legacy-argument native fixture58706 | Compile error from a nonexistent column-construction helper retained; not a runtime result. |
| Corrected native baselines5903/80073 | Exit134; original unparsed arithmetic argument reaches a declared-output mismatch. Conditions and BIGINT column are corrected to the actual native API before final verification, without weakening expected values/NULLs. |
| Legacy storage59040 / final proof42384 | Own-path storage build0. Complete18 native and24 wire all0; genuine58 receipts/cache/repeat/source/frozen fences pass. Each repair changes one CPP; this is not another fresh58 build. |
| Original complete CTE current18901 | Exit0 with unchanged recursive SQL, three output rows and three write effects. Full reader, nested scope, sequence and negative-name assertions also pass. |
| Original complete CTE actual PostgreSQL18.6 reference | Exit0, including original SQL/effect assertions and error/savepoint controls. |
| Final unchanged BIT38482775 | Exit0;384 actual180006 comparisons,zero differences. |
| Full standard driver86574 | Started and verified live; compiling its own normal test objects. No terminal result or full-pass claim yet. |

The new native legacy-call controls retain exact BIGINT arithmetic, a real
mixed-case canonical routine name, casts, source TEXT with spaces, runtime
NULL, literal `NULL`, empty string and an apostrophe. No runtime row values
are interpolated into argument SQL. The original complete scalar FETCH92,
default protocol, stored-function ORDER and quoted-range fixtures pass in the
final24-entry matrix. No expected SQL/rows/OIDs/counts/deadlines weakened.

Final frozen binary: `dbms_main.legacy-scalar-initial.frozen`.
SHA256: `99ab40541a1b35110c339bfb050b5892b73e424864b7b666c12a4b7bf098ac9b`.
Source seal: `b2b376a3d1bf819cd3f1c0db5a30e7dcc45fe30e0a1aec1c6bc2e13fb9bbb905`.
Preceding HAVING generation and all failed epochs remain available.

## Remaining verified problems and scope

Additional exact SQL probes against the final frozen binary still fail for
grouped columns named `"candy"` and `"x(y)"`: both return two rows for HAVING
equal to1 instead of one. The exact same SQL passes on actual PostgreSQL18.6.
Both complete results are retained in
`having-keyword-identifiers-current.log` and
`having-keyword-identifiers-reference18.log`. Frontend substring splitting at
`and` and the storage aggregate-call text heuristic still misclassify these
identifiers. This next repair is not hidden by the passing53-control fixture.

General HAVING expression/grouping-set execution, broad stored-function
graphs and preceding whole-suite failures remain open. The source is frozen
for the full driver; its live handle must be monitored, not restarted merely
because an observation times out. No new sanitizer or real-TLS runtime proof.

Original273 statuses remain22 complete/166 partial/70 unverified/15 user-deferred.
The item entries are unchanged. No push, Actions activation or user-deferred
branch restart. README remains evergreen in `00675e31`.
