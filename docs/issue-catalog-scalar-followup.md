# Catalog, scalar query output and FETCH grammar follow-up

This is scoped repair evidence, not completion of the PostgreSQL gap audit.
README remains evergreen in independent commit `db35e522`; audit results
belong here and in the progress ledger, not in the project entry point.

## Independent local commits

| Master commit | Change |
| --- | --- |
| `6f687e7a` | Route the existing typed pg_database reader before prepared comparison hosts; retain explicit qualification and user table/view shadows. |
| `5d829bb8` | Resolve actual scalar child UNKNOWN output to TEXT through the catalog declaration callback and retain the child AST/output descriptor. |
| `52504ada` | Preserve original invalid WITH TIES SQL as negative grammar controls and add ordered FETCH positives. |
| `f5856fde` | Require real typed pg_type projection instead of the obsolete unsupported expectation; keep unsupported full-schema and pg_enum negatives. |
| `a56cf1ca` | Reject LIMIT WITH TIES even with ORDER BY; retain structured grammar diagnostics for FETCH WITH TIES without ORDER BY. |
| `a1cd35b4` | Preserve DATE/TIMESTAMP declaration spelling and exact literal payloads in the original parser fixture. |

No push was performed; GitHub Actions remain disabled. User-deferred branches
were not resumed. Original SQL, result values and protocol descriptors were
not removed to obtain a passing subset.

## Reproduction and verification

Artifacts: `/tmp/dbms-root-catalog-followup.PAz6zkPp`.

- The unchanged prior frozen binary failed the original pg_database filtered
  query with 42P01 and the original scalar child string with 42704. Complete
  original CTE clause inputs were retained with collect-errors.
- Owned PostgreSQL 18.6 returned text OID25 for string, NULL, empty-result and
  nested scalar query outputs. LIMIT WITH TIES, including ordered LIMIT,
  and FETCH WITH TIES without ORDER BY returned 42601. Ordered signed FETCH
  positives returned the original expected rows/counts.
- Three genuine incremental Root builds completed with exit0: Main, binder,
  then parser. All 58 current own-path object receipts and the cache signature
  were verified; a repeated build changed no object and matched the frozen
  binary. This is not another cold fresh58 build or foreign-path object reuse.
- Frozen SHA256: `ab56ff57ff4a640d8c49af50ea7f272cc4ce21e1955d633f6a3e73deed90759d`.
  The complete 8-native/10-wire invocation used source seal
  `9667e15f50d2ab04701bfe04d077a362de284e5b953da36f78ba91ef70dcd186`;
  final receipts/cache/source/binary fences passed.
- That invocation exited1: seven native and eight wire tests passed, three
  failed. The complete default `postgres_protocol_test.py` passed. Both the
  catalog SQLSTATE fixture and the complete original CTE clause fixture,
  including four new typed scalar-output checks, passed.
- Native parser reached its later old DATE spelling assertion and failed134;
  the corrected exact-spelling/payload fixture in `a1cd35b4` then completed
  its entire original test with exit0. This test-only change does not require
  recompiling production objects; it is a separate verification, not an
  assertion that the failed 8/10 invocation became green.

## Remaining failures and next work

| Retained original fixture | Remaining failure |
| --- | --- |
| `cte_prepared_owner_protocol_e2e_test.py` | Chained CTE CASE expression reports column "=" does not exist. |
| `scalar_fetch_ties_protocol_e2e_test.py` | EXISTS/ties results, unprojected sort keys, and numeric scalar Simple/Extended descriptors remain incorrect or unsupported. |

No new complete 761-auto-native +2 Main /424-registered run has completed.
The preceding complete run's other original failures remain open. Source
repair count is 166; the original273 statuses remain 22 complete, 166 partial,
70 unverified and 15 deferred_by_user. Focused repairs do not approve a whole
catalog/type/query family, TLS runtime, sanitizers or overall compatibility.

## Subsequent CTE expression, Describe and FETCH dispatcher repairs

| Master commit | Actual scope |
| --- | --- |
| `276fc6d1` | WITH SELECT retains the CTE CASE expression instead of rewriting its condition into evaluator-only pseudo-tokens before child binding. Original chained/nested CASE and lazy writing-arm checks now pass. |
| `014f72f5` | Source-free scalar query projections use whole-query pure binding for protocol Describe. Child labels cannot lend their table/attribute origin to scalar output. Original integer/bigint descriptors and execution-count checks now pass; empty bigint and string children add coverage. |
| `5a7251e2` | Admit genuine FETCH peer queries to the typed sort consumer. Its first dispatcher change only reached parenthesized expressions; the complete failed invocation is retained. |
| `19e9314b` | Share the same actual FETCH envelope consumer with the ordinary SELECT dispatcher, before legacy lowering rejects unprojected ORDER keys. Preserve ordinary/parenthesized counterparts and all original queries. |

These are three additional scoped source repairs, not four independent issues;
the FETCH dispatcher correction remains separately committed for provenance.
Total scoped source repairs: 169. Original273 states and item hash remain
unchanged; no family has been marked complete from these focused checks.

The first CTE build92591 exited1 because the Network source changed during
compilation; the source-change guard correctly prevented linking. Its actual
current Main receipt and Network fresh-unit83437 exit0 were retained. With
inputs held constant, final Root build93193 exited0; all58 own-path current
receipts/cache/repeat/frozen/source fences passed. CTE/Describe five-wire
invocation14666 exited1, with3 passes and2 failures; the complete CTE matrix
fell from9 failed assertions to4, while original scalar FETCH failures became
6 execution-only assertions. Default full protocol passed. This is not a
cold fresh58 build or whole-suite success.

First FETCH build68253 exited0; complete8-native/9-wire invocation9699 exited1
with all8 native and7 wire passes and2 failed wire fixtures. The full90 scalar
controls had8 failed assertions: the ordinary dispatcher was not yet reached.
These failures and that frozen generation are retained, not relabeled PASS.

Final corrected dispatcher build69336 exited0. Current frozen SHA256 is
`4b0aa390b33b266f3a73c85cc97328d280b905dd3637513dcd4cf132ca3b6ded`,
source seal `53da00258c155f50f0af32970f904a8f029e02750a0ef9f74020e423b453a6ed`.
All58 current Root receipts/cache/repeat/frozen fences passed. Complete final
8-native/9-wire invocation96558 exited1: all8 native and7 wire passed,2 wire
fixtures still failed. The scalar fixture reached all92 controls with only
the original EXISTS assertion failing; all original hidden/expression sort
keys, OFFSET, NULL peer, exact bigint, Simple/Extended descriptors and actual
routine-call count checks passed. Default full protocol again passed. The
unchanged original BIT384 differential6726 completed0 with zero differences.
No SQL, expectation, OID, count or timeout was weakened.

Owned PostgreSQL18.6 completed the entire updated CTE matrix and scalar FETCH
matrix, including the final ordinary/parenthesized counterparts, with zero
failed assertions. These reference runs retain actual version verification
and rollback their fixture transactions.

Remaining complete-fixture failures:

- CTE stored recursive writer argument resolution42883 and its consequent
  missing three output/effect rows; independent stored SQL readers22023.
- EXISTS with a nonempty FETCH child returns false in the legacy frontend.
  A separate genuine native probe43824 reached all6 existence controls and
  rejected all6 with0A000, including multi-column and source-free children.
  The same6 SQL against actual PG18.6 returned correct booleans/OID16. This
  requires a real existence consumer, not just trimming ORDER text or routing
  a multirow existence child through scalar cardinality.

The next work retains genuine child cursor, output-demand, correlation,
early-close and effect ownership for EXISTS, then the remaining CTE/routine
and prior full-run failures. New full761-auto +2 Main /424-registered has not
completed. No push, Actions activation or user-deferred branch restart.
