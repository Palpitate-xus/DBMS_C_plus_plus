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
