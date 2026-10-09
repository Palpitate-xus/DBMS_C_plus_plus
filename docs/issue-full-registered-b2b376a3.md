# Complete registered regression on the b2b376a3 source generation

The unchanged standard command `bash scripts/build_tests.sh` has terminated
with exit1 (session86574). It ran the actual Root-path build/test objects and
the frozen production binary, not copied private-worktree objects.

| Registered scope | Passed | Failed | Total |
| --- | ---: | ---: | ---: |
| Automatic C++ and actual Main frontend drivers | 760 | 6 | 766 |
| Protocol/E2E entries | 416 | 11 | 427 |

The766 native/Main entries are764 automatic tests plus2 actual Main drivers.
PASS labels are counted by matching the actual standard-driver marker even
if preceding test output shares its line. No compile/link failure occurred.
This is a failed complete run, not a complete PostgreSQL compatibility proof.

Artifacts: `/tmp/dbms-root-catalog-followup.PAz6zkPp/root-full764-427.log`.
Frozen binary: `dbms_main.legacy-scalar-initial.frozen`, SHA256
`99ab40541a1b35110c339bfb050b5892b73e424864b7b666c12a4b7bf098ac9b`.
Final Root `src/scripts/tests/cmake` seal, unchanged through terminal state:
`b2b376a3d1bf819cd3f1c0db5a30e7dcc45fe30e0a1aec1c6bc2e13fb9bbb905`.
This generation's source commit is8de3e746; documentation-only commits made
during its run did not mutate the sealed inputs. Source freeze is now released.

## Actual original failures and follow-up state

| Original failed entry | Evidence / next action |
| --- | --- |
| `dml_semantics_test` | Original SQL is invalid LIMIT WITH TIES. Test-only correctione5114a10 preserves it as a negative and adds legal FETCH; production handoff75480b5e separately fixes executing/publishing this invalid query. Verified privately, not yet integrated. |
| `domain_foreign_key_base_test` | Original-object exit139 backtrace reaches destroyed declaration dictionaries during engine shutdown. Privatecf4e62a0 and its reproducing exit fixture pass; not yet integrated. |
| `enum_quoted_type_identity_test` | Original failure retained. No new user-filtered creator investigation. |
| `pattern_cast_parser_test` | TIMETZ expected spelling is stale; interval field qualifier is a genuine parser failure. Current interval candidate is undergoing fresh own58-unit and full adjacent verification; not yet approved. |
| `stale_temp_startup_recovery_test` | Original failure retained; no new filtered temporary/recovery investigation. |
| `table_owner_atomicity_test` | Original failure retained; no new filtered security investigation. |
| `postgres_protocol_test.py` | Actual timeout in `extended_query_describe`. The unchanged complete fixture passes on the prior private LIMIT generation; the Root full-run timeout remains a failed result. No increased deadline or full-pass substitution. |
| `enum_comparison_binding_protocol_e2e_test.py` | Original failure retained; no new filtered creator investigation. |
| `view_trigger_typed_values_protocol_e2e_test.py` | Original failure retained; no new filtered trigger investigation. |
| `derived_type_protocol_e2e_test.py` | Ordinary scalar child `SELECT (SELECT count(*) FROM typed_src) AS c` returns0A000. Genuine prepared aggregate lowering remains open. |
| `set_operation_structured_protocol_e2e_test.py` | Hidden-key FETCH now succeeds; old assertion expects0A000. Exact original query returns one row/OID23 on real PostgreSQL18.6 and the prior candidate. Preserve SQL and replace the stale refusal assertion with exact positive values/metadata; entire remaining fixture still requires a run. |
| `function_result_protocol_e2e_test.py` | Ordinary SQL-function reader now returns5; old assertion expects an error. Actual PostgreSQL18.6 and the prior candidate return the exact row/OID23. A separate zero-argument SQL-function probe still returnsXX000 instead of reference42883; genuine arity diagnostic repair remains open. |
| `documentation_status_test.py` | Old fixture requires live audit markers in evergreen README. Independent test-only private893f628f -> Root26d981dd fixes the contract; actual Root test passes. Progress links remain required in the dedicated status docs. |
| `index_corruption_protocol_e2e_test.py` | Original failure retained; no new filtered security investigation. |
| `explain_join_protocol_e2e_test.py` | Original failure retained; no new filtered EXPLAIN investigation. |
| `pg_stat_activity_protocol_e2e_test.py` | Original failure retained; ordinary INSERT into its same-name table reportsXX001. Not repaired or reclassified as passing. |
| `stored_function_atomicity_protocol_e2e_test.py` | Ordinary SQL-function body cannot bind `current_user`, reporting42883. Session-value binding remains open; no new security/atomicity audit. |

`domain_ancestry_storage_test` passes in this new full run. Do not carry its
failure from an older generation forward as if it failed here.

## Continuing the original objective

1. Finish the interval grammar/value and explicit syntax-handoff candidate,
   preserving all49 PostgreSQL18.6 reference expectations and failed baselines.
2. Integrate the already independently committed repairs only after actual
   candidate verification; build and verify genuine current Root-path objects.
3. Strengthen the stale hidden-FETCH/SQL-reader assertions without removing
   their SQL or weakening the complete fixtures. Independently fix ordinary
   scalar aggregate lowering, routine arity and session-value binding.
4. Continue the original273 requirement ledger and all in-scope unresolved
   failures. A focused passing matrix is not whole-inventory or whole-family
   completion. User-deferred security/TDE and filtered branches stay deferred.

Original273 remains22 complete,166 partial,70 unverified and15 user-deferred.
No push or Actions activation. README remains evergreen in aed6e80a.
