# Complete registered regression on a85e653e

The unchanged Root driver85873 completes with exit1:769/775 native/Main tests
and431/437 wire tests pass. All1212 terminal test markers are present. One native
fixture prints a literal `\\n` before its PASSED marker; count that actual
marker too, not only line-start matches. Both real Main frontend drivers pass.
All58 own production receipts/cache/source/frozen checks pass at start and
terminal. The independent test-object layer compiles its own production inputs;
no private objects or binaries are borrowed. The entire original default
protocol passes in this generation. TLS is stub, not actual TLS runtime proof.

| Test | Kind | Retained failure / next action |
| --- | --- | --- |
| date_functions_test | Native | Manual callable AST current_date has no intrinsic kind; verify and correct fixture/API contract without weakening date/time assertions. |
| enum_quoted_type_identity_test | Native | Quoted MixedRank is resolved as lowercased mixedrank,42704; repair actual declaration identity. |
| plpgsql_quoted_scalar_binding_test | Native | Valid current_date intrinsic rejected42883 by PL scalar classifier; independent candidate reproduces and repairs. |
| reindex_atomicity_test | Native | Catalog image changed while loading58030; investigate concurrency/stability, not a security exemption. |
| stale_temp_startup_recovery_test | Native | Retain original startup/recovery failure; ordinary recovery remains in scope. |
| table_owner_atomicity_test | Native | Retain failure; deferred security/ACL overlap does not excuse separable transaction/storage correctness. |
| sql_value_keyword_protocol_e2e_test | Wire | Five NAME-return declaration positives fail; independent committed NAME repair exists. |
| enum_comparison_binding_protocol_e2e_test | Wire | Retain actual enum binding failure; no ordinary enum deferral. |
| view_trigger_typed_values_protocol_e2e_test | Wire | Retain original typed view/trigger failure. |
| fromless_structured_protocol_e2e_test | Wire | Mixed SQL-value/pg_typeof projection fails42883, also reproduced on old/private candidates. |
| index_corruption_protocol_e2e_test | Wire | Retain original index corruption check failure; investigate ordinary fail-closed storage behavior. |
| explain_join_protocol_e2e_test | Wire | Retain original EXPLAIN/JOIN failure; repair actual executing/display semantics. |

Artifacts:/tmp/dbms-root-sql-value.x5X8O5vm.
Log:root-full775-437.log.
Frozen:dbms_main.root-sql-value-final.frozen.
Source seal:a85e653e42b8a466780138e3ae9faf001db78874419dc1cbd8f455f949b8350a.
Frozen SHA256:beb50f30e23cf2a3ddc12e6aaf2a83bfcf23fc9f451287667e968c4920eb97fb.

The result is not a full pass, does not close any whole incomplete family, and
is not proof for later imported source. Original273 items/statuses remain
22complete166partial70unverified15deferred. Only the15 explicit security/TDE
requirements are deferred. No push or Actions activation.
