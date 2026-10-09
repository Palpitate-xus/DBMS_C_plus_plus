# Root ROW generation whole registered regression: terminal failure retained

Unchanged complete driver18383 exits1 on source seal
795479c69ecc0f8429013e7c4706858e59fbfe3659a1aa503824dbfd09ef393d,
frozen SHA25602c69e429bedb08cbfba0bc35536c54c9c7578f2e616cb18475d4c85a64669fe.
Actual markers:773native/Main=769pass/4fail,435registered wire=427pass/8fail.
All1208 registered entries have terminal markers; the wrapper's own58 receipts,
cache/source/frozen terminal checks pass. No assertion, deadline or fixture was
removed or widened. This is not a full-suite pass.
The build uses the TLS stub; an intentional TLS-test skip is not runtime TLS
verification, regardless of its driver marker.

Artifacts:/tmp/dbms-root-row-record.Kijp5GbF/root-full773-435.log.

| Failed entry | Layer | Follow-up |
|---|---|---|
| ctas_test | Native | Retained; ordinary declaration correctness remains unfinished. |
| enum_quoted_type_identity_test | Native | Retained; ordinary type/declaration correctness remains unfinished. |
| stale_temp_startup_recovery_test | Native | Retained; ordinary TEMP/recovery correctness remains unfinished. |
| table_owner_atomicity_test | Native | Retained; filtered owner/security branch not resumed. |
| postgres_protocol_test.py | Wire | Original57014 timeout retained; no full-pass claim. |
| dml_explain_execution_protocol_e2e_test.py | Wire | Retained; ordinary EXPLAIN/execution correctness remains unfinished. |
| enum_comparison_binding_protocol_e2e_test.py | Wire | Retained; ordinary type/declaration correctness remains unfinished. |
| view_trigger_typed_values_protocol_e2e_test.py | Wire | Retained; ordinary typed-value/trigger correctness remains unfinished. |
| index_corruption_protocol_e2e_test.py | Wire | Retained; ordinary corruption diagnostics remain unfinished. |
| explain_join_protocol_e2e_test.py | Wire | Retained; ordinary EXPLAIN/join correctness remains unfinished. |
| pg_stat_activity_protocol_e2e_test.py | Wire | Ordinary target fix34d5d956 independently verified, imported deb49f76. |
| stored_function_atomicity_protocol_e2e_test.py | Wire | Ordinary keyword fix217bdcb9 independently verified in scope, imported86ba021d. |

After terminal, four independently committed source/test repairs are imported
with provenance (-x), without rewriting history or pushing:

| Independent commit | Root commit | Scoped repair |
|---|---|---|
| 34d5d956 | deb49f76 | Physical shadows of virtual relation targets |
| 217bdcb9 | 86ba021d | SQL-value grammar/types/context ownership |
| 94eea9bf | 8eed940c | Known stored-name signature binding before arguments/Parse |
| dc534a93 | 6fc40db8 | Public parser statement-owned syntax failure results |

Final private parser gate88485 exits1:33native all0,21/23wire pass; native49,
arity50 and all original neighbors pass. Original five NAME controls and default
CREATE deferred_child timeout remain. Actual BIT384/56735 against180006 exits0;
all58 current own terminal checks pass. This partial proof does not certify Root.

Root is now190 scoped repairs,773automatic+2Main/437registered/58production TU.
Source seal matches the final private generation:
a85e653e42b8a466780138e3ae9faf001db78874419dc1cbd8f455f949b8350a.
Its own fresh57 build97646 and actual Main52631 now both end0; all58 current
Root receipts/cache/normal repeat/start source/frozen checks pass. Binary bytes
match the final private generation only after Root's actual independent58
compile. Publication33native/23wire3492 is live, not yet a terminal proof.
Artifacts:/tmp/dbms-root-sql-value.x5X8O5vm. No foreign object/binary copies.
Root publication and new complete775/437 run are pending, not counted as passes.

All273 original items/statuses/hash unchanged. Original incomplete families and
ordinary native decimal API continue. The exact deferred security/TDE scope is
documented in [deferred-scope.md](deferred-scope.md); ordinary failures remain
unfinished rather than automatically deferred. No push, Actions activation,
restart of those15 deferred requirements, or goal-completion claim.
