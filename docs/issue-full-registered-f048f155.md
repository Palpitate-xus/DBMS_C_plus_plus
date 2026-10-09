# Complete registered regression on the f048f155 source generation

The unchanged Root-path `bash scripts/build_tests.sh` invocation (session
88630) terminated with exit 1. All registered entries ran; this is a failed
complete run, not proof of PostgreSQL compatibility or checklist completion.

| Scope | Passed | Failed | Total |
| --- | ---: | ---: | ---: |
| 766 automatic C++ tests plus 2 actual Main drivers | 765 | 3 | 768 |
| Registered protocol/E2E entries | 420 | 10 | 430 |

Counts use actual standard-driver labels, including labels sharing a line
with preceding output. Neither compile nor link failures occurred. The
complete default protocol fixture passes in this Root generation; failures
from its earlier generations are not carried forward as current failures.

Log: `/tmp/dbms-root-interval-fields.M3CeQmTN/root-full766-430.log`.
Frozen binary: `dbms_main.root-interval-fields-final.frozen`, SHA256
`2acba11d424beed6dbc1302d1cabaf0341ce81fff3f807426cb090612a2efbed`.
Sealed inputs: `src/scripts/tests/cmake`, SHA256
`f048f155e3008b6ffd556f5780e4ad72491890a5bf27833af241cb9b74309e44`.
Only README and documentation commits changed master during the run.

## All retained failures

| Entry | State and next action |
| --- | --- |
| `enum_quoted_type_identity_test` | Original failure retained. No new user-filtered creator investigation. |
| `stale_temp_startup_recovery_test` | Original failure retained. No new filtered TEMP/recovery investigation. |
| `table_owner_atomicity_test` | Original failure retained. No new filtered security investigation. |
| `enum_comparison_binding_protocol_e2e_test.py` | Original failure retained. No new filtered creator investigation. |
| `view_trigger_typed_values_protocol_e2e_test.py` | Original failure retained. No new filtered trigger investigation. |
| `legacy_sql_pattern_consumers_protocol_e2e_test.py` | Actual timeout while issuing the original SAVEPOINT at line 110. Earlier emitted pattern cases retain their exact results; this script did not finish successfully. No enlarged timeout or substituted pass. |
| `derived_type_protocol_e2e_test.py` | Original `SELECT (SELECT count(*) FROM typed_src) AS c;` returns 0A000, requiring actual prepared aggregate lowering. The entire private collect-errors matrix also identifies this as its sole collected failure. Repair is in progress. |
| `materialized_view_dml_target_protocol_e2e_test.py` | Original failure retained. No new filtered MATVIEW investigation. |
| `create_database_options_e2e_test.py` | Original failure retained. No new filtered CREATE-error investigation. |
| `index_corruption_protocol_e2e_test.py` | Original failure retained. No new filtered security/corruption investigation. |
| `explain_join_protocol_e2e_test.py` | Original failure retained. No new filtered EXPLAIN investigation. |
| `pg_stat_activity_protocol_e2e_test.py` | Original failure retained; the ordinary same-name-table INSERT issue remains open. No new privacy/security audit. |
| `stored_function_atomicity_protocol_e2e_test.py` | Original SQL function `atomic_sql_user()` returns 42883 for actual `current_user` in its body. Ordinary SQL value-function binding remains open; no new security/atomicity investigation. |

## Subsequent source generation

After this run actually terminated, two independent private repairs were
imported using `git cherry-pick -x`:

- `c9a25ab6` -> `772cc7dc`: postfix interval literal field qualifiers.
- `aee0b1bf` -> `ef034862`: durable interval column modifiers and assignment.

Current master has 182 scoped repairs, 768 automatic native tests plus 2
Main drivers, 432 registered protocol/E2E entries and 58 production units.
This new inventory does not inherit the preceding generation's results.
Actual Root own-path fresh 57 units/Main compilation (57803/3143) is running
in `/tmp/dbms-root-interval-columns.sbteRgYZ`. Root source inputs are frozen
until the new publication proof finishes; private aggregate work is separate.

The column candidate's actual final matrix was 46/46 native and 39/40
protocol, with the original default TEMP CTAS timeout retained. Its source
commit is not a claim that the whole current Root generation passes. See
`issue-interval-literal-and-column-modifiers.md` and the imported
`issue-interval-column-modifier.md` for all reference and failed generations.

Original 273 items remain 22 complete, 166 partial, 70 unverified and 15
user-deferred. No push or Actions activation. No family completion claim.
