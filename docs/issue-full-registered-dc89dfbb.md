# Complete dc89 regression and independent repair integration

The unchanged standard driver 84918 is terminal, exit 1. It ran every
registered entry: 768 automatic native fixtures plus two actual Main drivers,
and all 432 registered protocol/Python entries. Native/Main results are
766 pass / 4 fail; protocol results are 424 pass / 8 fail. No compile or link
failure was reported. Both Main entries and the complete default protocol pass.

The prepared SRF native fixture prints a literal backslash-n before its status,
so counting only statuses at the beginning of a physical line misses its PASS.
Counting the actual driver markers confirms all 770 native/Main entries.
No fixture was removed or changed to obtain these counts.

| Original failed entry | Follow-up scope |
| --- | --- |
| alter_array_type_envelope_test | Ordinary parser regression; independent private repair 4c5b1cdc imported as c23b18cd after terminal. |
| enum_quoted_type_identity_test | Retained user-filtered creator failure; no new investigation. |
| stale_temp_startup_recovery_test | Retained user-filtered branch; no new investigation. |
| table_owner_atomicity_test | Retained user-filtered branch; no new investigation. |
| enum_comparison_binding_protocol_e2e_test.py | Retained user-filtered creator failure. |
| view_trigger_typed_values_protocol_e2e_test.py | Retained user-filtered branch. |
| derived_type_protocol_e2e_test.py | Original scalar COUNT child 0A000; independent aggregate repair 409507fb imported as ecbaf702 after terminal. |
| materialized_view_dml_target_protocol_e2e_test.py | Retained user-filtered branch. |
| index_corruption_protocol_e2e_test.py | Retained user-filtered branch. |
| explain_join_protocol_e2e_test.py | Retained user-filtered branch. |
| pg_stat_activity_protocol_e2e_test.py | Ordinary same-name-table INSERT XX001 remains open. |
| stored_function_atomicity_protocol_e2e_test.py | Ordinary SQL-body session-value binding 42883 remains open. |

Log: /tmp/dbms-root-interval-columns.sbteRgYZ/root-full768-432.log.
The full driver's source seal stayed unchanged through terminal:
dc89dfbb5f098e11493b6c6289f4bde27cfb6a452a4bfc7882204a3787bd932f.
The preceding actual Root frozen binary still matches production at terminal:
43b0c7cffcd4a14c07168b840435cd48c2649fb47fce6c651e7b0f63719e5812.
No copied/private binary, observation timeout, altered test deadline or later
source state substitutes for this failed generation.

After terminal, three independent source repairs were imported with -x Git
provenance: ecbaf702 (409507fb), c23b18cd (4c5b1cdc), c9e951ec (2144d7d7).
Root now has 185 scoped source repairs, 770 automatic native fixtures plus
two Main drivers, 434 registered protocol entries and 58 production units.

Fresh Root-owned compilation is running in
/tmp/dbms-root-numeric-columns.VVbNxCXQ: sessions 79599 / 43176 for own 57
non-Main units and actual Main. No private object or focused proof is borrowed.
Root source inputs are frozen while proving this imported generation. This is
not yet an actual Root publication proof or a new whole-suite pass.

ROW constructor/record semantics are being implemented independently in the
private worktree. Original 29-control candidate baseline has 22 failures, real
PostgreSQL 18.6 reference has 30 including BEGIN and zero failures. Initial
native candidate retained a record type-identity failure and the raw native
NULL comparison defect; corrected native/complete gate 49242 is still live.
The complete strict global aggregate protocol now passes all 55 controls in
that private ROW generation, but ROW's own tests expose further typed-carrier
and root constant-planning issues. No ROW source repair is approved or committed.

All 273 original statuses remain 22 complete / 166 partial / 70 unverified /
15 user-deferred. README remains evergreen cfcab3cd. No push or Actions activation.
