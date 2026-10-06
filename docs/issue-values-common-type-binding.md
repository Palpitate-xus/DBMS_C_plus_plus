# Prepared VALUES common types and input conversion

## Defect and scope

The pure query binder described a VALUES column from its first row and left
later rows uncoerced. A typed consumer therefore mislabeled BIGINT values as
INTEGER, failed to resolve UNKNOWN strings against later integer expressions,
and missed incompatible types or invalid input before executing a writing CTE.
This predates the simple CASE equality patch; it was exposed while preparing
the ordinary CASE consumer. The legacy standalone VALUES implementation had a
different common-type path, so replacing it without this fix would regress it.

The binder now transforms all VALUES expressions in row order, checks width
after each complete row, selects each column's common type in source order,
and inserts genuine implicit CastExpr nodes using the shared common-type/input
conversion rule. It does not execute routines, rows, arithmetic or already
typed numeric narrowing casts during metadata preparation. A late unknown
routine still precedes conversion of an earlier UNKNOWN string.

This follows the shared [VALUES type resolution rule](https://www.postgresql.org/docs/18/typeconv-union-case.html).
Custom/domain cast catalogs and all set-operation coercion are not claimed
complete by this builtin-column correction.

## Evidence

Private artifact root: `/tmp/dbms-simple-case-binding.KtyWx31T`.

`values-common.baseline.log` (tool 82617, exit 134) records 14 actual native
assertion failures: wrong descriptors and input row types for INT/BIGINT,
typed NULL/BIGINT, UNKNOWN/INT, INT/NUMERIC, REAL/INT and UNKNOWN-only TEXT;
missing `42804` for INT/TEXT and missing `22P02` for unknown `'bad'`/INT.
The build-error log from a first missing-test-include attempt is not a defect
reproduction; the retained executable baseline was compiled successfully.

The strict PostgreSQL 18.6 reference (`server_version_num = 180006`),
`values-common.reference18.log`, passes the permanent protocol fixture.
Matching old prepared execution, `values-common.wire.baseline.log` (tool 28732,
exit 1), fails 11 assertions: the BIGINT CTE arithmetic incorrectly raises
`22003`, and missed input/type errors allow writes and irreversible sequence
effects. Later no-effect assertion failures are consequences of those earlier
accepted invalid statements, not separate root causes.

The candidate `values-common.wire.candidate.log` passes with the original
arithmetic, OID 20, input SQLSTATE, empty-table and sequence `55000` assertions.
`build.values.log` (tool 32580, exit 0) records six fresh `-O2` native successes:
`values_common_type_binding_test`, `simple_case_constant_demand_test`,
`simple_case_equality_binding_test`, `case_common_type_test`,
`prepared_query_execution_test`, `query_binding_test`.
The latter four binder/CASE controls also pass scoped ASan/UBSan in
`values.asan.log` (tool 88948, exit 0).

There is no header/layout change. The all-58 fresh simple CASE API basis is
retained; binder and the independently merged ROOT RETURNING DML source are
freshly compiled, the prior fresh constant-demand execution object is used,
and the other 55 source/header/flag hashes match the immutable donor.
`values/{sources,headers}.audit.txt`, `values/other55.sources.audit.txt`,
`values/binary.sha256` and sanitizer final audits record the exact combination.

Ordinary CASE query dispatch is still a separate pending consumer change:
the expanded ordinary matrix remains an actual 50-failure baseline here and
is not represented as passing by the six native tests above.
