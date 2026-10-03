# Issue 973: keep empty text distinct from NULL in semi joins

Date: 2026-10-03  
Source/test commit: `d6ce9adf`  
Status: locally committed; not pushed. `QRY-04` remains partial.

`SemiJoinOp` could treat an empty text value as SQL NULL when it fell back to
inspecting the rendered column value. The operator's row metadata already
records the actual NULL state, so the semi-join now uses that authoritative
metadata for scalar and multi-column keys. This preserves empty strings as
ordinary values while retaining three-valued `IN` / `NOT IN` behavior for
actual NULLs.

The regression was exposed while running the composite `NOT IN` compatibility
case: a prior test had covered the composite empty-text path, and the full
protocol run found the corresponding scalar path. Tests now cover both scalar
and composite `IN` / `NOT IN` cases with empty text and SQL NULL.

Verification:

- `tests/composite_semi_join_key_test.cpp`: passed with scalar and composite
  empty-text / NULL cases.
- `tests/composite_not_in_null_semantics_protocol_e2e_test.py`: passed,
  including scalar empty-text `IN` / `NOT IN` checks.
- Final registered test suite, run with 120-second protocol/startup/shutdown
  timeouts via `scripts/build_tests.sh`: exit 0, `All tests passed`.
- Full compatibility differential against the local PostgreSQL 18.6
  reference (`server_version_num=180006`, `en_US.utf8`): `cases=464 failed=0`,
  exit 0. All 464 cases, including the composite NULL and explicit NULL-order
  cases, matched.
- No changes were pushed. The user-skipped security/TDE audit remains deferred.

This closes the reproduced empty-text-versus-NULL bug in these semi-join
paths, not the broader `QRY-04` subquery semantics, which remain partial.
