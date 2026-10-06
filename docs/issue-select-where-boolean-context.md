# SELECT WHERE must retain its boolean input context

INSERT SELECT previously prepared an UNKNOWN bare WHERE string without
transforming it to boolean. The row consumer therefore saw UNKNOWN/TEXT and
returned an unsupported-shape result; legacy execution then returned `42601`
for the valid original `INSERT ... SELECT '3' WHERE 'true' RETURNING id`.

The structured SELECT binder now validates a known WHERE type as boolean and
creates a genuine implicit boolean cast for UNKNOWN input, keeping actual AST
ownership/source spans. Literal input conversion is pure and before any
source or projection routine. A boolean NULL filters out rows; a typed TEXT
NULL is an analysis `42804`, not an implicitly valid false predicate.

Evidence under `/tmp/dbms-materialized-target.fBH1T6Yr`:

- `consumer-native-baseline/insert_select_boolean_context.log`: terminal 134;
  four expected precise errors and six valid true/false/NULL/table-source
  statements incorrectly chose legacy fallback. No writer ran.
- `boolean-final-build.log`: terminal 0; only the changed binder was freshly
  compiled against the same all-58 O0 basis, with exact TM/DML/55 other source
  hashes and all 101 unchanged headers audited. Eleven matching native tests
  pass, including all ten new boolean input controls, primitive preparation,
  UPDATE RETURNING, original pureplanner, binding/execution/cursor/CASE,
  interval INSERT SELECT, DML RETURNING and WITH primary binding.
- `assignment-wire-final.log`: terminal 0. The unsplit original 18 controls and
  all original valid UPDATE/NULL/INSERT SELECT/output assertions are unchanged;
  four additional invalid-predicate no-effects controls pass. The monotonic
  sequence reaches exactly 22 with no resets, each failed statement preserves
  rows and emits no partial success tag/result, and the final INTEGER OID is 23.
- `assignment-wire-reference18-final.log`: the same complete script passes
  against strict PostgreSQL `180006` in a rolled-back isolated transaction.
- Matching UPDATE typed predicate and interval INSERT SELECT wire scripts
  pass (`boolean-update-adjacent.log`, `boolean-interval-adjacent.log`), as do
  WITH primary DML and CASE common-type scripts (`boolean-with-adjacent-valid.log`,
  `boolean-case-adjacent-valid.log`). Two earlier wrapper attempts used
  nonexistent WITH script filenames and exited 2; they are retained and are
  not counted as protocol test failures or successful gates.

Candidate binary SHA256:
`47d54f3923eccda18059576ad92ebe9acdce244e36629d8126139b04a734db9e`.
There is no public header/layout change. The complete formerly known-gap script
is now `tests/prepared_primitive_assignment_protocol_e2e_test.py`, registered
in the default E2E manifest, without a skip switch or reduced expectations.

This closes these tested ordinary assignment and WHERE consumer defects, not
all WHERE/HAVING/operator signatures, materialized-view target semantics,
geometry CAST, or the overall SQL/type families. No push or Actions ran.
