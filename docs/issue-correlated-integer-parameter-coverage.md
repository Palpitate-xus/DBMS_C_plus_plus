# Genuine INTEGER correlated-parameter coverage

The original `parameter_origin_correlated_child_test.cpp` has 24 execution
cases plus a reach assertion. Its `makeIntColumn("i", true, 4)` factory makes
an eight-byte BIGINT. That test remains required and unchanged; describing its
physical input as INTEGER was inaccurate. The separate existing 12-case wire
fixture does use genuine SQL INTEGER.

`correlated_integer_parameter_origin_test.cpp` adds independent coverage:

- Actual registry resolution and persisted four-byte INTEGER schema.
- Actual bound INTEGER source descriptor and Boolean output metadata, with
  no writer effects during preparation.
- All twelve NULL/zero/one, BETWEEN/NOT BETWEEN, typed-cursor/native-fallback
  cases, genuine descriptor-ordered input cells and exactly two writer effects.
- Reach assertions: 28 complete controls, without removing the original 25.

## Actual current-root evidence

Artifacts: `/tmp/dbms-root-parameter-origins.jChCYiKO`.
The unchanged new fixture compiled against actual old Root `8505b94e` headers
and its receipt-verified normal project objects passes all 28 controls, exit 0
(session 50034, `true-integer-old-root.log`). The actual old Root binary is
`c7abd647dad7e633de9f4e09f387640f100cbdae121ac8afdd1fb03c3eec568b`.

The four independently composed demand/origin/carrier/context commits at
`2585d80d` genuinely compile all 58 production units from scratch, exit 0
(session 5065). Its own normal binary is
`0d4d368bde7db00a4bcbc2f69892ec4df7e589b1ec91cf3e338b77f93bcb8bec`.
All 15 complete native fixtures pass on its own current normal objects,
including original 25 and new 28 (session 5407, `current-core-native15.log`).
All source/header/compiler/flag receipts, build stamp, frozen binary and input
seal are checked before and after that gate. This is a coverage/documentation
correction, not evidence of a new INTEGER production defect or completion of
the full INTEGER family. The independent 9,649-control integer fixture still
fails 4,116 controls on this four-commit composition (session 30030,
`current-core-integer9649-baseline.log`); its separate integer repairs remain
necessary.
