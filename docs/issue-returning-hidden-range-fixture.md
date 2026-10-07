# RETURNING fixture: hidden OLD and target range names

The current exacta7 normal-O2 protocol run rejects a hidden OLD range with
42P01. The existing fixture incorrectly requires its older unsupported-path
0A000. Actual PostgreSQL18.6, version180006 on the owned15486 endpoint,
independently returns42P01 for both hidden OLD and the target name hidden by
an UPDATE alias.

Only these two expected states and their explanatory comment change. Original
SQL, OLD/NEW rows, output labels/OIDs, INSERT/UPDATE/DELETE/UPSERT/MERGE command
tags, and both error-before-mutation data assertions remain intact.

The entire unsplit corrected test exits0 on the matching immutable ROOTa7
normal-O2 binary, SHA256
`b38f83e0e570d250840e4cf361213e9cb55464e9c6cc1db27cde355f5ce33225`,
session60113, `returning-fixtures.log` under
`/tmp/dbms-root-native-rechecks.nqfnjb9y`.

The identical entire test also exits0 against actual180006, including MERGE.
`reference-returning-fixture.py` changes only the server transport, not test
SQL/assertions. It verifies version180006, creates a unique owned schema,
uses the original15s timeout, logs exact SQL/results, and drops that schema
in finally. `returning-fixtures-pg18.log` retains the complete reference proof.
No production source or public API changes, and no full-suite claim.
