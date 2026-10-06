# Preserve the typed overflow contract in integer_bounds_test

The canonical full run on `1e0a8c5a` terminated this test with `DbError` SQLSTATE
`22003` at `BIGINT_MIN / -1`. The old assertion expected an empty result sentinel,
contradicting the corrected typed overflow contract. Production code is unchanged.

The test now catches exactly `DbError` / `22003`. All storage width and INSERT /
UPDATE assertions remain, as does the subsequent valid `BIGINT_MIN % -1 == 0`
query, which also checks that evaluation can continue after the exception.

Corrected test session 71894 exited 0 against the original ROOT 55 production
objects, fresh O2 test compilation and matching stubs in an isolated directory.
Log: `/tmp/dbms-insert-omitted-null.iHZNTrqu/integer-bounds-corrected.log`.
This is a test contract correction, not an additional production repair or a
claim that every integer/numeric gap is complete.
