# UPDATE RETURNING preparation errors must not choose a fallback

The target-only prepared UPDATE consumer previously checked legacy RETURNING
capability before preparing the whole statement. An unknown RETURNING routine
or column therefore chose a legacy fallback. The wire executor then reported
`XX000`/"Update failed" instead of `42883` or `42703`.

After existing target resolution and privilege checks, the consumer now binds
the original whole statement before making the legacy capability decision.
An unsupported but valid RETURNING shape can still fall back; an unresolved
name is a real structured SQL error before SET, source reads or mutation. The
existing assignment/WHERE/RETURNING input-priority ordering is retained.

Evidence is retained under `/tmp/dbms-materialized-target.fBH1T6Yr`:

- `returning-baseline/test.log`: terminal 134, four unknown-name controls chose
  legacy fallback; invalid bare SET input already produced `22P02`.
- `consumer-native-final/update_returning_preparation.log`: terminal 0, all
  five errors precise, no published partial result, unchanged target and no
  writer sequence call; valid UPDATE RETURNING value/type/tag also pass.
- `consumer-native-returning-adjacent`: DML RETURNING, WITH transition RETURNING
  and transition binding native tests pass with matching objects.
- `assignment-wire-returning-candidate.log`: unchanged unsplit protocol matrix
  now reports `42883` for the original failing UPDATE; all monotonic sequence
  and rollback checks pass. It still exits 1 solely for the separate legal
  INSERT SELECT unknown-boolean WHERE consumer error and dependent aborted
  transaction assertions. No assertion or error expectation was removed.

Candidate binary SHA256:
`21df8734ae96b58bd6dc6c7a41d5db6347f3c30587d692e60817b4e68ee3cd35`.
Only DmlExecutor was recompiled against the same full-58 O0 source/header basis
as the preceding primitive input fix; source and all 101 header hashes match.
There is no public API/header/layout change. The first candidate test expected
the literal spelling `integer` from legacy storage; its positive check now
asserts the canonical INTEGER type (accepting the equivalent storage spelling
`int`). Exact wire OID remains 23. The original failed fixture log is retained.
The initial adjacent DML RETURNING test expected the old bool-error contract for
the deliberately hidden `old` alias. Full preparation now throws the precise
`42P01`; the same SQL and post-failure unchanged-row assertion are preserved,
and the native negative control now requires that exact structured SQLSTATE.
The uncaught initial failure is retained rather than counted as a pass.

This is not a claim that all UPDATE/FROM/DEFAULT/RETURNING paths, materialized
view targets or the overall DML family are complete. No push or Actions ran.
