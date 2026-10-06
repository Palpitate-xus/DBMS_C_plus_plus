# Preserve CAST SQLSTATE as structured execution metadata

The ordinary typed UPDATE native regression exposed an existing CAST boundary:
`CAST('bad' AS integer)` threw `std::runtime_error` with SQLSTATE only in its
message, not `DbError`. A direct native baseline (session 15063) tested 21
syntax/range/unsupported-cast cases and all 21 had this missing structured
contract (exit 1). The wire adapter could recognize their old text, which was
not evidence that native error metadata was correct.

Twenty-six existing CAST failure sites now construct `DbError` with their
existing hard-coded SQLSTATE and message separately. Native consumers can read
the code without parsing user data, translated text or the CLI diagnostic.
Normal casts, NULL values, numeric widths and existing SQLSTATE classifications
are unchanged. There is no public header/API/layout change.

Final proof is `/tmp/dbms-typed-cast-errors.LpOcGr/verified-v2.lRrq4N`:

- Session 53743 exited 0: fresh changed evaluator at normal O2, 55 unchanged
  formal ROOT objects, all 56 signatures/source/header/manifest hashes audited,
  fresh stubs and nine isolated freshly compiled/linked native tests. This is
  not a fresh private 56-unit build.
- New native checks 26 negative cases plus valid integer minimum/maximum,
  boolean/date casts and typed NULL. It requires exact `DbError.sqlState()`
  and one correctly formatted CLI suffix, including bytea and typmod errors.
- Frozen SHA-256:
  `75f1019d665b74b3685f90a2c547612cc31da9dd4d7371ae3f07dd963e6c6883`.
- Session 32553 exited 0: six adjacent wire scripts covering integer/floating
  arithmetic widths, result OIDs, character cast Describe typmods, quoted
  arithmetic Describe and the ordinary arithmetic bridge.
- Initial candidate 85321 nine natives and 90596 six wires also exited 0;
  final formatting/extended native cases were separately recompiled/reverified.
  Initial candidate patch, logs and frozen binary are retained separately.

This closes the reproduced structured CAST error defect only. Other builtin
error boundaries, interval input/operator/storage ranges, assignment coercion
and broad type families remain open. ROOT canonical 40539 remains live; this
private commit is not yet a ROOT integration or canonical full-suite pass.
