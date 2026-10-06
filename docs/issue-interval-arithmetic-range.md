# Interval arithmetic field widths and inclusive minima

Interval arithmetic previously accepted month/day results outside their
32-bit ranges, returned SQL NULL for time-field overflow, and rejected the
valid INT64 minimum when subtraction produced it. The independent repair
checks calendar fields against signed 32-bit bounds and microseconds against
signed 64-bit bounds. Addition, subtraction and scaling report structured
22008 on overflow; the inclusive minimum remains a valid non-NULL value.
Existing division-by-zero behavior remains structured 22012.

The original native baseline (session 86603, exit 134) and wire baseline
(session 6306, exit 1) remain under
`/tmp/dbms-interval-arithmetic.igKW0oZj`. The wire's first overflowing
INT64-max-plus-one expression previously returned a successful NULL.
Three old assertions in `interval_arith_test.cpp` now distinguish two actual
overflow errors from the valid minimum; ordinary value assertions remain.

Private commit `19f6107ee50f86e69cc132f91a586ff8f902531b` depends on the
separate interval-input and arithmetic structured-error repairs. Final
normal-O2 verification (session 51719, exit 0) covers 11 freshly linked
natives and eight protocol scripts. Its frozen server SHA256 is
`7d4682705af6be22982e72d7ca13f65403a5ae1f665a548f042c264944265790`.
The unchanged 55 production sources and 95 headers were audited against an
immutable, matching 56-source basis; this is not a fresh ROOT build.

The dedicated wire test retains 14 overflow controls, ten success/NULL
controls, exact OID 1186, explicit transaction failure (25P02) and recovery.
The same expanded test passed against an actual PostgreSQL 17.2 diagnostic
reference, not PostgreSQL 18. The selected evaluator/test ASan+UBSan run
(session 66748, exit 0) covers three natives; the other production objects
were not instrumented and leak detection was disabled.

ROOT integrates this as one local interval-arithmetic commit. A fresh
combined production build and matching regressions remain necessary.
Storage's separate interval-input parser, fractional scaling semantics,
other temporal operations, and the complete interval/date family remain
open; these focused checks do not close those requirements.
