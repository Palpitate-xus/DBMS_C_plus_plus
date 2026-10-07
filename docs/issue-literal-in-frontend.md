# Keep literal IN as one typed predicate

The ordinary single-table SELECT frontend expanded a literal IN list into
independently executed OR/AND scan branches. That discarded list member
type/empty/NULL information, concatenated independently sorted rows, and
gave incorrect global ORDER BY, LIMIT, OFFSET and NOT-IN NULL results.

Ordinary SELECT now retains the original literal list as a single typed
predicate. Existing subquery expansion and non-SELECT callers retain their
previous default behavior. The real expression helper and binder resolve
BIT list member signatures, preserve implicit unconstrained UNKNOWN input,
and reject typed incompatible members before an empty scan can hide them.
Preparation uses real schema types and does not execute nonliteral operands.
No original SQL, assertions, protocol deadline, public header or layout changes.

## Complete evidence

Artifacts are under `/tmp/dbms-bit-literal-typing.zyVFLv/`.

- `literal-in-frontend-baseline-whole56.log`: all 56 exact SQL controls
  against strict PostgreSQL 18.6 version 180006 and frozen pre-IN production,
  shell 27690 exit 1, 31 differences. This includes BIT empty/NULL/duplicate
  lists, ordinary TEXT data containing comma/quote, INTEGER lists, global
  ascending/descending order, LIMIT/OFFSET and populated/empty type errors.
- `literal-in-strict-reference18-final56.log`: the permanent complete
  56-control fixture against real strict version 180006, direct exit 0.
- `literal-in-final56-baseline-whole.log`: exactly that permanent fixture
  against frozen quoted-column production SHA8f6598e9, shell 88948 exit 1,
  31 failures. All controls execute, rather than stopping at the first red.
- `literal-in-candidate-normal-native-v1.log`: shell 1067 exit 0, normal
  production and all ten complete native drivers. `literal-in-candidate-
  whole56-v1.log` shell 66775 exit 1 retains four empty-scan type errors.
  The same original 91-control fixture remains red, shell 55711 exit 1.
- `literal-in-candidate-normal-native-v2.log`: shell 20186 exit 0, normal
  production and ten complete native drivers, including current-module
  512-row typed group keys. `literal-in-candidate-whole56-v2.log`, shell
  79390 exit 0, all 56 permanent controls. That does not certify Main via
  the native grouping driver: the driver links 57 production project
  modules, not Main. The separate three-query CLI is a new process per
  query against one persistent data directory, not one backend session.
- The original 91-control fixture still finds empty INTEGER BETWEEN,
  shell 2398 exit 1. BETWEEN preparation/pre-scan validation is a separate
  follow-up root; this commit alone is not declared whole-path READY.
- `literal-in-candidate-normal-native-v5.log`: combined candidate after
  that independent follow-up, shell 39498 exit 0, all eleven complete
  native drivers, current all-58 receipts/repeat/build stamp verified.
  Six changed CPPs have genuine normal O2 objects; 52 unchanged objects
  are exact source/header/flags/all-58-donor-receipt/object-byte-proved
  normal 90c6 donors. This is not a fresh58 claim.
- `literal-final-whole-wrapper-v5.log`: combined candidate shell 39642
  exit 0. Ten complete wire fixtures (original91, new56, indexed156,
  quoted26, original bit, stored21, comparison32, ARRAY6, original bounds,
  quantified-demand84) and the actual three-query CLI all pass. Each full
  output is retained as `literal-final-control-*-v5.log`.
- `literal-typing-candidate-original132-v5.log`: original strict matrix
  unchanged, shell 57024 exit 1, five remaining differences: two unknown
  b/x bit input controls and three INTEGER unknown empty-input errors.
  All original literal-IN order/NULL controls now match exactly.

All earlier v1-v4 red logs remain untouched. Two v4 initialization runs
hit the original timeout; the identical-deadline 91-control repeat, shell
86348, exits 0. No timeout was extended. BIT operator return signatures,
typmods/descriptors, binary padding and numeric casts, TYPE-11 and the
original checklist remain open outside this root.
