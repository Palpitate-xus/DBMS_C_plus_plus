# Preserve BIT literal type across stored predicate adapters

The prior SQL adapter decoded a genuine B/X literal to bare bits but lost
its BIT type. A TEXT value `01` and INTEGER value `1` could consequently
match `B'01'`, whereas PostgreSQL rejects both operators with `42883`.
Decoded list elements also lost empty values, SQL NULL and their literal
identity; a column named `01` could be mistaken for a quoted RHS datum.

This root preserves the BIT operand type in the existing right-operand
metadata slot, validates the real comparison signature before scanning and
before NULL can suppress that validation, and retains original literal
tokens for compact BIT IN/BETWEEN lists. The actual list comparison retains
empty members and NULL truth, never reinterprets a decoded literal as a
column, and preserves the native data-only API contract. Quoted scalar SQL
RHS literals retain their literal identity as well.

The actual query binder and the existing ExprHelper preparation path both
validate ordinary BIT comparison operand types, with unconstrained implicit
UNKNOWN inputs. Direct runtime comparison also checks types before NULL.
No public header, storage format, Main or timing/deadline is changed here.

## Evidence and dependencies

All artifacts are under `/tmp/dbms-bit-literal-typing.zyVFLv/`.

- `literal-typing-baseline-whole132.log`: complete strict PostgreSQL 18.6
  version 180006 versus frozen 90c6 production, 132 unchanged SQL/row/NULL/
  OID controls, 84 differences, shell 67458 exit 1.
- `literal-typing-pre-cc81-whole132.log`: the same whole matrix versus the
  preceding frozen BIT ARRAY production, shell 1951 exit 1, 106 differences.
  The adapter introduced actual false matches on TEXT/INTEGER columns;
  preexisting missing errors are not relabeled as newly introduced.
- `literal-type-strict-reference18-v1.log`: all 91 permanent SQL controls
  pass against actual strict version 180006, exit 0. Owned unique schemas are
  dropped in cleanup; shared PostgreSQL public data is untouched.
- `literal-type-final78-baseline-native.log`: exact permanent native driver
  against all-58 receipt/source/header/flags/byte-proved normal 90c6 inputs,
  first quoted-data/column-collision assertion fails, shell 7297 exit 134.
- `literal-type-baseline-whole91.log`: original 91-control fixture against
  frozen 90c6, exit 1 at explicit TEXT versus BIT comparison.
- `literal-type-candidate-normal-native-v1.log`: four changed CPPs freshly
  compiled normal O2 with 54 exact-input-proved normal 90c6 donors; current
  all-58 receipts, build stamp and immediate repeat verified. Shell 62414
  exit 0. Eight complete native drivers passed: new 78-control compact SQL
  type/empty/list/NULL/collision driver, unchanged stored-literal and native
  data-only drivers, comparison, ARRAY constructor, original bit, type
  registry and quantified query execution.
- Candidate production SHA256 is
  `1f0009c00c406de4b385cf25f2bc5beebd60d84b53baca730c3cd8d6bfc526f7`.
  The donor is the clean exact 90c6 tree and frozen SHA3513ca8f production
  at `/tmp/dbms-bit-varbit-semantics.JYF0FS/repo`. The private build helper
  audits all 58 donor receipts and every reused source/header/flags/object
  byte input. This is not a fresh-58 or all-SAN claim.
- `literal-type-candidate-whole132-v1.log`: complete unchanged strict 132
  matrix, shell 13298 exit 1, 27 remaining differences. All adapter fake
  matches/type errors, quoted RHS collision and compact list controls are
  fixed; unrelated remaining errors are preserved, not normalized away.
- `literal-type-candidate-whole91-v1.log`: shell 27160 exit 1. The permanent
  fixture correctly exposes the existing frontend literal-IN expansion:
  `WHERE v IN (B'',B'01') ORDER BY id` returns IDs 3,1 instead of 1,3. The
  unchanged broad matrix also retains `NOT IN (B'',NULL)` and a quoted
  numeric left-column lowering defect. Those frontend roots must be fixed
  independently before this combined SQL path is handed off as READY.

This root's native gate is complete, but its 91-control wire gate is not
green alone. No SQL/assertion/deadline has been weakened to hide that fact.
BIT b/x textual input, modifier/descriptor limits, binary padding, operator
return signatures, numeric casts, TYPE-11 and the original whole checklist
remain separately open.
