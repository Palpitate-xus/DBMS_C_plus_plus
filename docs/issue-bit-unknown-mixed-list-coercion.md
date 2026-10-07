# Resolve UNKNOWN literal list comparisons per pair

Literal-IN preparation pinned a shared UNKNOWN left operand to BIT when
any list member was BIT. Valid heterogeneous BIT/INTEGER lists then raised
42883, including first-match, NULL and empty-table cases. An additional
runtime issue used numeric-looking value comparison without real BIT input
coercion, so a leading-zero/width mismatch could incorrectly match before
an INTEGER member was checked.

Preparation now distinguishes a homogeneous BIT context from independent
heterogeneous comparisons. Each pure UNKNOWN literal input is validated
against every relevant member before opening a scan, but heterogeneous
literal comparisons do not receive a global left BIT cast. Homogeneous BIT
lists retain one BIT context and unconstrained implicit UNKNOWN members.
A genuine UNKNOWN ParameterExpr cannot infer inconsistent BIT/INTEGER
targets: it reports the real PG 42P08, rather than pretending to be a new
constant at each occurrence. Nonliteral operands are not executed to prepare.

The real runtime evaluates the left operand once and uses existing resolved
comparison/coercion for each BIT pair. Only a real untyped SQL literal is
relabeled UNKNOWN for that BIT consumer; other pair/data consumers retain
their existing types. BIT widths, leading zeros and empty values therefore
remain significant. No Main, public header/layout or deadline changes.

## Complete actual evidence

New artifacts are under `/tmp/dbms-bit-unknown-lists.hOGDEeJY/`; all preceding
trees and logs under `/tmp/dbms-bit-literal-typing.zyVFLv/` remain frozen.

- Original strict 40 controls: the frozen pre-IN tree has 20 differences
  (`unknown-list-before-in-baseline-whole40.log`, shell 36894 exit 1), while
  the preceding ba3f candidate has 32
  (`unknown-list-candidate-baseline-v9-whole40.log`, shell 64297 exit 1).
  New v1 normal shell 14538 exits 0 and
  original40 shell 5293 exits 0. Original SQL/rows/NULL/OIDs/errors remain.
- `unknown-list-strict-reference18-whole128-v1.log`: permanent 128 complete
  controls against actual strict PostgreSQL 18.6 version 180006, direct 0.
  `unknown-list-baseline-whole128-v1.log`: same entire fixture against
  frozen ba3f, shell 63666 exit 1, 82 failures, all controls executed.
- `unknown-list-native-baseline-final235.log`: the exact final permanent
  235-control driver against all-58-receipt-proved normal ba3f production
  modules, shell 96748 exit 134 at the first real helper assertion. Source,
  headers, flags, repeat stamp and frozen production were audited first.
- `unknown-list-strict-reference18-prepared-parameters.log`: actual owned
  connection-local PREPARE/EXECUTE against strict 180006. Both mixed member
  orders reject with 42P08; homogeneous BIT/UNKNOWN and NULL member cases
  return exact true/false/NULL. No shared prepared statement is touched.
- V2's normal 19 complete native files, shell 45272, pass, including the
  then-139 control driver; permanent128 wire shell 47062 exits 0. The
  original132 remains five red, shell 53021 exit 1. These are intermediate
  successes, not a claim that mixed BIT widths were already correct.
- Strong independent `bit_unknown_mixed_list_length_protocol_e2e_test.py`
  adds 288 exact B/X width, leading-zero, NULL, operand-order, populated/
  empty and error controls. Strict real180006 direct exit 0; the same whole
  fixture against V2 exits 1, shell 30160, 108 failures. These cases separate
  BIT equality from INTEGER equality so a coincident numeric hit cannot
  mask wrong BIT coercion. Original128 assertions remain unmodified.
- `mixed-admission-strict-reference18-v1.log` and actual candidate shell
  12469 both exit 0, all 16 genuine owned routine controls: typed invalid
  admission never invokes the writer, even on an empty table; an admitted
  volatile left argument runs once for true, false and SQL NULL. Frozen
  ba3f positive control shell 3959 also exits 0, not a fabricated failure.
- `unknown-list-candidate-normal-native-v3.log`, shell 84179 exit 0:
  all 19 complete native files, including final235 actual helper/binder/
  prepared-cursor/parameter/writer controls, previous3387/272/78/16/8,
  unchanged original11 set, actual binding and current module group512.
  Group512 links 57 production project modules, not Main.
- Normal source uses three genuine changed CPP objects (latest stage fresh
  ExprEvaluator and exact-current helper/binder objects), plus 55 individually
  source/header/flags/all-58-donor-receipt/object-byte-proved ba3f normal O2
  donors. Current all58 receipts, build stamp and immediate repeat pass.
  This is not fresh58 and does not reuse ROOT's newer incompatible headers.
- V3 whole first run shell 75714 exits 1 only at original128 CREATE SCHEMA
  timeout; the remaining 14 full wire files and CLI pass. Original40/132
  concurrent initialization runs shells 80972/57956 also time out. All red
  logs are retained; no SQL/assertions/deadline is changed.
- Exact same full fifteen-wire-plus-CLI repeat, shell 72113 exit 0: new128,
  new288, admission16, unknown33, original91, list56, indexed156, routine16,
  quoted26, original bit, stored21, comparison32, ARRAY6, original bounds,
  quantified-demand84, and actual three-query CLI all complete. Full outputs
  are `unknown-list-final-v3-repeat-*.log`. CLI uses one persistent directory
  with a new process per query, not one backend session.
- `unknown-list-candidate-original40-v3-repeat.log`, shell 48521 exit 0,
  all original 40 strict controls match.
- `unknown-list-extra-unknown-rhs-v3.log`, direct exit 0: all 64 additional
  complete actual strict180006/candidate comparisons with unknown right
  members mixed with BIT/INTEGER, both orders, width-divergent literals,
  NULL/empty/input-error and IN/NOT IN. No shared reference objects created.
- `unknown-list-final-strict-wrapper.log`, direct exit 0: all eight whole
  strict180006 files rerun from this tree, new128/new288/admission16 plus
  unknown33/original91/list56/indexed156/routine16. Individual full logs are
  `unknown-list-final-strict-*.log`; each connection verifies version 180006.
- `unknown-list-candidate-original132-v3-second-repeat.log`, shell 85030
  exit 1, completes all original132 and retains exactly the five existing
  red controls below. Earlier 57956/54392 initialization timeouts remain
  separate failed logs and are not classified as a complete 132 run.

Final normal production/frozen SHA256 is
`fa7947d34755a3cebd54796c8a00ff67fd3b9349f3ba668069f372423c9b20c5`.
Frozen donor is clean ba3f/SHAf3df8e1b. Original five open comparisons are
`B'01'='b01'` (PG true, candidate22P02), `B'01'='x1'` (PG false,
candidate22P02), and stored INTEGER `i=''`, `i<>''`, `i<'' ORDER BY id`
(PG22P02, candidateSELECT0). Numeric/operator signatures, typmods/descriptors,
binary padding, TYPE-11 and the original checklist are not declared complete.
ROOT integration must preserve its newer enum/AST bindings, hash NULL
ownership, implicit BIT constructor and Boolean BETWEEN metadata; newer
public headers require a genuinely fresh58 integration build.
