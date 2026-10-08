# INTEGER BETWEEN: comparison-owned UNKNOWN input preparation

This is a finite ordinary SQL input-preparation issue, not a TYPE-11 family
completion. It is based on immutable Root Source 104, `168596fc`, in a new
private repository. No Root workspace, frozen demand candidate, reference
`public` objects, Actions setting, or remote branch was changed.

## Actual defect and root cause

The unchanged original 1296-query driver reported exactly 26 differences on
Root 104. For example, `'b01' BETWEEN '01' AND 1` succeeded instead of returning
PostgreSQL `22P02`. NULL bounds and reversed bound order could hide the invalid
integer input. Existing range preparation and evaluated comparison dispatch
were enabled only for BIT inputs, although the ordinary comparison binding and
integer input codecs already owned the correct conversion.

Preparation now also admits SMALLINT/INTEGER/BIGINT ranges and resolves each
comparison independently. A genuine UNKNOWN **literal** left operand remains a
separate input to each logical comparison: UNKNOWN/UNKNOWN first selects TEXT,
and an INTEGER peer in the second comparison still validates its own literal
input. A real shared UNKNOWN **parameter** is different: its first comparison
sets the shared context, so `$1 BETWEEN NULL AND 1` must reject the TEXT/INTEGER
signature with `42883`, not reinterpret every rendered occurrence separately.
Explicit TEXT operands likewise retain their type and reject integer signatures.

The change reuses actual `resolveComparison`, `coerceCaseInput`, and
`comparePrepared`; no SQL child, row provider, parameter value, or volatile
function is executed to infer a type. The wire Parse paths call the actual
whole-query metadata binder before publishing a prepared statement. Physical
single-source ranges with UNKNOWN inputs also go through that binder when
syntax-only inspection cannot know a stored routine's declared result type.
This catches `writer(i) BETWEEN NULL AND 'b01'` without executing `writer`.
Typed invalid signatures are validated before Parse/Describe, including empty
and all-NULL physical tables. Fromless shared parameters retain their actual
`$n` source coordinates and existing CAST/context inference. The admission
guard recognizes the parser's uppercase, unqualified grammar role, not a
quoted or schema-qualified routine with a similar name.

## Permanent, non-weakened controls

- `integer_between_input_test.cpp`: 9649 assertions. All 4608 scalar cells run
  through both actual helper and bound plan/cursor owners; 70 shared UNKNOWN,
  TEXT, and explicit-width parameter cases, 24 real stored writer checks, and
  all 288 actual empty/populated/NULL and projection-cap 0/1 cases also execute.
- `integer_between_input_protocol_e2e_test.py`: all 5208 SQL controls, including
  4608 scalar cases, 576 physical projection/WHERE/index/OR cases, and 24 owned
  volatile writer admissions. Error cases require no RowDescription/DataRow;
  successful cases retain exact rows, NULLs, names, tags, and OIDs. Writer
  effects are checked using a genuine sequence outside rollback masking.
- `integer_between_parameter_protocol_e2e_test.py`: all 350 actual Parse,
  statement Describe, Bind, portal Describe, Execute, and Sync cases. Explicit
  OIDs 0/21/23/20/25, first comparison context, invalid input, width overflow,
  and NULL values remain strongly asserted.
- `integer_between_parse_protocol_e2e_test.py`: all 312 physical-table Parse,
  Describe, cap-1 Execute, empty/NULL, typed-signature, and metadata-only stored
  writer controls. Capped portals that suspend are drained within their actual
  transaction, with the reference's resumed `SELECT 0` tag retained.

All three SQL fixtures use owned unique schemas/statements and retain the
runner's original 15-second wire and 20-second startup deadlines. Their
reference mode checks actual PostgreSQL 18.6 (`server_version_num=180006`).
An early baseline draft stopped at the first writer-effect assertion; its log
is retained. The final fixture still makes that assertion, but collects every
effect failure before its terminal assertion, so all 5208 baseline SQL cells
are now reached: Root 104 actually fails 3198 controls. A draft portal fixture
incorrectly resumed after an implicit transaction and expected a cumulative
tag; those failed strict runs remain recorded, and the final explicit-
transaction fixture was verified against real 180006 before candidate use.

## Independent dependencies and evidence

Extending the strong input matrix uncovered two older, independent defects:
a bound's postfix CAST could incorrectly declare the entire BETWEEN result
SMALLINT/INTEGER/BIGINT, and Boolean-to-character casts emitted internal `t/f`
instead of SQL `true/false`. Neither is hidden by dropping OID/value assertions.
They have separate source commits and permanent controls. The input-only v1
binary already makes the unchanged original 1296 matrix pass, but the complete
new 5208 matrix still reports its 774 old descriptor failures; only the combined
candidate is approved for the complete matrix.

Evidence root: `/tmp/dbms-integer-between-input.fHTGGaWj`.

- `integer-between-original1296-baseline-root104.log`: actual 1, all 1296,
  exactly 26 differences.
- `integer-between-original1296-candidate-v1.log`: actual 0, all 1296 unchanged.
- `integer-between-new5208-baseline-root104-full.log`: actual 1, all 5208,
  3198 strong failures; the earlier prematurely stopped baseline remains.
- `integer-between-new5208-candidate-v1.log`: actual 1, all 5208, 774 independent
  descriptor failures retained.
- `integer-between-new5208-candidate-v2.log`: actual 0, all 5208.
- `integer-between-new350-baseline-root104.log`: actual 1, all 350, 250 phase
  differences; strict and candidate complete 350 runs actually return 0.
- `integer-between-new312-strict-v5.log` and
  `integer-between-new312-candidate-v5.log`: actual 0, all 312.

## Final combined candidate: actual terminal gates

The production changes are only four existing CPP files; all public headers,
AST coordinates/layouts, Root enum/aggregate/CTE/window/integer/BIT declarations,
manifest and compiler/link/TLS flags match the immutable Source 104 donor.
`build-normal-and-native.sh` proves every donor source, all headers and flags,
all 58 original donor receipts/stamp, and each reused object byte. The first
normal compiled four changed CPPs and used 54 exactly proved Root 104 normal
O2 donors. Later functional edits compiled their actual changed CPPs; every
normal repeated and verified all 58 current receipts and the full input stamp.
This is **not** a fresh-58 claim, nor reuse of an older private ABI.

Final binary: `dbms_main.integer-between-v5.frozen`, SHA-256
`b3dd7391fd70dcc5352f794a27b5c54d75ea54a87a3d00e26b255fa8cceea0f9`.
It exactly matches the current normal `dbms_main` byte for byte.

| Actual gate | Terminal outcome | Immutable log |
| --- | --- | --- |
| normal/repeat/current 58 receipts | 0 | `integer-between-final-normal-native.log` production phase; the first draft's two fixture failures remain |
| complete 36 native files, isolated current-normal O2 objects | 0 | `integer-between-final-native-all.log` |
| complete 33 whole protocol files | 0 | `integer-between-final-whole-wrapper.log` and every per-file `integer-between-final-whole-*.log` |
| complete 20 strict PG18.6/180006 protocol files | 0 | `integer-between-final-strict-wrapper.log` and all per-file logs |
| two full adjacent INTEGER strict files | 0 | `integer-between-final-strict-integer-wrapper.log` |
| complete CLI file, three queries/new processes sharing its isolated data dir | 0 | `integer-between-final-cli.log` |

The 36 native files are the original complete 23-file BIT/adjacent type list,
all eight prepared/CASE/bound-routine controls, the three new input/type/cast
files, and full `expression_evaluator` and `aggregate_argument_expression`.
Original `typed_group_key` really completes here; it is not inferred from a
different cursor fixture. The 33 wire files retain all original 22 whole BIT
files, the five new files and six ordinary INTEGER/character/CASE adjacent
files. Strict reference uses its exact same SQL and strong rows/NULL/errors/
names/tags/OIDs, not an easier selected subset of a fixture. These finite
gates are not the repository-wide native/frontend/registered-wire suite.

The original immutable drivers also reach their full terminal counts:

| Driver | Actual terminal | Remaining exact differences |
| --- | --- | --- |
| 132 literal typing | 0 | 0 |
| 384 BIT text input | 1 | 23: 16 typed VARBIT/parser and 7 physical prefix scalar inputs, unchanged |
| 1296 BETWEEN input | 0 | 0; original 26 INTEGER input differences fixed |
| 250 parameter / 40 positions / 50 CAST parameter | 0 / 0 / 0 | 0 / 0 / 0 |
| writer12 | 1 | all original six demand/evaluation-count differences, unchanged |
| constant15 | 1 | three old differences: two NULL BIT(0) modifiers and `0 BETWEEN 1 AND 1/0` eager evaluation |
| domain-role8 | 1 | seven old catalog/type-name differences, unchanged |

Logs are `integer-between-original*-final.log`. The original wrapper merely
records each real exit status; its own exit 0 is not a pass for red matrices.
The untouched Source 104 baseline of constant15 was rerun and also actually
has the same three differences (`integer-between-originalconstant15-baseline-
root104.log`). The frozen demand candidate has two because it repairs the
arithmetic short-circuit; this tree does not contain that demand source and
does not misattribute its result. Postcommit clean-tree, repeated normal,
all-current receipts, source/header/flags donor proof and frozen byte equality
are audited in `integer-between-postcommit-audit.log`.

## Deliberately still open

This integer change does not import or amend `d14a008a` demand semantics. Its
old writer12 matrix still has six evaluation-count differences. BIT(0)/VARBIT(0)
modifier validation, VARBIT typed-literal/catalog identity and physical prefix
inputs are separate issues. No original matrix, SQL, deadline, result/OID
assertion, or old failure is relabeled as passed.
