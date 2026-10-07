# Accept the complete BIT textual input representation

BIT/VARBIT textual input accepted only bare binary digits. Genuine `b`/`B`
and `x`/`X` prefixes, including empty prefixed values, failed in casts,
implicit operator inputs, ARRAY constructors and real text-format network
parameters. For example, original `B'01'='b01'` must be true, while
`B'01'='x1'` must be false: hex input retains the full four-bit nibble.

The expression evaluator now shares its binary/hex digit decoder between
SQL B/X literals and textual input, while preserving their different input
syntax. A text value `B'01'` is not a SQL literal wrapper and still rejects.
Implicit BIT conversion uses the existing unconstrained VARBIT input route;
explicit BIT without a modifier still retains its length-one default.
Already typed BIT datums remain canonical binary data, not encoded input.
Ordinary TEXT bytes and typed TEXT/BIT operator rejection are unchanged.

The network text-parameter entry also uses that actual prepared input
codec before emitting its canonical BIT literal. Binary parameters, binary
padding, SQL substitution architecture and result descriptor behavior are
not changed by this issue. The actual PostgreSQL input implementation is
[PostgreSQL 18.6 varbit.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varbit.c);
all regression expectations were checked against the live strict 180006
reference, not inferred solely from that source.

## Actual complete evidence

Artifacts are under `/tmp/dbms-bit-text-input.MoHEgLcd/`. The preceding
1c4a3ba7 tree, source, all58 receipts and SHAfa7947d3 frozen Main under
`/tmp/dbms-bit-unknown-lists.hOGDEeJY/` remain unchanged.

- `bit-text-input-original384-baseline.log`, shell 9738 exit 1: full 384
  strict-reference/candidate controls complete with 177 differences. This
  deliberately includes independently open parser and stored-predicate
  controls; it is not replaced by an easier green fixture below.
- Permanent `bit_text_input_protocol_e2e_test.py` retains all 404 scalar
  cast, implicit comparison, ARRAY/ANY, mixed-list admission, real TEXT,
  empty, NULL, hex width, error and OID checks. Strict whole404 direct exit
  0 in `bit-text-input-strict-reference404-v1.log`; exact 1c baseline whole404
  shell 75060 exits 1 with 160 failures; expression-only candidate shell
  41127 exits 0. Tests do not claim to repair the separate BIT descriptors.
- Permanent `bit_text_parameter_protocol_e2e_test.py` sends actual owned
  Parse/Bind/Describe/Execute/Sync packets for both BIT OID1560 and VARBIT
  OID1562, every text spelling, invalid input and SQL NULL: 42 full controls.
  Strict whole42 direct exits 0; expression-only frozen Main baseline shell
  18492 exits 1 with 20 failures, all controls executed. Each owned statement
  is closed; no reference relation or shared prepared statement is changed.
  The initial assertion that prematurely expected RowDescription on a red
  baseline was corrected to collect actual input errors, without changing
  SQL, successful field/row checks, expected states or deadlines; its failed
  initial log remains `bit-text-parameter-baseline-whole42-v1.log`.
- `bit-text-input-native-baseline-final-v4.log`, shell 76611 exit 1: exact
  final permanent native driver links all57 individually verified production
  1c modules. Every semantic pair and helper/cursor/parameter path is attempted;
  baseline records 3354 actual checks /1750 failures because failed input
  conversions cannot produce the subsequent value/hash assertions. The
  candidate executes all 5344 strong assertions. Earlier compile-only test
  mistakes and the earlier abort-on-helper-error logs are retained separately,
  not mislabeled as a complete baseline run.
- Final two-CPP normal build and twenty complete native files pass in
  `bit-text-input-candidate-normal-native-v3.log`, shell 57738 exit 0.
  Normal production has genuinely fresh ExprEvaluator/NetworkServer objects
  and 56 individually source/header/flags/donor58-receipt/object-byte-proved
  normal O2 donors. All current58 receipts, build stamp and immediate repeat
  pass. Public headers/layout are unchanged; this is not fresh58.
- Final permanent driver error collection and all twenty complete native
  files also pass in `bit-text-input-candidate-normal-native-v4.log`, shell
  38436 exit 0: all5344 assertions, previous3387/235/272/78/16/8, the entire
  unchanged original11 set, real query binding and current-module group512.
  Group512 links production modules, not Main. The normal/frozen SHA is exact.
- Final `bit-text-input-final-v3-wrapper.log`, shell 19379 exit 0: seventeen
  whole wire files, new404/new42, previous128/288/admission16/unknown33/
  original91/list56/index156/routine16/quoted26 plus unchanged original bit,
  stored21/comparison32/ARRAY6/bounds/quantified-demand84 and complete CLI.
  CLI uses three processes sharing a persistent directory, not one session.
- `bit-text-input-final-strict-wrapper.log`, shell 90465 exit 0: ten whole
  strict-reference files, new404/new42 and the previous eight complete
  mixed-input/owner/pre-scan controls. Every connection verifies 180006.
- Final original132 and original384 use the exact frozen two-CPP Main:
  shell 32902 reports actual exit 1 for each, verifies all132/all384 rows
  were executed, and retains exactly three /49 failures in
  `bit-text-input-original132-final-v3.log` and
  `bit-text-input-original384-final-v3.log`. After normalizing only the owned
  schema name, all final49 red SQL are a subset of preceding177 red SQL;
  no new red SQL is introduced. Original SQL/rows/NULL/OIDs/errors remain.

Final frozen normal Main is `dbms_main.text-input-v3.frozen`, SHA256
`3939fb28bff97b705399f992ee3ffec175e9e1175c945407e516ace96d2b9003`.

## Remaining independent issues

Original132 now retains only stored INTEGER `i=''`, `i<>''`, `i<'' ORDER BY id`
(PG22P02, candidate SELECT0), assigned to the separate INTEGER input repair.
Full384 still retains VARBIT typed-literal syntax (42703), compact stored
UNKNOWN scalar inputs (missing canonical conversion/error), and grammar
BETWEEN UNKNOWN coercion (42883). They are not marked repaired here.
BIT numeric/operator signatures, modifier limits/error priority, binary
padding, type descriptors, original273 and TYPE-11/checklist completion remain
open. ROOT integration must preserve its current enum/AST binding ownership,
hash NULLs, BIT constructor, Boolean BETWEEN metadata and new Window fields;
its newer public headers require a genuinely fresh58 integration build.
