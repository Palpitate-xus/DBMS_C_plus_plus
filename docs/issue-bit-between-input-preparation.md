# Prepare the real BIT BETWEEN comparison inputs

The grammar retains BETWEEN as one three-argument FunctionCallExpr. Existing
binding checked adjacent BIT comparison signatures, but did not convert their
genuine UNKNOWN inputs. Thus `B'01' BETWEEN 'b01' AND 'x1'` could fail with
42883 instead of returning the actual lexical BIT comparison result. Invalid
input could also reach a stored writer before its 22P02 preparation error.

The two comparisons now prepare their actual input conversions in order.
Pure UNKNOWN literals are validated through the real unconstrained BIT input
codec; parameters, row values, queries and routines are not executed to infer
their inputs. A literal left operand has two independently typed occurrences:
`'01' BETWEEN B'01' AND 'b01'` compares BIT in its first pair and TEXT in its
second, and returns true. Assigning one shared BIT cast would change that
result. A genuine shared ParameterExpr instead retains the first pair's
inference, so `$1 BETWEEN NULL AND B'01'` rejects text <= bit with 42883.

Two existing adapters also lacked this input boundary. Table projection's
legacy boolean splitter treated the BETWEEN AND as a separate boolean arm,
and empty/receiver-zero projections skipped preparation. Real grammar range
ASTs now use the complete typed evaluator, and validate pure inputs before
provider/index scans and receiver limits. The finite ordinary network owner
prepares one fromless BIT range's actual parameter sites before publishing
Parse, records ordered inferred OIDs, and preserves typed Bind values. It does
not claim parameter inference for arbitrary statements or relation-backed
prepared ranges. Describe uses the bound static result and executes no writer.

No public header/layout, Main source, original SQL, assertions or deadline was
changed. ROOT's newer enum/binding/window public layout and separate Boolean
BETWEEN result-typing hunk must be retained when integrating this private
source delta. These old-ABI objects are not donors for ROOT's current ABI.

## Actual evidence

All artifacts are under `/tmp/dbms-bit-between-input.24ukiice/`. Base e53 and
the preceding 1c/5bb trees, frozen binaries, tests and build receipts remain
unchanged. The intermediate normal binary is `dbms_main.between-input-v3.frozen`, SHA256
`bece507f3c602f07d5b7f07f084f6c55c7cf872a8d76db607f6d9658f9694c87`.

- `between-inputs-original1296-baseline-v1.log`, actual shell48436 exit 1:
  all1296 original scalar controls collected, 666 differences. The exact
  unchanged driver finishes all1296 against the frozen candidate, shell30004
  exit 1, in `between-inputs-original1296-final-v3.log`: 26 remain, all ordinary
  non-BIT INTEGER UNKNOWN input, not a green subset or a family approval.
- New permanent `bit_between_input_protocol_e2e_test.py` retains all1192
  strong rows/NULL/SQLSTATE/OID/name/tag controls: all904 BIT-containing scalar
  triples from that original matrix, and 288 table projection/WHERE/index/OR
  controls across BIT/VARBIT populated, empty and physical NULL inputs.
  Real strict PG18.6/180006 shell29098 exits 0. Exact frozen e53 baseline
  shell48675 exits 1, all1192 /792 differences. The intermediate candidate
  shell83011 exits 1, all1192 /44 table-projection failures retained. Final
  frozen candidate shell38062 exits 0, all1192 unchanged controls.
- New permanent real Parse/Bind/Describe/Execute/Sync parameter fixture checks
  all250 combinations: ten original statements, five actual OIDs, five
  values including invalid and NULL. Its first strict run found five incorrect
  new expected error states: INTEGER converts the first `'b0'` literal to
  22P02 before resolving the later BIT comparison. The SQL/assertions/deadlines
  were preserved, expectations corrected to that actual PG result, and the
  complete v2 strict run exits 0. Exact frozen e53 baseline shell77528 exits 1,
  all250 /383 phase failures, in `between-parameter-wire250-baseline-v2.log`.
  Final candidate shell4924 exits 0, all250 controls and strong Boolean field
  descriptor `(16,1,-1,0)` checks.
- Original `probe_between_parameters.py` independently compares every actual
  phase/parameter OID/row descriptor/row/state across its unchanged250 controls.
  Frozen e53 shell7322 exits 1, all250 mismatches. Intermediate literal-only
  candidate also mismatched all250 phases, and introduced18 additional final
  SQLSTATE mismatches (84 before /102 after); it was not READY. Final frozen
  candidate shell23639 exits 0, all250 exact comparisons, in
  `between-parameters-original250-candidate-v3.log`.
- Permanent native fixture has all2008 complete controls: 904 scalar triples
  at both genuine helper and bound cursor owners; ordered real parameters;
  actual table projections at receiver0/1; and real volatile writer input
  admission. Early baseline attempts aborted on existing unstructured errors
  and remain in v4/v5 logs; they are not full baseline proofs. The final
  same-SQL/strong-assertion driver collects those errors as failures instead
  of aborting: shell55087 exits 1, all2008 /1421 failures, against every exact
  input-proved frozen e53 production module, in
  `between-input-native-complete-baseline-v6.log`.
- `between-input-final-normal-native-v5.log`, shell53398 exits 0: all23
  complete native files, new2008 plus all preceding22 controls, including
  full5344/3387/235/272/78/16/8, real binding and module-linked group512.
  A final identical-controls run recompiles the final exception-collecting
  fixture; its terminal status is recorded below, not inferred from v5.
- Normal objects use five current changed CPP inputs and 53 exact e53 normal
  O2 donors, each verified source/header/compiler-flags/manifest/donor58
  receipts/object bytes. All current58 receipts, build stamp and immediate
  repeat are verified. This is not fresh58. Intermediate normal shell83636
  exits 1 after detecting source edits during compilation; this honest red is
  retained and superseded by actual clean final normal runs, not relabeled.
- `between-input-final-whole-wrapper.log`, shell3350 exits 0: all20 whole wire
  fixtures and complete three-query CLI (three independent processes sharing
  one data directory), including original BIT/bounds/quantified demand.
  `between-input-final-strict-wrapper.log`, shell80739 exits 0: all13 whole
  strict-reference fixtures, each verifying actual180006 with owned isolated
  objects and unchanged deadlines.
- Original132 shell11120 exits 1, all132 collected /three original INTEGER
  empty-string predicate errors still open. Original384 shell79338 exits 1,
  all384 collected /33 differences, reduced from49. After normalizing only the
  owned random schema name, final33 are a strict subset of original49: all16
  BETWEEN differences resolved, no new red SQL. Exact logs are
  `between-input-original132-final-v3.log` and
  `between-input-original384-final-v3.log`.

## Final candidate and introduced-regression audit

Final frozen normal is `dbms_main.between-input-v7.frozen`, SHA256
`337e6193171e3fa942b811b7178c032488580169043e7f62fb48bdea1498aec4`.
It is the same five-CPP finite input-owner issue, with no header/layout change.
The preceding v3/v4/v5/v6 frozen binaries and all red evidence remain separate.

- Actual private review discovered bound cells use occurrence order, not SQL
  `$n` positions. The new unchanged paired40 diagnostic against v3 finishes
  shell48606 exit 1 /30 differences, including unused unknown PG42P18.
  Mapping now follows the real ParameterExpr's retained source/use provenance,
  not its internal cell index. Permanent40 checks every Parse/Bind/Describe
  phase and both parameter OIDs: strict reference exits 0, frozen v3 baseline
  shell86726 exits 1 /35 phase failures. Corrected v4 shell68918 exits 0;
  the original paired40 shell21284 also exits 0. None of these partial stages
  was announced READY while its follow-up checks remained red.
- New unchanged paired50 CAST diagnostic against v4 finishes shell68393 exit
  1 /45 differences: the unresolved-parameter guard rejected normal explicit
  BIT/VARBIT input casts. True inner cast input owners are now inferred in
  occurrence order after comparison binding; a global cast-first pass would
  wrongly override the first TEXT context of `$1 BETWEEN NULL AND $1::varbit`.
  v5 finishes shell48121 exit 1 /one remaining real phase difference: TEXT
  `'2'` cast to VARBIT raised 22P02 after RowDescription, while PG rejects
  during Bind with no RowDescription. This real failure was retained.
- A finite structural primitive-value proof now admits only a complete
  single-range fromless AST of genuine literals and unqualified built-in
  BIT/VARBIT/TEXT/Boolean/integer casts, with no other statement expressions.
  Functions, parameters, columns, operators, SQL children, quoted/custom/
  qualified cast types and all other receivers do not pass this proof. It
  executes the real primitive codec during Bind planning, not a static
  signature surrogate, so no stored writer is folded or called.
- New permanent90 verifies the entire original50 plus40 shared CAST/context
  combinations: default BIT(1) versus explicit BIT(2), VARBIT, nested TEXT and
  INTEGER casts, both comparison positions, repeated shared parameters,
  first-owner TEXT rejection, NULL, invalid input and exact parameter/result
  OIDs. Strict reference exits 0. First v4 attempt aborted while its test
  reader expected a missing RowDescription; that partial log is retained.
  The same SQL/strong assertions now collect unexpected phase errors without
  aborting: full v4 baseline shell47634 exits 1 /55 phase failures across all90.
  Exact final normal shell59436 exits 0 /all90. Permanent250/40/90 additionally
  reject any RowDescription or DataRow preceding their Bind error.
- Final `between-input-final-normal-native-v7.log`, shell15337 exits 0:
  all23 complete native files and current2008 new assertions, old22 retained.
  Normal and repeat verify every58 current object receipt/build stamp, plus
  every53 exact e53 source/header/flags/manifest/donor58-receipt/object-byte
  donor. Changed5 CPP objects are current normal O2 production objects,
  assembled through actual staged normal compilations, not fresh58.
- Final `between-input-final-v7-whole-wrapper.log`, shell67699 exits 0:
  all22 whole wire fixtures and complete three-process CLI; includes unchanged
  1192,250,40,90 and every preceding18 whole wire control file.
  `between-input-final-v7-strict-wrapper.log`, shell69159 exits 0, all15 whole
  strict180006 reference files, with original SQL/assertions/deadlines and
  isolated owned objects unchanged.
- `between-input-original-seven-matrices-v7-wrapper.log`, shell15196 exits 0
  only for honest completion/status checks, not family success: each actual
  process completes its entire original matrix. Actual132=1 /three errors;
  384=1 /33 errors; 1296=1 /26 errors; 250=0; 40=0; 50=0; writer12=1 /six errors.
  Exact logs are `between-input-{original132,original384,original1296,
  original250,original40,original50,writer12}-final-v7.log`. The final384 audit
  again shows33 are a subset of original49, sixteen resolved and no new red
  SQL, normalizing only the owned random schema name.

## Remaining independent issues

The unchanged original writer12 matrix still finishes with actual exit 1:
ten differences before, six after, in `between-writer-original12-candidate-v3.log`
(shell9973). Illegal pure input now rejects before any writer call, matching
PG22P02 with zero nontransactional sequence effects. Existing demand/repetition
semantics are still wrong: PG repeats an admitted left writer twice when the
second pair is demanded, once when the first pair determines the result; it
does not execute an unused upper writer. The current evaluator eagerly runs
one left/one upper. Its six red SQL are deliberately preserved for a separate
root-cause fix, not described as universally "evaluate once":

- `writer(B'01') BETWEEN B'01' AND B'01'`: PG true /2 calls, candidate true /1.
- `writer(NULL::varbit) BETWEEN B'01' AND B'11'`: PG NULL /2, candidate NULL /1.
- `B'00' BETWEEN B'01' AND writer(B'11')`: PG false /0, candidate false /1.
- `B'00' NOT BETWEEN B'01' AND writer(B'11')`: PG true /0, candidate true /1.
- `writer(B'01') BETWEEN '01' AND '01'`: PG true /2, candidate true /1.
- `writer(NULL::varbit) BETWEEN '01' AND '11'`: PG NULL /2, candidate NULL /1.

The 26 original1296 non-BIT INTEGER conversions, 33 original384 VARBIT typed
literal/parser and compact UNKNOWN predicate differences, three original132
INTEGER errors, broader prepared-range owners, numeric/operator/typmod/binary
descriptor gaps and original273 TYPE-11 coverage remain open. This finite
input-admission issue does not mark BETWEEN, TYPE-11 or the review complete.
