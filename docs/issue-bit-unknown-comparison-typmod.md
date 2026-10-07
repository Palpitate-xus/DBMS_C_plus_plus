# BIT comparison input conversion must be unconstrained

## Root cause and bounded change

`ExprEvaluator::coerceComparison` used the explicit scalar `::bit` cast when
a resolved comparison required BIT from an UNKNOWN datum. Explicit BIT
defaults to `BIT(1)`; operator input conversion has no such length modifier.
Consequently `01` became `0`, and an empty input became `0`. The wrong
converted values also reached the real quantified SQL child/hash consumers.

The six-line production change validates through the existing unconstrained
VARBIT codec and then retains the resolved BIT type. It does not change
explicit scalar casts, operator resolution, NULL semantics, public headers,
the storage format, Main, wire descriptors or server timing/deadlines.

The permanent native fixture covers the public resolution, conversion,
comparison and hash APIs in both operand positions. It also uses genuine
UNKNOWN `QueryBindingDatum` / `ParameterExpr` sites with
`StorageEngine::prepareBoundQuery`, the actual public planner/cursor, ARRAY
comparisons and quantified SQL child cursors. It does not substitute SQL
parameter strings, copy an evaluator, or invoke an operand to infer a type.

The registered protocol fixture contains 33 pure SELECT controls spanning
UNKNOWN input, leading zeroes, empty values, invalid binary input, NULLs,
and genuine quantified SQL child queries. Successful rows retain BOOLEAN
OID 16, size 1, typmod -1, format 0, the `value` label and `SELECT 1` tag.
Invalid binary data must report `22P02`; values/errors are not normalized.

## Actual evidence

Artifacts are owned by `/tmp/dbms-bit-unknown-comparison.Uy63kh/`:

- `strict-reference18-final33-complete.log`: all 33 permanent statements
  against the actual PostgreSQL 18.6 instance, strict version 180006, exit 0.
- `baseline-wire-final33-complete.log`: the exact same final 33 controls
  against frozen 90c6ef61 production; 12 failures, shell session 93454,
  authoritative terminal exit 1.
- `baseline-normal-native.log`: exact final native driver, 3387 controls,
  742 failures, shell session 14466, authoritative terminal exit 1. All 58
  normal 90c6ef61 donor objects are source/header/flags/receipt/byte proved;
  the unchanged production is relinked and its repeat/stamp audited.
- The donor is `/tmp/dbms-bit-varbit-semantics.JYF0FS/repo`, exactly
  `90c6ef617509f765abe50f23638deb29e6a3b77e`, with production frozen at
  `/tmp/dbms-bit-varbit-semantics.JYF0FS/dbms_main.comparison-literal-group.frozen`,
  SHA-256 `3513ca8f908815d1ac9599e69ab96d76e288d9e4ef3a2c5ba4e712862dc3bc5f`.
- `build-normal-and-native.sh` proves that donor and its complete original
  58 object receipts first. It separately verifies current relative header
  hashes, manifest, flags, matching CPP source and donor object bytes before
  reusing unchanged normal objects. A changed ExprEvaluator CPP is compiled
  fresh; current all-58 receipts, build stamp and immediate repeat are audited.
  These checks are **not** a fresh-58 or an all-SAN build claim.
- `candidate-normal-native.log`: one fresh normal-O2 ExprEvaluator and 57
  exact-input-proved normal donors, all current 58 receipts/stamp/repeat
  verified. Shell session 54487 terminated with exit 0. The exact new
  native fixture ran all 3387 controls with zero failures; the unchanged
  `bit_comparison_value`, `bit_array_constructor_value`, `bit`,
  `bit_stored_literal`, `bit_stored_literal_api_data`,
  `quantified_query_execution`, `text_comparison_type`,
  `name_comparison_type`, and `interval_comparison_value` full drivers
  also passed: ten complete native drivers, zero failures.
- `candidate-whole-wrapper.log`: shell session 33129 terminated with exit
  0. Eight complete files passed, with independent unmodified-result logs
  `candidate-whole-<fixture>.log`: the new 33-statement protocol fixture,
  original `bit_protocol`, original 32-statement `bit_comparison`, original
  six-statement `bit_array_constructor`, original 21-statement
  `bit_stored_literal`, original `explicit_array_bounds`, original
  84-control `quantified_query_demand`, and the actual three-control
  `bit_group_comparison_cli`. No SQL, assertion or deadline in the existing
  project controls was changed.
- Candidate and frozen production
  `dbms_main.unknown-comparison.frozen` compare byte-for-byte, SHA-256
  `0ca27147bae7a1ec2d82827c07e3d33cb406677b514aa8fcc108d1591a5b0e42`.
  The local discovery count is 689 native-auto files plus the actual
  frontend, 352 registered protocol/E2E files and 58 production TUs. These
  are inventory counts, not a claim that all discovered tests were run.

The initial extra wire control `SELECT B'01'::bit AS value` incorrectly
expected typmod -1; actual PostgreSQL reported typmod 1. Its first reference
failure is preserved in `strict-reference18-v1.log`. Before final baselines,
that extra control was replaced with the BOOLEAN comparison
`(B'01'::bit)=B'0'`, preserving its explicit BIT(1) value check without
conflating this input-conversion root with the separately tracked descriptor
issue. The native fixture still directly checks that explicit result `0`.
Earlier short reference/baseline runs are retained separately. Existing
project BIT fixtures and their original SQL/assertions are unchanged.

## Scope not closed

This issue is not completion of TYPE-11 or of the original 273-file gate.
It does not fix BIT text-input b/x prefixes, BIT/VARBIT function return
signatures, scalar/wire typmods, binary parameter padding, modifier limits,
ordinary mixed-type SQL predicate adapters, or security/TDE work.
The original broad checklist and family-wide acceptance gates remain open.
