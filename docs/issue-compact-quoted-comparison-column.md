# Compact comparison receiver splits whitespace inside a quoted column

## Reproduced cause and finite fix

The real compact predicate `="i I" '1'` was split at the first space after
the operator, yielding column `"i` and RHS `I" '1'`. This is a physical
column delimiter error, not a string input/type inference problem.
`StorageEngine::parseConditions` now finds the first whitespace outside
the existing SQL protected-byte mask. Literal decoding, RHS type ownership,
computed-expression handling and comparison operators are unchanged.

The permanent native test checks spaces, embedded quotes, case, dots and
escaped/space-containing data. It then creates an actual table and secondary
index, executes the public planner's actual IndexScan and heap scan, and
checks their results with real SQL NULL rows.

## Evidence

Artifacts: `/tmp/dbms-empty-integer-preparation.megX23IB`.

| Complete invocation | Actual result |
| --- | --- |
| Exact Root Source80 donor native baseline, `compact-quoted-root80-baseline-native.log`, session 97380 | Body 134; printed the incorrect column/RHS before its assertion |
| Final fresh driver/stub, `candidate-final-complete-11-native.log`, session 20405 | 0; this native and all ten complete neighbours passed |
| Final entire protocol set, `candidate-final-complete-12-whole-wire.log`, session 37162 | 0; all twelve original complete wrappers passed, including ordinary quoted INTEGER/TEXT/NULL consumers |

The final production binary is the receipt-checked private Source80 build,
SHA256 `fd92f1210c5114c175005180067f6ec79dbeef9bca746d7ccfc5519671eb4972`.
The [integer preparation issue](issue-integer-unknown-comparison-preparation.md)
records the build's exact donor/ABI scope and preserved unsuccessful runs.
This finite receiver repair does not claim global quoted SQL, BIT, or the
root's later Window-header ABI is complete. No push or Actions were run.
