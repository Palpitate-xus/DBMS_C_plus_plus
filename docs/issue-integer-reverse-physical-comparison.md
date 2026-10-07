# Integer literal-left comparison loses its physical column/index owner

## Reproduced cause and finite fix

`'1'=i` and the other ordering directions selected incorrect rows through
the single-real-table frontend, even though `i='1'` used the correct public
IndexScan. Actual EXPLAIN ANALYZE on the original receiver reproduced reverse
TableScan/zero rows versus forward IndexScan/one row. Early rejection of an
invalid left literal would not fix these valid-input consumers.

The actual parsed ColumnRef/UNKNOWN Literal pair is now oriented for that
same physical receiver only when the column belongs to the precise source
range/ordinal and its non-array descriptor is SMALLINT/INTEGER/BIGINT.
`<`/`>` and `<=`/`>=` are inverted; equality/not-equality are unchanged.
Proper identifier quoting remains intact. Other typed expressions, parameters,
arrays and non-integer families are not rewritten.

EXPLAIN derives its compact predicate from the genuinely bound comparison's
exact source owner, ordinal and original literal source coordinates. It keeps
the existing physical/index consumer after whole-query pure preparation.
It does not force an alternate TypedSource graph. An earlier implicit-cast
experiment that changed forward IndexScan into TypedSource was rejected;
its actual diagnostic logs remain in the artifact directory.

## Complete evidence

Permanent `tests/integer_reverse_comparison_protocol_e2e_test.py`: six
operators, both literal directions, empty/populated/NULL sources, PRIMARY and
secondary indexes, non-indexed BIGINT, quoted space-containing fields,
genuine Parse/Bind/Describe/Execute and four real executed JSON EXPLAIN plans.
The four index plans must actually return one row, and the candidate must
actually retain IndexScan. The final quoted-alias projection control is
preserved and depends on the separately documented projection issue.

| Complete invocation | Actual result |
| --- | --- |
| Strict owned PostgreSQL 18.6/version 180006/C/libc; `reverse-strict-reference18-whole.log` | 0; 122 controls |
| Exact original Root Source80 frozen donor; `reverse-root80-baseline-whole.log`, session 53406 | 1; original wrong reverse results retained |
| Final candidate, `candidate-final-complete-12-whole-wire.log`, session 37162 | 0; all 121 reverse controls plus all eleven whole neighbours passed |
| Final fresh native/stub set, `candidate-final-complete-11-native.log`, session 20405 | 0; all eleven passed |

Artifacts: `/tmp/dbms-empty-integer-preparation.megX23IB`; final production
SHA256 `fd92f1210c5114c175005180067f6ec79dbeef9bca746d7ccfc5519671eb4972`.
The count difference is the reference BEGIN control. Prior complete runs
that exposed quote-space/projection bugs remain failures, not prefix passes.
See the [integer issue](issue-integer-unknown-comparison-preparation.md) for
Source80 build receipts and current-Root ABI limitations. This is a finite
ordinary integer source repair, not global operator-family compatibility.
No push or Actions were run.
