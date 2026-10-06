# Structured unary overflow errors

The three unary overflow creation sites now throw `DbError("22003", ...)`
instead of a generic C++ exception with SQLSTATE text embedded in its message.
Integer range behavior, typed NULLs and valid adjacent values are unchanged.
The existing lower-level MONEY evaluator branch also retains structured range
errors; this is not a claim that PostgreSQL has a unary MONEY SQL operator.

## Actual failures and scope

The initial fresh native baseline recorded eight failures: direct evaluation
and checked plans both exposed raw exceptions for the three integer widths
and the existing MONEY branch. Checked execution rethrew the unknown C++
exception, rather than returning a checked SQL error. The final native fixture
checks all three legitimate integer SQL operators and the lower-level MONEY
branch separately, without asserting nonexistent PostgreSQL SQL support.

An early new reference fixture incorrectly expected MONEY unary minus to be
22003. Actual strict PostgreSQL 18.6 disproved that draft with 42883, because
the operator does not exist. The failed reference log is retained and the
missing pure unary operator-signature check remains a separate active defect.
No old assertion or original test was weakened to hide it.

## Verification

Artifacts: `/tmp/dbms-unary-overflow-sqlstate.h2apEHZG`.

- `unary-state-native-baseline.log`: actual raw-exception baseline, exit 1.
- `unary-state-native-candidate.log`: initial nine matching native tests, exit 0.
- `unary-state-native-candidate-v2.log`: final native fixture and the same nine
  matching native entries, exit 0 (session 11491). Arithmetic/type inference,
  checked SQLSTATEs, CASE, NAME and interval neighbors remain unchanged.
- `unary-state-reference18.log`: the contradicted MONEY draft, exit 1.
- `unary-state-reference18-integer.log`: final strict server_version_num 180006
  reference, exit 0.
- `unary-state-wire-candidate-v2.log`: final candidate integer protocol matrix,
  exit 0 (session 43933): exact 22003, NULL OIDs 21/23/20, safe adjacent values,
  no completion on error, failed transaction 25P02 and savepoint recovery.
- The first wire-launch helper failed before SQL because its binary environment
  override was not supported. That failure is retained in the unsuffixed wire
  log; the corrected helper copies the exact frozen binary into its own tree.

The final candidate SHA256 is
`7db8fd394622b99130d3b584d3eb6a04d15f8d45794b5586c6da33dcd14e28bc`.
Only ExprEvaluator.cpp is freshly compiled with normal O2 flags. All other
57 production source bytes, every header and all donor signatures are checked
against immutable source 4f23f997, whose complete 58-TU normal build passed.
Native stubs and each test are fresh. Later ROOT public-layout changes require
a new matching all-58 build; this artifact is not current ROOT runtime proof,
a full registered-suite pass or closure of all unary/operator families.
