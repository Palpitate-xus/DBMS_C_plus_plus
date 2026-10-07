# Ordinary scalar UNION ALL uses its retained typed execution root

The independent Append API/body-runtime repair passed the complete old
37-query bound-DML diagnostic. Its new complete 21-query protocol matrix still
failed at ordinary top-level array/CASE UNIONs and at a left writer executing
before a right branch's constant-planning error. These original SQL, row,
OID, error, rollback and cumulative-sequence expectations are unchanged.

The ordinary entry now recognizes genuine UNION ALL AST structure and supported
scalar/source roles through a pure metadata shape probe. It prepares the whole
query and passes its actual owned AST to `PreparedWithDmlRuntime::runRead`.
The resulting Append consumes the root's compiled carrier, not a discarded
preflight copy or SQL reconstructed from a SELECT body. Only this actual root
enables constant planning; branch and ordinary-child defaults remain false.

The shape probe uses the actual engine and session database. V1's missing
`setCurrentDB` caused the stored VOLATILE writer to be misclassified and remain
on the old route; its extra irreversible call was exposed by the unchanged
final sequence sentinel (37 rather than 36). That failed log remains retained.
V2 fixes the real metadata context instead of invoking a function or examining
its result. Aggregate/window/backend-local SRF and unmodeled source roles
retain their existing entry. The ordinary entry also retains the existing
set ORDER/LIMIT path, because the current parser does not yet distinguish
global versus branch clause ownership. This is not a claim to repair that
independent parser/execution contract.

## Final matching proof

Private root `/tmp/dbms-bound-dml-cursor.rTuMF7gk`:

- `union-all-reference18-corrected.log`: whole 21 controls pass strict
  PostgreSQL 18.6 (`180006`, isolated core profile). All OIDs, NULL/empty/text
  NULL values, true child demand, two distinct scalar sites, correlation,
  errors, rollback and every cumulative sequence sentinel are checked.
- `union-all-baseline.log`: matching old b3 binary's full red matrix.
- `candidate-union-v1/whole21.wire.log`: API candidate's full remaining red
  matrix, not relabeled green.
- `candidate-union-consumer/whole21.wire.log`: V1 actual extra writer call.
- `candidate-union-consumer-v2`: V2 fresh main object; audited exact unchanged
  57 objects/headers/flags/stubs from the wholly fresh 58-object Append epoch.
  Build handle 23668 and serial protocol handle 30166 are terminal 0.

Final SHA256:
`b5e97de53f0157d60e4d576f22f138bdd99e28c2b5a12398da3d3c897ab6d2ed`.
The complete original 21-query matrix, original 37-query diagnostic and nine
adjacent scripts all pass (qualification planning, ordinary Q DML, physical
child restart, primary/multisource WITH DML, Q demand, PL binding, prepared
root planning, original structured set operations). The unchanged nine
matching native tests from the Append epoch already pass; this main-only
consumer does not change native-core objects or count repeated native runs as
new distinct tests. Post-gate source/header/object audits are retained.

No public API/header/layout or TU changes in this consumer commit. ROOT's
newer FunctionCall `resolvedResultType` ABI is not this private epoch; combined
integration must preserve that field in existing clones and fully rebuild
all 58 objects. Ordinary set Parse/Describe, other set operators, clause
ownership, general SRF branches, domain/user-cast rules and captured logical
producer correlation remain open; whole query/DML families are not complete.
