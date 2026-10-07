# Parser-owned BETWEEN must have Boolean metadata

## Actual database-use failure

The original protocol regression executes
`UPDATE pred_t SET name = 'y' WHERE id BETWEEN 2 AND 3` and
`DELETE FROM pred_t WHERE id NOT BETWEEN 2 AND 3`. Both currently report
42804 (`argument of WHERE must be type boolean`) instead of changing the
expected rows. All prior original SELECT/NOT LIKE/range statements succeed.

Root independently executed all sixteen original SQL controls and printed
their actual results against exact Source77 production and strict PostgreSQL
18.6, version 180006. The engine leaves ids 2/3 at `x` and keeps id 1;
the reference updates ids 2/3 to `y` and deletes id 1. These are genuine
statement errors, not an old snapshot, socket timeout or difference in
numeric values.

The parser represents ordinary BETWEEN/NOT BETWEEN as schema-free
three-argument `FunctionCallExpr` grammar nodes. Runtime evaluation already
returns Boolean and has existing three-valued-logic controls. Static
`inferAstResultType` recognized the analogous pattern-ESCAPE grammar nodes
but omitted these two range nodes. Bound metadata followed an operand type;
unbound inference returned TEXT. Genuine DML predicate validation therefore
rejected a valid Boolean predicate.

## Bounded repair

Add BETWEEN and NOT BETWEEN to that existing parser-owned Boolean metadata
branch. There is no change to the evaluator, operand execution, SQL predicate
validation, numeric comparison, mutation/transaction ownership, public
headers or storage formats. Integer/non-Boolean WHERE predicates must still
reject 42804 without effects. The new protocol fixture is registered in the
real original runner.

`between_predicate_type_test.cpp` checks actual parser node shape, static
types, resolved projection metadata, execution-owned copies, bound integer/
TEXT/NULL predicates and empty UPDATE/DELETE preparation without sequence
effects. Its physical INT fixture uses the factory's actual scale code 2,
and verifies the prepared source's canonical integer type before supplying
integer cells; the genuine source-cell type guard is not relaxed.

`between_predicate_type_protocol_e2e_test.py` keeps the original SQL, values,
row assertions and actual DML command tags. Additional controls cover Boolean
OID16, SQL NULL versus false, integer/TEXT/BIGINT values, empty-source UPDATE/
DELETE, sequence effects and unchanged 42804 rejection. Strict reference
creation uses a unique owned schema inside BEGIN/ROLLBACK; expected errors
use a savepoint. No shared reference database/schema is dropped.

## Actual evidence

Artifacts: `/tmp/dbms-between-predicate-type.buAkBPKj/`; original complete
diagnostics are also retained under `/tmp/dbms-ddl-wal-cursor.ZMu3A0ka/`.

- Exact Source77 original sixteen-statement diagnostic and unchanged strict
  reference diagnostic both fully execute; their process exit0 is diagnostic
  execution, **not** a matching-results PASS.
- Baseline complete new protocol, session78472, actually exits1 and records
  both real populated and empty-source 42804 failures. The strict180006 same
  full permanent protocol actually exits0.
- Baseline native29871 actually ends1/body134 at the first exact Boolean
  inference assertion (`2 BETWEEN 1 AND 3` is wrongly TEXT).
- Private normal3324 actually exits0: fresh sole ExprHelper plus57 exact
  source/header/manifest/actual-flag/original58-receipt/object-byte-proved
  current84e normal donors. All current58 receipts/stamp/repeat/frozen pass;
  this is not a fresh-all58 or sanitizer build. Production SHA-256:
  `a3ce263cd307a3701dc906a8ec511a7c0a893742e740aeadfc028836a921aab7`.
- First full11-native22297 ends1: ten original drivers pass, while the new
  author's incorrect factory scale4 creates BIGINT but supplied integer
  cells. The actual XX000 failure is retained as a fixture-author error,
  not hidden as a database defect. Correcting only that INT seed and adding
  the exact source-type assertion yields full11-native70823 exit0.
- Complete new protocol3044 actually exits0 on default disk, retaining the
  same SQL/values/NULLs/error states/command tags as the strict reference.
- The complete unchanged original `postgres_protocol_test.py`27151 actually
  exits1 at its original prepared ALTER ADD-column socket timeout (line1197),
  before reaching the original range section. Its full log remains
  `candidate-original-complete-postgres-protocol-default-disk.log`; it is
  not relabeled whole-protocol green or used as proof of the later section.

This private tree starts from Source77, not the newer public ENUM AST epoch.
Root must verify the actual newer combined inputs, compile the changed
ExprHelper freshly against those headers, prove the other57 normal donors
and rerun complete consumers before importing it into master.

## Not closed

This repairs the ordinary range node's Boolean return metadata, not every
BETWEEN input/operator preparation rule, enum-rank range consumer, symmetric
range grammar, volatile rewrite semantics, general DML feature or the entire
273-item audit. Genuine remaining original failures stay open. No push,
Actions activation or deferred security/TDE work.
