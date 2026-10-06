# PL query result destination

The preferred prepared-statement host reported a successful SELECT or DML
RETURNING descriptor, but the interpreter ignored its result when there was
no procedural destination. PostgreSQL reports `42601`, including a query
that returns zero rows. PERFORM must remain a valid explicit discard.

The interpreter retains PERFORM's discard intent in its private statement
node. After the host executes successfully, a nonzero column count without
INTO/PERFORM reports `42601`. Execution errors retain their original SQLSTATE.
The guard is deliberately after execution: a nextval projection executes
once even when the missing-destination error follows. SELECT INTO retains
its existing earlier typed/nullable/cardinality path.

Private branch `/tmp/dbms-plpgsql-destination.iX4pFjLl/repo`, parent
`7f51f576`; only the utility translation unit changes, with no public API or
header/layout change. Matching immutable 58-object development basis, fresh
utility (`-O0` after shared flags), and native tests compiled with `-O2`.

Actual evidence:

- Native old `35089`: plain SELECT incorrectly returned success instead of
  `42601`, `destination.native.baseline.V2.log`.
- Wire old `63707`: five real SQLSTATE assertions failed, retained in
  `destination.baseline.log`. Initial native prototype `7313`/`68109` did not
  compile because the test omitted the existing nullParams argument; these
  are not claimed as runtime reds.
- Official PostgreSQL 18.6 reference: strict `180006` gate, identical eight
  functions and sequence/savepoint controls, `destination.reference18.log`,
  terminal 0. Older PostgreSQL 17 runs are not relabeled as this oracle.
- Fresh native `12512` and full protocol `29635`: terminal 0, including
  plain/empty SELECT, RETURNING, unknown function, PERFORM, INTO, exactly one
  sequence effect and no inserted rows.
- Seven adjacent native `98065` and three adjacent protocol `59762`: terminal
  0; SELECT INTO, query binding and stored-function atomicity expectations
  unchanged. No old runaway fixture or full protocol suite was started.
- Fresh utility ASan/UBSan plus matching instrumented parser/binder/DML and
  stubs: three native tests `57392`, terminal 0. Other 53 non-main objects are
  matching uninstrumented development objects; leak detection disabled.
- `verified/` retains exact source/header and binary signature audits.

The deprecated execStmt callback has no result descriptor; its compatibility
fallback is unchanged, not claimed to enforce this typed boundary. FOUND,
ROW_COUNT, procedural DML RETURNING INTO, and other PL compatibility work
remain independent. The database's complete PL/SQL family is not closed by
this fix.
