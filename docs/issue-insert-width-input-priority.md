# INSERT width/input transformation priority

## Root cause and scope

The SQL INSERT consumer rejected a VALUES row's target width before preparing
that row. This hid malformed typed input (`22007`) and unknown functions
(`42883`) behind `42601`. The metadata binder already processes all siblings,
then width, then contextual conversion of bare unknown strings.

The consumer now prepares the complete original SQL prefix through the
offending row before reporting its width error. The existing protected-byte,
balanced-row and strict whole-AST provenance checks are retained. This does
not evaluate arithmetic, numeric narrowing casts, functions or source rows;
the original complete SQL is still parsed before any prefix is prepared.

## Evidence

Private worktree: `/tmp/dbms-case-width.uMSm8LDz/repo`, parent `14685eec`.
The immutable donor has all 58 translation units and matching public headers
from the previous INSERT SELECT/Network fixes. Only DmlExecutor is rebuilt.
Development objects use the shared flags followed by `-O0`.

- Actual old native failure: handle `10400`,
  `../width.native.baseline.log`, first typed bad INTERVAL row actual `42601`,
  expected `22007`.
- PostgreSQL **17.2** reference: `../width.reference.log`, terminal 0. The
  original six-query wire matrix is also retained in
  `/tmp/dbms-assignment-input.gZVw19Xo/compound_source.current.log`.
- Focused new native: all 10 cases pass. Focused wire: handle `38140`,
  `../width.wire.final.repeat.log`, terminal 0, including typed/unknown input,
  later-row precedence, malformed whole SQL, no sequence effects and NULL/OID.
- Four adjacent protocol entries: handle `79947`,
  `../width.adjacent.wire.log`, terminal 0: interval input classification,
  INSERT SELECT input, direct INSERT SELECT frontend preflight, strict VALUES
  grammar. No assertions or default timeout settings are relaxed.
- Fresh ASan/UBSan DmlExecutor plus the matching instrumented parser,
  stubs and tests: handle `66000`, `../width.asan.log`, terminal 0 for width,
  INSERT SELECT and interval-input native tests. Other 55 non-main objects
  are matching uninstrumented development objects; leak detection is disabled.

Old wire handles `86856`/`63481` stopped during server startup, and the first
candidate group `31465` timed out at fixture CREATE TABLE before test SQL.
Their logs remain; these are not claimed as SQL red/green results.

## Open boundaries

This is width/error-order closure, not all INSERT typing. CASE common types,
WITH-final DML execution, general casts/domains/arrays and no-explicit-target
short VALUES support remain separately tracked. VALUES input `1/0` or a
numeric narrowing CAST is not folded during analysis merely to find an error.
