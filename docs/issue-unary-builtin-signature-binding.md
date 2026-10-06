# Builtin unary plus/minus signature binding

The whole-query binder resolves builtin prefix `+` and `-` from declared operand types instead of accepting the operand's type unchanged. The selected signature is represented by a genuine implicit operand CAST when needed. Resolution does not evaluate routines, source rows, children, or parameters, and typed SQL NULL does not bypass operator lookup.

The exact catalog inventory was queried from the isolated official PostgreSQL 18.6 instance after checking `server_version_num = 180006`: six numeric signatures for each sign, and an additional INTERVAL signature for minus. MONEY and POINT have neither prefix operator. Unary plus on unknown string/NULL selects preferred DOUBLE PRECISION; unary minus on unknown string/NULL remains ambiguous (`42725`) between numeric and interval categories. TIME can implicitly convert to INTERVAL for minus. These choices follow the [official operator-resolution rules](https://www.postgresql.org/docs/18/typeconv-oper.html) and the [18.6 operator catalog](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_operator.dat).

Lexical integer sign folding is preserved before width selection. A signed integer literal can fit INTEGER/BIGINT where its unsigned spelling cannot. CASTs, parameter values and routines are not folded by this rule; their existing runtime overflow/demand contract remains intact. The independent structured integer-overflow creation-site fix is a dependency, not a value change in this commit.

## Actual failures and scope

The original pure native test aborts because unary MONEY was admitted instead of raising `42883`. An additional actual-engine test retains five incorrect bindings, including physical/logical sources, a declared MONEY writer, and a writing CTE. The sequence remains uncalled during metadata analysis, including on the failing baseline; this is not claimed as a new baseline side-effect failure.

The complete unchanged wire matrix passes on strict PostgreSQL 18.6. Against the old immutable candidate it has 48 failed assertions. The binder-only candidate reduces that to 37 but is **not a complete protocol fix**: old ordinary SELECT/VALUES entry points still fail to consume the prepared AST, so unsupported operand types and preferred unknown casts can be lost. The retained writing-CTE wire control additionally demonstrates a sequence effect before its wrong error. The independent genuine scalar consumer must close these failures; the test remains registered and its SQL, expected types, error states and effects are not weakened.

## Evidence

All artifacts are under `/tmp/dbms-unary-operator-binding.ju2tBXPl`.

| Artifact | Actual result |
| --- | --- |
| `operator-inventory.reference18.log` | Version 180006 and all 13 actual catalog signatures |
| `reference18.log` | Complete strict 18.6 wire matrix passes; unique schema, SAVEPOINT error recovery and final ROLLBACK |
| `native.baseline.log` | Pure native baseline abort retained |
| `native.engine.baseline.log` | Five actual-engine binding failures retained, exit 134 |
| `baseline.log` | Full wire baseline: 48 failed assertions, exit 1 |
| `binding.protocol.log` | Binder-only wire: 37 failed assertions, exit 1; explicitly still open |
| `build.binding.log` | All 58 production translation units and stubs freshly compiled, exit 0 |
| `native.binding.log` | Seven fresh optimized native drivers pass, including existing integer errors, simple CASE and geometric controls |
| `native.engine.binding.log` | Actual-engine physical/logical/write-CTE metadata and nullable typed execution pass |
| `optimized.binding.log` | Fresh optimized binder and stubs; both new native tests pass |
| `asan.binding.log` | Both new native controls pass under scoped ASan+UBSan, exit 0 |

The full candidate uses normal shared flags with final `-O0`; the optimized proof replaces the binder with a fresh `-O2` object and optimized stubs/drivers. Sanitizer instrumentation covers the changed binder, stubs and test drivers; the other matching translation units remain uninstrumented and leak detection is disabled. Header/source hashes pass before and after native tests. Full candidate SHA256: `fc73e23cc21ca17bf9ab100cb81bde04625b82ef77539c17f7ff13d71654c4cb`. No objects from a different public-header layout were reused.

This commit does not close the ordinary consumer, general user-defined/domain/operator catalogs, finite INTERVAL component-wise runtime negation, or the whole unary/type family. The separate low-level evaluator MONEY error-class fixture is retained as a legacy API test, not SQL MONEY operator support.
