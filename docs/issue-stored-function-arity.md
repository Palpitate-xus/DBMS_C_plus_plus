# Bind known stored names before argument evaluation and Parse publication

Ordinary valid unqualified SQL routines are created in isolated owned test
databases. The preceding frozen SQL-value generation returnsXX000 for a missing
or extra argument, including expressions that would otherwise divide by zero.
Extended Parse/Describe also publishes a TEXT descriptor for these nonexistent
signatures. Reference PostgreSQL18.6/server180006 rejects them with42883 before
ParseComplete or any RowDescription, row or CommandComplete.

The strict registered fixture has50 controls: valid zero/one-argument calls,
missing/extra arguments, false predicates, division-by-zero arguments, same-query
volatile writes, actual no-write checks, success afterwards and SQL NULL. The
whole actual reference exits0; unchanged preceding frozen baseline exits1 with
19 failures. Parse failures cannot send ParseComplete. Assertions and timeouts
are not widened. The original function_result fixture's previous "any error"
assertion for fn_sql_add() is strengthened to exact42883, retaining all its
other original statements unchanged.

Main's prepared admission now recognizes a real stored name even when its
signature does not resolve. Network Parse has the corresponding metadata-only
source-free admission. Both use the actual namespace search path, exclude
implicit temporary routine namespaces and delegate error checking to the same
whole-query binder. Neither evaluates an argument, executes a function nor opens
a row provider to decide whether the name exists. Existing matched builtins,
valid routines and unrelated query consumers retain their current dispatch.

This is the ordinary wrong-signature path, not a new CREATE-error, security,
privilege, TEMP, trigger, WAL or EXPLAIN investigation. Whole namespace/overload,
parameter, routine return-OID and procedure families are not declared complete.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Logs:stored-arity-reference18.log,stored-arity-original-baseline.log,
stored-arity-strict-parse-reference18.log,stored-arity-strict-parse-baseline.log.
Own affected Main72710 and Network30165 CPP compiles exit0; no existing public
header changes or borrowed objects. Normal builder links those current own
objects, and the repeat performs no recompilation. Expanded gate85145 exits1:
33/33native and21/23wire pass. The strict50-control fixture and original complete
function_result fixture both pass, as do all original stored-body, ROW, virtual
relation, numeric, array, catalog and query neighbors. All58 current own receipts,
cache, no-recompile repeat, source and frozen-binary terminal checks pass.

The two failed entries remain unchanged: SQL-value's five NAME-return creator/
dependent controls and default protocol socket timeout at its original CREATE
TEMP TABLE ctas_drop ON COMMIT DROP AS SELECT id FROM t. They are not removed,
relaxed or investigated as a new user-filtered branch. The whole gate is not
green. Initial scoped pilot30355 also passes50/50; original BIT384/19117 exits0
against actual verified18.6 on exactly the same frozen binary.

The scoped signature admission repair is independently committed with this
partial proof, not a full-suite/family approval. Frozen:
dbms_main.stored-arity-initial.frozen.
SHA256:91a620d82c3774c39520d46f58e5ac14bd1918d726bf3134260aef1901d205fc.
Source seal:3142a185e39d4bf8ee58fa7bd632c93c7f41886b1d15bdee27deb7cfd54c18af.
Expanded log:stored-arity-initial-gates.log.
Master full18383 remains live and unchanged, so this source is private only.

Independent parser API probe also confirms invalid current_user()/localtime()/
temporal precision expressions throw42601 directly from public SQLParser::parse;
valid bare/precision controls return success. That separately tracked repair
will follow after this candidate's verification and commit, not be bundled here.

Original273 status/hash unchanged. No push or Actions activation.
