# Stored scalar calls must retain their StorageEngine owner

## Reproduced defect

On the independent WHERE/resolver baseline `7353cd9d`, a native caller using
its own `StorageEngine` and an existing transaction evaluated
`ORDER BY ownerwriter(id)` through `ExprHelper`. The resolver silently selected
the process-global `g_engine`. The nested function therefore started and
committed a different engine's transaction, leaving its write visible to an
independent observer before the caller committed, and outside the caller's
rollback boundary.

The actual baseline native executable exited 134 at the assertion that the
observer must see zero rows (retained terminal handle `84997`). This is an
observed transaction leak, not merely a possible metadata lookup error. The
baseline uses the freshly built, matching WHERE/resolver objects in
`/tmp/dbms-function-where.8RONb5`, with the failing executable retained as
`/tmp/dbms-function-order.9Iwljh/engine_owner_baseline`.

## Repair and scope

`ExprHelper::evalString`, `evalStringWithNulls`, `evalBool`, and `evalCheck`
accept a final optional `StorageEngine* functionEngine`. They pass that owner
to the shared metadata-only scalar binder. The null default retains the
frontend's existing global-engine convention; callers using an independent
engine must provide their actual instance.

Storage member bridges now pass that instance for row predicates, projection
and expression sorting, default and generated expressions, CHECK and exclusion
predicates, RLS and partial-index predicates, and PL scalar/coercion callbacks.
This changes neither the `StorageEngine` object layout nor the public query
executor contract. Preparation still does not execute a stored function.

The native regression creates separate local/global database directories and
engine instances. It checks that native and legacy-display expression-sort,
projection, WHERE, DEFAULT,
generated/CHECK lookup, and nested PL RETURN work remains inside the local
transaction, is invisible to the global observer, and disappears on rollback.
The global engine's same-spelled function in its separate database remains
independent. The generated lookup uses an explicitly finished prior INSERT
command; an immutable function correctly must not see writes made in its own
still-active caller command.

## Validation boundary

The owner API was rebuilt from source for all 55 development translation units
with the configured zlib/ICU features and selected TLS stub, using the normal
build flags plus `-O0`. Retained build handle `24766` completed successfully.
A final same-header `TableManage.cpp` rebuild (`93393`) also completed; unchanged
source/header hashes and final source/binary hashes are retained under
`/tmp/dbms-function-engine-owner.nBNBgt`. The final native owner regression
(`75985`) passed without lowering its original visibility assertion.

Expanded generated-reader runs `20988` and `98174` first failed because the
test fixture did not finish its prior SQL command; those failures are retained.
Correcting the command boundary did not weaken the expected value, NULL, or
observer-visibility checks.

Eight adjacent native executables passed: scalar resolver, statement atomicity,
native PL query host, quoted scalar binding, function/procedure, generated
columns, trigger-before-generation, and multiple CHECK constraints. The first
five completed in `92762`; the final three completed in `57894`.
Six focused protocol scripts passed: stored-function WHERE execution (`45425`),
WHERE function scope, stored-function atomicity, PL SELECT INTO, quoted scalar
binding (the first four scripts in `49526`), and constant Boolean predicates
(`5341`). Handle `49526` ultimately exited 2 because its final script filename
was mistyped; the corrected existing script passed separately.

An additional `constraint_expr_test` did **not** pass: both this candidate
(`92762`) and the matching immutable `7353cd9d` baseline (`13997`, with the old
expression-helper header and matching objects) exited 134 at its existing
`sum('abc')` ambiguity assertion. The metadata binder rejects the evaluator's
preexisting SUM/AVG special role before its typed `42725` diagnostic. That is
a separate unresolved binder-role issue, not a successful adjacent test or an
owner regression. The unchanged clause diagnostic (`65809`) still reports
exactly seven expected failures.

This independent repair does not fix SQL ORDER BY dispatch, scalar-subquery
error propagation, EXPLAIN ANALYZE execution, or arithmetic UPDATE assignment.
The original clause-execution diagnostic and its PostgreSQL expectations stay
unchanged, and the related review families remain partial. This development
candidate is not a claim that the final ROOT optimized combination, full
protocol gate, all native entry points, procedures/DO, or all routine semantics
have passed.

A final audit caught the legacy display-sort bridge still using the default
owner. Its actual-owner argument and a direct native visibility control were
added to this same independent repair. The final matching `TableManage.cpp`
rebuild (`52296`) and native/WHERE/atomicity recheck (`14232`) passed. A copied
object in exploratory run `80853` was already the post-fix object (matching
its final SHA), so that successful run is candidate evidence, **not** an
additional red baseline; its retained executable is `legacy_owner_candidate`.
The earlier binary with SHA `88331f038aec2fc297bff8d0e9a6a960fbc5d142f4e57127b5b2b3cff6e9da6a`
is preserved as `dbms_main.pre-legacy-owner`. Final source/header hashes and
the final binary SHA are recorded by the `.final` audit files above.
All eight adjacent native executables were also freshly relinked against this
final object set and passed in `69083`.
