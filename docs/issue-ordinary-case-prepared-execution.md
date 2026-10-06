# Ordinary CASE prepared execution

Ordinary SELECT and VALUES previously bypassed the already-correct simple
CASE binder. This accepted missing comparison operators, miscompared wide
numeric operands and executed routines before discovering later invalid CASE
expressions. The ordinary consumer now performs whole-query preparation and
executes its retained typed AST using the shared source contract.

## Original evidence

The strict PostgreSQL 18.6 reference checks version `180006`. The original
ordinary matrix failed 50 assertions against the immutable pre-consumer
candidate. It retains incompatible operator errors for SELECT, WHERE FALSE
and VALUES; error priority before bad casts; BIGINT and REAL cross-type
comparisons; quoted wide versus narrow columns; output alias ORDER; switch
once; runtime source NULL versus constant NULL demand; all-target preparation
before a volatile target; and writing CTE preparation before sequence effects.

Simple CASE resolves a comparison operator for each WHEN and evaluates the
switch once. Constant planning demand and runtime NULL demand are distinct.
[Conditional expressions](https://www.postgresql.org/docs/18/functions-conditional.html),
[Operator type resolution](https://www.postgresql.org/docs/18/typeconv-oper.html).

The initial narrower candidate passed these controls, but exposed a real
simple-view regression. The source correction and its stronger JOIN/view
demand controls are required dependencies, not unsupported-shape exceptions.
See `issue-prepared-source-contexts-and-view-execution.md` for that evidence.

## Whole preparation and consumer

The dispatcher parses original SQL and recognizes genuine CASE expression
nodes. Eligible SELECT and VALUES use `prepareBoundQuery` before opening a
source or evaluating any target. The shared reader consumes actual CTE, view,
JOIN and supported virtual source occurrences with typed NULL cells.

The read wrapper preserves the caller's ordinary query transaction. A pure
SELECT does not start a DML savepoint just to evaluate a constant. Writing
CTEs use the existing atomic owner, demand referenced producers and complete
unused writers only after the primary command succeeds. LIMIT 0 still
completes a successful unused writer but never opens an undemanded read view.

The parser-boundary native control also verifies that CURRENT_TIMESTAMP,
CURRENT_DATE and other SQL value keywords are FunctionCall nodes, not
structural literals; ordinary and quoted variables are ColumnRefs; and PL
datums/raw positional parameters become nullable typed ParameterExpr nodes.
The constant planner therefore cannot freeze those values as literal NULLs.

## Verified scope

`ordinary.sources.candidate.v3.log` and
`ordinary.reference18.sources.final.log` under
`/tmp/dbms-simple-case-binding.KtyWx31T` are successful complete matrices.
The matching all-58 source/header/flag build, final replacements, twelve
initial native controls, six final native controls and scoped four-test
sanitizer evidence are documented with the source dependency.
The eleven matching adjacent protocol scripts also finished successfully in
`source.adjacent.protocol.log`. The final development binary SHA256 is
`da5a8bcf507ec853e10bed426bd9f253ce754b858d1f38b9e454eb5c5f9f0021`.

This closes the tested ordinary scalar CASE and VALUES shapes; it is not
blanket closure of CASE, type, operator or query families. Strong ELSE-derived
projection names have their own real baseline failure and correction.
Aggregate/window/set-operation consumers, wider virtual schemas and the
remaining literal-width/geometric/INTERVAL diagnostics are not erased.
