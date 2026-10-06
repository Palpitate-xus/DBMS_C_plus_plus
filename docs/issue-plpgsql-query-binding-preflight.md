# Open PL/pgSQL SQL-statement binding and preparation gaps

Date: 2026-10-06. Status: reproduced, not fixed by the scalar identity commits.
SQL-04/FUNC-06 remain partial. The recorded local candidate includes the native
SQLSTATE and quoted scalar fixes; its SQL-statement path is separate from the
scalar-expression binder. This report is not a completion claim.

## Actual observations

One isolated candidate server was started for the probes and cleaned up after
completion. The diagnostic source is retained at
`/tmp/dbms-plpgsql-binding-diag.kv8gDi/probe.py`. Reference objects were temporary
tables/functions/sequences, with per-case savepoints in a transaction ultimately
rolled back. The observed reference version was 170002 (PostgreSQL 17.2), with
`plpgsql.variable_conflict=error`, not PostgreSQL 18.6.

| Reached SQL statement inside a stored function | Reference | Candidate |
|---|---|---|
| Local id=99 and source column id in bare projection | `42702` | Returns 99 |
| Same collision in WHERE id=1 | `42702` | Successful SQL NULL |
| Ambiguous projection with WHERE FALSE | `42702` | Successful SQL NULL |
| Parameter id and same-named source column | `42702` | Returns parameter 99 |
| Function-name-qualified parameter | Returns 99 | `42P01` |
| Quoted local and source column both named ID | `42702` | Returns local 99 |
| Writing CTE followed by ambiguous projection | `42702` before CTE runs | Returns 99 and inserts 10 |
| Local `"C"` and `COLLATE "C"` identifier | Returns a | `42704`; collation became cast |
| AT TIME ZONE with local tz=UTC | Correct timestamp | `22023` |
| IS DISTINCT FROM with local wanted=2 | True | `42703` |

These are observed failures, not merely inferred from code. Controls passed for
qualified table columns, nonambiguous locals, quoted case distinctions, qualified
quoted columns, DOUBLE PRECISION and a simple implicit alias. The corresponding
ordinary SQL literal COLLATE/time-zone/DISTINCT expressions also passed, isolating
the three procedural substitution/parser defects. No earlier-commit comparison
was made, so the report does not label every defect a newly introduced regression.

The reference writing CTE uses nextval on its private sequence. After the
ambiguous statement reports 42702 and its savepoint is rolled back, the sequence
still has is_called=false and the target is empty. Sequence increments are not
undone by transaction rollback; this establishes analysis before execution, not
just execution followed by removal of inserted rows. A fix must preserve that
observable ordering even after the separate function-transaction fix lands.

## Cause

`src/utils/plpgsql.cpp` substitutes variable values before handing the completed
SQL statement to `host.query`. A lexical role heuristic can recognize relation,
alias and type labels, but cannot determine whether a value ColumnRef also names
a visible source column. Once it has replaced id with a literal, the downstream
SQL binder cannot recover or reject the lost ambiguity. Looking at result rows
would also miss the error for an empty input or unreachable result row.

The qualifier path preserves table references but lacks the implicit function
label for parameters. Scalar positional binding fixes case collisions in scalar
expressions only; it does not provide that statement namespace.

Three more specific roles are wrong: FROM in IS DISTINCT FROM is treated as a
relation introducer; the time-zone operand is mistaken for an implicit alias;
COLLATE's identifier is substituted as a data value. Additionally, the SQL parser
stores AT TIME ZONE's zone in a unary operator string instead of retaining it as
an independently bound value expression. Correcting only one lexical skip rule
cannot provide arbitrary zone-expression semantics.

## Required preparation contract

Data positions accept variable parameters, but relation/column/function labels
are not data parameters. A default variable/source-column collision is an error;
explicit table, function and block qualification resolves the applicable scope.
Reached statements need analysis before execution, while unreachable procedural
branches must not be eagerly executed or prepared as commands. These contracts
are described in the [PostgreSQL 18 variable substitution and preparation
documentation](https://www.postgresql.org/docs/18/plpgsql-implementation.html).

The intended implementation preserves raw SQL after removing procedural INTO,
supplies canonical procedural scopes/types/NULLs to preparation, and binds value
AST nodes against independently constructed source and procedural namespaces.
Both namespaces matching a value reference yields 42702; only a procedural match
becomes a typed parameter. Function/block labels and quoted components must be
represented explicitly rather than reconstructed from a dotted string.

Preparation must derive CTE/derived/subquery/view output metadata without running
their SQL, including all branches and writing-CTE RETURNING shapes. It must finish
name/type/function analysis before writes or volatile expression evaluation.
Catalog/temp/search-path visibility and correlated query levels need explicit
contracts. If prepared plans are cached, values remain per execution and DDL,
database, search_path, temporary namespace and type changes must invalidate the
appropriate plan. A result-width check or broad keyword whitelist is not this
binder.

## Work and verification still required

- Independently fix the three demonstrated lexical/value-expression roles, each
  with a red baseline, actual engine/wire verification and its own local commit.
  Include ordinary SQL, quoted labels, invalid collation/zone, NULL/type and
  genuine zone-expression controls, not only one literal-zone spelling.
- Implement the preparation-stage namespace/type binder and function/block label
  parameter binding. Cover the seven ambiguity/qualification observations above,
  CTE/derived/correlation scopes, empty rows and the nontransactional sequence
  negative control.
- Recheck the final combination with the independent SPI transaction/CID changes.
  Rollback of failed function writes is necessary but not a substitute for
  rejecting an ambiguous query before any CTE or volatile execution.
- Preserve neighboring quoted-scalar, SELECT INTO, ordinary query and type tests;
  fresh formal builds must match any changed public API/header layout.

No new full registered suite or PostgreSQL 18.6 differential was run for this
diagnostic. Earlier complete-protocol timeout failures and I/O amplification
remain open. No git push, Actions enablement or user-deferred security/TDE work.
