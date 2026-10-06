# ON CONFLICT EXCLUDED metadata namespace

## Root cause

Reached PL SQL statements and direct metadata preparation had only the
INSERT target in their conflict-expression namespace. Qualified `excluded.v`
therefore failed with `42P01`; bare references incorrectly bound only to the
target. The error could occur before executing a valid upsert.

DO UPDATE preparation now includes a logical EXCLUDED row with the complete
copied target descriptor. Both qualified and unqualified value references
participate in normal resolution, including target/EXCLUDED `42702` ambiguity.
SET target names remain grammar roles. The transition row has no physical
relation identity and is removed from the namespace before RETURNING.
DO NOTHING and statements without a conflict update do not introduce it.

## Evidence and boundaries

Private branch `/tmp/dbms-conflict-namespace.vRmowtQI/repo` is based on the
independently committed width fix and target-only fixture correction.
Public headers are unchanged. One fresh query-binding translation unit is
linked with the matching immutable 58-object development basis (shared
flags, followed by `-O0`; native tests themselves compile with `-O2`).

- Actual native baseline `43707`: qualified valid conflict expression
  reported `42P01`. `conflict.native.baseline.log` is retained.
- Actual reached-statement wire baseline `83367`: seven assertions failed,
  including both valid upsert calls, `42P01` instead of missing-column `42703`,
  and successful unqualified input instead of `42702`.
- PostgreSQL **17.2** same reached-statement/rollback controls:
  `conflict.reference.V3.log`, terminal 0. This is not a PostgreSQL 18 oracle.
- Fresh native `58773` and ASan/UBSan `35533`: terminal 0. ASan instruments
  the new binder and matching parser/DML/stubs/test; other 54 non-main objects
  are matching uninstrumented development objects, leak detection disabled.
- Final reached-statement wire `48226`: `conflict.wire.final.repeat.log`,
  terminal 0, including two valid upsert calls, unknown-column and ambiguous
  failures, sequence no-effects, all three RETURNING visibility negatives,
  and final ROLLBACK. Exact source/header/binary audits are in `verified/`;
  binary SHA-256 `96def7689dd6d5114812f52739e877af5601dc1114b504a3b53f10ffc23a341e`.

The first new reference used an incorrect target-unqualified positive and
reported four failures; that log remains, and the corrected test has a real
unqualified `42702` negative. A native descriptor assertion initially declared
SMALLINT while expecting INTEGER; the fixture now declares INTEGER, preserving
the type assertion. The first candidate wire failed at startup before SQL;
it is not counted as a SQL result.

The dedicated wire consumer is reached PL SQL. Legacy direct INSERT VALUES
may still bypass whole preparation; that independent consumer gap is covered
by the upcoming CASE/prepared-expression change. PostgreSQL 18 RETURNING
OLD/NEW namespace preparation, domains/custom casts and full conflict grammar
remain separately tracked. This commit does not execute metadata queries or
claim the complete DML family is finished.

## Additional verified PostgreSQL 18.6 reference

The independent test-only followup adds explicit `--reference18`; it uses
`verify_reference_version` to require `180006` before test SQL. The older
`--reference` remains an explicitly checked PostgreSQL 17.2 diagnostic. On
the separately built official PostgreSQL 18.6 reference, the unchanged final
matrix passes in `conflict.reference18.log`, terminal 0. This new run does not
retroactively relabel the earlier PostgreSQL 17 logs.
