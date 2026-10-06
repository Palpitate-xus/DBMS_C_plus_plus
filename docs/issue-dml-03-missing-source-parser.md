# DML-03 — Missing FROM/USING sources must not become target-only mutations

Family status: partial. Source/test commit: `ea7e46d9`.
Follow-up source/test commit: `9357d433`.

## Reproduction

The old parser returned a successful UPDATE/DELETE AST after an explicitly
present FROM/USING clause failed to produce a source relation. The executor
then interpreted the statement as ordinary target-only DML.

In an isolated protocol test database containing `(1,10), (2,20)`:

- `UPDATE missing_source_probe SET val = val + 100 FROM;` returned `UPDATE 2`
  and changed the rows to `(1,110), (2,120)`.
- `DELETE FROM missing_source_probe USING;` returned `DELETE 2` and left the
  table empty.

The new parser regression also failed against the old production parser
object because `UPDATE t SET v = 1 FROM` was accepted. The wire regression
failed because malformed UPDATE succeeded instead of returning 42601.

## Fix and verification

If an explicitly present FROM/USING clause has no parsed relation, return a
parse failure before publishing an executable AST. The DML bridge declares
SQLSTATE 42601 for parser failures while retaining its existing bool/handled
error contract. The first candidate rejected mutation but its diagnostic was
mapped to XX000 after the CLI severity prefix was stripped; the explicit
SQLSTATE closes that error-reporting gap.

The production build, `dml_missing_source_parser_test`, `dml_semantics_test`,
`parser_phase1_test`, UPDATE/DELETE source protocol E2E and DML CTE protocol
E2E passed. The protocol test covers empty sources and unfinished INNER/LEFT
joins, no successful command tag on errors, all three target rows unchanged
after each error, and subsequent valid source-driven UPDATE/DELETE.

The new C++ test is included by the existing `tests/*_test.cpp` discovery;
the modified protocol test was already registered. Full registered suite
and PostgreSQL 18.6 differential were not run.

## Follow-up: parsed JOIN sources with no condition

Even with both source tables present, an ordinary JOIN without ON/USING
produced a non-null AST and was evaluated as a Cartesian source. In an
isolated database, `UPDATE join_guard_target t SET val = t.val + 100 FROM
join_guard_source a JOIN join_guard_source b;` returned UPDATE 2 and changed
both target values. The analogous DELETE USING returned DELETE 2 and emptied
the target. The strengthened native test failed against the prior production
parser object on `UPDATE t SET v = 1 FROM a JOIN b`.

`9357d433` recursively checks source JOIN nodes before publishing UPDATE or
DELETE ASTs. Ordinary joins require an ON expression or non-empty USING list;
CROSS/NATURAL preserve their implicit join semantics. A condition-less inner
JOIN nested below a CROSS join is also rejected.

The production build, missing-source and parser-phase1 native tests linked to
the matching production parser object, and source UPDATE/DELETE and DML CTE
protocol E2E passed. Native positive controls admit CROSS, NATURAL and valid
ON/USING; wire error cases assert 42601, no success tag and every target row
unchanged. The broader dml_semantics native suite passed on the preceding
ea7e46d9/208f34d8 combination and was not rerun for this follow-up.

## Remaining scope

This does not prove complete FROM grammar, binding, arbitrary join trees,
outer/lateral/subquery DML sources, duplicate-source policy or cursor mutation
support. Required condition presence is now checked recursively on the DML
source tree; complete ON/USING expression grammar, typing/binding and nested
query grammar still require verification and implementation. DML-03 and
SQL-01 remain partial. No push; Actions remain disabled.
