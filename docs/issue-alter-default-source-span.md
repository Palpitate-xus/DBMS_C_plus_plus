# Preserve the actual ALTER COLUMN DEFAULT expression

The full UPDATE DEFAULT priority matrix retained a real failure: after ALTER
TABLE SET DEFAULT with a nested qualified routine, UPDATE reported 42883 for
the unqualified basename instead of reaching the genuine constant-dead CASE.
The old persistence path called Expr::toString, whose FunctionCall rendering
omits its schema. This is a stored-expression codec problem, not a failed routine
or a reason to rewrite the UPDATE SQL.

ALTER TABLE string entry points now parse with source provenance and retain a
CPP-private thread-local statement/source scope. ALTER COLUMN SET DEFAULT copies
only the actual value expression's checked original byte span. The matching
statement identity prevents borrowing another statement's source; RAII restores
the previous scope on nested execution and exceptions. Qualified/quoted names,
CAST modifiers and dead branches remain the original expression. No global AST
pretty printer, CREATE FUNCTION implementation or public layout is changed.

The new UPDATE DEFAULT native asserts persisted exact qualified definitions,
no routine call during metadata/planning, 22012 for a reached constant argument
under WHERE false, and success/no call for the constant-dead branch. The complete
strict180006 protocol matrix retains quoted schema/routine identity, current
defaults, effects and rollback expectations. Candidate V2's qualifier/constant
priority failures remain in the evidence directory; they were not weakened.

Embedded hand-built DDL ASTs without an original source owner keep the old
toString fallback. Supporting a durable canonical codec for that API, all stored
routine identity freezing and all DDL expression syntax remains separate work.
