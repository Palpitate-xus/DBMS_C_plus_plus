# Native multidimensional subscript oracle

The original helper input is preserved verbatim:
`ARRAY[ARRAY[1,2],ARRAY[3,4]][2]`. Its old fixture expected the nested text
`{3,4}`. That is not a SQL multidimensional scalar fetch: a two-dimensional
array with only one supplied subscript produces an integer NULL.

There is a grammar distinction. PostgreSQL 18.6 rejects the exact unparenthesized
constructor-postfix SQL bytes with 42601; the internal ExprHelper accepts this
convenience grammar. The equivalent standard PostgreSQL operator expressions
are `(ARRAY[ARRAY[1,2],ARRAY[3,4]])[2]`, `[2][2]` and `[2:2]`. Their actual
180006 reference results are respectively integer NULL, integer 4 and integer[]
`{{3,4}}`. This does not claim the original helper spelling is legal PostgreSQL
SQL.

A matching native AST probe verifies the actual helper parser retains a Binary
`[]`, a single literal index 2, and a receiver containing two ArrayExpr children,
each with two values. Evaluating that receiver yields integer[] `{{1,2},{3,4}}`
with dimensions `[1:2][1:2]`; both the retained AST and actual ExprHelper return
integer NULL for the original input. The first candidate run of the unchanged
native fixture exits 134 at its old `{3,4}` assertion, and that log is retained.

The fixture correction keeps the original input, changes only that incorrect
expectation, and strengthens full two-index access, 2D slicing, dimensions and
actual NULL-element controls. It is committed independently of production
array/parser/storage changes.

Evidence is retained outside the repository in
`/tmp/dbms-explicit-array-bounds.gYPJNr09/native19-v1.log`,
`native-subscript-reference18.log` and `native-array-ast-probe.log`. All original
other array expression inputs and assertions remain.
