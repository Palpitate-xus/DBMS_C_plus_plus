# Postfix cast consumes the following pattern operator

The actual expression parser's postfix `::` type reader stopped at arithmetic,
boolean and clause boundaries, but omitted LIKE, ILIKE, SIMILAR, ESCAPE, IN
and BETWEEN. It therefore parsed `'a'::CHAR(2) LIKE 'a'` as a single cast whose
type spelling included the whole predicate. Some legacy consumers returned
the left cast value instead of the BOOL result; a cast of the pattern before
ESCAPE could instead produce an invalid concatenated type and wrong 42883.

The type reader now stops at those actual operator/grammar tokens. Quoted
identifiers remain identifiers: a type named `"like"` is not the LIKE token.
Multiword temporal/INTERVAL types, qualified names, modifiers, array suffixes
and the existing negative-operator boundary retain their original grammar.
No SQL is rewritten and no operand is wrapped to hide the parser failure.

## Evidence

Private source basis is ROOT `5a0d1520` in
`/tmp/dbms-cast-pattern-parser.dXD9lMxX/repo`; artifacts are in its parent.

- The fresh unchanged parser O0 binary's `baseline-parser.log` exits134 with
  **11 failed controls out of16**, retaining every AST role and parsed type.
- The fresh candidate parser O2 binary's `candidate-parser-O2.log` exits0:
  **all16 exact root-role/type-envelope assertions pass**.
- Five existing parser-only tests are freshly compiled/linked with the same
  candidate O2 parser object and actually exit0: DROP lists, duplicate CTE
  names, quoted constraint columns, comma/JOIN source precedence, and missing
  DML sources. Their authoritative terminal output is retained in the tool
  record for session95484, not represented as a nonexistent filesystem log.
- An initial standalone compile omitted the interfaces include directory;
  that is a harness compile failure, not the runtime baseline. The corrected
  successful compile and the actual134 baseline are retained separately.

The original expanded protocol SQL/expectations also pass actual PostgreSQL
18.6/180006. The immutable pattern V3 still fails the whole expanded gate:
this parser error accounts for incorrect cast values/descriptors and the
ESCAPE cast signature, while paired DML ownership is a separate root.
Standalone parser tests are not proof that the whole protocol, database,
planner/type family, or original273-item audit passes. The pattern consumer
and current formal production combination still require matching verification.

Only parser.cpp changes; no public header/ABI or new production TU is added.
The new permanent native test is discovered by the unchanged native runner.
No push, GitHub Actions enablement or user-skipped security work is performed.
