# Set query clause ownership and analysis

The original parser recursively gave unparenthesized trailing ORDER/LIMIT/
OFFSET/FETCH clauses to the right operand. A global left-side label such as
`SELECT 10 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1` therefore failed pure
analysis with `42703`. Parenthesized branch clauses had no retained owner.

The parser now retains a composed set wrapper for actual global clauses and
leaves parenthesized local clauses on their genuine child SelectStmt. A
clause-less first set remains the original inline-left shape, preserving its
existing native assertions. CTE definitions are owned by the composed root
and visible to both branches. Original token provenance is used for clause
expressions; no executable query/body is recreated from rendered SQL.

The public `PreparedQuery::setOrderColumns` map binds each actual set root's
retained ORDER entry to its expanded common output ordinal. It does not
invent a physical source or obtain rows to discover labels/types. Whole
branch transformation and common-type selection precede output-name/ordinal
resolution; ORDER expression transformation still reports unknown routines
and invalid UNKNOWN inputs before rejecting additional set-key expressions.
The permitted global keys are result labels/ordinals, with exact ambiguity,
missing-name, qualified-range, ordinal, and invalid-expression states.

Protocol analysis/Describe also consumes a retained set child inside a genuine
scalar/quantified grammar envelope. The actual binder still uses the original
whole SQL, and scalar labels come from the actual prepared child descriptor.
Failed set grammar is not published as a named statement.

## Evidence and dependency

Private source `/tmp/dbms-set-clause-ownership.oSu5ps6G/repo`, parent metadata
`d4ca4a88caac7d1e5a8ef063b5948dec0316a303` plus canonical-cell correction.
This foundation changes `query_binding.h` layout and requires all 58 TUs and
stubs freshly matched; it adds no TU. The final candidate is a combined
foundation plus separate actual consumer, not a claim that analysis alone
fixes execution demand.

* Matching old metadata-only binary `8c8cf369...`: complete 35-case
  `baseline.whole35.log`, real exit 1; no assertions or counters were reset.
* Strict `180006`: complete strengthened 43-case matrix exit 0 in
  `reference18.whole43.log`. Initial new fixture incorrectly guessed a scalar
  label; `reference18.v1.log` remains failed, and the measured `n` label is
  preserved in every final assertion.
* Fresh all58 O0 V1 build `53565` and 3-TU same-header V2 build `95154`:
  both actual exit 0 and matching source/header/object/flag manifests.
* Four matching native tests exit 0, including new pure clause binding and
  new real typed clause execution. Four production parser/binder/plan/carrier
  TUs plus three native drivers passed scoped ASan/UBSan (`88919`); remaining
  production objects were not sanitizer-instrumented. This is not full-core
  sanitizer or normal-O2 proof.

The following remain separate families: parameter inference and parameterized
protocol descriptors; nonliteral LIMIT/OFFSET forms; locking; general domain,
operator, or collation resolution; unsupported local relational WITH TIES;
and UNION DISTINCT/INTERSECT/EXCEPT execution. No whole-set/query closure is
claimed by this foundation.
