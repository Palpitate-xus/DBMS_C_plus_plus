# SIMILAR class conversion and final verification

Additional strict 180006 probes exposed three neighboring grammar defects:
`[a-b-c]` must reject with 2201B, `[[:<:]]`/`[[:>:]]` are word constraints,
and SQL conversion's lexical bracket depth differs from the regex class parser
when a class contains a literal nested `[`. The latter affects following SQL
wildcards, metacharacters, and escaped-quote separators.

The SQL lexer now preserves that conversion state separately from the class
AST. The range parser rejects reusing an endpoint, and word-constraint bracket
spellings produce zero-width assertion nodes. Normal SQL `.`, `^`, `$` remain
literal; the new controls also preserve the reference's conversion semantics
after a nested literal bracket. The matcher still interprets SQL directly, not
through a byte-regex backend. Existing public value/class layouts are unchanged.

The original 13 SQL/assertions and 15-second default wire deadline remain
byte-for-byte unchanged (SHA a86c820a17e4c2e1ca6abe4600fc7ab6f07cf70ac6d949ec5b60698d3b736ca8).
The adjacent fixture has 78 scalar statements and 12 real table statements,
including Unicode values, WHERE/UPDATE/DELETE effects, OIDs/tags, NULL and empty
input, trailing-escape demand, classes, repetition, alternatives, escapes,
captured partitions/backreferences, and invalid regex states.

Before this correction V2 really failed eight of those 90 statements; its
entire fixture and serial wrapper logs remain. The first extended differential
also failed 26 cases; it remains `differential-class-extended.log`. After the
correction all 4,200 differential inputs pass the same strict reference.

Final private V3 validation (all actual terminal 0):

- Own fresh58 normal O2 compile/link, repeat-link check, every production
  source/header/compiler-flag/object receipt and binary build stamp audit.
- One strictly serial wrapper: unchanged original13, full adjacent90, original
  pattern-predicate and priority fixtures, and table-lexical fixture; no owned
  server remains. Original fixtures' SQL/assertions/deadlines are unchanged.
- Eight full native tests: helper, parser, predicate binding, expression
  evaluator, regexp functions, quoted row binding, NULL predicates, and the
  original full collation test.
- Shared helper ASan/UBSan with leak detection, plus sole-production-TU
  ExprEvaluator ASan/UBSan linked with 56 matching normal production objects
  for full expression-evaluator and predicate-binding native tests. This is
  scoped instrumentation, not an all58 sanitizer claim.
- Strict PG18.6 original13, adjacent90, both original pattern fixtures, exact
  native trailing-escape/NULL oracle, and concrete separator-state contract.

Evidence is `/tmp/dbms-pattern-unicode.pJ1dBcP2/{fresh58-build-v3.log,
final-whole-matrix.log,final-*.log,native-final8.log,scopedSAN-class-final.log,
expression-scopedSAN-compile.log,expression-scopedSAN-native-final.log,
reference-*.log,differential-class-fixed.log,audit-final.log}`. The final frozen
normal binary SHA is 4d50671f7a55b80a12dd3cbee83797c07d76492606e7d31a7717501e9192374f.

Only after the original and adjacent whole fixtures genuinely passed are they
registered. This evidence covers the requested C.UTF-8 and explicit C/POSIX
controls; it does not assert that every locale/collation or ARE extension is
implemented. The independent native-oracle and quote-state commits retain their
own historical failures.
