# SQL pattern Unicode and execution demand

LIKE, ILIKE and SIMILAR TO previously matched text bytes. SIMILAR additionally
delegated its language to ECMAScript regex: SQL-literal `.`, `^` and `$`, Unicode
wildcards, multibyte ESCAPE, and newline matching did not follow SQL semantics.
ILIKE lowercased ASCII bytes, and LIKE treated a trailing escape as false before
matching could determine whether the invalid escape was actually demanded.

The shared `SqlPattern.h` decodes UTF8 into codepoints (BYTEA LIKE retains byte
units). LIKE retains the match/false/abort distinction used by PostgreSQL's
matcher. A trailing escape is an error only when matching reaches it; NULL and
unevaluated expressions retain their existing evaluator demand boundaries.

SIMILAR parses SQL wildcards, literal metacharacters, classes, alternatives and
repetition directly and matches complete strings with memoized endpoint states.
It does not substitute an ECMAScript or ICU regex interpretation for SQL. Its
escaped quote partitions retain the one capturing part and associated ARE
backreference; ordinary SQL parentheses remain noncapturing. Invalid classes,
bounds and quantified zero-width assertions reject with 2201B.

C.UTF-8 ILIKE uses the reference platform's simple codepoint lowercase, not full
case folding or contextual lowercasing. Explicit C/POSIX uses ASCII lowercase
and character classes. This work does not claim the whole locale/collation or
POSIX/ARE-extension family is complete. No existing public class or value layout
changed; ExprEvaluator is the only production consumer of the new inline helper.

The original 13 SQL/assertions/default deadlines are unchanged. The private
candidate's first full 13-case run passed. Full adjacent runs retained a real
quote-separator SQLSTATE discrepancy (2200B instead of 2200C); that independent
contract correction and final matching-header production verification follow in
a separate commit. Native expectation correction is also independent.

Evidence is retained under `/tmp/dbms-pattern-unicode.pJ1dBcP2`: original baseline
and V1 full logs, strict 180006 reference logs, exact native oracle and its old
134, fresh58 V1 build receipts, helper sanitizer, and differential probe logs.
The first 4,080-case differential found quantified assertions incorrectly
accepted; its failure log remains. After rejecting them, the same matrix passed.
