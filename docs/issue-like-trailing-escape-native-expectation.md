# LIKE trailing escape demand: native expectation correction

The existing expression evaluator native test expected false for the exact
inputs text=`a#`, pattern=`a#`, ESCAPE=`#`. PostgreSQL 18.6 reports 22025: after
matching `a`, another text character remains, so matching demands the trailing
escape. With text=`a` and the same pattern/escape, matching instead returns
false. With a NULL text, the result remains NULL.

The original longer-text input is retained and now asserts SQLSTATE 22025.
The shorter-text false result and the NULL result are additional controls.
The original 13-case pattern_unicode_known_gap.py is unchanged.

The exact three inputs, and the independent SIMILAR quote-separator 2200C
contract, were asserted through the strict 180006 reference wire protocol.
Evidence is `/tmp/dbms-pattern-unicode.pJ1dBcP2/reference-native-oracle.log`.
The old native exit 134 remains in `native-v1.log`. This change corrects the
native oracle; the LIKE runtime correction is a separate commit.
