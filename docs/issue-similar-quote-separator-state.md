# SIMILAR escaped-quote separator SQLSTATE

The exact PostgreSQL 18.6 input `SELECT 'a' SIMILAR TO '#"#"#"' ESCAPE '#';`
reports 2200C (invalid use of escape character), not 2200B. The matcher now
reports that state when more than two escaped-quote separators are demanded.

The standalone matcher asserts that state. The adjacent protocol test retains
the exact SQL and expected state, alongside escaped-quote capture/backreference,
Unicode, classes, repetition, NULL, demand, and real WHERE/UPDATE/DELETE controls.
The original 13-case fixture remains unchanged. The strict 180006 full reference
for the 80 adjacent SQL statements passed; matching-header candidate completion
is checked separately before registering this fixture.

The initial candidate V1 and V1b whole fixtures really failed this SQLSTATE;
their logs remain in `/tmp/dbms-pattern-unicode.pJ1dBcP2/v1-adjacent-full.log` and
`v1-adjacent-with-rows-full.log`. The initial reference expectation mistake
(2200B) is also retained in `reference-adjacent-v1.log`. The exact state contract
is asserted in `reference-native-oracle.log`, and all expanded reference SQL
statements in `reference-adjacent-expanded-final.log`.
