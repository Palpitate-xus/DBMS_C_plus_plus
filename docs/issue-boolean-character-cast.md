# Boolean-to-character conversion must use SQL Boolean output

The native Boolean datum often contains `t`/`f`; table/provider values may
contain `true`/`false`, and these represent the same Boolean. Root's character
conversion copied that internal datum verbatim, so a genuine Boolean cast to
TEXT or VARCHAR could produce `t` instead of PostgreSQL's `true`. The unchanged
outer `(1 BETWEEN 0 AND 2)::text` control exposed this independently of the
integer input and result-descriptor issues.

The correction is confined to `castToCharacter`: an actual source type whose
TypeRegistry identity is Boolean is rendered as `true` or `false` before the
existing UTF-8 length/truncation/padding rules. Non-Boolean TEXT values are not
interpreted or normalized; literal `t`, `f`, spaces and empty strings retain
their original character data. NULLs still use the existing NULL path. Integer,
BIT, provider/routine admission, and parser type ownership are unchanged.

`boolean_character_cast_test.cpp` contains 211 assertions over 105 actual
datum/target pairs, each through both the actual CAST evaluator and true
bound query plan/cursor. It covers real Boolean and bool aliases, all valid
internal truth spellings, NULL and true TEXT passthrough, TEXT/VARCHAR/CHAR and
explicit VARCHAR(3)/CHAR(6) targets. Type identity uses the actual registry's
normalization; the expected textual value, NULL and cursor exhaustion are
not weakened to ignore alias-dependent errors.

`boolean_character_cast_protocol_e2e_test.py` contains 105 strongly asserted
wire controls: actual Boolean literals, logical and range expressions, NULL,
unaltered TEXT character inputs, all five targets, and genuine OID-16 Parse,
statement Describe, Bind, portal Describe and Execute cells. Exact values,
rows, names, command tags, OIDs, size, typmods and format remain asserted. Its
strict mode verifies PostgreSQL 18.6 (`180006`) with the original runner
deadlines; it creates no shared reference objects.

All evidence is under `/tmp/dbms-integer-between-input.fHTGGaWj`.
`boolean-character-new105-baseline-final-v2.log` actually returns 1 and collects
54 failures, retaining the separate outer-CAST metadata defect as well as
Boolean text-output defects. The final strict 105 run and final 20-file strict
batch actually return 0. A draft oracle omitted the one-space padding for an
empty TEXT-to-CHAR cast; its failed strict log remains, and the final oracle
was corrected against real PostgreSQL without changing its SQL, values to be
tested, type assertions, count, or deadlines. Earlier native draft assertions
also compared raw alias spellings instead of real type identity; those failed
logs remain separate from final proof.

The final combined candidate's normal object receipts, immutable SHA, complete
native/wire gates and untouched remaining matrices are recorded in
`issue-integer-between-input.md`. This Boolean character-output root is a
separate commit, not included in the integer comparison or metadata hunks.
