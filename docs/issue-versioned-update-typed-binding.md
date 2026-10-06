# Versioned RETURNING must retain typed UPDATE assignment binding

An OLD/NEW RETURNING namespace previously diverted target-only UPDATE away
from its prepared OLD-row consumer. The legacy updateContext stores names
in a case-folded value map. A table with both BIGINT `"V"=2147483648` and
INTEGER `v=3` therefore evaluated both column references as 3: the update
returned NEW `"V"=4` instead of 2147483649. A second simultaneous assignment
also read the wrong OLD column, producing -2147483643 instead of 2.

The same route ran a sequence-valued SET on a false-WHERE row, then returned
0A000; currval was 1. PostgreSQL returns UPDATE 0 and currval raises 55000.
The permanent control preserves exact quoted identities, OLD/NEW quoted
aliases, both simultaneous assignments, zero-row volatile demand, NULL
propagation, column OIDs and command tags.

The implementation keeps ordinary target-only/no-DEFAULT/no-CURRENT-OF
updates on executePreparedUpdate even with versioned RETURNING. Its physical
target descriptor excludes logical OLD/NEW occurrences, and its source
ordinal/typed cells bind SET to the actual OLD row. Actual storage row images
still supply the RETURNING channels. FROM/USING, DEFAULT and cursor routes
retain their separate boundaries.

Evidence under `/tmp/dbms-with-cursor-integration.xOfpugBC`:

- Exact strict180006 reference `versioned-update-reference18-v2.log`: exit0.
- Original optimized b4/1db6 frozen baseline `versioned-update-baseline-v2.log`,
  handle8463: actual exit1 with five failed checks, including the wrong
  columns and irreversible sequence call. Original ARRAY-expanded controls
  independently retain the same incorrect NEW wide value.
- The first draft reference check incorrectly assumed a runtime error has
  no RowDescription. PostgreSQL actually emits currval/OID20 before55000.
  That draft log is retained; the final effect control checks exact SQLSTATE,
  no data rows and no success tag, not a claim about error-frame metadata.

This source implementation awaits the new public-layout ROOT full58 build
and matching regression results. A local commit is not a passing-current-
combination claim. The prior frozen ARRAY/CASE/FROM private gates and the old
b4 baseline are not substituted for that pending validation. General UPDATE,
all RETURNING expressions and the full wire-error framing contract remain
partial.
