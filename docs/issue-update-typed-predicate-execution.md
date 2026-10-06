# Ordinary UPDATE executes bound typed predicates and SET expressions

The ordinary UPDATE host used legacy predicate/expression bridges instead
of executing the wholly prepared statement. CAST, CASE and arithmetic WHERE
expressions failed with XX000; routine-valued SET expressions could fail
resolution before their legitimate matched-row execution. The actual old
six-control baseline (session 93170, exit 1) recorded 12 failed assertions,
not 12 independent bugs. No discarded side-effect claim is inferred from
that baseline: its sequences still reported 55000.

This independent repair prepares the original complete UPDATE before any
SET execution. It binds the actual target owner/source/column ordinals and
declared types, propagates physical SQL NULL separately from empty text,
and evaluates WHERE and SET against a typed OLD-row context. All SET
expressions read the same OLD image and execute once for each matching row.
NULL WHERE is false; non-Boolean roots reject with 42804 even on empty
input. Unknown strings acquire Boolean/assignment context where supported,
and pure invalid constants reject before an empty UPDATE can hide them.
Pure folding excludes functions, columns, parameters and all scalar children,
including the latter's literal wrapper nodes. Metadata preparation does not
execute routines. The existing storage matcher/resolver and statement scope
retain constraints, indexes, affected-row counts, RETURNING and rollback.

The original UPDATE FROM, DEFAULT, CURRENT OF and versioned OLD/NEW RETURNING
routes remain in their existing host; adjacent controls retain their checks.
The new ordinary route preserves existing relation/column privilege checks.
This is not a claim of complete coercions, all array/composite assignments,
all RETURNING expressions, duplicate SET validation or every UPDATE shape.

## Retained evidence

Artifacts: `/tmp/dbms-update-typed-predicate.MDmJq5wn`.

- Initial production build 2495 freshly compiled all 57 normal-O2 units.
  Subsequent changed-TU rebuilds, full source/header object signatures and
  binary-stamp checks match the same public-header basis. No old 56-unit
  ABI group or development object layer substitutes for this evidence.
- Initial native run 23902 passed 23/24; its new native failed because the
  old CAST helper did not retain structured 22P02. The separate CAST commit
  a95e1076 fixes that error contract; it is not hidden inside this UPDATE
  commit. Initial expanded wire 10446 retained an actual late RAISE P0001
  versus XX000 mismatch and two nonexistent-script harness mistakes. The
  independent RAISE commit 57407b59 fixes the code while preserving the
  original P0001 assertion and full rollback/sequence checks.
- Candidate v2 native 59607 passed all 24. Wire 67629 passed ten of eleven
  scripts but exposed six assertions belonging to two correlated child
  controls. The added pure-folder had mistaken a prepared child wrapper
  for a constant. This genuine intermediate failure remains in
  `wire-typed-update-v2.log`; the original successful v1 correlations remain
  in `typed-update-wire-v1.log`.
- The v3 DML TU was freshly rebuilt (92020, exit 0). Native 23192 passed all
  24 fresh compile/link/isolated tests, including the two strengthened
  correlated SET/WHERE native controls. Wire 78100 passed all eleven
  scripts, including all original six predicate/effect controls and the
  expanded matrix. The final fixture's optional-reference-mode repeat
  54086 passed the unchanged candidate matrix.

The exact frozen v3 server SHA256 is
`f75c467c2645b00e5eb4d62512b3c4b96b9ce7d937f1b906f163d0a66ff17199`.
The expanded matrix preserves typed NULL versus unknown strings, CAST
errors before effects, zero matches, lazy/static binding, simultaneous
assignments, correlated SET and WHERE, empty text versus NULL, late P0001
after the previous matched row, transactional effect rollback and
nontransactional sequence advancement. The same final fixture passed
against actual PostgreSQL 17.2 using a unique schema, per-statement savepoints
and final ROLLBACK; it is explicitly not a PostgreSQL 18 differential claim.

The root-source integrations for metadata/carrier, CAST and RAISE stay
independent. ROOT must rebuild the changed DML TU against its new complete
57-unit public-header basis, verify every signature and binary stamp, and
run matching regressions. No private pass proves the integrated ROOT suite,
and the whole SQL/DML/query/type families remain partial.
