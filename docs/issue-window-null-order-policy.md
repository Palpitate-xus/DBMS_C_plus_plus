# Window input and output must apply the actual SQL NULL policy once

## Actual failure

On current production `3ffbb516` (byte-identical production in test epoch
`7b2223cf`), a window ordered by a nullable key DESC placed NULL after all
non-NULL keys. This changes row_number/rank and running aggregates, not just
the final display order. A direct public WindowOp plan also misplaced NULL
when its final query ordering was DESC.

Both comparators first selected a direction-dependent NULL order, then
reversed that comparison for DESC again. Explicit window NULLS FIRST/LAST
was not retained in the structured specification; the public planner's
existing explicit final-order flags were not passed to WindowOp.

## Change

Compare the real NULL bitmap using its effective FIRST/LAST placement,
independently of the ASC/DESC comparison of two non-NULL values. Default
ASC is NULLS LAST and default DESC is NULLS FIRST. Preserve stable peers.
Carry explicit window null placement from the existing frontend window
grammar to the structured spec, and carry the actual existing PlanContext
final-order flags into WindowOp. When the output inherits the first window's
order, inherit its actual null placement as well.

The new WindowFunctionSpec flags are appended to preserve earlier positional
source initialization. WindowOp's existing constructor call sites retain
default arguments, but its signature/layout and the public specification
change: all 58 production translation units must be freshly recompiled.
No stored data, schema/WAL format, enum comparison binding, BIT constructor,
Hash NULL ownership or Boolean range metadata is replaced.

## Exact verified evidence

Artifacts: `/tmp/dbms-window-null-order.mFUFHYCF/`.

- Original complete 16-shape native baseline 82887: actual wrapper1/native134,
  showing a wrong actual final-order row. It uses the original compatible
  headers/57 current normal production objects and fresh driver/stubs.
- Original complete default-only protocol 30690: actual1, all35 statements,
  nine differences. Its independent strict180006 reference completes38
  statements with zero failures (three extra isolated reference setup steps).
- Final full default/explicit input/output/empty/type/header/tag matrix:
  current-production baseline1282 actually ends1, all195 statements and62
  differences; the identical strict PG18.6 matrix actually ends0, all198
  statements/zero failures. Original SQL/rows/NULL/deadlines are retained.
- Immutable `verify-fresh58.sh` normal25277 actually ends0: all58 genuinely
  fresh normal O2 production CPP compilations, no seeded objects. Immediate
  repeat has no production compilation, all58 exact source/header/flag
  receipts and the production stamp match, and the complete source/script/
  test/manifest input hash is unchanged. Frozen production SHA-256:
  `072b80be8667f8eed9a28536e39de9ddd362afbc035eeed181664a9833ab6b50`.
- Complete15 matching native drivers68690 actually end0 on default disk:
  new144 real-planner input/output/default/explicit/bounded/empty controls,
  original window functions/metadata/Volcano51, binding/prepared execution/
  cursor/group512/CASE, current enum comparison/quoted identity, BIT array,
  Boolean BETWEEN, original full DDL19 and corrected complete TRUNCATE.
  All drivers and shared stubs are fresh and use the matching57 normal objects.
- Complete six whole default-disk protocol files51617 actually end0: the
  final195-statement window matrix, original window-type/window-e2e, enum
  comparison, Boolean BETWEEN and BIT array constructor. No narrowed
  statements, raised deadline, sanitizer or full-suite claim.

## Remaining work

This closes this concrete structured-window NULL policy defect, not all
QRY-08/QRY-10. Multiple/complex ordering keys, collation/NaN/type-aware sort,
all frame directions/exclusions, spill, quoted/qualified complex window
arguments and the complete original273 requirements remain open/partial.
The independent GROUPS UNBOUNDED boundary defect is still reproduced and
will be repaired separately; it is not hidden by the NULL policy correction.
The frozen Source80 original full suite still has its own original inputs
and is not restarted or relabeled as a current new-test-epoch success.
No push, Actions activation or deferred security/TDE work.
