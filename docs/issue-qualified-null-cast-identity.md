# Nullable CAST retains its resolved declaration identity

The real normal O2 native52276 reproduces the bug and aborts134:
`CAST(NULL AS pg_catalog.int2)` returns typeName `pg_catalog.int2`, unlike
the non-NULL scalar's actual SMALLINT identity. The qualified parameter
candidate23111 consequently fails unchanged BIT-input, INTEGER-BETWEEN and
BIT-BETWEEN controls with42883. Their original expectations are retained.
That candidate actually exits1 with10native/15wire,4 failed wire entries;
the separate unchanged fromless USER failure is the fourth. All58/repeat/
source/frozen terminal fences and the complete original default protocol pass.

The generic NULL cast branch must return the copied catalog declaration's
actual type identity, without modifiers, not the original SQL spelling.
Use the actual bound engine/session catalog, preserving genuine named types
and arrays rather than lowercasing or guessing a physical OID. This does not
execute an operand, inspect its value or create a missing type. Collation
propagation below that branch is unchanged.

Actual verified PostgreSQL18.6/server180006 passes24 corresponding CAST/::
controls for physical/quoted builtin names, numeric modifiers and physical
array names. Own normal ExprEvaluator compile33641 exits0; the final matching
11native/15wire publication terminal is recorded below after actual exit.
No headers/layout changes, foreign object copying, skipped regression
controls, full original gap closure, push or Actions activation.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Baseline: `qualified-null-cast-baseline.log`.
Compile: `qualified-null-cast-own-expression-build.log`.
Current publication: `pg-typeof-null-cast-publication11-15.log`.

Publication67366 actually exits1: all11native and13/15wire entries pass.
Qualified NULL native, pg_typeof56, parameter18, original keyword33, BIT-input42,
BIT-BETWEEN250, TEXT-width and the complete default protocol all pass.
INTEGER-BETWEEN350 has nine remaining error-phase differences (correct error
codes, but emitted RowDescription) caused by its separate closed Bind-planning
admission of generated qualified casts. The unchanged fromless fixture still
fails at bare USER. All58 own receipts/repeat/cache/source/frozen terminal
checks pass. Those two required failures are not relabeled successes.
