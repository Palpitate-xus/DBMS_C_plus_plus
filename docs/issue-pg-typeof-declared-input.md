# pg_typeof consumes the actual argument declaration

Root publication86219's only wire failure is the original mixed fromless
projection's missing pg_typeof (42883). A new genuine PostgreSQL18.6 oracle
also disproves the old Main literal-shape guess: BIGINT reports INTEGER,
typed NULL reports UNKNOWN, Describe reports TEXT, and errors/effects can be
skipped. Candidate43092 retains the next catalog and parameter failures.

Register a real polymorphic pg_catalog scalar role with stable volatility,
one positional argument and a genuine REGTYPE result declaration. Select
real exact typed stored overloads through search_path; UNKNOWN ambiguity,
explicit namespaces, wrong arity/named arguments and non-window diagnostics
remain distinct. Remove Main's literal-shape output guess and route its
same-named shortcut through the normal scalar binder instead.

Use static AST/column/parameter/routine declarations and copied real catalog
identities, not an evaluated first row or NULL datum. Preserve the physical
OID in ExprValue.objectOid. Evaluate the actual argument once, including
NULL-returning or volatile arguments, preserving division errors and effects.
WHERE false/LIMIT0/Describe preserve demand. A separately owned engine supplies
its own stored declarations. UNKNOWN705 is a separately committed genuine
catalog row; parameter identity is a separate network repair, not fabricated
types inside this builtin.

Actual verified180006 passes all56 controls: metadata/runtime, UNKNOWN, typed
NULL, numeric widths, arrays, current values, nested regtype, OID conversion,
CASE, enums/domains, bound parameters, effects, errors and namespace overloads.
Matching own normal O2 native46533 also passes declaration/OID, actual argument
effects and errors and independent-engine NULL routine ownership. Final
matching publication85985 actually exits1: all11native and14/15wire entries
pass, including all56 new pg_typeof controls, all18 parameter controls, original
keyword33, BIT-input42, INTEGER-BETWEEN350, BIT-BETWEEN250, TEXT-width,
engine-owner and complete original default protocol. All58 own-object receipts,
normal/repeat/cache/source/frozen start and terminal checks0. The only failed
entry is the unchanged original fromless fixture's separate bare USER42703.
Earlier red matrices remain in their original logs, not relabeled passes.
Frozen44074863ae7524fe; source seal
`d0f95be62a998eaed01e5a8dd22aaba73e258e24bcabe5f910d4b517b0abc2fc`.

Do not claim full polymorphic overload catalogs, nested SRF receivers or all
original TYPE/FUNC/QRY requirements complete. The unchanged original fromless
fixture has also exposed a separate bare USER grammar failure (42703), which
remains required work. Original273 item statuses and15 explicit user-deferred
security/TDE items are unchanged. No push or Actions activation.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Oracle: `pg-typeof-parameters-reference18.log`.
Final log: `pg-typeof-bind-planning-publication11-15.log`.
Native preliminary: `pg-typeof-unknown-corrected-native.log`.
