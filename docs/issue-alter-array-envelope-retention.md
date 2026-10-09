# ALTER COLUMN declaration envelope retention

The unchanged original `alter_array_type_envelope_test` failed after ALTER
started using the shared declared-type parser: rendering its boolean `isArray`
descriptor collapsed `CHARACTER VARYING(7)[][]` to one array suffix. The same
loss affected explicit array bounds. This is a parser regression, not a change
to PostgreSQL array storage dimensionality.

ALTER still validates the declaration using `consumeDeclaredType`. It now
retains the complete validated type-token span in the ALTER AST instead of
reconstructing spelling from a descriptor that cannot retain array rank/bounds.
The original fixture and assertions are unchanged. Four additional native
controls cover explicit bounds, qualified interval fields, time zones, and
negative numeric scale; each envelope also reparses as an array declaration.

## Actual verification

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.

| Evidence | Actual result |
| --- | --- |
| Original independent fixture baseline | Session 42973 exits 134 on the original multidimensional assertion; binary/log retained. |
| Own parser translation unit | Session 88845 exits 0; no public header changed. |
| Native regression group | Session 55834 exits 0: all 8 entries pass, including the entire original envelope fixture. |
| Complete focused protocol group | Session 52764 exits 0: all 8 unchanged entries pass. |

Native group: ALTER array envelope/values, ALTER column type, interval column
modifier/field type, declared-type syntax status/cast envelope, type registry.
Protocol group: ALTER array envelope, array element typmods, interval column
modifier/literal fields/field type/cast input, declared modifier ownership,
pattern predicates. The original interval cast-input differential retains all
136 controls. No test deadline, expected SQLSTATE, input or original assertion
was weakened.

Normal build and repeat pass with all 58 current own object receipts, cache
signature and unchanged object hashes. Start/terminal source seals and frozen
binary comparisons pass. No foreign objects are used.

Frozen binary: `dbms_main.alter-array-envelope-final.frozen`.
SHA256: `d87242eff21b2c19f55d089c9dddf95c7ad1b1f85dda2843d211913baaa2fd30`.
Source seal: `51f7d381d04ac2eca476d1631b604b5589cb682dd2ed21d007733877344a94f3`.

This independent repair is in the private worktree, not yet imported into
master while its preceding full regression is running. It does not close the
entire array/type family or alter the original 273 item statuses. The separate
global-aggregate test still has four NUMERIC modifier failures and a ROW
constructor failure. No push or GitHub Actions activation.
