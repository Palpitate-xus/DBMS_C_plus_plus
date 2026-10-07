# ALTER COLUMN TYPE loses array/type-modifier syntax

The original repair covered the parser/DDL envelope. The subsequent real array
value conversion work is recorded below; neither repair closes the broader
array type family.

The matched private baseline is f6d1476b plus original ARRAY dc7469ea and VIEW
bbc89ca5. Its normal-O2, 58-source audited frozen executable is
`/tmp/dbms-physical-array-typmod.YCjPAKiX/baseline-immutable.udmW5swZ/dbms_main.frozen`
(SHA256 `967ed6a358372fceac37c0ddd0c2eb796a3961c02f992fe26b767ab6341f8c0e`).

`TYPE NUMERIC(8,3)[]` parsed as just `NUMERIC`; SET DATA TYPE also discarded
the comma and array suffix. The DDL conversion helper did not retain array
declaration suffixes. The old native assertion failed with actual `NUMERIC`
(exit 134, `alter-array-envelope-baseline-corrected.log`); an earlier fixture
compile error remains separately retained. The old protocol returned scalar
OIDs 1043/1700 after a nominal array ALTER instead of array OIDs 1015/1231.

Both ALTER spellings now retain qualified/multiword names, modifier separators
and repeated empty array suffixes. The existing DDL envelope converts those
suffixes into ColumnDef.isArray before normal column construction. No new
public header/layout or special value-based type inference was added.

The normal-O2 candidate build 63541, repeated build, 58 source/header/object
signatures and binary stamp passed. SHA256
`571dc3348464a8ac0836d0204c92889058c5be00132dd4b2b18d00f8215efe21`.
Native group 27876 passed the new envelope test and unchanged alter_column_type,
temp_alter_column_catalog and type_alias tests. The full protocol diagnostic
is deliberately unregistered: its NULL-valued modified array ALTER and an
unmodified non-NULL INT[] column now preserve identities/values, but its next
non-NULL INT[] -> BIGINT[] ALTER still fails in storage scalar prevalidation
with XX000. The complete strict PostgreSQL 18.6 (180006) reference passes.
That genuine storage conversion problem remains OPEN, with the original
strong expectation retained in alter_array_type_envelope_protocol_e2e_test.py.

All logs are under `/tmp/dbms-physical-array-typmod.YCjPAKiX/`; protocol default
15 seconds is unchanged and test-owned servers were stopped in finally.
Using tmpfs for this semantic probe is not disk-I/O performance evidence.

## Real array values, 2026-10-07

On ROOT 42bd7168, the original non-NULL INT[] -> BIGINT[] ALTER failed because
storage prevalidation parsed the whole brace literal as one integer. The
rewrite also copied a scalar factory's fixed-width flag, and NUMERIC-array
reinsertion attempted to parse the whole array as a scalar NUMERIC.

The storage primitive now validates the target array layout and converts every
non-NULL element through the existing scalar cast before deleting any heap
file. It retains the parsed element modifiers supplied by the DDL executor,
uses assignment character-width checks, preserves whole-value NULL, element
NULL, empty arrays, and nested dimensions, and clears FSM/VM caches before the
rewrite. Array reinsertion owns its array validation and does not run scalar
validators on the brace literal. No table-schema layout, physical relation
identity, WAL record format, or recovery-generation logic was changed.

All original protocol SQL, assertions, and the default 15-second timeout are
retained. The expanded strict PostgreSQL 18.6 matrix passes. Candidate V4
passes the complete original sequence, real integer/numeric/character values,
element modifiers, dimensions, primitive overflow, assignment rejection even
on an empty table, primary/secondary array index lookups, statement rollback,
and transaction rollback. Its last added unique-array rounding collision
returns the existing generic XX000 instead of PostgreSQL's 23505; that full
failed fixture remains retained and unregistered pending the separate DDL
SQLSTATE correction.

All 58 normal O2 production objects were compiled freshly after the public
storage-method signature change, with later changed parser objects rebuilt.
Every source/header/flags/object receipt, repeated build, and binary stamp
matches. The new direct-storage test and the unchanged envelope, scalar ALTER,
temporary catalog ALTER, and type-alias native tests pass. Evidence is under
`/tmp/dbms-alter-array-values.yC5kFpDd/`, including the preserved
`wire-v1-full.log` and `wire-v4-full.log` failures, native logs, strict reference
logs, and audited build/frozen binary records. Tmpfs protocol data is semantic
evidence only. Crash/restart rewrite-generation validation remains with its
separate storage/recovery owner.
