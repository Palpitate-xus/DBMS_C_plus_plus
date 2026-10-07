# ALTER COLUMN TYPE loses array/type-modifier syntax

Independent parser/DDL-envelope repair; this does not close array-value ALTER
conversion or the broader array type family.

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
