# Register builtin NAME for ordinary declarations

The original ledger defers only15 security/TDE requirements, not ordinary NAME
declaration correctness. Prior reports calling this CREATE-error user-filtered
overstated that scope; all historical failing controls remain preserved.
No permission, authentication, ACL/RLS or SECURITY DEFINER code changes here.

Builtin NAME is present in runtime values/catalog OID19, but missing from
TypeRegistry's declarations. Ordinary RETURNS NAME consequently fails42704.
The unchanged whole33-control keyword fixture18183 exits1 with five NAME
creator/dependent failures on preceding frozenbeb50/a85e. Actual reference18.6
passes all33 controls, with the same SQL, assertions and deadlines.

Register canonical name with actual64-byte/c/plain/String metadata, no typmod,
and its existing array identity. The original complete type_registry test adds
strict metadata/type-mod/alias/OID assertions and passes9061. Own affected
registry CPP19174=0, normal build succeeds58952; original whole33 wire controls
now all0. Real quoted char/unquoted CHAR identities remain separate.

The first new native fixture authoring attempt65279 used nonexistent typed-cell
fields on QueryResult's string rows and failed compilation; correct the fixture
to actual structuredRows/structuredNulls without changing expectations.
Corrected preceding native22903 fails134 at missing NAME registration.
After registration52395 reaches all three valid routine declarations, then fails
134 on a separate existing bound-output OID0 defect. That assertion is retained
for the next independent binder repair, not weakened or hidden. Registration
and protocol shape pass do not prove all native result-OID requirements.

Own current58 production receipts/cache/normal no-recompile repeat and frozen
inputs are verified for this scoped declaration commit. No foreign objects or
fresh58 compilation claim. Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Logs:name-declaration-whole-original-baseline.log,
name-registry-original33-reference18.log,name-registry-unit.log,
name-declaration-registry-whole33.log,name-declaration-typed-baseline.log,
name-declaration-initial-native.log.

This is ordinary declaration registration, not complete NAME column storage,
length/collation/array semantics or the complete TYPE/FUNC family. All original
incomplete requirements remain. No push or Actions activation.
