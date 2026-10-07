# DROP DOMAIN DEFAULT native oracle correction

The original domain_ancestry_metadata_test expected a newly created outer domain
to resume its parent's default after DROP DEFAULT. Strict PostgreSQL 18.6
(180006) disproves that assertion: typdefault becomes SQL NULL and an INSERT
using DEFAULT into the inherited NOT NULL domain raises 23502. The unchanged
scenario is recorded in native-default-correction.reference18.log (exit0);
the initial mistaken protocol reference remains in reference18.initial.log.

The independent test-only correction asserts absent own default for a new D3
record. A genuine five-field legacy_outer record preserves the original old
parent-fallback assertion instead of pretending a stale caller structure can
downgrade D3. All width, quoted identity, cycle, corruption and constraint
assertions remain. Final eight-native group 42608 and the production3 scoped
sanitizer group 14525 both pass. Production D3 persistence is a separate commit.
