# Verify the binary SP-GiST sidecar, exact coordinates and safe fallback

The canonical full run on `1e0a8c5a` failed the old geometric test's search for
decimal point text inside a V2 binary sidecar. Production encoding stores
checksummed IEEE-754 coordinates and physical RIDs, not decimal text.
Production code is unchanged.

The corrected test decodes V2, checks its three entries and both adjacent-double
coordinate bit patterns against independent heap-derived RIDs. It then checks
both exact RIDs through malformed-sidecar heap fallback and restored-sidecar
reload, using fresh StorageEngine owners for each phase. Existing heap precision,
geometry validation and invalid search predicate assertions remain intact.

Corrected V2 test session 88512 exited 0 against the original ROOT 55 production
objects, fresh O2 test compilation and matching stubs in an isolated directory.
Log: `/tmp/dbms-insert-omitted-null.iHZNTrqu/geometric-corrected-v2.log`.
First correction session 85756 exited 134 because the fixture encoded RIDs with
the wrong shift (16 instead of the production format's 32); that failure log is
retained. This is a stronger format-aware test correction, not a production
precision fix or full geometric/index-family completion.
