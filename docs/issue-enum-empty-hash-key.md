# Physical hash keys distinguish empty enum/TEXT values from SQL NULL

Scope: a second independent consumer defect after the counted empty-enum
schema reader. TYPE-08 remains partial. This does not close enum projection
comparison, custom-type descriptors, ALTER SQL grammar, enum transactions,
concurrency, complete catalogs, dump/restore, or the full test suite.

## Real counterexample and implementation

The first independently committed schema repair is 89ea4d11. Against its
actual normal O2 TableManage object and the proven unchanged 56 non-main
production donors, the complete new physical hash native driver preserves
the original HashIndex::search("").size()==1 assertion and exits 134:
ENUM_EMPTY_HASH_ACTUAL_RIDS=0. The same actual five-label type and six rows
include a non-NULL empty enum label, a text NULL label, and a separate SQL NULL.
The hash builder had skipped every zero-byte value, conflating real empty keys
with absence. Successful SQL heap fallback was not evidence of a hash repair.

Hash maintenance now follows the real NULL bitmap instead of value.empty().
It retains genuine empty enum/TEXT keys through build, INSERT, UPDATE,
DELETE, versioned undo, INSERT undo, deleted-row restoration, cold hash
repopulation and VACUUM FULL hash repopulation. Nonempty key encoding,
HashIndex's own file format/checksum/public API, B-tree/bloom behavior,
WAL logic, schema markers, public headers and object layout are unchanged.
Only the hash population branch is changed in rebuildIndexesAfterRecovery;
its relation enumeration/admission policy is not replaced.

OLD keys are not new writes. During native enum RENAME the new schema no
longer admits the old empty label, but its old physical key must still be
removed or restored by undo. The candidate's initial attempt incorrectly
used current label admissibility there. The added full native rename control
found the stale old empty key (134), and the final private helper identifies
the enum datatype and the actual NULL bit independently. DML still validates
every new value before heap/index mutation.

Old valid hash files may omit empty keys. DELETE/UPDATE allow only that known
empty-key omission; a missing nonempty old mapping still fails normally.
SQL retains its safe empty-key heap fallback because such old files have no
durable full-empty-coverage marker. The native test recreates an actual
historical omission using HashIndex's real remove/flush API, then verifies
unchanged-value UPDATE installs exactly the newly versioned RIDs. It does not
invent schema bytes, silently accept a missing nonempty mapping, or claim
every historical access path is upgraded merely by opening it.

## Permanent exact controls

tests/enum_empty_hash_key_test.cpp preserves the exact original raw empty-key
assertion and checks every indexed RID against the genuine visible row and
NULL bitmap. It covers initial build; INSERT; nonempty to empty; empty to NULL;
NULL to empty; UPDATE/DELETE inside savepoint and full transaction rollback;
historical omission upgrade; exact cold index key counts; VACUUM FULL's real
RID rewrite; native empty-to-blank enum rename and cold named-schema labels.
It also covers ordinary empty TEXT versus SQL NULL/text NULL and real INSERT,
UPDATE and rollback key maintenance. No expected value or original assertion
is loosened to make a failing candidate pass.

Registered tests/enum_empty_hash_protocol_e2e_test.py reuses every original
empty-label SQL/value/NULL/rollback/cold-process assertion with actual
CREATE INDEX empty_rank_idx ON ranks USING HASH(r). The existing B-tree
mode is unchanged. --reference18 verifies real 180006 and uses only a unique
owned schema in a rolled-back parent transaction; no shared public mutation.
All asserted result types are true builtin integer/boolean descriptors.
This does not mask the independently OPEN custom enum TEXT25 descriptor.

## Current build/evidence boundaries

Artifacts: /tmp/dbms-enum-ordering.mkADoCwT/. Baseline is dbd4dcf7 plus
89ea4d11, not the latest ROOT combination. Original production donor sources,
relative headers, manifest, flags, original all-58 receipts and bytes were
checked against d661ca4b before reuse. Each candidate freshly compiles only
TableManage.cpp; its other 57 production objects are byte-proved donors.
This is not a fresh all-58 build, sanitizer build, or latest ROOT-master gate.

Final normal O2 V2 build, repeat build, all current 58 receipts, original
57 donor bytes, unchanged headers/stamp and immutable freeze audit end 0.
Final frozen SHA256:
28aac00ed519675226d3dfb9423071b74dd16c37c172990c865832d8a09bb42e.
hash-v2-final-strong-native-default-disk.log is the complete final strong hash
native matrix on the original default disk, terminal 0.
hash-v2-final-complete-18-native-tmpfs.log is the separate full-driver
TMPDIR=/dev/shm regression group, not a default-disk group. Its exact 18
complete drivers include both new enum drivers; all original enum_test
(seven sections), enum_alter_test (five sections), lock-order/create_type/
type_registry/text comparison; generated indexes; empty-index equality;
index NULL metadata; INSERT/UPDATE/DELETE index-failure controls; actual native
array API and historical schema; schema-format and schema-write atomicity.
Its authoritative group terminal is 0, with all 18 complete-driver markers.

Final V2 complete strong HASH and unchanged B-tree cold-process wire runs
both end 0 on tmpfs; strict18 complete HASH oracle ends 0. The initial
default-disk HASH startup abort and B-tree query Timeout remain in
hash-strong-wire-candidate.log / hash-combined-empty-wire.log. They are not
made green by the tmpfs diagnosis. Earlier broader default-disk wire failures
and both exact old/new tmpfs postgres_protocol line-2871 failures are retained
in the first issue's logs. No full-suite PASS or cause of disk failures is
asserted. Fixture compile failures (wrong optional UPDATE API and unqualified
SqlRow alias) and the genuinely failing V1 rename control are also retained.

Independent remaining enum data gaps are recorded in
docs/issue-enum-empty-label-schema.md. No push or Actions; skipped security/TDE
work remains deferred.
