# Counted empty enum labels survive physical schema reload

Scope: one independently reproduced TYPE-08 storage-reader defect. TYPE-08
remains partial; this is not enum-family, protocol-suite, or full-suite closure.

## Actual baseline and fix

The unchanged enum sidecar and catalog already accept PostgreSQL's legal
zero-byte enum label. The physical schema writer counts that label and writes
its fixed-width bytes, but the reader discarded any decoded empty label.
CREATE TYPE rank_type AS ENUM ('zeta','','alpha','NULL','it''s') followed by
CREATE TABLE ranks (id INT PRIMARY KEY,r rank_type) therefore produces a
five-label catalog and a four-label physical column. Real baseline native
assertion exits 134 with ENUM_EMPTY_SCHEMA_LABEL_COUNT=4; the same wire
six-row INSERT fails 22023, leaving no rows.

The reader now retains every counted label in order. No schema marker, field
width, catalog OID, sidecar, disk encoding, public header, or SQL input changes.
The candidate actual physical column contains all five labels. An enum whose
only label is '' also remains an enum rather than becoming an empty label list.

## Permanent verification

tests/enum_empty_label_schema_test.cpp checks declaration bytes/order, named
physical and catalog type identity, attribute OID, real native values including
empty label/text NULL/SQL NULL/escaped quote, predicate rank, equality before
and after a B-tree index, and a reopened owner's complete typed values/NULL bits.
The index equality check exercises the existing safe heap fallback for empty
keys; it does not claim a complete empty-key access method.

Registered tests/enum_empty_label_protocol_e2e_test.py uses the same real SQL
values on both endpoints and asserts only genuine builtin output types
(integer/boolean). It checks the five-label and sole-empty-label types, SQL NULL,
empty equality, index-adjacent reads, UPDATE and rollback. The project process
is terminated and a new process opens the same real data directory, after
which all reads are repeated. --reference18 requires actual PostgreSQL
18.6 / 180006 and uses only its own unique schema inside a rolled-back
transaction; its UPDATE undo is a parent savepoint, not a PostgreSQL restart.
The reference and candidate strong wire runs both end 0; the original candidate
baseline fails the identical INSERT. This test deliberately does not assert a
TEXT fallback for custom enum result columns: that descriptor defect is OPEN.

## Build and complete-driver evidence

All artifacts are under /tmp/dbms-enum-ordering.mkADoCwT/.
Baseline source is dbd4dcf7; original 58 normal O2 donor production sources,
relative headers, manifest, flags, original receipts and object bytes were
checked against /tmp/dbms-canonical-forked-archive.jVkV94dk/repo at d661ca4b.
Baseline frozen SHA256:
29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675.
Candidate freshly compiles only TableManage.cpp with the unchanged 57 donors;
repeat build/current all-58 receipts/stamp/freeze audit ends 0. It is NOT a
fresh all-58 build or all-58 sanitizer run. Candidate frozen SHA256:
8e99f763256bc0970d90fa55fe0cf2c35d2231133f6a9b56ec9cf083fc178478.

original-native-baseline.log contains the complete original enum_test (all
seven sections), enum_alter_test (all five sections), enum cache lock-order,
create_type, type_registry and text-comparison drivers, all 0.
schema-native-9.log contains those unchanged drivers, the strong new schema
driver, original native array API and actual old-array-schema drivers, all 0.
candidate-native-schema-4.log adds complete schema_format,
schema_write_atomicity, empty_index_equality and index_scan_null_metadata, all 0.
No original assertion, SQL, deadline or failure control is removed or relaxed.

The broader default-disk seven-wire run ends 1: two pass, three query Timeout
failures and two startup connection-abort failures. Complete original
postgres_protocol_test on the old frozen binary also times out at the same
CREATE INDEX t_id_idx stage; this is not a seven-wire PASS.
The separately recorded TMPDIR=/dev/shm seven-wire diagnostic ends 1:
six pass, with unchanged original postgres_protocol_test at line 2871
(UPDATE WHERE id BETWEEN 2 AND 3, actual rows []). The exact old frozen
binary on tmpfs fails the same original assertion. Tmpfs evidence does not
replace or erase the original default-disk failures. Both complete logs and
every failure remain recorded; no cause or whole-suite closure is claimed.

## Independently OPEN consumers

The strong raw HashIndex::search("").size()==1 assertion was separated from
the schema-reader driver, not weakened: its separate actual native probe is
134 with zero physical empty-key RIDs. A SQL fallback PASS is not its repair.
That builder/maintenance issue requires an independent commit.

The broad actual matrix additionally finds that SELECT-list enum comparison
uses lexical order while direct WHERE/ORDER BY use declaration rank, and enum
result descriptors incorrectly report TEXT25. ADD/RENAME empty/escaped labels
have separate frontend parsing defects. An identical committed-type
BEGIN/ADD VALUE/INSERT/ROLLBACK sequence proves the candidate accepts a new
value where strict18 returns 55P04, and retains the added label after rollback
(candidate later INSERT succeeds; strict18 returns 22P02). These evidence
probes remain under this artifact directory. None is claimed repaired here;
enum transactions/concurrency, complete comparison/hash, dump/restore and
catalog SQL remain unclosed. Security/TDE stays deferred, no push or Actions.
