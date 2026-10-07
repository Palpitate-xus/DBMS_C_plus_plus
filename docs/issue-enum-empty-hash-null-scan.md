# Empty hash-key follow-up: do not search SQL NULL's nonexistent OLD key

This is a narrow follow-up to `4724583a`'s real empty-key maintenance, not a
replacement for its schema/index correctness evidence. The original READY tree
and its input/binary receipts remain unchanged.

## Root cause and change

DELETE computed `hidx->search(val)` before checking whether the actual OLD image
was indexed. UPDATE similarly computed `oldMapped` unconditionally. SQL NULL's
stored bytes are empty, so these paths copied and searched the entire legitimate
empty-key RID bucket even though NULL has no OLD hash entry. With many empty
values and many NULL updates/deletions this repeated unnecessary work depends on
the size of the unrelated empty bucket. This is a source-path diagnosis, not a
claimed measured benchmark or new NULL/empty correctness counterexample.

Search OLD keys only when `hashKeyIncluded` says the actual OLD image is indexed.
Retain the historical omitted-empty-key compatibility and all existing missing
nonempty-key failures. No key format, index layout, public header, SQL fallback,
transaction behavior or recovery enumeration changes.

The existing permanent `enum_empty_hash_key_test.cpp` keeps its original exact
raw `search("").size() == 1` assertion and full lifecycle/undo/legacy omission/
vacuum/native rename/cold restart/TEXT controls. It additionally constructs 65
real empty-key RIDs plus 16 NULL rows, then performs unchanged-NULL UPDATE and
DELETE and verifies the exact original empty-key RID vector remains unchanged.
This is a semantic guard; the no-NULL-search property follows from the production
short-circuit, not a timing threshold.

## Verification and version boundary

- Base: private Source2 `4724583a2ce3a31b91b1bb4aefffc73f4b715e04`, not current ROOT
  master or its separate BIT expression change.
- Normal production O2 build and repeat: terminal 0. Sole TableManage.cpp fresh;
  other 57 objects have matched source, all headers, manifest, compiler/flags,
  original donor receipts and exact object bytes. All 58 current receipts valid.
  This is not a claimed fresh-58 build.
- Frozen candidate SHA-256:
  `20ca1c762e52acf28349d4e00e40410482502db92b24fb9b6c1f2b1872892ddb`.
- Original strong enum hash native plus the new large empty/NULL control, normal
  O2/default disk: terminal 0.
- Full original 18 native drivers, normal O2 with `TMPDIR=/dev/shm`: terminal 0.
  Includes the complete original enum, ALTER, cache-lock, create-type, registry,
  text comparisons, both new enum fixtures, generated/empty/NULL indexes,
  INSERT/UPDATE/DELETE fault controls, resolved and actual-old-schema arrays,
  strict schema format and atomic schema writes. This execution-location scope
  is separate from default-disk evidence.
- Original complete HASH protocol including rollback and actual cold-process
  restart, normal O2/default disk: terminal 0. Exact strict PostgreSQL 18.6
  (`server_version_num=180006`) HASH reference matrix: terminal 0, unique owned
  schema in rolled-back reference transaction only.
- Original complete BTREE protocol and strict 18.6 reference matrix: terminal 0
  with the same original SQL/assertions/deadlines; the candidate run is also
  normal O2/default disk with its actual cold-process restart.

External preserved receipts/logs are in `/tmp/dbms-enum-hash-scan.MO0T7otl/`:
`build-followup.sh`, `build-followup.log`, `native-strong-default-disk.log`,
`native-complete-original-18-tmpfs.log`, `wire-hash-default-disk.log` and
`strict-reference18-hash.log`, `wire-btree-default-disk.log`,
`strict-reference18-btree.log` and `audit-followup.log`. An initial driver invocation using nonexistent
`enum_alter_type_test.cpp` was an infrastructure naming error after the complete
original enum driver passed; its failed log remains separately preserved and was
not substituted for the correctly named full 18-driver run.

TYPE-08 projection comparison binding, custom enum protocol OIDs, and unsafe
transactional ALTER ADD/RENAME remain separate OPEN items.
