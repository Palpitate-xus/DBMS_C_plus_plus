# Dynamic table use of the domain's own default

The preserved baseline uses a table created before ALTER DOMAIN SET DEFAULT 2,
but INSERT omitted/DEFAULT still writes 1. DROP COLUMN DEFAULT loses the domain
fallback, and a domain with no creation default ignores a later SET DEFAULT.
Changed defaults also bypass the expected NOT NULL/CHECK result by retaining the
old value. The complete strict180006 reference retains the same SQL, rows, types,
sequence effects and rollback assertions.

Column now records DefaultOrigin: LegacyFrozen, Column, or Domain. Only the Domain
origin refreshes from the actual domain metadata. Explicit column defaults win,
including NULL and a literal equal to the domain's old value. Omitted input gets
the current default; explicit SQL NULL does not. ALTER COLUMN SET/DROP DEFAULT
updates that origin through the real schema writer and catalog attributes.

Schema B (0x4442000B) retains the prior payload, full DFT1 expressions, range
endpoints and RID1, then appends DSO1 origin/name cells. It is bounded and rejects
invalid origins, counts, names, truncation and trailing bytes. Existing schema9/10
files retain their exact default bytes as LegacyFrozen. The old default flag
cannot distinguish a copied domain default from an explicit column default;
equal values are never used to guess that provenance. There is no automatic
rewrite of user data or ambiguous legacy defaults. An explicit ALTER COLUMN
SET/DROP DEFAULT opts that column into a known source. Older binaries fail closed
on B; retaining backups and using the new binary consistently is required.
RID1 and heap tuple encoding do not change. Legacy9 parser acceptance does not
prove that downgrading a live SID1/WAL generation is a valid cold-start migration.

ALTER DOMAIN default changes use the genuine DDL transaction/savepoint snapshot.
Physical restore reads physical layouts without logical default decoration under
an engine/thread-owned RAII scope; its retained transaction schema can otherwise
refer to domains absent from the restored directory. Normal metadata/DML still
resolves defaults, and domain-input checks remain fail closed. The initial three
rollback regressions and their actual stderr trace remain preserved.

New native tests cover source identity, same-value/NULL priority, cold metadata,
B with long defaults, legacy9/10 bytes, RID1 and failed-restore scope cleanup.
Whole scripts cover default VALUES/omission, nested creation snapshots, NOT NULL,
CHECK, multirow atomicity, real nextval effects, metadata-only P/D, full rollback,
savepoint recovery and committed existing objects. No timeout/assertion was relaxed.
Private all58+stubs O0 plus matching CPP-only rebuilds are not normal O2/full-suite
proof; ROOT integration must rebuild its current headers together.

UPDATE DEFAULT's old consumer boundary, generic default-expression input priority,
stored-routine identity freezing, export/inheritance edge cases, domain-over-array,
composites/general casts and constraint revalidation are not closed by this work.

Final evidence under /tmp/dbms-domain-default-origin.TFPrh6Es: the immutable
pre-fix whole script exits1 with 11 failed assertions (baseline.domain-default.log,
not 11 distinct bugs). Initial reference expectations were corrected only after
the retained strict180006 failure; the expanded snapshot/barrier reference and
the separate transaction reference both exit0. Candidate V1's three rollback
script failures and observed snapshot=0 trace remain preserved.
The final expanded 12-script serial group 67084 exits0 with all original SQL,
row/OID/state/effect assertions and deadlines intact; no candidate server remains.
Eight natives 42608 exit0, including source identity, legacy formats, cold metadata,
RID and restore-scope cleanup. Scoped ASan/UBSan production3 plus five drivers
14525 and final two drivers 19547 exit0. The final candidate SHA256 is
282db4e70d4f52f6b3733830a6d0f189af1dacdfde714618f7bcc554f25c70fd.
All58 source, header and object receipts are retained in candidate-v1/candidate-v3;
post-commit verification must retain the exact same production bytes.
