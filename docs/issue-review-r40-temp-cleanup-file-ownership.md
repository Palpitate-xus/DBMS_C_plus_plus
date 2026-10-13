# R40: TEMP filename cleanup must preserve persistent relation ownership

## Defect and correction

R19's extended restart fixture found that a valid persistent table named
`__tmp_12_` kept its table-list entry but lost its schema/heap/index files at
startup. The filename `__tmp_12_.stc` satisfied the TEMP prefix predicate even
though the actual table name did not. Column indexes such as
`__tmp_34_tag.idx` or `__tmp_12_orphan.idx` owned by persistent table `__tmp`
had the same ambiguity. This could lose persistent data on an ordinary restart.

Cleanup now checks the original durable table list before deleting a
TEMP-looking file. A non-TEMP relation's exact sidecar stem or the existing
shared `isRelationPhysicalFileName` ownership contract protects that entry.
It is not a broad neighboring-table prefix exemption: an actual orphan TEMP
heap can still be removed while a persistent column index with a similar
basename is retained. Persistent ownership takes precedence over inferred
backend-prefix ownership; late TEMP list/namespace cleanup remains unchanged.

## Actual evidence

Private baseline `7577835d` (R19) in
`/tmp/dbms-review-temp-startup.GM7uTNvC/repo`.

| Verification | Original | Candidate |
| --- | --- | --- |
| Original extended R19 known-gap fixture, unchanged SQL/native assertions | exit 134 | exit 0 |
| New file-ownership fixture | exit 134 at missing persistent index | exit 0 |
| Six complete stale startup/enumeration, TEMP restart/prepared, UNLOGGED and schema-marker neighbors | not used as red reproductions | all exit 0 |

The final ownership fixture explicitly verifies four persistent schemas/heaps,
primary and secondary index contents, TOAST definition files and actual stored
values. It also checks three actual TEMP heaps disappear, including a dotted
logical name and an orphan whose basename resembles a live persistent column
index. The original full known-gap fixture is still present and passed without
changing its empty-tail name or weakening its assertions.

The first probe was rejected by the build-receipt guard after an uncompiled
source edit (exit 1), and is not a product failure. A first fixture dereferenced
the deliberately missing index (exit 139); an explicit non-null assertion was
added before dereference, producing the accepted exit-134 reproduction. Both
logs are retained. TOAST file-existence assertions were subsequently added and
the strengthened candidate fixture completed with exit 0.

Logs in the private parent include `r40-native-baseline-final.log`,
`r40-original-known-gap-{baseline,candidate}.log`, `r40-candidate-native.log`,
`r40-final-native.log` and `r40-candidate-build.log`. Only TableManage.cpp was
genuinely recompiled, against unchanged headers and the verified R19 production
cache. Native gates verified all 58 current receipts and unchanged final input
signature. Candidate Main SHA-256:
`65dcfe5ab7284c225b57ed8364d243ea4e15766abb2224266c55790444706733`.

This is ordinary relation-file ownership, not the deferred security/TDE or
checkpoint专项. It does not claim untested external-tablespace orphan cleanup
or complete power-loss recovery coverage.
