# Domain creation captures its inherited default

Strict PostgreSQL 18.6 (`180006`) disproved the initial live-parent assumption:
after `CREATE DOMAIN base AS INT DEFAULT 1; CREATE DOMAIN mid AS base;`
changing base to DEFAULT 2 does not change mid's own default. DROP DEFAULT on
mid removes its default instead of reverting to the current parent default.
An explicit DEFAULT NULL is a present default and is inherited at creation.
The initial incorrect reference expectation and its failure are retained.

New domain records use `@D3`: `DomainInfo::defaultResolved` marks a creation-time
snapshot, including an absent-default barrier. Constraint ancestry still walks
all parents. createDomain captures only metadata; it never evaluates a default.
alterDomain preserves the durable D3 barrier even when an embedded caller keeps
its pre-CREATE structure with the new flag unset. It cannot silently downgrade a
new record to legacy behavior. SQL SET/DROP DEFAULT explicitly resolves the new
own-default state.

Legacy five-field and @D2 records remain readable with their historical behavior.
The reader does not fabricate their past creation snapshot from today's values.
Older binaries reject D3: use the new binary consistently after writing D3;
this is not heterogeneous-reader forward compatibility or an automatic migration.

`domain_default_snapshot_test.cpp`, the fresh-table controls in the complete
domain-default protocol script and a strict18 trace of the original native
DROP scenario validate this distinction. The old native parent-fallback assertion
is separately corrected to the real oracle, with a genuine old-record fallback
control retained. Evidence is under `/tmp/dbms-domain-default-origin.TFPrh6Es/`.

This is default creation/alteration metadata, not all domain constraints, array
domains, composite domains, user casts or existing-data revalidation.

Final evidence: strict180006 expanded snapshot/barrier reference exit0;
new snapshot native plus seven adjacent natives exit0 (42608); final complete
12-script serial protocol group exit0 (67084), including the original ancestry,
FK, sequence and provider-path matrices. Scoped sanitizer tests instrument the
three changed production translation units, not the whole executable; both
the five-driver group (14525) and final two-driver update (19547) exit0.
The final private executable is candidate-v3/dbms_main, SHA256
282db4e70d4f52f6b3733830a6d0f189af1dacdfde714618f7bcc554f25c70fd.
Its all58+stubs base is private O0, with source/header-matched TableManage-only
rebuilds. This is not normal O2 or full-suite evidence, and the foundation alone
does not implement the separate dynamic table-default consumer.
