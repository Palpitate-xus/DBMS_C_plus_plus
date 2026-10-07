# Actual bound UPDATE source consumers

The old UPDATE FROM adapter separately resolved RETURNING against only the
target, accepted ambiguous names, dropped SQL NULL RHS updates, confused an
ordinary source text `NULL` with NULL, and could reject or skip real volatile
consumers. Correcting the original source-DML name oracle exposed this runtime
gap; its failures are retained independently of the DELETE consumer.

The frontend now consumes the actual whole prepared non-DEFAULT UPDATE FROM
through the existing prepared mutation runtime and real source provider. The
direct public native bridge also consumes the exact prepared UPDATE root through
BoundDmlExecution. A private native provider builds real typed table scans,
retained derived/view query cursors and JOIN contexts. Each source occurrence
keeps its prepared ordinal, column descriptor, actual NULL bits and collation;
outer joins supply typed NULL rows. No row is rendered into replacement SQL.
Pure construction does not open a scan or execute a routine. Native execution
creates its bound writer once inside the atomic statement owner rather than
discarding a throwaway preflight AST and re-parsing another string consumer.

The DEFAULT, ONLY, cursor and view-target boundaries retain their existing
owners. This change does not replace ROOT's newer DEFAULT callback/lowering
implementation. It changes only main.cpp and DmlExecutor.cpp: no public header,
layout, WAL, SID, row retirement, snapshot or storage generation change. Native
source function-range/writing-CTE lowering and complete cursor-positioned
mutation are not claimed closed. The standalone native DELETE USING adapter is
a separate, still-open consumer, not silently covered by a frontend success.

The new full wire matrix passes a strictly checked PostgreSQL 18.6 reference
(server_version_num 180006). It retains exact NULL/empty/text-NULL seeds,
Unicode padded CHAR, BYTEA and nondefault array bounds. Controls include pure
42702/no-effects, FALSE demand and zero-row descriptors, missing function 42883,
constant-planning 22012, real volatile SET/WHERE routines, exact sequence counts,
same-owner audit writes and late rollback, parent prior NULL writes/savepoints,
source NULL assignments, OLD/RETURNING provenance, nullable LEFT sources and
duplicate sources affecting a target once. The unchanged 15-second wire deadline
is used on both sides. The original source fixture retains ambiguous SQL as
negative controls and qualified positives. Its original LEFT JOIN SQL now
actually succeeds on our typed source plan; the historical 0A000 contract is
preserved in earlier logs, not treated as a PostgreSQL requirement.

The new native fixture uses the public bridge and actual SQL DDL catalog setup.
It checks the same pure name/type errors, zero-row metadata, true projection
effects, implicit/parent rollback, NULL bits, text/empty distinctions, actual
array lower bounds and nullable outer sources. Because SQL RETURNING has no
ordering contract, only complete (row, NULL-bitmap) pairs are sorted for its
unique-id comparisons; every original expected cell and bit is retained. The
first authored ordering assumption's three failed assertions are retained.
The exact final native fixture fails on matching old production objects with
twelve assertions/exit134, and passes on the new consumer.

Private tree: `/tmp/dbms-update-from-carrier.Pjk5Ul4Z/worktree`, based on
`b9b0c3d8583a9e4487dafd70f245099d2e843973`, not ROOT's newer public-header epoch.
The initial 58 objects were explicitly migrated from the immutable DELETE tree
only after comparing every source, all relative header names and bytes, flags,
manifests, old receipts and object bytes. New-path receipts were regenerated
after comparisons. Both changed CPPs were then officially compiled with O2,
and all 56 unchanged production objects re-audited. This is not 58 fresh CPP
compilations. Normal frozen SHA256:
`c55b40a08a916c6a5e78e80915499f5a4ab94bc6128f71935be8a2a0f1484c04`.

Scoped ASan/UBSan instruments the two changed CPPs plus native test/stubs; its
other 56 production TUs are matching normal objects. Leak detection is disabled;
this is not all-58 sanitizer coverage. Scoped frozen SHA256:
`8eef6faa13e836c4ab4ce47a228c901aa7cc202c998e0c0dcbfd5a6c0ea86400`.
All exact failures, original/strengthened native runs, strict-reference and
complete whole-fixture logs remain in the private artifact directory. The
initial adjacent native group really fails at the original dml_semantics
ambiguous RETURNING oracle; that result is not silently registered as green.
