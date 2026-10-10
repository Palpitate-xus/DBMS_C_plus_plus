# Bind routine result declarations with their catalog OIDs

After independent NAME registry repaire55c863e, the strict native declarations
are accepted but query output still loses its type OID (52395 exits134). The
function binder previously returned only metadata.functionType's spelling,
unlike CAST/CASE declaration binding. Registered SRF elements did the same.

Route both actual scalar and registered SRF result declarations through the
existing metadata.declaredType owner, retaining type spelling plus actual OID.
This does not execute a routine or provider to infer metadata, substitute a
first row's datum type, or invent a builtin OID for an unregistered user type.
No registry/header/layout changes in this separate binder repair.

Own affected binder CPP8552 exits0. Strict native NAME parameter/result/OID,
NULL return, scalar lower/length/abs and registered unnest checks63477 exit0.
The preceding valid NAME/OID assertion remains exact; no output expectations
or original positive controls are relaxed. Original whole keyword33 now pass
with NAME registry and this binder, as does actual PostgreSQL18.6 reference33.

An added generate_series mixed scalar/SRF positive fails42883 in the prepared
host (77428 exits134). Actual verified18.6 confirms its two rows and OIDs
25/23/23/23. The host currently registers only unnest and pg_listening_channels;
generate_series needs its own implementation, not a fabricated scalar callback.
Preserve the exact discovered query in a separate permanent strict native test,
generate_series_bound_metadata_test.cpp, and include its real failed execution
in the expanded matrix. The NAME/OID fixture also checks the actual existing
unnest provider branch; that does not replace or weaken generate_series's case.

Expanded35native/23wire14272 ends1:34/35native and22/23wire pass. The ordinary
generate_series positive fails134, as expected from the missing host. Original
default protocol's unchanged pg_notify empty-row assertion also fails. A direct
comparison shows preceding frozenName registry and actual18.6 return the empty
VOID cell/OID2278, while the candidate's computed-argument form fails42704
because the shared builtin catalog producer lacks its VOID row. Do not commit
that regression or bypass the declaration resolver.

The metadata repair includes the actual VOID2278/4/byval/p/P/i/p/no-array/no-
collation catalog row, verified directly against actual18.6. Original VOID
native80712 fails134 at its missing row. Own affected catalog CPP54822=0;
strict cold/warm/disk/pure-binding native62544=0, and all three original-shape
pg_notify probes now match old/reference. No header/layout change or security
mutation. Corrected36native/23wire53041 now ends1:35/36native and23/23wire pass.
The only failed entry is the independently retained valid generate_series
binding case. All original33native, new NAME and VOID native, all original23wire
and the entire unchanged default protocol (including its original pg_notify
assertion) pass. All58 current own receipts/cache/normal repeat/source/frozen
terminal checks pass. Original BIT384/48089 against verified180006 exits0 on
the same corrected frozen binary. Both original failed generations remain.

The usable result metadata repair includes both binder and required real VOID
catalog identity in one independent atomic source commit. The positive
generate_series regression is separately committed as a test and remains
unfinished, not an expected error. Neither whole matrix nor complete SRF/TYPE/
FUNC family is marked complete. No public header/layout change or fresh58 claim.

Corrected frozen:dbms_main.routine-result-oid-void-corrected.frozen.
SHA256:6e64993ec0bc497a754e2e222cb34defecac3ffd9dc597ad2f2402df9d35e5a5.
Source seal:eac6f79b7b6f0aab7a086e25faa4b3c733ba31054ed2c1a61e65c4a5d5a1285d.
Log:routine-result-oid-void-corrected-gates.log.

Original33native are retained, plus three new strict native fixtures. The
complete reference-only three-query metadata/result probe also passes, including
NAME's64/c/p/S/1003 catalog row and both SRF queries. Broader SRF overloads,
general user-type routine identity, NAME storage/length/collation and all original
incomplete TYPE/FUNC/QRY requirements remain; no family or goal completion.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Logs:routine-result-oid-initial-gates.log,routine-result-oid-owned-provider.log,
routine-result-oid-initial-native.log,name-and-srf-reference18.log.
No push, Actions activation or implementation of the15 deferred security/TDE
requirements. Root full85873 stays frozen on190 repairs, not this candidate.
