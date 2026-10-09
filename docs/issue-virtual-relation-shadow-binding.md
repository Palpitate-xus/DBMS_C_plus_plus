# Physical targets shadowing virtual relation descriptors

The unchanged original pg_stat_activity protocol entry fails at INSERT into a
same-name user table with XX001: UPDATE default target has no physical column
metadata. It is reproduced against the verified preceding ROW frozen program;
artifact:virtual-relation-original-baseline.log, actual exit1.

The pure whole-query relation metadata callback always selected its virtual
pg_stat_activity/pg_settings descriptor before checking the same-name user
table. Main's established dispatcher already excludes those virtual paths
when a same-name user table/view exists. Consequently SELECT binding rejects
the real id column and INSERT/UPDATE default preparation targets a nonexistent
catalog heap instead of the actual user table.

Binding now uses that same existing table/view existence admission, without
opening sources or running a query. Explicit pg_catalog qualification remains
virtual. pg_class retains its existing dispatcher/catalog identity. pg_type's
preceding shadow admission is unchanged and tested as an adjacent control.
This restores the existing project contract; it is not a claim that implicit
pg_catalog search-path precedence matches PostgreSQL in all namespaces.

New native binding fixture retains physical identity, one INTEGER/id descriptor,
explicit public qualification, unqualified SELECT/INSERT/UPDATE and qualified
catalog access for pg_stat_activity, pg_settings and adjacent pg_type.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Initial native fixture compile9702 failed due to a missing DbError include.
The next two diagnostic generations99732/53242 exit1 with19 controls/12fail;
six failures are actual shadow-binding errors, six are fixture type errors.
makeIntColumn scale1 is the legacy tinyint slot, not INTEGER; actual descriptors
show smallint. The fixture now requests the real scale2 INTEGER slot without
weakening any type assertion. Corrected unchanged-source30701 exits1 with
19controls/6fail; all qualified physical and adjacent pg_type controls pass.

Affected actual private CPP94653 exits0. Expanded42803 exits1:27/27 native and
17/18 protocol entries pass. Native19/0 and the entire original pg_stat_activity
and pg_settings entries pass, as do all preceding ROW/composite/planning controls.
The complete default protocol entry fails at its original role_rows assertion
([]), retained without altering its SQL, assertion or deadlines. No new filtered
investigation is opened. The entire unchanged entry is being repeated against
the exact same frozen generation; repeat outcomes cannot erase the first failure.
Original BIT384/22944 exits0 against verified180006.
All58 current own receipts/cache/repeat/source/frozen terminal fences pass.
No foreign objects or new public header are used; only the affected own CPP was
rebuilt, with the other current own57 receipts validated, not a fresh58 claim.
Frozen:dbms_main.virtual-shadow-final.frozen,
SHA256:c823783546e7e8dd1b803f3aba83470b10a54d25b9e144edf1ddb240c3daa079;
source seal:fc3346b1d6d4cffdac95bd22d95692d1a7e23fca54ef6df989db7af841479569.
The first failed matrix is not a whole-suite pass or completion claim.

Entire unchanged default protocol repeat91426 exits0 on the exact same c823
generation and unchanged deadlines. This does not erase42803's failed complete
matrix. Entire27native/18protocol matrix7823 now exits0 with all original
entries intact, a new immutable frozen filename and no source edits. No filtered
diagnosis, assertion rewrite or deadline extension is performed.

All58 current own receipts/cache/no-recompile repeat/source/frozen terminal
fences pass again. New frozen dbms_main.virtual-shadow-repeat.frozen has the
same c823783546e7e8dd1b803f3aba83470b10a54d25b9e144edf1ddb240c3daa079
bytes and fc3346b1d6d4cffdac95bd22d95692d1a7e23fca54ef6df989db7af841479569
source seal after actual normal builder checks. The independent repair is
approved for its own local source commit, not a whole-family/total-goal claim.

Next ordinary SQL-value diagnosis69124 exits0 as an artifact-only probe:
actual prepareBoundQuery rejects current_user/session_user/current_role/
current_catalog/current_schema with42883, and temporal keywords have missing
or wrong metadata. Verified180006 reference logs all ten actual labels/OIDs,
valid quoted current_user callees, invalid bare-keyword parentheses and
precision grammar. Actual pg_proc lists only four corresponding genuine
zero-arg NAME/STABLE callees (current_database/current_schema/current_user/
session_user). That independent grammar/type/context issue is not patched by
this relation binding change and remains open.

Master full18383 remains live on unchanged ROW source795479c6/02c69e42 frozen
generation with773native/Main/435wire. Master stays186scoped/771auto+2Main/
435registered/58TU. This private candidate is not imported while that full
driver runs. All original273 statuses/scope and user-deferred investigations
remain unchanged. No push or Actions activation.
