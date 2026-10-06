# Preserve precise errors through checked scalar test consumers

ROOT checked-plan35594bc0 retains SQL failures as an explicit false result,
original SQLSTATE/message/exception, and cleared partial rows. SQL hosts
must use throwIfFailed to propagate that original exception. ROOT75f586b8
already adapts the real prepared scalar WHERE/ORDER host accordingly.

The later new original WHERE and ORDER native fixtures still expected
executePlanChecked itself to throw. True ROOT fresh58 normal-O2 combination
9480 exits1 with94/96 passes and exactly these two assertion failures.
This is retained in /tmp/dbms-ready-integration.n9mTFFAn/focused-native.log;
it is not a fully passing original gate and not a window metadata regression.

The two fixture run wrappers now explicitly inspect false-result error
metadata and absence of all partial row/cell/NULL outputs, then call the
original typed throwIfFailed. Every existing P0001/22P02/21000, NULL versus
empty/text-null, ordering, row demand and genuine-site count assertion stays
unchanged. No accepted SQLSTATE or expected effect is broadened.

Both freshly compiled/linked originals on the exact unchanged ROOT58 optimized
production group pass with session77069/exit0. All58 signatures/binary stamp
and unchanged public headers/manifest were audited first. Artifact:
/tmp/dbms-window-query-metadata.tMzQDmUe/checked-contract-native-v2.log.
This does not change production source/layout or erase the original failed
96-native result. The remaining WITH wire failure, window metadata work,
quantifiers, sort identity and complete audit families remain separate.
