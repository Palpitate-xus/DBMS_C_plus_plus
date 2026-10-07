# Canonical typed Append cells

The common descriptor is canonical, but an already compatible branch could
retain its CAST spelling in the emitted `ExprValue.typeName`: typed SQL NULL
from `NULL::BIGINT` was `BIGINT` while the actual set output was `bigint`.
After the existing exact canonical source/target validation and any required
typed coercion, Append now publishes the declared canonical common type.
It does not change NULL/value bytes, codec, width, or conversion demand.

The original native Append matrix gained `SELECT NULL::BIGINT UNION ALL
SELECT 2`, retaining a strict `bigint` typed-NULL assertion. The additional
set-clause execution native first reproduced the raw type as an actual
exit-134 (`candidate-v1/prepared_set_clause_execution.expanded.log`). Both
native controls pass with normalization in the matching final V2 object
group (`build-candidate-v2.log`) under
`/tmp/dbms-set-clause-ownership.oSu5ps6G`. That group includes the separately
pending set-clause binder/consumer changes and genuinely fresh 58-TU header
epoch; it is not a standalone old-header binary or formal O2 claim. This
one-line production correction itself changes no public header or layout.
