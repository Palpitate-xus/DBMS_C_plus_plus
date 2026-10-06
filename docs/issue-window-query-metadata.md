# Bind standard window function metadata to actual builtin identity

## Real failure and reference

The exact ROOT75f586b8 fresh58 normal-O2 production group rejects pure
prepareBoundQuery("SELECT row_number() OVER ()") with42883. New native
baseline52486 exits134 with the original structured diagnostic, after
auditing all58 object signatures/stamp and every public header/manifest.
This also blocks the stronger duplicate UPDATE analysis case whose derived
window ORDER key contains CAST('bad' AS INT): PostgreSQL requires22P02,
while the separate routine metadata rejection reports42883 first.

Actual PostgreSQL17.2 reference-signatures.log checks thirteen legal window
signatures and ten error cases with a unique schema, transaction, per-case
savepoints and final ROLLBACK. It preserves OID20 for ranking counters,
701 for percent_rank/cume_dist, 23 for ntile/integer value functions,
25 for TEXT and20 for BIGINT values, even with WHERE false/no input rows.
Missing OVER is42809, wrong signature/namespace is42883 and non-aggregate
FILTER is0A000. This is not a PostgreSQL18 reference claim.

## Repair and preserved boundaries

The owning engine's pure functionType callback resolves supported standard
window query-host metadata without requiring a scalar evaluator callback.
Actual canonical builtin namespace and arity/integer-offset signature are
checked. Wrong namespace/quoted-case and mismatched signatures retain the
original real routine resolver, not an unqualified fallback. OVER and FILTER/
DISTINCT contracts are explicit. No routine/window/row evaluation is used
to infer a result type.

Ranking and ntile descriptors use the resolved canonical routine name;
polymorphic value windows use their actual bound value argument type.
First candidate55227's predecessor1869 fails a real quoted-name type case:
pg_catalog."row_number"() incorrectly becomes TEXT through raw spelling
inference. Diagnostic23516 retains that failure. V2 uses canonical identity
and passes without removing the legitimate quoted assertion.

Only TableManage.cpp production source changes. No public header, API layout,
manifest, registry or new production TU changes. Normal O2 compile28674 exits0;
other57 production sources, all public headers and manifest match the true
ROOT58 optimized basis. No fake private full58 cold-build/cache claim is made.

## Actual final proof

Artifacts: /tmp/dbms-window-query-metadata.tMzQDmUe.
Candidate native55227 exits0: all thirteen positive and ten original precise
error contracts pass. Server link65828 exits0, frozen SHA256
`5cf29e688071456d8d4006255b29465ac883b22c0b5ae2d03ed888e0120f3ee9`.
Final ten fresh adjacent natives53916 exit0. Three original wire entry points
9337 exit0: window_e2e, window_type_protocol_e2e and typed EXPLAIN's46 TEXT/JSON
controls. Every run uses the matching58-source/header production basis.

Initial adjacent23483 exits1 solely because the newer ORDER fixture still
expects checked API to throw directly. Same failure also occurs with the
unchanged ROOT combination9480, which has94/96 passes and both newer
WHERE/ORDER fixture wrapper failures. They are independently corrected in
726affc6 (ROOT06ef197d), preserving all old SQLSTATE/effect/NULL/demand/site
assertions and adding false-result metadata/cleared-partial-row assertions.
Their independent ROOT-basis proof77069 exits0. Neither original failed
gate is relabeled green. This is not a production window regression.

Complete window runtime, general common-type/default coercion, unknown ordinal
input conversion, unsupported range/group/frame modes and duplicate UPDATE
combined final proof remain separate audit work. No complete-family claim,
all-sanitizer claim, push or Actions enablement is made.
