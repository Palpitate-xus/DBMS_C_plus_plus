# Admit every compact BIT list member before scanning

The SQL frontend's retained typed IN expression passed its full controls,
but that did not validate the independent native compact-condition owners.
Both `StorageEngine::queryExpr` and `QueryPlanner` checked only the left
column against a generic BIT marker. Later typed members or invalid UNKNOWN
input were validated during row evaluation, after a first match could return.
Empty and all-NULL inputs bypassed the check entirely. Thus BIT `b` with
compact `inb B'01' 1` could return its matching row instead of PG42883.

Both real pre-scan owners now resolve every actual parsed member against
the schema-owned column type, including both BETWEEN bounds. Pure genuine
literal input is validated through the prepared comparison codec; row
references, ParameterExpr and routines are not executed during admission.
Known row-member types and missing unqualified members use the real schema.
No left operand is evaluated or repeated to prepare. Runtime NULL truth and
first-match evaluation remain unchanged after successful admission.

The queryExpr receiver's cap-zero/cap-one and OR-alternative paths bypassed
filterRows. They now perform this same signature/input admission before
receiver limits, any index/streaming scan or a matching earlier OR branch.
Public headers/layout, deadlines, raw `apicond` provenance and typedexpr
routine metadata ownership are unchanged.

## Actual complete evidence

All new artifacts are under `/tmp/dbms-bit-compact-list.w0fJjNO5/`. Preceding
1c4a/5bb source trees, permanent controls, normal58 receipts and frozen Main
are unchanged; this is a new independent follow-up, not edited old evidence.

- Original permanent `bit_compact_list_pre_scan_test.cpp` stays unchanged:
  fourteen IN/NOT IN/BETWEEN typed/input combinations, populated first-hit,
  empty and physical NULL tables, both actual queryExpr and QueryPlanner.
  Exact frozen 5bb normal production baseline shell19162 exits 1, all84
  complete controls /68 failures, in `bit-compact-list-native-baseline84-v1.log`.
- `bit-compact-list-native84-strict-sql42.log`: every identical SQL counterpart
  to those 84 owner controls against actual strict PG18.6/180006, direct
  exit 0, all42 verified rows/states/no partial command completion. Each
  reference object uses one owned unique schema, dropped afterwards.
- Initial two-CPP candidate shell71577 exits 0, all21 complete native files,
  including unchanged84/old11. This is an intermediate stage: it does not
  claim that the receiver bypass was already fixed.
- Strong independent `bit_compact_list_receiver_admission_test.cpp`: all284
  genuine receiver0/receiver1/OR, PK-consumed and disjunctive planner, real
  FilterOp.open with actual TableScanOp, known/missing row members, unexecuted
  ParameterExpr preparation, legal empty/NULL, and actual volatile stored
  routine checks. Precise frozen 5bb all57-module baseline shell87779 exits
  1, all284 /261 failures; no baseline assertion stops the remaining controls.
  The writer exposes premature admission: invalid input must not write,
  while an admitted projection runs exactly once.
- `bit-compact-list-candidate-normal-native-v2.log`, shell10026 exit 0:
  all22 complete native files, unchanged84/new284/old11 plus full5344/3387/
  235/272/78/16/8, real query binding and current-module group512. Group512
  links production modules, not Main. Invalid real writer writes zero;
  valid receiver demand writes once. Real ParameterExpr is not executed
  merely to prepare its declared UNKNOWN input context.
- Normal uses genuinely fresh TableManage/ExecutionPlan objects plus 56
  source/header/flags/manifest/donor58-receipt/object-byte-proved normal O2
  5bb donors. All current58 receipts, build stamp and immediate repeat pass
  in `bit-compact-list-pre-final-normal-audit.log`, shell57292 exit 0. This
  is not fresh58; ROOT's newer incompatible ABI objects are not substituted.
- New permanent wire36 checks the complete frontend's independent typed
  member/input counterparts across all three populations. Strict reference
  direct exit 0 and frozen Main 5bb baseline shell8353 exit 0: this is an
  honest positive control, not a fabricated frontend failure. The two extra
  unknown BETWEEN bounds are included in full42 reference and native84,
  while the separate frontend grammar coercion remains explicitly open.
- `bit-compact-list-final-v2-wrapper.log`, shell62589 exit 0: eighteen whole
  wire files, new36 plus the entire preceding17 set, and complete three-query
  CLI. Its independent output files are `bit-compact-list-final-v2-*.log`.
  CLI uses three separate processes sharing a persistent directory.
- `bit-compact-list-final-strict-wrapper.log`, shell38752 exit 0: all eleven
  whole strict-reference files, previous ten plus new36. Each connection
  verifies actual180006; original SQL/rows/NULL/OIDs/states/deadlines remain.
- Full original132 and full384 use exact frozen final normal Main, shell
  37477: each actual process exits 1, verifies all132/all384 controls were
  collected, and retains precisely three /49 original differences in
  `bit-compact-list-original132-final-v2.log` and
  `bit-compact-list-original384-final-v2.log`. Only the owned random schema
  name is normalized for auditing: final49 red SQL remain preceding49,
  with no new red SQL. These matrices are not relabeled as green subsets.

Final frozen Main `dbms_main.compact-v2.frozen` SHA256:
`625e4fbc1cee97bb474be2cbad77010e91c6f09cacb26268aa272f770fa27a6d`.

## Still open

Original132 retains the three stored INTEGER empty-string comparisons
assigned to the independent INTEGER repair. Full384 retains VARBIT typed
literal parser recognition, scalar compact UNKNOWN input transformation,
and frontend grammar BETWEEN UNKNOWN coercion. Numeric/operator signatures,
modifier limits/error priority, binary padding and type descriptors, full273,
TYPE-11 and total checklist completion remain open. This issue does not
approve the entire family. ROOT source review/integration must preserve its
newer enum bindings, hash NULL ownership, BIT constructor, Boolean BETWEEN
metadata and Window fields, and build its newer public ABI genuinely fresh58.
