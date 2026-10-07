# Backend-session SRF ownership and pure descriptors

Two complete original diagnostics are preserved, now registered without a
filtered green-only mode: UNKNOWN-input SRFs and backend-session SRF ownership.
The latter's original LIMIT 0 produced 42883/no descriptor while ordinary
`pg_listening_channels()` used a legacy textual expansion.

The actual query host now has one immutable provider registry shared by pure
ownership, whole-query binding and runtime: UNNEST and the backend's committed
listening-channel reader. Their canonical identity, arity, static result binder
and C++ row reader are one registration. Explicit public/stored scalar calls
do not inherit this role; fixed pg_catalog signatures preserve the default
builtin/public-shadow distinction. No rows, routine calls or SQL reconstruction
are used to infer descriptors. The original FunctionCall resolvedResultType,
typed parameter/column identities, and typed Append interfaces remain intact.

The execution-owned ProjectSet prepares every target/WHERE before execution,
and preserves root-only constant planning. Direct provider slots are zipped
with genuine typed NULL padding; ordinary targets use the same owned carrier.
Provider reads occur in next(), after qualification, never in metadata/open.
An actual current backend PID/database selects NotificationManager's committed
subscriptions, so staged LISTEN/UNLISTEN and other backends/databases cannot leak.
Both Parse-before-statement-publication and Describe reuse the wholly bound
provider descriptor for supported source-free, no-parameter shapes.

Genuine PostgreSQL 18.6 demand evidence matters: an ordinary volatile target is
evaluated on ProjectSet's terminal attempt too. A qualified input with no SRF
rows still calls that target once; two rows call it three times. WHERE false,
SQL NULL qualification and LIMIT 0 call neither the providers nor those targets.
The full matrix keeps these cumulative sequence expectations, with no resets.

Evidence is retained under `/tmp/dbms-qualified-srf-host.4Sw55aIH/`:

- Original immutable 931 baselines: public.unnest wrong-success and host LIMIT 0
  42883/lost descriptor. New complete strict `180006` host references include
  mixed/quoted/qualified targets, subscriptions and rollback, public shadow,
  typed values/NULL, P/D errors before effects, descriptors and demand.
- `provider-v1`: all 58 TUs plus fresh stubs, normal flags followed by O0;
  no old FunctionCall/Append ABI objects imported. `provider-v2` changes only
  TableManage/Network against verified other 56, headers and flags. V3 changes
  only Network against verified other 57 and the same public headers.
- V2 whole-host failure is retained: rejecting only during Describe left a
  named statement and caused later 42P05. The fixed Parse analysis rejects
  before publication. Its original fixed names and error assertions remain.
- Final V3 binary SHA256:
  `7afe60ab20a316d9cbc5b54ee27958c6961eedad80e0d329ae88ab6d777589b4`.
  Seven matching V2 natives and original ProjectSet V3 native pass. New native
  reader instrumentation wraps the actual registry and proves zero reads
  during metadata/open/false/NULL/LIMIT 0, real PID/database isolation, staged
  actions, typed padding and fresh committed data on reopen.
- Final serial handle 81540: all 13 complete scripts pass, including both
  unfiltered diagnostics, qualified matrix, ProjectSet, original CLI UNNEST,
  FROM-less positives/demand, INTO, PL binder, atomicity, quantified demand,
  WITH multisource and EXPLAIN. Original unsplit MV target/source matrix also
  passes (85373). All owned processes finished and cleaned up.
- Native/inline-provider scoped ASan/UBSan passes (66085). Core production
  objects in that driver are not all sanitized; this is not a whole-engine
  sanitizer claim. Failed/absent early harness names remain logged and are
  not counted among passed scripts/natives.

Public changes: QuerySetReturningBinding::Kind gains ListeningChannels;
ExecutionPlan.h appends an optional PreparedSetReturningReader to
buildPreparedSetReturningPlan; the inline registry is a new header. No new TU.
Any integration needs a full 58-TU fresh public-header epoch.

This closes these full diagnostics, not all SRF/frontend families. General
nested/relational SRFs, other host routines, parameterized prepared Describe,
routine overload/search-path rules, helper/XML and broader routine/query shapes
remain separately open. The next set-operation metadata/clauses issue remains
independent; no existing set API or asserted gap is removed here.
