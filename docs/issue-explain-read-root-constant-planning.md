# Ordinary EXPLAIN execution-root planning

Ordinary, non-quantified EXPLAIN used the convenience typed-plan builder with
its deliberately false root-planning default. FALSE predicates/LIMIT 0 could
hide constant division, narrowing and interval overflow. ANALYZE could invoke
a volatile writer before a later constant error. Physical unary targets also
fell back to an unsupported legacy path, and unreferenced WITH definitions
lost the genuine prepared envelope.

Supported typed EXPLAIN now retains the existing whole-query prepared holder
and uses the same statement-owned read runtime as quantified EXPLAIN. Its
actual execution root already opts into the shared execution-owned pure
planner. The compiled constants, lazy child sites, typed source contexts and
CTE demand are consumed by the displayed/executed operator graph, not a
discarded preflight or a second SELECT. Ordinary unary +/- explicitly selects
that typed path. Existing simple physical/index/aggregate legacy routes are
not universally diverted to a new planner.

Child, derived, VIEW and CTE builders keep their false defaults: they cannot
reacquire an output pruned by the root or plan a child in a dead CASE arm.
The helper can consume the already-bound holder instead of preparing it
again. ANALYZE still completes unused write CTEs only after successful
execution, and publication retains the outer statement commit boundary.

## Actual evidence

Artifacts are retained under `/tmp/dbms-explain-root-projectset.ahA83RRE`:

- Basis: ROOT `a7d460c7`, independent geometric input/descriptor fixes and the
  verified finite interval/root-read-flag prerequisites. `basis-build.log`
  freshly compiles main/evaluator against our own previously fresh 58-TU O0
  basis; all other 56 source bytes and all 102 headers match. No ROOT mutable
  build object or old public-layout object is used. Immutable binary SHA256
  `3e4b7a61eaa7c2f11efae8fe524f0b2a3790be167383460717cf78e5a778c95a`.
- `explain-root.reference18.log`: strict PostgreSQL `180006`, all original
  19 controls in four formats (plain/ANALYZE × TEXT/JSON), no-effect guards
  and one-call positive ANALYZE pass in an isolated rolled-back transaction.
- `explain-root.baseline.log`: authoritative exit 1 with the unchanged full
  matrix. It retains successful partial plans for expected errors, physical
  interval `0A000`, unreferenced WITH `XX000` and an actually advanced writer
  sequence. The sequence is never reset to mask that effect.
- `explain-candidate-build.log`: freshly compiled main, exact other 57 source
  objects and all matching headers, terminal 0. Candidate SHA256
  `6f07a832be15fc76ebcc37eb26dbbd2fb325346128ad5011da60543dafd20dc8`.
- `explain-root.candidate.log`: the same complete 19×4 matrix and all strong
  state, no-partial-plan/tag, row, demand and effect guards pass, terminal 0.
- `explain-adjacents.log`: initial wrapper exit 1 solely for four nonexistent
  harness filenames; four actual scripts pass. This failed wrapper is kept,
  not relabeled as an eight-script pass.
- `explain-adjacents-corrected.log`: authoritative terminal 0 for all eight
  serial original scripts: EXPLAIN 46×2/extended, read-root planning, original
  FROM-less 24 controls, simple CASE demand, ordinary CASE, quantified demand,
  unchanged stored-function diagnostic and WITH scalar subqueries. Every
  owned server is finally stopped.
- `explain-native.log`: seven matching native tests pass: typed EXPLAIN,
  ANALYZE, JSON actuals, timing options, pure constants, source contexts and
  finite interval values. These preserve adjacent contracts; the actual
  changed private main entry is proven by the protocol matrix.

This commit changes no public API, existing header/layout or production TU.
The new protocol test is registered normally, with original SQL/deadlines and
reference expectations intact. No push or Actions ran. ProjectSet+quantified
children and original FROM-less UNNEST are independent remaining consumer
repairs, not hidden behind a broader error guard. Complete EXPLAIN optimizer,
cost/node-source metadata and planner-family coverage are not claimed.
