# Preserve actual UNKNOWN inputs and bound routine result declarations

The typed unnest signature callback used inferParsedResultType, which correctly
finalizes UNKNOWN output as TEXT but is wrong for an argument that still needs
its overload context. Genuine NULL and untyped string inputs therefore raised
42883 instead of PostgreSQL's 42725, even under WHERE false or LIMIT0.

The shared metadata-only inferParsedInputType retains UNKNOWN; the existing
output helper alone applies TEXT fallback. Whole binding also records each
function's actual resolved static result type on its live AST. A nested
already-contextualized NULLIF/CASE/COALESCE is not reinterpreted as a raw unknown
leaf. Both execution-owned AST copy paths retain that declaration. Parameters,
stored routine declarations and scalar SQL-child types remain metadata, not
values obtained by opening a cursor, running a routine or inspecting a row.
The new AST/header declarations require a wholly matching 58-TU ABI epoch;
there is no storage-format change.

## Exact evidence and remaining scope

Artifacts: `/tmp/dbms-srf-unknown-input.o6yj1Uq6`.

- `reference-180006-unknown-input.log`: whole permanent matrix passes strict
  PostgreSQL18.6/180006. It retains qualified/quoted names, WHERE/LIMIT error
  priority, typed NULL arrays, real SQL children, NULL bitmaps/OIDs, normal
  output TEXT fallback and zero sequence effects before the final sentinel.
- `baseline-srf-owner-unknown-input.log`: prior actual frozen SRF owner basis
  fails eleven UNKNOWN-state assertions and two qualified-public wrong-success
  assertions. This is that private immutable basis, not an exact5ca build.
- V1 all58 fresh O0/repeat/audit exits0; V1 native fails its unchanged NULLIF
  assertion, and whole wire retains NULLIF context plus qualified dispatch.
- `candidate-fresh58-v2-O0.log`: fresh all58 again after actual function-result
  AST layout changes, repeat/source/header/flags/stamp audits exit0.
- `candidate-matching-v3-O0.log`: two changed copy CPPs rebuilt against the
  V2 fresh58 epoch; all58 matching receipts/repeat/stamp exit0. Frozen SHA256
  `4da42a959e4f215b5165a9f2fd381b953c6dd9530a99a3d8bf92541234d6c3ef`.
- `candidate-native-v3.log` (90157, exit0): eight fresh matching native tests
  pass, including real static NULLIF declaration, UNKNOWN/output separation,
  typed NULL parameter array, context errors and no routine side effects.
- Nine complete serial adjacent scripts pass in tool session64935, exit0:
  ProjectSet, original UNNEST, ordinary quantified DML, unknown input priority,
  routine atomicity, PL binding, physical array descriptors, qualification
  planning and typed VIEW triggers. The tool record is authoritative; no
  filesystem log is claimed for that first serial launch.
- `candidate-whole-unknown-input-v3.log`, exit1: every input/context/value/OID
  assertion passes except the separate legacy `public.unnest(NULL)` dispatch
  falsely succeeding. Its state and no-partial-success assertions remain red.
  The whole source is named `prepared_srf_unknown_input_known_gap.py`, retains
  all assertions, and is deliberately not registered as a fake green gate.
- `candidate-whole-original-host-v1.log`, exit1: the original entire host
  diagnostic now retains only the independent listening-channel LIMIT0
  preflight failure. No full SRF/qualified-routine family completion is claimed.

ROOT formal O2 integration/full-suite verification is still separate. Broader
NULLIF/operator overloads, qualified legacy frontend and backend provider
ownership must satisfy their own complete controls. No push, Actions
activation or user-skipped security/TDE work is performed.
