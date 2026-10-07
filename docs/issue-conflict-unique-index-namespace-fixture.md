# ON CONFLICT unique-index fixture: genuine target predicate namespace

The original full4f native run fails in `on_conflict_unique_index_test` with
42702 for the unqualified `WHERE payload=''`. Both the target relation and
EXCLUDED expose that column; this is not a broken conflict arbiter.

Actual PostgreSQL18.6 (`server_version_num=180006`, owned port15486) independently
rejects the same predicate with42702. Qualifying the target accepts it and
returns `(3,'','filled')`, preserving the preexisting empty-text unique key.

The fixture now retains the original ambiguous SQL as a negative control:
exact42702, no inserted id4, and unchanged structured row `(3,'','')` with
three non-NULL flags. The original positive UPDATE assertion uses
`unique_single.payload` and still requires the exact command tag and row.
All other standalone/composite/collation/float/NULL/named-constraint checks
remain unchanged; no production source or namespace behavior is relaxed.

Private proof `/tmp/dbms-root-native-rechecks.nqfnjb9y`: all58 currenta7 normal-O2
object signatures/config stamp and every private header/source byte audited
before fresh stubs/test compilation. Both the complete unique-index test and
unchanged `insert_conflict_binding_test` exit0 in wrapper40823;
`native-fixtures.log` retains all output. This is not a full-suite pass.
