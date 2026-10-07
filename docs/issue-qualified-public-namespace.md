# Explicit public namespace after DROP

This follows the qualified frontend fix; it does not modify routine execution.
`DROP SCHEMA public` is actually supported. Exempting its name from a namespace
lookup reported 42883 after a successful drop, instead of 3F000.

Pure binding now checks explicit public just like other user namespaces. A cold
native database may have no persisted catalog snapshot yet: its actual validated
`.schema_public` marker provides namespace existence, without initializing or
mutating the catalog. A removed namespace has neither that marker nor its copied
catalog row. The system pg_catalog namespace remains bootstrap-owned.

Evidence in `/tmp/dbms-qualified-public-namespace.dPvhb7O1/`:

- `baseline.fixed.log`: actual successful DROP followed by 42883; exit 134.
- `candidate.log`: after the fix, initial public lookup remains 42883, successful
  DROP changes the exact later lookup to 3F000, pg_catalog remains available.
- `reference18.log`: genuine 180006, isolated newly created reference database;
  all original lookup/DROP/false/LIMIT0 states, no sequence effects, and builtin
  positive assertions pass. The disposable reference database was removed.
- `build-v3.log`: sole fresh TableManage TU; other 56 source objects, main,
  headers and flags match the prior full-58 qualified epoch. No header change.

The first candidate's premature cold-native 3F000 and the initial fixture's
missing include compile failure are retained, not counted as passes. This does
not claim all schema DDL, name search paths, or routine overloads are complete.
