# Actual scalar routine creation namespace

An unqualified CREATE FUNCTION previously always stored in public, even
when the first actual namespace in the session's explicit search_path was
another schema. CREATE OR REPLACE and transaction undo used that wrong target.
Empty or entirely missing paths incorrectly created public artifacts.

Creation now parses the actual session path, expands $user, ignores missing
namespace entries and selects the first real catalog/regular-marker namespace.
Quoted dots remain decoded identifiers, not storage-name separators. Explicit
declarations override the path. The selected namespace flows through the
existing storage/replacement/undo identity. Callable lookup's implicit
pg_catalog entry is not inserted into the creation path. The independently
committed cold-marker fix is retained.

Evidence under `/tmp/dbms-routine-creation-path.1pbTosJh`:

- The original whole fixture passes strict PostgreSQL18.6/180006. Exact c465
  matching O2 native fails13 assertions; its whole fixture first fails qualified
  lookup with42883, followed by aborted-block cascades. Those are not13 or39
  independent bugs.
- The first path-only candidate has a passing core native and original whole,
  but introduced a cold declaration failure. Its frozen source and all failed
  neighbor results remain. The separate cold repair precedes this commit.
- V2 compiles one fresh O2 DdlExecutor, links57 proven unchanged parent normal
  objects and checks all58 original receipts, relative headers and actual flags.
  This is not a fresh all58 epoch. Frozen SHA256:
  `eee0466aed18ab0d6bbba32bbf5b9688aaf1308a32f13deea9acfec56edbe015`.
- Matching native11 finishes exit1: the new creation-path and cold drivers plus
  eight adjacent namespace/provider/catalog/domain-FK drivers finish0. The
  unchanged full ddl_ast_bridge still fails its independent corrupt-domain
  bool-error case with an escaping58030 exception.
- V2 unchanged whole7 finishes exit1:5 pass /2 fail. The new creation-path and
  qualified-frontend scripts exceed the original disk/default15 deadline:
  creation-path fails in ROLLBACK TO, frontend fails in its next rejected SELECT
  after SAVEPOINT. Their actual prefixes and cleanup failures remain. The complete
  lifecycle, SRF search-path, routine-array and both original SRF scripts pass.
  No whole-group or latest combined ROOT approval is claimed.

Both new fixtures retain their original SQL, values, error states, metadata and
undo assertions. No public layout changes. Lazy pg_temp creation, stored
pg_catalog callable identity, arbitrary overload/default/named signatures,
qualified DROP and all remaining routine-family requirements are separate.
No push, Actions activation or user-deferred security/TDE work is authorized.
