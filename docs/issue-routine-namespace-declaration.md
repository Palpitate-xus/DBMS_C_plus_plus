# Canonical scalar routine namespaces

On the same frozen `0fdb5314` baseline, `CREATE SCHEMA path_other` succeeds but
`CREATE FUNCTION path_other.path_scalar() RETURNS INT LANGUAGE SQL AS
$$SELECT 17$$` fails with 42601. The parser consumed only the first identifier,
leaving the qualification dot in front of the parameter/return declaration.

CREATE FUNCTION now retains separately decoded namespace and routine components.
Metadata APIs accept a final canonical schema argument defaulting to `public`.
Public's historical flat `.funcs/<name>.func` files remain compatible; non-public
files live under `.funcs/.namespaces/<schema>/<name>.func`. A quoted public name
containing a dot is not the same identity as that namespace's routine. Existing
identifier validation still prevents path traversal. No raw SQL qualification
or concatenated `schema__name` key is used as the storage identity.

The CREATE undo record retains that same namespace, so real transaction rollback
removes only the newly created namespace occurrence. Replacement looks up the
same occurrence instead of an unrelated public definition. A missing declared
namespace is 3F000 before creation.

The native declaration test checks quoted dot components, array declarations,
separate public/non-public metadata and real CREATE rollback. Shared final
search_path proof additionally exercises actual bound execution of those records.
Strict PostgreSQL 18.6 and matching candidate logs, all-58-source/header manifests
and the original 42601 evidence are in `/tmp/dbms-provider-search-path.tD6Q3AdD/`.
Final combined V6 verification is 14 complete serial protocol scripts and ten
matching native drivers, all exit 0. Four new native drivers also pass scoped
ASan/UBSan with parser/evaluator/DDL-undo production instrumentation. This is a
private O0 header epoch, not a normal O2 or full canonical gate.

Public headers change: `CreateFunctionStmt::schema` and defaulted schema arguments
for scalar UDF create/read/exists/drop/invoke. Rebuild every TU and stubs together.
This is not complete routine DDL: same-schema overload catalogs, default argument
resolution, search_path-selected unqualified creation, namespaced TVFs/procedures,
qualified DROP/signature grammar and namespace-wide routine introspection remain
separate capabilities.
