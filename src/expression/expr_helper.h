#pragma once

#include <map>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace dbms {
class StorageEngine;
class Expr;
struct ColumnRefExpr;
struct PreparedQuery;

// ----------------------------------------------------------------------------
// Expression evaluation helper
//
// Wraps SQLParser + ExprEvaluator so storage/DDL code can evaluate a SQL
// expression string against a simple row context without depending directly on
// the parser.
// ----------------------------------------------------------------------------

struct ExprEvalResult {
    std::string value;   // textual result; meaningful only when ok && !isNull
    std::string typeName; // evaluator result type, for structured metadata
    std::string collation; // Explicit collation propagated by expression evaluation
    bool isNull = false; // true if expression evaluated to NULL
    bool ok = false;     // true if parse + eval succeeded
    std::string error;   // set when ok == false
    std::string sqlState; // structured DbError code; error retains legacy diagnostics
};

class ExprHelper {
public:
    // Metadata-only structural identity. The consumer supplies already
    // validated source occurrence/column ordinal/type/collation identities.
    static std::string scalarExpressionIdentity(
        const std::string& exprSql,
        const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
        const std::string& currentDB = "",
        StorageEngine* functionEngine = nullptr);
    // Sort-slot equivalence only: a retained SQL child is compared using its
    // prepared tree/ranges/typed slots, never SQL spelling or runtime values.
    // This does not merge distinct evaluation sites in the execution carrier.
    static std::string preparedSortExpressionIdentity(
        const Expr* expression, const PreparedQuery& query,
        const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
        const std::string& currentDB = "",
        StorageEngine* functionEngine = nullptr);
    // Retained prepared AST consumer: no rendering/reparsing and no routine
    // execution. Parameters retain stable typed slots; child sites are unique.
    static std::string scalarExpressionIdentity(
        const Expr* expression,
        const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
        const std::string& currentDB = "",
        StorageEngine* functionEngine = nullptr,
        const std::function<std::optional<std::string>(const Expr*)>& preparedIdentity = {});
    // Infer the PostgreSQL-visible result type of an expression without
    // evaluating it. Column types are supplied by the caller; unknown string
    // literals resolve to text only at the outer expression boundary.
    static std::string inferResultType(
        const std::string& exprSql,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        StorageEngine* functionEngine = nullptr);
    // Metadata-only inference on the caller's live AST. Never serialize and
    // reparse canonical identifiers, and never evaluate stored functions.
    // Prepared parameter cells retain their declared type even when NULL.
    static std::string inferParsedResultType(
        const Expr* expression,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        StorageEngine* functionEngine = nullptr);

    // Parse-only collation analysis; does not evaluate row-dependent or
    // volatile functions. Raises DbError for syntax/collation errors.
    static std::string analyzeExplicitResultCollation(
        const std::string& exprSql);

    // VALUES analysis shared by execution and protocol Describe.  The first
    // helper preserves PostgreSQL integer literal widths and unknown string
    // literals; the second chooses one common type for a VALUES column.
    static std::string canonicalResultTypeName(std::string typeName);
    static std::string inferValuesResultType(const std::string& exprSql);
    static bool resolveValuesResultType(
        const std::vector<std::string>& inputTypes,
        std::string& resultType, std::string& error);

    // Evaluate `exprSql` against the supplied row values.
    //
    // `row`      : column name -> value string (empty string means NULL)
    // `typeHints`: column name -> canonical type name (e.g. "integer",
    //              "character varying"). Columns without a hint are treated as
    //              "text".
    // `functionEngine`: owner of stored-function metadata and transactions.
    // A null pointer preserves the frontend's global-engine convention;
    // independent StorageEngine callers must pass their actual instance.
    static ExprEvalResult evalString(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        const std::string& currentUser = "",
        StorageEngine* functionEngine = nullptr);

    // Variant for callers that retain SQL NULL metadata separately from the
    // textual value.  This preserves a real empty string as distinct from
    // NULL while constructing the evaluator's row context.
    static ExprEvalResult evalStringWithNulls(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::set<std::string>& nullColumns,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        const std::string& currentUser = "",
        StorageEngine* functionEngine = nullptr);

    // Convenience: evaluate a boolean expression. NULL is treated as false.
    // Returns false and writes the error message to `error` (if non-null) on
    // parse/eval failure.
    static bool evalBool(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        std::string* error = nullptr,
        const std::string& currentDB = "",
        const std::string& currentUser = "",
        StorageEngine* functionEngine = nullptr);

    // CHECK constraints reject only FALSE. SQL UNKNOWN/NULL satisfies the
    // constraint, while parse and evaluation errors still fail closed.
    static bool evalCheck(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        std::string* error = nullptr,
        const std::string& currentDB = "",
        const std::string& currentUser = "",
        StorageEngine* functionEngine = nullptr);

    // Parse an expression and report whether it references a logical column.
    // nullopt means the stored expression could not be parsed safely.
    static std::optional<bool> referencesColumn(
        const std::string& exprSql, const std::string& columnName);

    // Rewrite references to one logical column while preserving the original
    // SQL text around them. Function names, qualifiers, type names and string
    // literals are left untouched. nullopt means the expression could not be
    // parsed or mapped back to its source tokens without ambiguity.
    static std::optional<std::string> renameColumnReferences(
        const std::string& exprSql, const std::string& oldName,
        const std::string& newName);
};

} // namespace dbms
