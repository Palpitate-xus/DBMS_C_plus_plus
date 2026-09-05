#pragma once

#include <map>
#include <optional>
#include <set>
#include <string>

namespace dbms {

// ----------------------------------------------------------------------------
// Expression evaluation helper
//
// Wraps SQLParser + ExprEvaluator so storage/DDL code can evaluate a SQL
// expression string against a simple row context without depending directly on
// the parser.
// ----------------------------------------------------------------------------

struct ExprEvalResult {
    std::string value;   // textual result; meaningful only when ok && !isNull
    bool isNull = false; // true if expression evaluated to NULL
    bool ok = false;     // true if parse + eval succeeded
    std::string error;   // set when ok == false
};

class ExprHelper {
public:
    // Evaluate `exprSql` against the supplied row values.
    //
    // `row`      : column name -> value string (empty string means NULL)
    // `typeHints`: column name -> canonical type name (e.g. "integer",
    //              "character varying"). Columns without a hint are treated as
    //              "text".
    static ExprEvalResult evalString(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        const std::string& currentUser = "");

    // Variant for callers that retain SQL NULL metadata separately from the
    // textual value.  This preserves a real empty string as distinct from
    // NULL while constructing the evaluator's row context.
    static ExprEvalResult evalStringWithNulls(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::set<std::string>& nullColumns,
        const std::map<std::string, std::string>& typeHints = {},
        const std::string& currentDB = "",
        const std::string& currentUser = "");

    // Convenience: evaluate a boolean expression. NULL is treated as false.
    // Returns false and writes the error message to `error` (if non-null) on
    // parse/eval failure.
    static bool evalBool(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        std::string* error = nullptr,
        const std::string& currentDB = "",
        const std::string& currentUser = "");

    // CHECK constraints reject only FALSE. SQL UNKNOWN/NULL satisfies the
    // constraint, while parse and evaluation errors still fail closed.
    static bool evalCheck(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        std::string* error = nullptr,
        const std::string& currentDB = "",
        const std::string& currentUser = "");

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
