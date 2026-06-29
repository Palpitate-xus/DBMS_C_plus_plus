#pragma once

#include <map>
#include <optional>
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
        const std::string& currentDB = "");

    // Convenience: evaluate a boolean expression. NULL is treated as false.
    // Returns false and writes the error message to `error` (if non-null) on
    // parse/eval failure.
    static bool evalBool(
        const std::string& exprSql,
        const std::map<std::string, std::string>& row,
        const std::map<std::string, std::string>& typeHints = {},
        std::string* error = nullptr,
        const std::string& currentDB = "");
};

} // namespace dbms
