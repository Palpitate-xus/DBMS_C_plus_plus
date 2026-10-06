// ============================================================================
// PlPgsql — a minimal PL/pgSQL interpreter for stored function bodies.
//
// Scope (P1-7 first slice):
//   DECLARE var [type] [:= default];  ...
//   BEGIN ... END;
//   assignment:      var := expr;
//   RETURN expr;     RETURN;            (function form)
//   IF cond THEN ... [ELSIF cond THEN ...] [ELSE ...] END IF;
//   WHILE cond LOOP ... END LOOP;
//   FOR i IN [REVERSE] a..b LOOP ... END LOOP;
//   EXIT [WHEN cond];
//   RAISE NOTICE/WARNING/ERROR 'fmt' [, args...] (fmt %s placeholders);
//   SELECT expr INTO var FROM ...   (single-row; executes via callback)
//   SQL statements without INTO run via the callback (PERFORM/INSERT/...)
//
// Expressions and conditions inside the body are evaluated by a pluggable
// scalar evaluator (typically the SQL expression evaluator) so PL/pgSQL
// reuse the host's typing/coercion rules.  Statement execution (SELECT
// INTO, PERFORM, DML) goes through the executor callback.
// ============================================================================

#pragma once

#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace dbms {

// SQL NULL has its own bit: empty text and the text "null" are ordinary
// values. Hosts must retain row/column counts even when no first row exists.
struct PlPgsqlQueryResult {
    bool ok = false;
    size_t columnCount = 0;
    size_t rowCount = 0;
    std::vector<std::string> columnTypes;
    std::vector<std::optional<std::string>> firstRow;
    std::string sqlState;
    std::string message;
};

// Executor demand, not a SQL LIMIT rewrite or a cap on scanned input rows.
// Zero runs to completion; INTO needs one row, INTO STRICT needs two rows
// so that a second result can be distinguished from an exactly-one result.
struct PlPgsqlQueryOptions {
    size_t maxRows = 0;
};

// Callbacks the interpreter needs from the host.
struct PlPgsqlHost {
    // Evaluate a scalar SQL expression text with the current variable
    // bindings substituted (`$name` style used internally).  Returns nullopt
    // on evaluation error.
    std::function<std::optional<std::string>(const std::string& expr,
                                             const std::map<std::string, std::string>& vars)>
        evalExpr;
    // Execute a SQL statement (no result binding); returns false on error.
    std::function<bool(const std::string& sql,
                       const std::map<std::string, std::string>& vars)> execStmt;
    // Run "SELECT <selectList> ..." and bind the first row's values to the
    // requested INTO variables.  Returns: 0 ok, 1 no row, 2 error.
    std::function<int(const std::string& selectRest,
                      const std::vector<std::string>& intoVars,
                      std::map<std::string, std::string>& vars)> selectInto;
    // Preferred SELECT INTO boundary: receives the whole SELECT with only
    // the procedural INTO target list removed and variables substituted.
    std::function<PlPgsqlQueryResult(const std::string& sql,
                                    const PlPgsqlQueryOptions& options)> query;
    // Preferred scalar-expression boundary. Expressions and bindings remain
    // separate, so text that looks numeric never acquires a numeric type.
    // A successful result contains exactly one nullable row/column cell.
    std::function<PlPgsqlQueryResult(const std::string& expr,
                                    const std::map<std::string, std::string>& vars,
                                    const std::set<std::string>& nullVars,
                                    const std::map<std::string, std::string>& variableTypes)>
        evalExprTyped;
    std::map<std::string, std::string> parameterTypes;
    // Assignment/default/INTO coercion uses the source SQL type, not the
    // textual spelling of a value. The result has the same scalar shape.
    std::function<PlPgsqlQueryResult(const std::optional<std::string>& value,
                                    const std::string& sourceType,
                                    const std::string& targetType)>
        coerceValueTyped;
};

class PlPgsql {
public:
    // Interpret a function body.  Returns true and fills returnValue when a
    // RETURN executed (empty for a bare RETURN); returns false on runtime
    // error and sets error.  Parameters arrive as pre-bound variables.
    // `notice` (optional) receives RAISE NOTICE/WARNING output.
    using NoticeSink = std::function<void(const std::string& level, const std::string& msg)>;
    static bool run(const std::string& body,
                    const std::map<std::string, std::string>& params,
                    const PlPgsqlHost& host,
                    std::string& returnValue,
                    std::string& error,
                    NoticeSink notice = nullptr,
                    bool* returnIsNull = nullptr,
                    const std::set<std::string>* nullParams = nullptr,
                    std::string* errorSqlState = nullptr,
                    std::string* returnType = nullptr);
};

}  // namespace dbms
