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

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

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
                    NoticeSink notice = nullptr);
};

}  // namespace dbms
