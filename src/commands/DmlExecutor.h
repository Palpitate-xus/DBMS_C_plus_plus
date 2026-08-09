// ============================================================================
// DML AST Executor — Phase 4 Wave 0.4
//
// This module is the structured execution entry point for the DML subset that
// is safe to execute from the parser AST.  Unsupported INSERT/UPDATE/DELETE
// shapes deliberately return handled=false so the compatibility path can own
// features that have not migrated yet.  MERGE is fully owned here and rejects
// unsupported branches explicitly because its old string executor was removed.
// ============================================================================

#pragma once

#include "parser/ast.h"
#include "Session.h"
#include <map>
#include <string>
#include <vector>

namespace dbms {

struct DmlResult {
    bool available = false;
    std::vector<std::string> columns;
    // PostgreSQL type names for structured RETURNING columns.  Empty means
    // that the protocol layer should infer metadata from the relation/name.
    std::vector<std::string> columnTypes;
    std::vector<std::vector<std::string>> rows;
    std::string commandTag;
};

DmlResult takeLastDmlResult();
void clearLastDmlResult();

// DML AST bridge entry point.  The return value follows main.cpp::execute():
// false means success, true means an error.  `handled` is true when this
// executor owns the statement, including statements it rejects explicitly.
bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled,
                  const std::string& rawSql = {});

} // namespace dbms
