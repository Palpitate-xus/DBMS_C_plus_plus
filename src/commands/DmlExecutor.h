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
    // Incremental SELECT migration may publish exact RowDescription metadata
    // while rows still come from the legacy text executor.
    bool metadataOnly = false;
    std::vector<std::string> columns;
    // PostgreSQL type names for structured RETURNING columns.  Empty means
    // that the protocol layer should infer metadata from the relation/name.
    std::vector<std::string> columnTypes;
    std::vector<std::vector<std::string>> rows;
    // Per-cell SQL NULL metadata.  Text equal to "NULL" remains a four-byte
    // value; only a true entry here is sent as a protocol NULL.
    std::vector<std::vector<bool>> nulls;
    std::string commandTag;
};

enum class StructuredSetOperation { Union, Intersect, Except };

struct StructuredSetError {
    std::string sqlState;
    std::string message;
};

// Combine two already-evaluated SELECT results without converting their
// cells to display text.  SQL NULL is part of row identity for set semantics,
// but remains distinct from both an empty string and the text value "NULL".
bool combineStructuredSetResults(
    const DmlResult& left, const DmlResult& right,
    StructuredSetOperation operation, bool all,
    const std::string& database, const std::string& username,
    DmlResult& output, StructuredSetError& error);

DmlResult takeLastDmlResult();
void clearLastDmlResult();
// Publish a structured DML result (used by paths outside this executor,
// e.g. INSTEAD OF view triggers emitting RETURNING rows).
void publishLastDmlResult(DmlResult result);

// DML AST bridge entry point.  The return value follows main.cpp::execute():
// false means success, true means an error.  `handled` is true when this
// executor owns the statement, including statements it rejects explicitly.
bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled,
                  const std::string& rawSql = {});

} // namespace dbms
