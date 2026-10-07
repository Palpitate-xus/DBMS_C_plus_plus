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
#include "expression/prepared_query_execution.h"
#include <map>
#include <string>
#include <vector>

namespace dbms {
class Operator;

struct DmlResult {
    bool available = false;
    // Incremental SELECT migration may publish exact RowDescription metadata
    // while rows still come from the legacy text executor.
    bool metadataOnly = false;
    // A wholly prepared query whose actual executor has started may expose
    // its descriptor before a runtime error. Never publish partial rows or
    // a success tag, and never set this for failed preparation.
    bool runtimeErrorMetadata = false;
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

// Genuine children of a wholly prepared WITH statement. Static checks never
// evaluate rows/routines; runtime values retain their NULL bits and original
// statement/column identities. The reader executes prepared SELECT children
// in the calling statement's transaction/snapshot, not a new SPI command.
struct PreparedDmlSourceRows {
    // Namespace order, including genuine merged USING occurrences. Every
    // row supplies exact source/column ordinal cells, including NULL extension.
    std::vector<size_t> occurrences;
    std::function<bool(size_t, RowContext&)> read;
};
using PreparedDmlSourceFactory = std::function<PreparedDmlSourceRows(
    const Stmt*, const FromItem*, const RowContext&)>;
void prepareBoundDml(Stmt* statement, Session& session,
                     const std::shared_ptr<PreparedQuery>& query,
                     PreparedDmlSourceFactory sourceFactory = {},
                     PreparedChildCursorFactory childCursorFactory = {},
                     bool planRootConstants = false);
DmlResult executeBoundDml(Stmt* statement, Session& session,
    const std::shared_ptr<PreparedQuery>& query, PreparedChildExecutor reader,
    PreparedDmlSourceFactory sourceFactory = {},
    PreparedChildCursorFactory childCursorFactory = {},
    bool planRootConstants = false);
DmlResult executeAtomicDmlUnit(Session& session,
    const std::function<DmlResult()>& command);

// Compile a real mutation operator without opening any source/child or
// executing the mutation. Its same retained carrier is driven exactly once
// by next(); structured rows are the actual RETURNING result, not a plan
// assembled after a second SQL execution. The caller owns provider lifetimes.
struct PreparedDmlPlanHooks {
    // Weak owners prevent reader/child callbacks from retaining their own
    // carrier in a cycle. Source/INSERT SELECT lowering borrows the genuine
    // root's already-planned copies rather than throwing them away.
    std::function<PreparedChildExecutor(std::weak_ptr<PreparedQueryExecution>)> reader;
    std::function<PreparedDmlSourceFactory(std::weak_ptr<PreparedQueryExecution>)> sources;
    std::function<void()> finishStatement;
    std::function<std::vector<Operator*>()> extraChildren;
};
std::unique_ptr<Operator> buildBoundDmlPlan(Stmt*, Session&,
    const std::shared_ptr<PreparedQuery>&, PreparedChildExecutor,
    PreparedDmlSourceFactory = {}, PreparedChildCursorFactory = {},
    bool planRootConstants = true, bool ownsAtomicUnit = true,
    PreparedDmlPlanHooks hooks = {});

// Record real TEMP relation access during preparation, without executing any
// expression or modifying rows. Shared by SQL PREPARE and wire Parse.
void notePreparedTemporaryObjectAccess(const std::string& sql, Session& session);

// DML AST bridge entry point.  The return value follows main.cpp::execute():
// false means success, true means an error.  `handled` is true when this
// executor owns the statement, including statements it rejects explicitly.
bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled,
                  const std::string& rawSql = {});

} // namespace dbms
