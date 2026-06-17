#pragma once

#include <cstdint>
#include <fstream>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "DateType.h"
#include "table_schema.h"
#include "storage_engine.h"
#include "BPTree.h"
#include "BufferPool.h"
#include "PageAllocator.h"
#include "FreeSpaceMap.h"
#include "VisibilityMap.h"
#include "PageWrapper.h"
#include "CommitLog.h"
#include "LockManager.h"
#include "WAL.h"
#include "HashIndex.h"
#include "SPGiSTIndex.h"

namespace dbms {

// MVCC header size per row
constexpr size_t MVCC_TXID_SIZE = 8;      // uint64_t creatorTxnId
constexpr size_t MVCC_ROLLBACK_SIZE = 8;  // uint64_t rollbackPtr (0 = no older version)
constexpr size_t MVCC_HEADER_SIZE = MVCC_TXID_SIZE + MVCC_ROLLBACK_SIZE;

// Result code for data operations (alias for the unified DBStatus)
using OpResult = DBStatus;

// Backward-compatible constants for code that still uses OpResult::Xxx names
constexpr DBStatus OP_SUCCESS = DBStatus::OK;
constexpr DBStatus OP_TABLE_NOT_EXIST = DBStatus::TABLE_NOT_FOUND;
constexpr DBStatus OP_DATABASE_NOT_EXIST = DBStatus::DATABASE_NOT_FOUND;
constexpr DBStatus OP_TABLE_ALREADY_EXIST = DBStatus::TABLE_ALREADY_EXISTS;
constexpr DBStatus OP_INVALID_VALUE = DBStatus::INVALID_VALUE;
constexpr DBStatus OP_NULL_NOT_ALLOWED = DBStatus::NULL_NOT_ALLOWED;
constexpr DBStatus OP_SYNTAX_ERROR = DBStatus::SYNTAX_ERROR;
constexpr DBStatus OP_DUPLICATE_KEY = DBStatus::DUPLICATE_KEY;
constexpr DBStatus OP_LOCK_CONFLICT = DBStatus::LOCK_CONFLICT;
constexpr DBStatus OP_SERIALIZATION_FAILURE = DBStatus::SERIALIZATION_FAILURE;

std::string sqlstateForDBStatus(DBStatus res);

class StorageEngine : public IStorageEngine {
public:
    StorageEngine();

    // ========================================================================
    // IStorageEngine interface overrides (Phase 0)
    // ========================================================================
    // The following methods adapt the richer StorageEngine API to the
    // IStorageEngine interface. Some are simple wrappers; row-level methods
    // that require explicit TxnId/Snapshot are stubs that will be fleshed out
    // when the executor/planner moves to the interface.
    DBStatus useDatabase(const std::string& dbName) override;
    std::vector<std::string> listDatabases() const override;
    std::vector<std::string> listTables(
        const std::string& dbName) const override;
    RowId insert(const std::string& dbName,
                 const std::string& tableName,
                 const std::map<std::string, std::string>& values,
                 TxnId txnId) override;
    std::vector<std::map<std::string, std::string>> query(
        const std::string& dbName,
        const std::string& tableName,
        const Snapshot* snapshot) override;
    size_t update(const std::string& dbName,
                  const std::string& tableName,
                  const std::map<std::string, std::string>& newValues,
                  const std::vector<RowId>& rowIds,
                  TxnId txnId) override;
    size_t remove(const std::string& dbName,
                  const std::string& tableName,
                  const std::vector<RowId>& rowIds,
                  TxnId txnId) override;
    DBStatus createIndex(const std::string& dbName,
                         const std::string& tableName,
                         const std::string& indexName,
                         const std::vector<std::string>& columns,
                         const std::string& indexType) override;
    DBStatus dropIndex(const std::string& dbName,
                       const std::string& indexName) override;
    TxnId beginTransaction(IsolationLevel level) override;
    DBStatus commitTransaction(TxnId txnId) override;
    DBStatus rollbackTransaction(TxnId txnId) override;
    DBStatus checkpoint() override;
    Lsn getCurrentLsn() const override;

    // Page size for a given storage format version
    static size_t pageSizeForFormatVersion(uint32_t formatVersion) {
        return formatVersion >= 2 ? 8192 : 4096;
    }

    // Database operations
    DBStatus createDatabase(const std::string& dbname, const std::string& charset = "utf8");
    std::string getDatabaseCharset(const std::string& dbname) const;
    DBStatus dropDatabase(const std::string& dbname);
    bool databaseExists(const std::string& dbname) const;
    std::vector<std::string> getDatabaseNames() const;

    // Table operations
    DBStatus createTable(const std::string& dbname, const TableSchema& tbl);
    DBStatus createTable(const std::string& dbname, const std::string& tablename, const TableSchema& tbl);
    DBStatus dropTable(const std::string& dbname, const std::string& tablename);
    DBStatus truncateTable(const std::string& dbname, const std::string& tablename);
    DBStatus alterTableAddColumn(const std::string& dbname, const std::string& tablename,
                                  const Column& col);
    DBStatus alterTableDropColumn(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    DBStatus alterTableRenameColumn(const std::string& dbname, const std::string& tablename,
                                     const std::string& oldName, const std::string& newName);
    DBStatus alterTableRenameTable(const std::string& dbname, const std::string& oldName,
                                    const std::string& newName);
    DBStatus alterTableSetDefault(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName, const std::string& defaultValue);
    DBStatus alterTableDropDefault(const std::string& dbname, const std::string& tablename,
                                    const std::string& colName);
    DBStatus alterTableSetNotNull(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    DBStatus alterTableDropNotNull(const std::string& dbname, const std::string& tablename,
                                    const std::string& colName);
    DBStatus alterTableAddCheckConstraint(const std::string& dbname, const std::string& tablename,
                                           const std::string& name, const std::string& expr);
    DBStatus alterTableAddUniqueConstraint(const std::string& dbname, const std::string& tablename,
                                            const std::string& name, const std::vector<std::string>& colNames);
    DBStatus alterTableAddFKConstraint(const std::string& dbname, const std::string& tablename,
                                        const std::string& name, const std::vector<std::string>& localCols,
                                        const std::string& refTable, const std::vector<std::string>& refCols,
                                        const std::string& onDelete = "restrict", const std::string& onUpdate = "restrict");
    DBStatus alterTableDropConstraint(const std::string& dbname, const std::string& tablename,
                                       const std::string& name);
    DBStatus commentOnTable(const std::string& dbname, const std::string& tablename,
                             const std::string& comment);
    DBStatus commentOnColumn(const std::string& dbname, const std::string& tablename,
                              const std::string& colname, const std::string& comment);
    std::string getTableComment(const std::string& dbname, const std::string& tablename) const;
    std::string getColumnComment(const std::string& dbname, const std::string& tablename,
                                  const std::string& colname) const;
    // Sequence support
    DBStatus createSequence(const std::string& dbname, const std::string& seqname,
                            int64_t start = 1, int64_t increment = 1);
    DBStatus alterSequence(const std::string& dbname, const std::string& seqname,
                            bool hasRestart, int64_t restart,
                            bool hasIncrement, int64_t increment);
    DBStatus dropSequence(const std::string& dbname, const std::string& seqname);
    int64_t nextval(const std::string& dbname, const std::string& seqname);
    int64_t currval(const std::string& dbname, const std::string& seqname);
    bool sequenceExists(const std::string& dbname, const std::string& seqname) const;
    std::vector<std::string> getSequenceNames(const std::string& dbname) const;
    bool tableExists(const std::string& dbname, const std::string& tablename) const;
    std::vector<std::string> getTableNames(const std::string& dbname) const;
    TableSchema getTableSchema(const std::string& dbname, const std::string& tablename) const;

    // Storage parameters (WITH clause)
    std::map<std::string, std::string> getStorageParams(const std::string& dbname,
                                                        const std::string& tablename) const;
    DBStatus setStorageParams(const std::string& dbname,
                              const std::string& tablename,
                              const std::map<std::string, std::string>& params);

    // View support
    DBStatus createView(const std::string& dbname, const std::string& viewname, const std::string& sql);
    DBStatus dropView(const std::string& dbname, const std::string& viewname);
    bool viewExists(const std::string& dbname, const std::string& viewname) const;
    std::string getViewSQL(const std::string& dbname, const std::string& viewname) const;
    std::vector<std::string> getViewNames(const std::string& dbname) const;
    // Get base table name for updatable views (empty if not updatable)
    std::string getViewBaseTable(const std::string& dbname, const std::string& viewname) const;
    // Get WITH CHECK OPTION for a view ("CASCADED", "LOCAL", or "")
    std::string getViewCheckOption(const std::string& dbname, const std::string& viewname) const;
    // Validate that a row (colValues) satisfies the view's WHERE clause
    bool validateViewCheckOption(const std::string& dbname, const std::string& viewname,
                                  const std::map<std::string, std::string>& colValues) const;

    // Materialized view (stores query results in a backing table)
    static std::string materializedViewPrefix(const std::string& viewname) { return "__mv_" + viewname; }
    bool isMaterializedView(const std::string& dbname, const std::string& viewname) const;
    std::string getMaterializedViewSQL(const std::string& dbname, const std::string& viewname) const;
    std::vector<std::string> getMaterializedViewNames(const std::string& dbname) const;
    DBStatus dropMaterializedView(const std::string& dbname, const std::string& viewname);

    // Stored procedures
    struct ProcParam {
        std::string name;
        std::string mode; // "IN", "OUT", "INOUT"
        std::string type;
    };
    DBStatus createProcedure(const std::string& dbname, const std::string& procname,
                             const std::vector<ProcParam>& params,
                             const std::vector<std::string>& statements);
    DBStatus dropProcedure(const std::string& dbname, const std::string& procname);
    bool procedureExists(const std::string& dbname, const std::string& procname) const;
    std::vector<std::string> getProcedureStatements(const std::string& dbname,
                                                     const std::string& procname) const;
    std::vector<ProcParam> getProcedureParams(const std::string& dbname,
                                               const std::string& procname) const;
    std::vector<std::string> getProcedureNames(const std::string& dbname) const;

    // Schema management
    DBStatus createSchema(const std::string& dbname, const std::string& schemaname);
    DBStatus dropSchema(const std::string& dbname, const std::string& schemaname, bool cascade);
    DBStatus renameSchema(const std::string& dbname, const std::string& oldname,
                          const std::string& newname);
    bool schemaExists(const std::string& dbname, const std::string& schemaname) const;
    std::vector<std::string> getSchemaNames(const std::string& dbname) const;
    std::vector<std::string> getTablesInSchema(const std::string& dbname,
                                               const std::string& schemaname) const;
    // Sequence reset (for TRUNCATE RESTART IDENTITY)
    void resetSequence(const std::string& dbname, const std::string& tablename,
                       const std::string& colname, int64_t val = 1);
    // Move table to another database/schema
    DBStatus alterTableSetSchema(const std::string& dbname, const std::string& tablename,
                                 const std::string& targetDbname);

    // Domain support
    struct DomainInfo {
        std::string name;
        std::string baseType;
        std::string defaultValue;
        std::string checkExpr;
        std::string constraintName;
    };
    DBStatus createDomain(const std::string& dbname, const DomainInfo& info);
    DBStatus alterDomain(const std::string& dbname, const std::string& name,
                         const DomainInfo& info);
    DBStatus dropDomain(const std::string& dbname, const std::string& name);
    DomainInfo getDomain(const std::string& dbname, const std::string& name) const;
    std::vector<std::string> getDomainNames(const std::string& dbname) const;

    // Composite types (ROW types)
    struct CompositeType {
        std::string name;
        std::vector<std::pair<std::string, std::string>> fields; // (fieldName, fieldType)
    };
    DBStatus createCompositeType(const std::string& dbname, const CompositeType& ct);
    DBStatus alterCompositeType(const std::string& dbname, const std::string& name,
                                const CompositeType& ct);
    DBStatus dropCompositeType(const std::string& dbname, const std::string& name);
    CompositeType getCompositeType(const std::string& dbname, const std::string& name) const;
    std::vector<std::string> getCompositeTypeNames(const std::string& dbname) const;
    bool isCompositeType(const std::string& dbname, const std::string& name) const;

    // Table inheritance: get all direct child table names for a parent table
    std::vector<std::string> getInheritedChildren(const std::string& dbname,
                                                   const std::string& parentName) const;

    // Advisory locks (session-level)
    bool advisoryLock(int64_t key);
    bool advisoryUnlock(int64_t key);
    bool advisoryLockShared(int64_t key);
    bool advisoryUnlockShared(int64_t key);
    bool advisoryLockExists(int64_t key) const;

    // User-defined functions (simple expression-based)
    struct UDFInfo {
        std::string name;
        std::string paramName; // single-param compat
        std::string expression;
        std::vector<std::string> paramNames; // multi-param support
        std::vector<std::string> paramTypes; // multi-param support
    };
    DBStatus createUDF(const std::string& dbname, const std::string& funcname,
                       const std::string& param, const std::string& expression);
    DBStatus createUDF(const std::string& dbname, const std::string& funcname,
                       const std::vector<std::string>& params,
                       const std::vector<std::string>& types,
                       const std::string& expression);
    DBStatus dropUDF(const std::string& dbname, const std::string& funcname);
    bool udfExists(const std::string& dbname, const std::string& funcname) const;
    UDFInfo getUDF(const std::string& dbname, const std::string& funcname) const;
    std::vector<std::string> getUDFNames(const std::string& dbname) const;

    // Table-valued functions (return a result set)
    DBStatus createTVF(const std::string& dbname, const std::string& funcname,
                       const std::string& param, const std::string& sql);
    DBStatus dropTVF(const std::string& dbname, const std::string& funcname);
    bool tvfExists(const std::string& dbname, const std::string& funcname) const;
    std::string getTVFSQL(const std::string& dbname, const std::string& funcname) const;
    std::string getTVFParam(const std::string& dbname, const std::string& funcname) const;
    std::vector<std::string> getTVFNames(const std::string& dbname) const;

    // Buffer pool stats (aggregated across all open tables and indexes)
    struct BufferPoolStats {
        size_t totalHits = 0;
        size_t totalMisses = 0;
        double hitRate = 0.0;
    };
    BufferPoolStats getBufferPoolStats() const;

    // Buffer cache entries (for pg_buffercache virtual table)
    struct BufferCacheEntry {
        std::string relname;  // table name or index name
        uint32_t pageId;
        bool dirty;
        int pinCount;
    };
    std::vector<BufferCacheEntry> getBufferCacheEntries() const;

    // Statistics
    struct ColumnStats {
        size_t cardinality = 0;
        std::string minVal;
        std::string maxVal;
        // Equi-depth histogram: each bucket is (low, high) boundary
        std::vector<std::pair<std::string, std::string>> histogram;
        // Most Common Values: (value, frequency), top-N
        std::vector<std::pair<std::string, size_t>> mcv;
    };
    struct TableStats {
        size_t rowCount = 0;
        std::map<std::string, ColumnStats> colStats;
        // Multi-column stats: key = "col1,col2", value = joint stats
        std::map<std::string, ColumnStats> multiColStats;
    };
    void analyzeTable(const std::string& dbname, const std::string& tablename);
    void analyzeMultiColumn(const std::string& dbname, const std::string& tablename,
                            const std::vector<std::string>& colnames);
    size_t getTableRowCount(const std::string& dbname, const std::string& tablename) const;
    ColumnStats getColumnStats(const std::string& dbname, const std::string& tablename,
                                const std::string& colname) const;
    ColumnStats getMultiColumnStats(const std::string& dbname, const std::string& tablename,
                                     const std::string& colKey) const;

    // Data operations
    DBStatus insert(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& values);
    DBStatus update(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& updates,
                    const std::vector<std::string>& conditions);
    DBStatus remove(const std::string& dbname, const std::string& tablename,
                    const std::vector<std::string>& conditions);
    struct OrderBySpec {
        std::string colName;
        bool ascending = true;
        // For ORDER BY expression support (e.g., ORDER BY length(name))
        std::string exprFunc = "";   // e.g., "length", "add", "sub"
        std::string exprArg = "";    // e.g., "name"
        std::string exprArg2 = "";   // second operand for arithmetic (e.g., "1" for age + 1)
        bool isExpression = false;
        bool nullsFirst = false;  // NULLS FIRST / NULLS LAST
        std::string collation = "";    // e.g., "nocase", "binary" (default), "unicode"
    };
    std::vector<std::string> query(const std::string& dbname, const std::string& tablename,
                                   const std::vector<std::string>& conditions,
                                   const std::set<std::string>& selectCols,
                                   const std::vector<OrderBySpec>& orderBy = {},
                                   bool forUpdate = false,
                                   bool noWait = false,
                                   bool skipLocked = false,
                                   int timezoneOffsetMinutes = 0,
                                   const std::vector<std::string>& distinctOnCols = {});

    // Scalar function expression for queryExpr
    struct SelectExpr {
        std::string displayName;
        bool isScalar = false;
        std::string funcName;
        std::vector<std::string> funcArgs;
        std::string colName;
        std::string sessionUser; // filled for current_user / session_user pseudo-functions
    };
    std::vector<std::string> queryExpr(const std::string& dbname,
                                        const std::string& tablename,
                                        const std::vector<std::string>& conditions,
                                        const std::vector<SelectExpr>& exprs,
                                        const std::vector<OrderBySpec>& orderBy = {});

    // information_schema virtual tables
    std::vector<std::string> queryInformationSchema(
        const std::string& tablename,
        const std::vector<std::string>& conditions,
        const std::set<std::string>& selectCols,
        const std::vector<OrderBySpec>& orderBy) const;

    // pg_catalog virtual tables (PostgreSQL compatibility)
    std::vector<std::string> queryPgCatalog(
        const std::string& tablename,
        const std::vector<std::string>& conditions,
        const std::set<std::string>& selectCols,
        const std::vector<OrderBySpec>& orderBy) const;

    // Aggregate item with optional FILTER (WHERE ...) clause
    struct AggItem {
        std::string func;
        std::string arg;
        std::vector<std::string> filterConds; // FILTER (WHERE ...) conditions
    };

    // Aggregate query: items = {("count","*"), ("max","score"), ...}
    std::vector<std::string> aggregate(const std::string& dbname, const std::string& tablename,
                                       const std::vector<std::string>& conditions,
                                       const std::vector<AggItem>& items);

    // Group aggregate: GROUP BY groupCol(s) with HAVING filter
    std::vector<std::string> groupAggregate(
        const std::string& dbname, const std::string& tablename,
        const std::vector<std::string>& conditions,
        const std::vector<AggItem>& items,
        const std::vector<std::string>& groupByCols,
        const std::vector<std::string>& havingConds);

    // Grouping Sets / ROLLUP / CUBE aggregate
    std::vector<std::string> groupAggregateSets(
        const std::string& dbname, const std::string& tablename,
        const std::vector<std::string>& conditions,
        const std::vector<AggItem>& items,
        const std::vector<std::string>& allGroupByCols,
        const std::vector<std::vector<std::string>>& groupingSets,
        const std::vector<std::string>& havingConds);

    // Sort result rows by expression (post-query ORDER BY expression)
    std::vector<std::string> sortByExpression(
        const std::string& dbname, const std::string& tablename,
        std::vector<std::string> rows,
        const std::vector<OrderBySpec>& exprSpecs) const;

    // JOIN query: INNER JOIN two tables on leftCol = rightCol
    std::vector<std::string> join(const std::string& dbname,
                                   const std::string& leftTable,
                                   const std::string& rightTable,
                                   const std::string& leftCol,
                                   const std::string& rightCol,
                                   const std::vector<std::string>& conditions,
                                   const std::set<std::string>& selectCols);

    // LEFT JOIN: preserve all left rows, fill NULL for non-matching right
    std::vector<std::string> leftJoin(const std::string& dbname,
                                       const std::string& leftTable,
                                       const std::string& rightTable,
                                       const std::string& leftCol,
                                       const std::string& rightCol,
                                       const std::vector<std::string>& conditions,
                                       const std::set<std::string>& selectCols);

    // RIGHT JOIN: preserve all right rows, fill NULL for non-matching left
    std::vector<std::string> rightJoin(const std::string& dbname,
                                        const std::string& leftTable,
                                        const std::string& rightTable,
                                        const std::string& leftCol,
                                        const std::string& rightCol,
                                        const std::vector<std::string>& conditions,
                                        const std::set<std::string>& selectCols);

    // FULL OUTER JOIN: union of LEFT and RIGHT JOIN
    std::vector<std::string> fullOuterJoin(const std::string& dbname,
                                            const std::string& leftTable,
                                            const std::string& rightTable,
                                            const std::string& leftCol,
                                            const std::string& rightCol,
                                            const std::vector<std::string>& conditions,
                                            const std::set<std::string>& selectCols);

    // CROSS JOIN: cartesian product
    std::vector<std::string> crossJoin(const std::string& dbname,
                                        const std::string& leftTable,
                                        const std::string& rightTable,
                                        const std::vector<std::string>& conditions,
                                        const std::set<std::string>& selectCols);

    // Transaction operations
    bool inTransaction() const { return inTransaction_; }
    bool isReadOnly() const { return readOnly_; }
    void setReadOnly(bool ro) { readOnly_ = ro; }
    DBStatus beginTransaction(const std::string& dbname);
    DBStatus commitTransaction();
    DBStatus rollbackTransaction();

    // Two-phase commit (PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED)
    DBStatus prepareTransaction(const std::string& xid);
    DBStatus commitPrepared(const std::string& xid);
    DBStatus rollbackPrepared(const std::string& xid);
    std::vector<std::string> listPreparedTransactions() const;

    // Savepoint support
    DBStatus savepoint(const std::string& name);
    DBStatus rollbackToSavepoint(const std::string& name);
    DBStatus releaseSavepoint(const std::string& name);

    // WAL crash recovery
    void recoverAllDatabases();

    // Checkpoint: flush all dirty pages and write checkpoint record
    void checkpoint(const std::string& dbname);
    // WAL archive directory
    std::filesystem::path walArchiveDir(const std::string& dbname) const { return dbPath(dbname).string() + ".archive"; }
    void archiveWal(const std::string& dbname);

    // Physical backup / restore
    bool physicalBackup(const std::string& dbname, const std::string& backupPath);
    bool physicalRestore(const std::string& dbname, const std::string& backupPath);

    // VACUUM: reclaim space from deleted rows
    size_t vacuum(const std::string& dbname, const std::string& tablename,
                  bool concurrent = false);
    // VACUUM FULL: rewrite table entirely, reclaiming all dead space
    size_t vacuumFull(const std::string& dbname, const std::string& tablename);

    // Auto-VACUUM: per-table dead tuple tracking and automatic triggering
    void maybeAutoVacuum(const std::string& dbname, const std::string& tablename);
    size_t getDeadTupleCount(const std::string& dbname, const std::string& tablename) const;
    void resetDeadTupleCount(const std::string& dbname, const std::string& tablename);

    // Auto-ANALYZE: per-table modification tracking and automatic triggering
    void recordModification(const std::string& dbname, const std::string& tablename, size_t delta = 1);
    void maybeAutoAnalyze(const std::string& dbname, const std::string& tablename);

    // Index metadata (partial + expression index support)
    struct IndexMetadata {
        std::string name;           // column name or expression
        bool isExpression = false;  // true for expression index (e.g. UPPER(col))
        bool descending = false;
        std::vector<std::string> includeCols;
        std::string whereCondition; // partial index WHERE clause
        std::string exprFunc;       // "UPPER" or "LOWER" for expression indexes
    };
    std::vector<IndexMetadata> getIndexMetadata(const std::string& dbname,
                                                 const std::string& tablename) const;

    // Secondary index (single-column B+ tree)
    DBStatus createIndex(const std::string& dbname, const std::string& tablename,
                         const std::string& colname, bool ascending = true,
                         const std::vector<std::string>& includeCols = {},
                         const std::string& whereCondition = "",
                         const std::string& expression = "",
                         bool concurrently = false);
    DBStatus dropIndex(const std::string& dbname, const std::string& tablename,
                       const std::string& colname);
    std::vector<std::string> getIndexedColumns(const std::string& dbname,
                                                const std::string& tablename) const;
    bool isDescendingIndex(const std::string& dbname, const std::string& tablename,
                           const std::string& colname) const;
    std::vector<std::string> getIndexIncludeColumns(const std::string& dbname,
                                                     const std::string& tablename,
                                                     const std::string& colname) const;

    // Hash index (single-column, O(1) equality lookup)
    DBStatus createHashIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname, bool concurrently = false);
    DBStatus dropHashIndex(const std::string& dbname, const std::string& tablename,
                            const std::string& colname);
    std::vector<std::string> getHashIndexedColumns(const std::string& dbname,
                                                    const std::string& tablename) const;
    class HashIndex* getHashIndex(const std::string& dbname, const std::string& tablename,
                                   const std::string& colname) const;

    // Composite index (multi-column)
    struct CompositeIndexInfo {
        std::string name;
        std::vector<std::string> columns;
        std::string whereCondition; // partial index WHERE clause
    };
    DBStatus createCompositeIndex(const std::string& dbname, const std::string& tablename,
                                  const std::vector<std::string>& colnames,
                                  const std::string& indexName,
                                  const std::vector<std::string>& includeCols = {},
                                  const std::string& whereCondition = "",
                                  bool concurrently = false);
    DBStatus dropCompositeIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& indexName);
    std::vector<CompositeIndexInfo> getCompositeIndexes(const std::string& dbname,
                                                         const std::string& tablename) const;

    // Reindex: rebuild all indexes for a table
    DBStatus reindex(const std::string& dbname, const std::string& tablename);
    BPTree* getCompositeIndexTree(const std::string& dbname, const std::string& tablename,
                                  const std::string& indexName) const;
    // Build composite key from row buffer
    static std::string buildCompositeKey(const std::string& rowBuffer, const TableSchema& tbl,
                                          const std::vector<std::string>& colNames);

    // Full-text index (simplified inverted index)
    DBStatus createFullTextIndex(const std::string& dbname, const std::string& tablename,
                                  const std::string& colname);
    DBStatus dropFullTextIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& colname);
    bool hasFullTextIndex(const std::string& dbname, const std::string& tablename,
                          const std::string& colname) const;
    std::vector<int64_t> fullTextSearch(const std::string& dbname, const std::string& tablename,
                                        const std::string& colname, const std::string& word) const;
    std::vector<std::string> getFullTextIndexedColumns(const std::string& dbname,
                                                        const std::string& tablename) const;

    // GIN index (Generalized Inverted Index): supports text/json/array multi-key queries
    DBStatus createGinIndex(const std::string& dbname, const std::string& tablename,
                             const std::string& colname);
    DBStatus dropGinIndex(const std::string& dbname, const std::string& tablename,
                           const std::string& colname);
    bool hasGinIndex(const std::string& dbname, const std::string& tablename,
                      const std::string& colname) const;
    std::vector<int64_t> ginSearch(const std::string& dbname, const std::string& tablename,
                                    const std::string& colname, const std::string& key) const;
    std::vector<std::string> getGinIndexedColumns(const std::string& dbname,
                                                   const std::string& tablename) const;

    // GiST index (Generalized Search Tree): simplified spatial/range index
    DBStatus createGiSTIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname);
    DBStatus dropGiSTIndex(const std::string& dbname, const std::string& tablename,
                            const std::string& colname);
    bool hasGiSTIndex(const std::string& dbname, const std::string& tablename,
                       const std::string& colname) const;
    // Search rows whose indexed value OVERLAPS with query range [low,high]
    std::vector<int64_t> giSTSearchOverlap(const std::string& dbname, const std::string& tablename,
                                            const std::string& colname,
                                            const std::string& low, const std::string& high) const;
    // Search rows whose indexed value is CONTAINED BY query range [low,high]
    std::vector<int64_t> giSTSearchContainedBy(const std::string& dbname, const std::string& tablename,
                                                const std::string& colname,
                                                const std::string& low, const std::string& high) const;
    std::vector<std::string> getGiSTIndexedColumns(const std::string& dbname,
                                                    const std::string& tablename) const;

    // BRIN index (Block Range Index): per-block min/max summary for range queries
    DBStatus createBrinIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname, size_t pagesPerRange = 64);
    DBStatus dropBrinIndex(const std::string& dbname, const std::string& tablename,
                            const std::string& colname);
    bool hasBrinIndex(const std::string& dbname, const std::string& tablename,
                       const std::string& colname) const;
    // Returns page ranges (start,end) that MAY contain rows matching the condition
    std::vector<std::pair<uint32_t, uint32_t>> brinSearchRange(
        const std::string& dbname, const std::string& tablename, const std::string& colname,
        const std::string& op, const std::string& value) const;
    std::vector<std::string> getBrinIndexedColumns(const std::string& dbname,
                                                    const std::string& tablename) const;

    // SP-GiST index (Space-Partitioned GiST): quadtree for POINT type
    DBStatus createSPGiSTIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& colname);
    DBStatus dropSPGiSTIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname);
    bool hasSPGiSTIndex(const std::string& dbname, const std::string& tablename,
                         const std::string& colname) const;
    std::vector<int64_t> spGiSTSearch(const std::string& dbname, const std::string& tablename,
                                       const std::string& colname,
                                       const std::string& op,
                                       const std::string& value) const;
    std::vector<std::string> getSPGiSTIndexedColumns(const std::string& dbname,
                                                      const std::string& tablename) const;

    // Condition parsing (public for execution plan use)
    struct Condition {
        std::string op;  // "<", ">", "=", "<=", ">=", "!=", "like"
        std::string colName;
        std::string value;
    };
    static std::vector<Condition> parseConditions(const std::vector<std::string>& cstr);
    static bool evalConditionOnRow(const Condition& cond, const std::string& rowBuffer, const TableSchema& tbl);
    static int64_t parseInt(const std::string& s);
    static bool stringToBuffer(const std::string& src, char* dst, size_t len);
    // extractColumnValue with optional dbname for TOAST resolution
    static std::string extractColumnValue(const std::string& rowBuffer,
                                           const TableSchema& tbl, size_t colIdx,
                                           const std::string& dbname = "");

    // RID encode/decode (public for external use)
    static int64_t encodeRid(uint32_t pageId, uint16_t slotId);
    static void decodeRid(int64_t rid, uint32_t& pageId, uint16_t& slotId);

    // MVCC ReadView
    struct ReadView {
        uint64_t creatorTxnId = 0;
        uint64_t upLimitId = 0;
        uint64_t lowLimitId = 0;
        std::set<uint64_t> activeTxnIds;
        std::set<uint64_t> subTxnIds;        // subtransaction IDs in progress
        const CommitLog* commitLog = nullptr; // for CLOG lookups
        bool isVisible(uint64_t rowTxnId) const;
        // PostgreSQL-style visibility using full HeapTupleHeader row buffer
        bool isVisible(const char* rowBuffer, size_t len, uint32_t formatVersion) const;
    };

    // Row iteration (public for execution plan use)
    // targetPartitions: empty = all partitions; only effective for partitioned tables
    void forEachRow(const std::string& dbname, const std::string& tablename,
                    const std::function<void(uint32_t pageId, uint16_t slotId, const char* data, size_t len)>& callback,
                    const ReadView* readView = nullptr,
                    const std::vector<std::string>& targetPartitions = {}) const;
    bool readRowByRid(PageAllocator* pa, int64_t rid, std::string& rowBuffer, const TableSchema& tbl) const;

    // Schema helpers (public for execution plan use)
    static std::string extractPKValue(const std::string& rowBuffer, const TableSchema& tbl);

    // Index access (public for execution plan)
    BPTree* getPKIndex(const std::string& dbname, const std::string& tablename) const;
    BPTree* getSecondaryIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname) const;
    PageAllocator* getPageAllocator(const std::string& dbname, const std::string& tablename) const;
    FreeSpaceMap* getFSM(const std::string& dbname, const std::string& tablename) const;
    VisibilityMap* getVM(const std::string& dbname, const std::string& tablename) const;

    // Table-level permissions
    enum class TablePrivilege { Select, Insert, Update, Delete, All, Usage, Execute };
    void grant(const std::string& dbname, const std::string& tablename,
               const std::string& username, TablePrivilege priv,
               const std::vector<std::string>& columns = {},
               bool withGrantOption = false,
               const std::string& grantedBy = "");
    void revoke(const std::string& dbname, const std::string& tablename,
                const std::string& username, TablePrivilege priv,
                const std::vector<std::string>& columns = {},
                bool cascade = false);
    bool hasPermission(const std::string& dbname, const std::string& tablename,
                       const std::string& username, TablePrivilege priv) const;
    bool hasGrantOption(const std::string& dbname, const std::string& tablename,
                        const std::string& username, TablePrivilege priv) const;
    // Column-level permission check: returns true if user has privilege on ALL given columns
    bool hasColumnPermission(const std::string& dbname, const std::string& tablename,
                             const std::string& username, TablePrivilege priv,
                             const std::vector<std::string>& columns) const;
    std::vector<std::string> getUserPermissions(const std::string& dbname,
                                                 const std::string& tablename,
                                                 const std::string& username) const;

    // Row-Level Security (RLS)
    struct RowPolicy {
        std::string name;
        std::string cmd;           // ALL, SELECT, INSERT, UPDATE, DELETE
        std::string usingExpr;     // USING expression (for SELECT/UPDATE/DELETE)
        std::string withCheckExpr; // WITH CHECK expression (for INSERT/UPDATE)
        std::vector<std::string> roles; // empty = PUBLIC
    };
    DBStatus createPolicy(const std::string& dbname, const std::string& tablename,
                          const RowPolicy& policy);
    DBStatus alterPolicy(const std::string& dbname, const std::string& tablename,
                         const std::string& policyName, const RowPolicy& policy);
    DBStatus dropPolicy(const std::string& dbname, const std::string& tablename,
                        const std::string& policyName);
    std::vector<RowPolicy> getPolicies(const std::string& dbname, const std::string& tablename) const;
    std::vector<RowPolicy> getApplicablePolicies(const std::string& dbname, const std::string& tablename,
                                                  const std::string& cmd,
                                                  const std::string& username) const;
    DBStatus enableRowLevelSecurity(const std::string& dbname, const std::string& tablename,
                                     bool force = false);
    DBStatus disableRowLevelSecurity(const std::string& dbname, const std::string& tablename);

    // RLS current user (thread-local, for transparent policy application inside engine)
    static void setRLSUser(const std::string& user) { rlsCurrentUser_ = user; }
    static std::string getRLSUser() { return rlsCurrentUser_; }

    // Partition pruning: given conditions, return partition names to scan
    // (empty = all partitions, only applicable for partitioned tables)
    std::vector<std::string> getTargetPartitions(const TableSchema& tbl,
                                                  const std::vector<Condition>& conds) const;

    // Declarative partition management (ATTACH/DETACH PARTITION)
    DBStatus attachPartition(const std::string& dbname, const std::string& tablename,
                              const std::string& partitionName,
                              const std::string& partitionSpec);
    DBStatus detachPartition(const std::string& dbname, const std::string& tablename,
                              const std::string& partitionName);

    // Lock manager access
    LockManager& getLockManager() { return lockManager_; }
    const LockManager& getLockManager() const { return lockManager_; }

    // Trigger support
    struct Trigger {
        std::string name;
        std::string timing;   // "before" or "after"
        std::string event;    // "insert", "update", "delete"
        std::string tableName;
        std::string action;   // SQL action (e.g., "insert into audit_log values (...)")
        std::string whenCondition; // WHEN (condition) — empty = no condition
        bool forEachRow = true;  // true = FOR EACH ROW, false = FOR EACH STATEMENT
        bool enabled = true;     // true = ENABLED, false = DISABLED
    };
    DBStatus createTrigger(const std::string& dbname, const Trigger& trg);
    DBStatus dropTrigger(const std::string& dbname, const std::string& trgName);
    DBStatus enableTrigger(const std::string& dbname, const std::string& trgName);
    DBStatus disableTrigger(const std::string& dbname, const std::string& trgName);
    std::vector<Trigger> getTriggers(const std::string& dbname, const std::string& tablename,
                                      const std::string& timing, const std::string& event) const;
    std::vector<Trigger> getAllTriggers(const std::string& dbname) const;

    // Trigger executor callback: action SQL -> success/failure
    using TriggerExecutor = std::function<bool(const std::string& actionSql)>;
    void setTriggerExecutor(TriggerExecutor executor) { triggerExecutor_ = executor; }
    bool executeTriggerAction(const std::string& actionSql) const {
        if (triggerExecutor_) return triggerExecutor_(actionSql);
        return false;
    }
    // WHEN condition evaluator: condition string + NEW/OLD values -> true/false
    using WhenConditionEvaluator = std::function<bool(
        const std::string& condition,
        const std::map<std::string, std::string>& newValues,
        const std::map<std::string, std::string>& oldValues)>;
    void setWhenConditionEvaluator(WhenConditionEvaluator evaluator) { whenEvaluator_ = evaluator; }
    bool evaluateWhenCondition(const std::string& condition,
                               const std::map<std::string, std::string>& newValues,
                               const std::map<std::string, std::string>& oldValues) const {
        if (whenEvaluator_) return whenEvaluator_(condition, newValues, oldValues);
        return true; // no evaluator = always true
    }

    // Current transaction ID (0 = not in a transaction)
    uint64_t currentTxnId() const { return currentTxnId_; }
    const ReadView* getCurrentReadView() const {
        return inTransaction_ ? &readView_ : nullptr;
    }

    // Snapshot export/import (for cross-backend snapshot sharing)
    std::string exportSnapshot() const;
    bool importSnapshot(const std::string& bytes);

    // Transaction isolation levels (unified with dbms::IsolationLevel in dbms_defs.h)
    void setIsolationLevel(IsolationLevel level) { txnIsolationLevel_ = level; }
    IsolationLevel getIsolationLevel() const { return txnIsolationLevel_; }
    void refreshReadView();  // For READ COMMITTED: re-snapshot before each query

public:
    std::filesystem::path dbPath(const std::string& dbname) const;

    // CommitLog access (lazy init)
    CommitLog* getCommitLog(const std::string& dbname) const;

private:
    std::filesystem::path schemaPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path paramsPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path dataPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path partitionDataPath(const std::string& dbname, const std::string& tablename,
                                            const std::string& partitionName) const;
    std::filesystem::path partitionDataPath(const std::string& dbname, const std::string& tablename,
                                            const std::string& partitionName,
                                            const std::string& subPartitionName) const;
    std::string getPartitionName(const TableSchema& tbl, const std::string& keyVal) const;
    std::string getSubPartitionName(const TableSchema& tbl, const std::string& keyVal) const;
    std::filesystem::path tableListPath(const std::string& dbname) const;
    std::filesystem::path walPath(const std::string& dbname) const;
    std::filesystem::path checkpointPath(const std::string& dbname) const;
    std::filesystem::path fsmPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path vmPath(const std::string& dbname, const std::string& tablename) const;

public:
    std::filesystem::path viewPath(const std::string& dbname, const std::string& viewname) const;
    std::filesystem::path viewsDir(const std::string& dbname) const;
    std::filesystem::path statsPath(const std::string& dbname) const;
    std::filesystem::path permPath(const std::string& dbname) const;
    std::filesystem::path rlsPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path seqPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path seclabelPath(const std::string& dbname) const;

    // Security Labels
    void setSecurityLabel(const std::string& dbname, const std::string& objType,
                          const std::string& objName, const std::string& label);
    std::string getSecurityLabel(const std::string& dbname, const std::string& objType,
                                 const std::string& objName) const;
    std::vector<std::tuple<std::string, std::string, std::string>> getAllSecurityLabels(
        const std::string& dbname) const;

    // Default Privileges
    void addDefaultPrivilege(const std::string& dbname, const std::string& owner,
                             const std::string& schema, const std::string& objType,
                             const std::string& privilege, const std::string& grantee);
    void removeDefaultPrivilege(const std::string& dbname, const std::string& owner,
                                const std::string& schema, const std::string& objType,
                                const std::string& privilege, const std::string& grantee);
    void applyDefaultPrivileges(const std::string& dbname, const std::string& schema,
                                const std::string& objType, const std::string& objName,
                                const std::string& owner) const;
    std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>>
    getDefaultPrivileges(const std::string& dbname) const;

    // TOAST (The Oversized-Attribute Storage Technique) for large values
    static constexpr size_t TOAST_THRESHOLD_BASE = 1000; // bytes for 4KB pages
    static constexpr const char* TOAST_PREFIX = "__TOAST__";
    // Dynamic threshold: larger pages can hold larger inline values
    static size_t toastThreshold(uint32_t formatVersion) {
        size_t ps = pageSizeForFormatVersion(formatVersion);
        return std::max(TOAST_THRESHOLD_BASE, ps / 4);
    }
    static std::filesystem::path toastDir(const std::string& dbname, const std::string& tablename);
    static std::filesystem::path toastMetaPath(const std::string& dbname, const std::string& tablename);
    uint64_t allocToastId(const std::string& dbname, const std::string& tablename);
    void writeToast(const std::string& dbname, const std::string& tablename, uint64_t toastId, const std::string& data);
    static std::string readToast(const std::string& dbname, const std::string& tablename, uint64_t toastId);
    void deleteToast(const std::string& dbname, const std::string& tablename, uint64_t toastId);
    void deleteRowToast(const std::string& dbname, const std::string& tablename, int64_t rid);
    // Parse toast ID from marker string __TOAST__<id>
    static bool parseToastMarker(const std::string& val, uint64_t& toastId);

    // Prepare values for insert/update: create TOAST entries for large var-len values
    void prepareToastValues(const std::string& dbname, const std::string& tablename,
                            const TableSchema& tbl, std::map<std::string, std::string>& values);

    // VACUUM orphaned TOAST files: remove toast entries no longer referenced by any row
    size_t vacuumToast(const std::string& dbname, const std::string& tablename);

    // ========================================================================
    // Tablespace management
    // ========================================================================
    // Resolve the physical directory for a tablespace.
    // "pg_default" returns dbPath(dbname); others read from dbname/pg_tblspc/<name>.path
    std::filesystem::path tablespaceDir(const std::string& dbname, const std::string& tablespaceName) const;
    // Create a new tablespace pointing to an absolute directory
    DBStatus createTablespace(const std::string& dbname, const std::string& tsName, const std::string& physicalPath);
    // Drop a user-defined tablespace (must be empty)
    DBStatus dropTablespace(const std::string& dbname, const std::string& tsName);
    // List all tablespaces
    std::vector<std::string> listTablespaces(const std::string& dbname) const;

private:

    // Auto-increment sequence helpers
    int64_t readNextSeq(const std::string& dbname, const std::string& tablename, const std::string& colname);
    void writeNextSeq(const std::string& dbname, const std::string& tablename, const std::string& colname, int64_t val);
    void removeSeq(const std::string& dbname, const std::string& tablename);

    void writeSchema(std::ostream& out, const TableSchema& tbl);
    TableSchema readSchema(std::istream& in, const std::string& tablename) const;

    // Page-based heap storage
    mutable std::map<std::string, std::unique_ptr<PageAllocator>> pageAllocators_;
    void closeAllPageAllocators();
    void migrateAllDataFiles();
    void migrateToPageStorage(const std::string& dbname, const std::string& tablename) const;

    // Free Space Map + Visibility Map (fork files)
    mutable std::map<std::string, std::unique_ptr<FreeSpaceMap>> fsmCache_;
    mutable std::map<std::string, std::unique_ptr<VisibilityMap>> vmCache_;
    void closeAllFSM();
    void closeAllVM();

    // CommitLog (pg_xact)
    mutable std::unordered_map<std::string, std::unique_ptr<CommitLog>> commitLogs_;
    void closeAllCommitLogs();

    // WAL manager per database
    mutable std::unordered_map<std::string, std::unique_ptr<WALManager>> walManagers_;
    WALManager* getWAL(const std::string& dbname) const;
    void closeAllWALs();

    // Per-database checkpoint LSN. Pages with pd_lsn <= this value trigger
    // a full-page write on their next modification.
    mutable std::map<std::string, Lsn> lastCheckpointLsns_;

    // Helpers to emit WAL records for heap operations.
    // These are no-ops for legacy formatVersion 0/1 tables.
    Lsn walPageImage(const std::string& dbname, const std::string& tablename,
                     uint32_t pageId, const char* pageBuf, size_t pageSize,
                     bool beforeImage);
    Lsn walXactCommit(const std::string& dbname, uint64_t xid);
    Lsn walXactAbort(const std::string& dbname, uint64_t xid);
    Lsn walCheckpoint(const std::string& dbname, uint64_t nextXid);

    // Mark a page as dirty and update its pd_lsn after a WAL record was written.
    void markPageDirtyAndLsn(PageAllocator* pa, uint32_t pageId, Lsn lsn, uint32_t formatVersion);

    // Redo a single WAL record.
    bool redoPageImage(const std::string& dbname, const std::string& tablename,
                       uint32_t pageId, const char* pageData, size_t pageLen,
                       Lsn recordLsn, bool force);
    bool redoXactCommit(uint64_t xid);
    bool redoXactAbort(uint64_t xid);

    // B+ Tree primary key index
    std::filesystem::path indexPath(const std::string& dbname, const std::string& tablename) const;
    mutable std::map<std::string, std::unique_ptr<BPTree>> pkIndexCache_;
    void closeAllIndexes();

    // Secondary index helpers
    std::filesystem::path secondaryIndexPath(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& colname) const;
    std::filesystem::path secondaryIndexMetaPath(const std::string& dbname,
                                                  const std::string& tablename) const;
    mutable std::map<std::string, std::unique_ptr<BPTree>> secondaryIndexCache_;

    // Hash index helpers
    std::filesystem::path hashIndexPath(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname) const;
    std::filesystem::path hashIndexMetaPath(const std::string& dbname,
                                             const std::string& tablename) const;
    mutable std::map<std::string, std::unique_ptr<HashIndex>> hashIndexCache_;

    // SP-GiST index cache
    mutable std::mutex spGiSTMutex_;
    mutable std::map<std::string, std::unique_ptr<SPGiSTIndex>> spGiSTCache_;

    // Trigger helpers
    std::filesystem::path triggerPath(const std::string& dbname) const;
    void writeTrigger(std::ostream& out, const Trigger& trg) const;
    Trigger readTrigger(std::istream& in) const;
    TriggerExecutor triggerExecutor_;
    WhenConditionEvaluator whenEvaluator_;

    // Evaluate a single row against conditions, returning matching row indices
    std::set<int64_t> filterRows(const std::string& dbname, const std::string& tablename,
                                 const std::vector<Condition>& conds);

private:
    mutable LockManager lockManager_;

    // Advisory locks (session-level, non-persistent)
    mutable std::mutex advisoryMutex_;
    std::map<int64_t, int> advisoryLocks_; // key -> exclusive count (positive) or shared count (negative)

    // Auto-VACUUM: per-table dead tuple tracking
    mutable std::mutex deadTupleMutex_;
    mutable std::map<std::pair<std::string, std::string>, size_t> deadTupleCounts_;

    // Auto-ANALYZE: per-table modification tracking
    mutable std::mutex modifyMutex_;
    mutable std::map<std::pair<std::string, std::string>, size_t> modifyCounts_;

    // Transaction state
    bool inTransaction_ = false;
    bool readOnly_ = false;
    std::string txnDB_;
    uint64_t currentTxnId_ = 0;
    ReadView readView_;
    IsolationLevel txnIsolationLevel_ = IsolationLevel::REPEATABLE_READ;
    struct TxnLogEntry {
        enum class Op { Insert, Update, Delete } op;
        std::string tableName;
        int64_t rowIdx;
        std::string rowData;
    };
    std::vector<TxnLogEntry> txnLog_;
    void logTxnInsert(const std::string& tableName, int64_t rowIdx);
    void logTxnUpdate(const std::string& tableName, int64_t rowIdx, const std::string& oldRowData);
    void logTxnDelete(const std::string& tableName, int64_t rowIdx, const std::string& oldRowData);

    // Savepoint support
    std::map<std::string, size_t> savepoints_; // name -> txnLog_ index
    std::vector<uint64_t> txnSubTxnIds_;       // current transaction's subtransaction IDs (reserved)

    // Catalog snapshot: consistent schema/table list view within a transaction
    struct CatalogSnapshot {
        std::map<std::pair<std::string, std::string>, TableSchema> schemas;
        std::map<std::string, std::vector<std::string>> tableNames;
    };
    mutable std::optional<CatalogSnapshot> catalogSnapshot_;
    mutable std::mutex catalogSnapshotMutex_;
    void captureCatalogSnapshot();
    void clearCatalogSnapshot();
    void invalidateCatalogSchema(const std::string& dbname, const std::string& tablename);
    void invalidateCatalogTableList(const std::string& dbname);

    // SSI (Serializable Snapshot Isolation) read/write sets
    mutable std::set<int64_t> txnReadRids_;    // RIDs read by current transaction
    mutable std::set<int64_t> txnWrittenRids_; // RIDs written by current transaction

    // Global active transaction tracking (for ReadView)
    static std::mutex globalTxnMutex_;
    static std::set<uint64_t> activeTransactions_;
    // Global SSI conflict tracking: txId -> set of txIds that it has rw-conflict with
    static std::mutex ssiMutex_;
    static std::map<uint64_t, std::set<int64_t>> ssiReadSets_;   // txId -> RIDs read
    static std::map<uint64_t, std::set<int64_t>> ssiWriteSets_;  // txId -> RIDs written
    static std::map<uint64_t, std::set<uint64_t>> ssiOutEdges_; // T1 -> {T2} means T1 read something written by T2
    static std::map<uint64_t, std::set<uint64_t>> ssiInEdges_;  // T2 -> {T1} means T1 read something written by T2

    // Row-Level Security: per-thread current user (for transparent RLS application)
    inline static thread_local std::string rlsCurrentUser_;
};

// Column type constructors
Column makeIntColumn(const std::string& name, bool isNull, int scale, bool isPK = false, bool isUnsigned = false);
Column makeStringColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeDateColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeVarCharColumn(const std::string& name, bool isNull, size_t maxLen, bool isPK = false);
Column makeNCharColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeNVarCharColumn(const std::string& name, bool isNull, size_t maxLen, bool isPK = false);
Column makeTimestampColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTimestamptzColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTextColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeBlobColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeBinaryColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeVarBinaryColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeJsonColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeJsonbColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeXmlColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeFloatColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDoubleColumn(const std::string& name, bool isNull, bool isPK = false);
Column makePointColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeINetColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeCidrColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDecimalColumn(const std::string& name, bool isNull, int precision, int scale, bool isPK = false);
Column makePgLsnColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeBooleanColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeUuidColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTimeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDateTimeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeIntervalColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeInt4RangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeInt8RangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeNumRangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTsRangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTstzRangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDateRangeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTsVectorColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTsQueryColumn(const std::string& name, bool isNull, bool isPK = false);

} // namespace dbms
