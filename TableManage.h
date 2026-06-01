#pragma once

#include <cstdint>
#include <fstream>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "DateType.h"
#include "BPTree.h"
#include "BufferPool.h"
#include "PageAllocator.h"
#include "LockManager.h"
#include "HashIndex.h"

namespace dbms {

constexpr size_t MAX_COLUMNS = 30;
constexpr size_t MAX_TABLE_NAME_LEN = 15;
constexpr size_t MAX_TYPE_NAME_LEN = 12;
constexpr size_t MAX_COL_NAME_LEN = 15;
constexpr size_t DATE_SIZE = 12;
constexpr size_t TIMESTAMP_SIZE = 8;
constexpr int64_t INF = 0x8000000000000000LL;

// MVCC header size per row
constexpr size_t MVCC_TXID_SIZE = 8;      // uint64_t creatorTxnId
constexpr size_t MVCC_ROLLBACK_SIZE = 8;  // uint64_t rollbackPtr (0 = no older version)
constexpr size_t MVCC_HEADER_SIZE = MVCC_TXID_SIZE + MVCC_ROLLBACK_SIZE;

struct Column {
    bool isNull = false;
    bool isPrimaryKey = false;
    bool isVariableLength = false;  // true for VARCHAR, false for fixed-length types
    bool isUnique = false;          // UNIQUE constraint
    bool isAutoIncrement = false;   // AUTO_INCREMENT / SERIAL
    bool isUnsigned = false;        // UNSIGNED for numeric types
    bool isArray = false;           // true for array types (INT[], VARCHAR[])
    std::string dataType;
    std::string dataName;
    size_t dsize = 0;  // For VARCHAR: max length; for fixed: actual bytes
    std::string defaultValue;       // DEFAULT value
    std::string checkExpr;          // CHECK constraint expression
    std::string checkConstraintName; // Name of the CHECK constraint
    std::string generatedExpr;      // GENERATED ALWAYS AS (expr)
    std::vector<std::string> enumValues;  // ENUM('a','b','c') values

    void print() const;
};

struct ForeignKey {
    std::vector<std::string> colNames;     // local columns (composite FK)
    std::vector<std::string> refCols;      // referenced columns
    std::string refTable;                  // referenced table
    std::string onDelete = "restrict";     // restrict | cascade | setnull
    std::string onUpdate = "restrict";     // restrict | cascade | setnull
    std::string name;                      // constraint name

    // Back-compat helpers
    bool isSingleColumn() const { return colNames.size() <= 1; }
    std::string singleColName() const { return colNames.empty() ? "" : colNames[0]; }
    std::string singleRefCol() const { return refCols.empty() ? "" : refCols[0]; }
};

struct TableSchema {
    std::string tablename;
    Column cols[MAX_COLUMNS];
    size_t len = 0;
    ForeignKey fks[MAX_COLUMNS];
    size_t fkLen = 0;

    // Composite primary key column indices (empty = use single-column isPrimaryKey)
    std::vector<size_t> pkColIndices;
    // Composite UNIQUE constraints: each inner vector is column indices
    std::vector<std::vector<size_t>> uniqueConstraints;
    std::vector<std::string> uniqueConstraintNames; // names parallel to uniqueConstraints

    // Partitioning
    enum class PartitionType { None, Range, List, Hash };
    PartitionType partitionType = PartitionType::None;
    std::string partitionKey;  // column name for partitioning
    std::vector<std::pair<std::string, std::string>> rangePartitions;  // name -> upper bound
    std::vector<std::pair<std::string, std::vector<std::string>>> listPartitions;  // name -> values
    size_t hashPartitions = 0;  // number of hash partitions
    bool isUnlogged = false;    // UNLOGGED table: no WAL, truncated on crash

    void append(const Column& ncol);
    void appendFK(const ForeignKey& fk);
    void print() const;
    size_t rowSize() const;

    // PK helpers
    bool hasPrimaryKey() const;
    std::string buildPKValue(const std::string& rowBuffer) const;
    std::string buildPKValue(const std::map<std::string, std::string>& values) const;

    // Variable-length helpers
    bool hasVariableLength() const;
    size_t fixedDataSize() const;       // sum of dsize of all fixed-length columns
    size_t varColCount() const;         // number of variable-length columns
    size_t getVarColIndex(size_t colIdx) const;  // index among var columns
    size_t getFixedColOffset(size_t colIdx) const; // offset within fixed data region
};

// Result code for data operations
enum class OpResult {
    Success = 0,
    TableNotExist,
    DatabaseNotExist,
    TableAlreadyExist,
    InvalidValue,
    NullNotAllowed,
    SyntaxError,
    DuplicateKey,
    LockConflict,
};

class StorageEngine {
public:
    StorageEngine();

    // Database operations
    OpResult createDatabase(const std::string& dbname, const std::string& charset = "utf8");
    std::string getDatabaseCharset(const std::string& dbname) const;
    OpResult dropDatabase(const std::string& dbname);
    bool databaseExists(const std::string& dbname) const;
    std::vector<std::string> getDatabaseNames() const;

    // Table operations
    OpResult createTable(const std::string& dbname, const TableSchema& tbl);
    OpResult createTable(const std::string& dbname, const std::string& tablename, const TableSchema& tbl);
    OpResult dropTable(const std::string& dbname, const std::string& tablename);
    OpResult truncateTable(const std::string& dbname, const std::string& tablename);
    OpResult alterTableAddColumn(const std::string& dbname, const std::string& tablename,
                                  const Column& col);
    OpResult alterTableDropColumn(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    OpResult alterTableRenameColumn(const std::string& dbname, const std::string& tablename,
                                     const std::string& oldName, const std::string& newName);
    OpResult alterTableRenameTable(const std::string& dbname, const std::string& oldName,
                                    const std::string& newName);
    OpResult alterTableSetDefault(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName, const std::string& defaultValue);
    OpResult alterTableDropDefault(const std::string& dbname, const std::string& tablename,
                                    const std::string& colName);
    OpResult alterTableSetNotNull(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    OpResult alterTableDropNotNull(const std::string& dbname, const std::string& tablename,
                                    const std::string& colName);
    OpResult alterTableAddCheckConstraint(const std::string& dbname, const std::string& tablename,
                                           const std::string& name, const std::string& expr);
    OpResult alterTableAddUniqueConstraint(const std::string& dbname, const std::string& tablename,
                                            const std::string& name, const std::vector<std::string>& colNames);
    OpResult alterTableAddFKConstraint(const std::string& dbname, const std::string& tablename,
                                        const std::string& name, const std::vector<std::string>& localCols,
                                        const std::string& refTable, const std::vector<std::string>& refCols,
                                        const std::string& onDelete = "restrict", const std::string& onUpdate = "restrict");
    OpResult alterTableDropConstraint(const std::string& dbname, const std::string& tablename,
                                       const std::string& name);
    OpResult commentOnTable(const std::string& dbname, const std::string& tablename,
                             const std::string& comment);
    OpResult commentOnColumn(const std::string& dbname, const std::string& tablename,
                              const std::string& colname, const std::string& comment);
    std::string getTableComment(const std::string& dbname, const std::string& tablename) const;
    std::string getColumnComment(const std::string& dbname, const std::string& tablename,
                                  const std::string& colname) const;
    // Sequence support
    OpResult createSequence(const std::string& dbname, const std::string& seqname,
                            int64_t start = 1, int64_t increment = 1);
    OpResult dropSequence(const std::string& dbname, const std::string& seqname);
    int64_t nextval(const std::string& dbname, const std::string& seqname);
    int64_t currval(const std::string& dbname, const std::string& seqname);
    bool sequenceExists(const std::string& dbname, const std::string& seqname) const;
    std::vector<std::string> getSequenceNames(const std::string& dbname) const;
    bool tableExists(const std::string& dbname, const std::string& tablename) const;
    std::vector<std::string> getTableNames(const std::string& dbname) const;
    TableSchema getTableSchema(const std::string& dbname, const std::string& tablename) const;

    // View support
    OpResult createView(const std::string& dbname, const std::string& viewname, const std::string& sql);
    OpResult dropView(const std::string& dbname, const std::string& viewname);
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
    OpResult dropMaterializedView(const std::string& dbname, const std::string& viewname);

    // Stored procedures
    struct ProcParam {
        std::string name;
        std::string mode; // "IN", "OUT", "INOUT"
        std::string type;
    };
    OpResult createProcedure(const std::string& dbname, const std::string& procname,
                             const std::vector<ProcParam>& params,
                             const std::vector<std::string>& statements);
    OpResult dropProcedure(const std::string& dbname, const std::string& procname);
    bool procedureExists(const std::string& dbname, const std::string& procname) const;
    std::vector<std::string> getProcedureStatements(const std::string& dbname,
                                                     const std::string& procname) const;
    std::vector<ProcParam> getProcedureParams(const std::string& dbname,
                                               const std::string& procname) const;
    std::vector<std::string> getProcedureNames(const std::string& dbname) const;

    // Schema management
    OpResult createSchema(const std::string& dbname, const std::string& schemaname);
    OpResult dropSchema(const std::string& dbname, const std::string& schemaname, bool cascade);
    OpResult renameSchema(const std::string& dbname, const std::string& oldname,
                          const std::string& newname);
    bool schemaExists(const std::string& dbname, const std::string& schemaname) const;
    std::vector<std::string> getSchemaNames(const std::string& dbname) const;
    std::vector<std::string> getTablesInSchema(const std::string& dbname,
                                               const std::string& schemaname) const;

    // User-defined functions (simple expression-based)
    struct UDFInfo {
        std::string name;
        std::string paramName; // single-param compat
        std::string expression;
        std::vector<std::string> paramNames; // multi-param support
        std::vector<std::string> paramTypes; // multi-param support
    };
    OpResult createUDF(const std::string& dbname, const std::string& funcname,
                       const std::string& param, const std::string& expression);
    OpResult createUDF(const std::string& dbname, const std::string& funcname,
                       const std::vector<std::string>& params,
                       const std::vector<std::string>& types,
                       const std::string& expression);
    OpResult dropUDF(const std::string& dbname, const std::string& funcname);
    bool udfExists(const std::string& dbname, const std::string& funcname) const;
    UDFInfo getUDF(const std::string& dbname, const std::string& funcname) const;
    std::vector<std::string> getUDFNames(const std::string& dbname) const;

    // Table-valued functions (return a result set)
    OpResult createTVF(const std::string& dbname, const std::string& funcname,
                       const std::string& param, const std::string& sql);
    OpResult dropTVF(const std::string& dbname, const std::string& funcname);
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
    OpResult insert(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& values);
    OpResult update(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& updates,
                    const std::vector<std::string>& conditions);
    OpResult remove(const std::string& dbname, const std::string& tablename,
                    const std::vector<std::string>& conditions);
    struct OrderBySpec {
        std::string colName;
        bool ascending = true;
        // For ORDER BY expression support (e.g., ORDER BY length(name))
        std::string exprFunc;   // e.g., "length", "add", "sub"
        std::string exprArg;    // e.g., "name"
        std::string exprArg2;   // second operand for arithmetic (e.g., "1" for age + 1)
        bool isExpression = false;
        bool nullsFirst = false;  // NULLS FIRST / NULLS LAST
        std::string collation;    // e.g., "nocase", "binary" (default), "unicode"
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
    OpResult beginTransaction(const std::string& dbname);
    OpResult commitTransaction();
    OpResult rollbackTransaction();

    // Savepoint support
    OpResult savepoint(const std::string& name);
    OpResult rollbackToSavepoint(const std::string& name);
    OpResult releaseSavepoint(const std::string& name);

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
    size_t vacuum(const std::string& dbname, const std::string& tablename);
    // VACUUM FULL: rewrite table entirely, reclaiming all dead space
    size_t vacuumFull(const std::string& dbname, const std::string& tablename);

    // Auto-VACUUM: per-table dead tuple tracking and automatic triggering
    void maybeAutoVacuum(const std::string& dbname, const std::string& tablename);
    size_t getDeadTupleCount(const std::string& dbname, const std::string& tablename) const;
    void resetDeadTupleCount(const std::string& dbname, const std::string& tablename);

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
    OpResult createIndex(const std::string& dbname, const std::string& tablename,
                         const std::string& colname, bool ascending = true,
                         const std::vector<std::string>& includeCols = {},
                         const std::string& whereCondition = "",
                         const std::string& expression = "");
    OpResult dropIndex(const std::string& dbname, const std::string& tablename,
                       const std::string& colname);
    std::vector<std::string> getIndexedColumns(const std::string& dbname,
                                                const std::string& tablename) const;
    bool isDescendingIndex(const std::string& dbname, const std::string& tablename,
                           const std::string& colname) const;
    std::vector<std::string> getIndexIncludeColumns(const std::string& dbname,
                                                     const std::string& tablename,
                                                     const std::string& colname) const;

    // Hash index (single-column, O(1) equality lookup)
    OpResult createHashIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname);
    OpResult dropHashIndex(const std::string& dbname, const std::string& tablename,
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
    OpResult createCompositeIndex(const std::string& dbname, const std::string& tablename,
                                  const std::vector<std::string>& colnames,
                                  const std::string& indexName,
                                  const std::vector<std::string>& includeCols = {},
                                  const std::string& whereCondition = "");
    OpResult dropCompositeIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& indexName);
    std::vector<CompositeIndexInfo> getCompositeIndexes(const std::string& dbname,
                                                         const std::string& tablename) const;

    // Reindex: rebuild all indexes for a table
    OpResult reindex(const std::string& dbname, const std::string& tablename);
    BPTree* getCompositeIndexTree(const std::string& dbname, const std::string& tablename,
                                  const std::string& indexName) const;
    // Build composite key from row buffer
    static std::string buildCompositeKey(const std::string& rowBuffer, const TableSchema& tbl,
                                          const std::vector<std::string>& colNames);

    // Full-text index (simplified inverted index)
    OpResult createFullTextIndex(const std::string& dbname, const std::string& tablename,
                                  const std::string& colname);
    OpResult dropFullTextIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& colname);
    bool hasFullTextIndex(const std::string& dbname, const std::string& tablename,
                          const std::string& colname) const;
    std::vector<int64_t> fullTextSearch(const std::string& dbname, const std::string& tablename,
                                        const std::string& colname, const std::string& word) const;
    std::vector<std::string> getFullTextIndexedColumns(const std::string& dbname,
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
        bool isVisible(uint64_t rowTxnId) const;
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

    // Table-level permissions
    enum class TablePrivilege { Select, Insert, Update, Delete, All };
    void grant(const std::string& dbname, const std::string& tablename,
               const std::string& username, TablePrivilege priv,
               const std::vector<std::string>& columns = {},
               bool withGrantOption = false);
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

    // Partition pruning: given conditions, return partition names to scan
    // (empty = all partitions, only applicable for partitioned tables)
    std::vector<std::string> getTargetPartitions(const TableSchema& tbl,
                                                  const std::vector<Condition>& conds) const;

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
        bool forEachRow = true;  // true = FOR EACH ROW, false = FOR EACH STATEMENT
    };
    OpResult createTrigger(const std::string& dbname, const Trigger& trg);
    OpResult dropTrigger(const std::string& dbname, const std::string& trgName);
    std::vector<Trigger> getTriggers(const std::string& dbname, const std::string& tablename,
                                      const std::string& timing, const std::string& event) const;
    std::vector<Trigger> getAllTriggers(const std::string& dbname) const;

    // Trigger executor callback: action SQL -> success/failure
    using TriggerExecutor = std::function<bool(const std::string& actionSql)>;
    void setTriggerExecutor(TriggerExecutor executor) { triggerExecutor_ = executor; }

    // Current transaction ID (0 = not in a transaction)
    uint64_t currentTxnId() const { return currentTxnId_; }
    const ReadView* getCurrentReadView() const {
        return inTransaction_ ? &readView_ : nullptr;
    }

    // Transaction isolation levels
    enum class IsolationLevel { ReadUncommitted = 0, ReadCommitted = 1, RepeatableRead = 2, Serializable = 3 };
    void setIsolationLevel(IsolationLevel level) { txnIsolationLevel_ = level; }
    IsolationLevel getIsolationLevel() const { return txnIsolationLevel_; }
    void refreshReadView();  // For READ COMMITTED: re-snapshot before each query

private:
    std::filesystem::path dbPath(const std::string& dbname) const;
    std::filesystem::path schemaPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path dataPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path partitionDataPath(const std::string& dbname, const std::string& tablename,
                                            const std::string& partitionName) const;
    std::string getPartitionName(const TableSchema& tbl, const std::string& keyVal) const;
    std::filesystem::path tableListPath(const std::string& dbname) const;
    std::filesystem::path walPath(const std::string& dbname) const;
    std::filesystem::path checkpointPath(const std::string& dbname) const;

public:
    std::filesystem::path viewPath(const std::string& dbname, const std::string& viewname) const;
    std::filesystem::path viewsDir(const std::string& dbname) const;
    std::filesystem::path statsPath(const std::string& dbname) const;
    std::filesystem::path permPath(const std::string& dbname) const;
    std::filesystem::path seqPath(const std::string& dbname, const std::string& tablename) const;

    // TOAST (The Oversized-Attribute Storage Technique) for large values
    static constexpr size_t TOAST_THRESHOLD = 1000; // bytes
    static constexpr const char* TOAST_PREFIX = "__TOAST__";
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

    // Trigger helpers
    std::filesystem::path triggerPath(const std::string& dbname) const;
    void writeTrigger(std::ostream& out, const Trigger& trg) const;
    Trigger readTrigger(std::istream& in) const;
    TriggerExecutor triggerExecutor_;

    // Evaluate a single row against conditions, returning matching row indices
    std::set<int64_t> filterRows(const std::string& dbname, const std::string& tablename,
                                 const std::vector<Condition>& conds);

private:
    mutable LockManager lockManager_;

    // Auto-VACUUM: per-table dead tuple tracking
    mutable std::mutex deadTupleMutex_;
    mutable std::map<std::pair<std::string, std::string>, size_t> deadTupleCounts_;

    // Transaction state
    bool inTransaction_ = false;
    bool readOnly_ = false;
    std::string txnDB_;
    uint64_t currentTxnId_ = 0;
    ReadView readView_;
    IsolationLevel txnIsolationLevel_ = IsolationLevel::RepeatableRead;
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

    // Global active transaction tracking (for ReadView)
    static std::mutex globalTxnMutex_;
    static std::set<uint64_t> activeTransactions_;
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
Column makeFloatColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDoubleColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDecimalColumn(const std::string& name, bool isNull, int precision, int scale, bool isPK = false);
Column makeBooleanColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeUuidColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeTimeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeDateTimeColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeIntervalColumn(const std::string& name, bool isNull, bool isPK = false);

} // namespace dbms
