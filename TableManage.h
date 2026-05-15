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
    std::string dataType;
    std::string dataName;
    size_t dsize = 0;  // For VARCHAR: max length; for fixed: actual bytes
    std::string defaultValue;       // DEFAULT value
    std::string checkExpr;          // CHECK constraint expression

    void print() const;
};

struct ForeignKey {
    std::string colName;        // local column
    std::string refTable;       // referenced table
    std::string refCol;         // referenced column
    std::string onDelete = "restrict";  // restrict | cascade | setnull
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
};

class StorageEngine {
public:
    StorageEngine();

    // Database operations
    OpResult createDatabase(const std::string& dbname);
    OpResult dropDatabase(const std::string& dbname);
    bool databaseExists(const std::string& dbname) const;

    // Table operations
    OpResult createTable(const std::string& dbname, const TableSchema& tbl);
    OpResult createTable(const std::string& dbname, const std::string& tablename, const TableSchema& tbl);
    OpResult dropTable(const std::string& dbname, const std::string& tablename);
    OpResult alterTableAddColumn(const std::string& dbname, const std::string& tablename,
                                  const Column& col);
    OpResult alterTableDropColumn(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    bool tableExists(const std::string& dbname, const std::string& tablename) const;
    std::vector<std::string> getTableNames(const std::string& dbname) const;
    TableSchema getTableSchema(const std::string& dbname, const std::string& tablename) const;

    // View support
    OpResult createView(const std::string& dbname, const std::string& viewname, const std::string& sql);
    OpResult dropView(const std::string& dbname, const std::string& viewname);
    bool viewExists(const std::string& dbname, const std::string& viewname) const;
    std::string getViewSQL(const std::string& dbname, const std::string& viewname) const;
    std::vector<std::string> getViewNames(const std::string& dbname) const;

    // Statistics
    struct ColumnStats {
        size_t cardinality = 0;
        std::string minVal;
        std::string maxVal;
    };
    struct TableStats {
        size_t rowCount = 0;
        std::map<std::string, ColumnStats> colStats;
    };
    void analyzeTable(const std::string& dbname, const std::string& tablename);
    size_t getTableRowCount(const std::string& dbname, const std::string& tablename) const;
    ColumnStats getColumnStats(const std::string& dbname, const std::string& tablename,
                                const std::string& colname) const;

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
    };
    std::vector<std::string> query(const std::string& dbname, const std::string& tablename,
                                   const std::vector<std::string>& conditions,
                                   const std::set<std::string>& selectCols,
                                   const std::vector<OrderBySpec>& orderBy = {});

    // Scalar function expression for queryExpr
    struct SelectExpr {
        std::string displayName;
        bool isScalar = false;
        std::string funcName;
        std::vector<std::string> funcArgs;
        std::string colName;
    };
    std::vector<std::string> queryExpr(const std::string& dbname,
                                        const std::string& tablename,
                                        const std::vector<std::string>& conditions,
                                        const std::vector<SelectExpr>& exprs,
                                        const std::vector<OrderBySpec>& orderBy = {});

    // Aggregate query: items = {("count","*"), ("max","score"), ...}
    std::vector<std::string> aggregate(const std::string& dbname, const std::string& tablename,
                                       const std::vector<std::string>& conditions,
                                       const std::vector<std::pair<std::string, std::string>>& items);

    // Group aggregate: GROUP BY groupCol(s) with HAVING filter
    std::vector<std::string> groupAggregate(
        const std::string& dbname, const std::string& tablename,
        const std::vector<std::string>& conditions,
        const std::vector<std::pair<std::string, std::string>>& items,
        const std::vector<std::string>& groupByCols,
        const std::vector<std::string>& havingConds);

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

    // VACUUM: reclaim space from deleted rows
    size_t vacuum(const std::string& dbname, const std::string& tablename);

    // Secondary index (single-column)
    OpResult createIndex(const std::string& dbname, const std::string& tablename,
                         const std::string& colname);
    OpResult dropIndex(const std::string& dbname, const std::string& tablename,
                       const std::string& colname);
    std::vector<std::string> getIndexedColumns(const std::string& dbname,
                                                const std::string& tablename) const;

    // Composite index (multi-column)
    struct CompositeIndexInfo {
        std::string name;
        std::vector<std::string> columns;
    };
    OpResult createCompositeIndex(const std::string& dbname, const std::string& tablename,
                                  const std::vector<std::string>& colnames,
                                  const std::string& indexName);
    OpResult dropCompositeIndex(const std::string& dbname, const std::string& tablename,
                                const std::string& indexName);
    std::vector<CompositeIndexInfo> getCompositeIndexes(const std::string& dbname,
                                                         const std::string& tablename) const;
    BPTree* getCompositeIndexTree(const std::string& dbname, const std::string& tablename,
                                  const std::string& indexName) const;
    // Build composite key from row buffer
    static std::string buildCompositeKey(const std::string& rowBuffer, const TableSchema& tbl,
                                          const std::vector<std::string>& colNames);

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
    void forEachRow(const std::string& dbname, const std::string& tablename,
                    const std::function<void(uint32_t pageId, uint16_t slotId, const char* data, size_t len)>& callback,
                    const ReadView* readView = nullptr) const;
    bool readRowByRid(PageAllocator* pa, int64_t rid, std::string& rowBuffer, const TableSchema& tbl) const;

    // Schema helpers (public for execution plan use)
    static std::string extractPKValue(const std::string& rowBuffer, const TableSchema& tbl);
    static std::string extractColumnValue(const std::string& rowBuffer, const TableSchema& tbl, size_t colIdx);

    // Index access (public for execution plan)
    BPTree* getPKIndex(const std::string& dbname, const std::string& tablename) const;
    BPTree* getSecondaryIndex(const std::string& dbname, const std::string& tablename,
                              const std::string& colname) const;
    PageAllocator* getPageAllocator(const std::string& dbname, const std::string& tablename) const;

    // Table-level permissions
    enum class TablePrivilege { Select, Insert, Update, Delete, All };
    void grant(const std::string& dbname, const std::string& tablename,
               const std::string& username, TablePrivilege priv);
    void revoke(const std::string& dbname, const std::string& tablename,
                const std::string& username, TablePrivilege priv);
    bool hasPermission(const std::string& dbname, const std::string& tablename,
                       const std::string& username, TablePrivilege priv) const;
    std::vector<std::string> getUserPermissions(const std::string& dbname,
                                                 const std::string& tablename,
                                                 const std::string& username) const;

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
    std::filesystem::path tableListPath(const std::string& dbname) const;
    std::filesystem::path walPath(const std::string& dbname) const;
    std::filesystem::path checkpointPath(const std::string& dbname) const;
    std::filesystem::path viewPath(const std::string& dbname, const std::string& viewname) const;
    std::filesystem::path viewsDir(const std::string& dbname) const;
    std::filesystem::path statsPath(const std::string& dbname) const;
    std::filesystem::path permPath(const std::string& dbname) const;
    std::filesystem::path seqPath(const std::string& dbname, const std::string& tablename) const;

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

    // Evaluate a single row against conditions, returning matching row indices
    std::set<int64_t> filterRows(const std::string& dbname, const std::string& tablename,
                                 const std::vector<Condition>& conds);

    // Lock manager
    mutable LockManager lockManager_;

    // Transaction state
    bool inTransaction_ = false;
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
Column makeIntColumn(const std::string& name, bool isNull, int scale, bool isPK = false);
Column makeStringColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeDateColumn(const std::string& name, bool isNull, bool isPK = false);
Column makeVarCharColumn(const std::string& name, bool isNull, size_t maxLen, bool isPK = false);
Column makeTimestampColumn(const std::string& name, bool isNull, bool isPK = false);

} // namespace dbms
