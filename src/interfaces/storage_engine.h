#pragma once

#include "dbms_defs.h"
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

// 前向声明
struct TableSchema;
struct Column;
struct ReadView;

// ============================================================================
// 存储引擎接口
// Phase 1+ 逐步实现：从当前 StorageEngine 类迁移至此接口
// ============================================================================

class IStorageEngine {
public:
    virtual ~IStorageEngine() = default;

    // ------------------------------------------------------------------------
    // 数据库生命周期
    // ------------------------------------------------------------------------
    virtual DBStatus createDatabase(const std::string& dbName) = 0;
    virtual DBStatus dropDatabase(const std::string& dbName) = 0;
    virtual DBStatus useDatabase(const std::string& dbName) = 0;
    virtual std::vector<std::string> listDatabases() const = 0;

    // ------------------------------------------------------------------------
    // 表生命周期
    // ------------------------------------------------------------------------
    virtual DBStatus createTable(const std::string& dbName,
                                  const std::string& tableName,
                                  const TableSchema& schema) = 0;
    virtual DBStatus dropTable(const std::string& dbName,
                                const std::string& tableName) = 0;
    virtual std::optional<TableSchema> getTableSchema(
        const std::string& dbName,
        const std::string& tableName) const = 0;
    virtual std::vector<std::string> listTables(
        const std::string& dbName) const = 0;

    // ------------------------------------------------------------------------
    // 行操作 (带 MVCC 快照)
    // ------------------------------------------------------------------------
    virtual RowId insert(const std::string& dbName,
                         const std::string& tableName,
                         const std::map<std::string, std::string>& values,
                         TxnId txnId) = 0;

    virtual std::vector<std::map<std::string, std::string>> query(
        const std::string& dbName,
        const std::string& tableName,
        const Snapshot* snapshot) = 0;

    virtual size_t update(const std::string& dbName,
                          const std::string& tableName,
                          const std::map<std::string, std::string>& newValues,
                          const std::vector<RowId>& rowIds,
                          TxnId txnId) = 0;

    virtual size_t remove(const std::string& dbName,
                          const std::string& tableName,
                          const std::vector<RowId>& rowIds,
                          TxnId txnId) = 0;

    // ------------------------------------------------------------------------
    // 索引
    // ------------------------------------------------------------------------
    virtual DBStatus createIndex(const std::string& dbName,
                                  const std::string& tableName,
                                  const std::string& indexName,
                                  const std::vector<std::string>& columns,
                                  const std::string& indexType) = 0;
    virtual DBStatus dropIndex(const std::string& dbName,
                                const std::string& indexName) = 0;

    // ------------------------------------------------------------------------
    // 事务
    // ------------------------------------------------------------------------
    virtual TxnId beginTransaction(IsolationLevel level) = 0;
    virtual DBStatus commitTransaction(TxnId txnId) = 0;
    virtual DBStatus rollbackTransaction(TxnId txnId) = 0;

    // ------------------------------------------------------------------------
    // Checkpoint / WAL
    // ------------------------------------------------------------------------
    virtual DBStatus checkpoint() = 0;
    virtual Lsn getCurrentLsn() const = 0;

    // ------------------------------------------------------------------------
    // 统计
    // ------------------------------------------------------------------------
    virtual size_t getTableRowCount(const std::string& dbName,
                                     const std::string& tableName) const = 0;
};

} // namespace dbms
