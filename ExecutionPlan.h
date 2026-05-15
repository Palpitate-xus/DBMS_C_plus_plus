#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "TableManage.h"

namespace dbms {

// ========================================================================
// Operator base class:火山模型 (iterator model)
// ========================================================================
class Operator {
public:
    virtual ~Operator() = default;

    // Initialize the operator (acquire resources, open files, etc.)
    virtual bool open() = 0;

    // Get the next output row. Returns false when no more rows.
    // outRow is the formatted string ready for display.
    virtual bool next(std::string& outRow) = 0;

    // Clean up resources
    virtual void close() = 0;
};

using OpPtr = std::unique_ptr<Operator>;

// ========================================================================
// TableScan: full table scan using forEachRow
// ========================================================================
class TableScanOp : public Operator {
public:
    TableScanOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    const std::string& tableName() const { return tablename_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    TableSchema tbl_;
    std::vector<std::pair<int64_t, std::string>> rows_;
    size_t pos_ = 0;
};

// ========================================================================
// IndexScan: use B+ tree index for equality lookup
// ========================================================================
class IndexScanOp : public Operator {
public:
    IndexScanOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename, const std::string& colname,
                const std::string& value);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    const std::string& tableName() const { return tablename_; }
    const std::string& colName() const { return colname_; }
    const std::string& value() const { return value_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::string colname_;
    std::string value_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    size_t pos_ = 0;
    bool isPK_ = false;
};

// ========================================================================
// IndexOnlyScan: covering index — return data directly from index keys,
// avoiding row lookup. Used when SELECT columns are all in the index and
// WHERE matches the index columns.
// ========================================================================
class IndexOnlyScanOp : public Operator {
public:
    // For single-column index: indexCols = {colname}, filterValue = the value
    // For composite index: indexCols = list of cols, filterValue = composite key
    IndexOnlyScanOp(StorageEngine* engine, const std::string& dbname,
                    const std::string& tablename,
                    const std::vector<std::string>& indexCols,
                    const std::string& filterValue,
                    const std::string& compositeIndexName = "");

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    const std::string& tableName() const { return tablename_; }
    const std::vector<std::string>& indexCols() const { return indexCols_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<std::string> indexCols_;
    std::string filterValue_;
    std::string compositeIndexName_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    size_t pos_ = 0;
};

// ========================================================================
// Filter: apply WHERE conditions
// ========================================================================
class FilterOp : public Operator {
public:
    FilterOp(OpPtr child, const TableSchema& tbl,
             const std::vector<StorageEngine::Condition>& conds);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    const std::vector<StorageEngine::Condition>& conditions() const { return conds_; }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<StorageEngine::Condition> conds_;
};

// ========================================================================
// Project: select specific columns
// ========================================================================
class ProjectOp : public Operator {
public:
    ProjectOp(OpPtr child, const TableSchema& tbl,
              const std::set<std::string>& selectCols);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::set<std::string> selectCols_;
};

// ========================================================================
// Sort: ORDER BY
// ========================================================================
class SortOp : public Operator {
public:
    SortOp(OpPtr child, const TableSchema& tbl,
           const std::string& orderByCol, bool asc);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::string orderByCol_;
    bool asc_;
    std::vector<std::string> buffer_;
    size_t pos_ = 0;
};

// ========================================================================
// Limit: LIMIT n
// ========================================================================
class LimitOp : public Operator {
public:
    LimitOp(OpPtr child, size_t limit);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    size_t limit() const { return limit_; }

private:
    OpPtr child_;
    size_t limit_;
    size_t count_ = 0;
};

// ========================================================================
// Distinct: remove duplicate rows
// ========================================================================
class DistinctOp : public Operator {
public:
    explicit DistinctOp(OpPtr child);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }

private:
    OpPtr child_;
    std::set<std::string> seen_;
};

// ========================================================================
// NestedLoopJoin: INNER JOIN two tables
// ========================================================================
class NestedLoopJoinOp : public Operator {
public:
    NestedLoopJoinOp(StorageEngine* engine, const std::string& dbname,
                     OpPtr left, OpPtr right,
                     const std::string& leftTable, const std::string& rightTable,
                     const std::string& leftCol, const std::string& rightCol);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* leftChild() const { return left_.get(); }
    Operator* rightChild() const { return right_.get(); }
    const std::string& leftTable() const { return leftTable_; }
    const std::string& rightTable() const { return rightTable_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    OpPtr left_;
    OpPtr right_;
    std::string leftTable_;
    std::string rightTable_;
    std::string leftCol_;
    std::string rightCol_;
    TableSchema leftTbl_;
    TableSchema rightTbl_;
    std::string curLeftRow_;
    bool hasLeft_ = false;
};

// ========================================================================
// HashJoin: INNER JOIN using hash table on right table
// ========================================================================
class HashJoinOp : public Operator {
public:
    HashJoinOp(StorageEngine* engine, const std::string& dbname,
               OpPtr left, OpPtr right,
               const std::string& leftTable, const std::string& rightTable,
               const std::string& leftCol, const std::string& rightCol);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* leftChild() const { return left_.get(); }
    Operator* rightChild() const { return right_.get(); }
    const std::string& leftTable() const { return leftTable_; }
    const std::string& rightTable() const { return rightTable_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    OpPtr left_;
    OpPtr right_;
    std::string leftTable_;
    std::string rightTable_;
    std::string leftCol_;
    std::string rightCol_;
    TableSchema leftTbl_;
    TableSchema rightTbl_;

    std::unordered_map<std::string, std::vector<std::string>> rightHash_;
    std::string curLeftRow_;
    std::vector<std::string> curRightMatches_;
    size_t matchPos_ = 0;
    bool hasLeft_ = false;
};

// ========================================================================
// MergeJoin: INNER JOIN on sorted inputs
// ========================================================================
class MergeJoinOp : public Operator {
public:
    MergeJoinOp(StorageEngine* engine, const std::string& dbname,
                OpPtr left, OpPtr right,
                const std::string& leftTable, const std::string& rightTable,
                const std::string& leftCol, const std::string& rightCol);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* leftChild() const { return left_.get(); }
    Operator* rightChild() const { return right_.get(); }
    const std::string& leftTable() const { return leftTable_; }
    const std::string& rightTable() const { return rightTable_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    OpPtr left_;
    OpPtr right_;
    std::string leftTable_;
    std::string rightTable_;
    std::string leftCol_;
    std::string rightCol_;
    TableSchema leftTbl_;
    TableSchema rightTbl_;

    std::vector<std::string> leftRows_;
    std::vector<std::string> rightRows_;
    size_t leftPos_ = 0;
    size_t rightPos_ = 0;
};

// ========================================================================
// Aggregate: COUNT/SUM/AVG/MAX/MIN
// ========================================================================
class AggregateOp : public Operator {
public:
    AggregateOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename,
                const std::vector<std::pair<std::string, std::string>>& items);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<std::pair<std::string, std::string>> items_;
    bool done_ = false;
};

// ========================================================================
// QueryPlanner: build operator tree from parsed SQL components
// ========================================================================
struct PlanContext {
    std::string dbname;
    std::string tablename;
    std::vector<StorageEngine::Condition> conds;
    std::set<std::string> selectCols;
    std::string orderByCol;
    bool orderByAsc = true;
    size_t limit = 0;
    bool distinct = false;
};

class QueryPlanner {
public:
    // Build operator tree for SELECT * FROM t WHERE ... ORDER BY ... LIMIT ...
    static OpPtr buildSelectPlan(StorageEngine* engine, const PlanContext& ctx);

    // Build operator tree for SELECT agg(...) FROM t WHERE ...
    static OpPtr buildAggregatePlan(StorageEngine* engine, const PlanContext& ctx,
                                     const std::vector<std::pair<std::string, std::string>>& items);

    // Build operator tree for JOIN
    static OpPtr buildJoinPlan(StorageEngine* engine, const std::string& dbname,
                                const std::string& leftTable, const std::string& rightTable,
                                const std::string& leftCol, const std::string& rightCol,
                                const std::vector<StorageEngine::Condition>& conds,
                                const std::set<std::string>& selectCols);

    // Get a human-readable description of the plan tree with cost estimates
    static std::string explain(OpPtr& plan, StorageEngine* engine,
                               const std::string& dbname);
};

} // namespace dbms
