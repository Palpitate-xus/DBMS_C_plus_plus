#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TableManage.h"
#include "executor.h"

namespace dbms {

// ========================================================================
// Operator base class:火山模型 (iterator model)
// 继承 IOperator 接口，Phase 0 接口统一
// ========================================================================
class Operator : public IOperator {
public:
    ~Operator() override = default;

    // Initialize the operator (acquire resources, open files, etc.)
    bool open() override = 0;

    // Get the next output row. Returns false when no more rows.
    // outRow is the formatted string ready for display.
    bool next(std::string& outRow) override = 0;

    // Scan-derived operators may expose NULL metadata for the row returned
    // by the most recent next() call.  Most operators do not own storage
    // metadata and therefore use the conservative default.
    virtual bool lastColumnIsNull(size_t colIdx) const {
        (void)colIdx;
        return false;
    }

    // Clean up resources
    void close() override = 0;
};

using OpPtr = std::unique_ptr<Operator>;

// MaterializedRows: adapter for result rows produced by a legacy or external
// executor.  It lets higher-level operators consume those rows through the
// same Volcano interface while the underlying producer is migrated.
class MaterializedRowsOp : public Operator {
public:
    explicit MaterializedRowsOp(std::vector<std::string> rows)
        : rows_(std::move(rows)) {}

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;

private:
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

// ========================================================================
// TableScan: full table scan using forEachRow
// ========================================================================
class TableScanOp : public Operator {
public:
    TableScanOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    const std::string& tableName() const { return tablename_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    TableSchema tbl_;
    std::vector<std::pair<int64_t, std::string>> rows_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
};

// ParallelTableScan: partition a non-partitioned heap by page ranges.  It
// falls back to the regular scan while a transaction is active, because the
// current transaction/SSI bookkeeping is backend-local rather than worker-
// local.  Results are gathered in page-range order for deterministic output.
class ParallelTableScanOp : public Operator {
public:
    ParallelTableScanOp(StorageEngine* engine, const std::string& dbname,
                        const std::string& tablename, int workers);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    const std::string& tableName() const { return tablename_; }
    int workers() const { return workers_; }
    bool usedParallelWorkers() const { return usedParallelWorkers_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    int workers_;
    TableSchema tbl_;
    std::vector<std::pair<int64_t, std::string>> rows_;
    size_t pos_ = 0;
    bool usedParallelWorkers_ = false;
    int64_t lastRid_ = 0;
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

// BitmapHeapScan: intersect candidate RIDs from multiple equality indexes,
// then fetch the heap rows once.  FilterOp remains above this node as the
// visibility/condition recheck boundary.
class BitmapHeapScanOp : public Operator {
public:
    BitmapHeapScanOp(StorageEngine* engine, const std::string& dbname,
                     const std::string& tablename,
                     const std::vector<StorageEngine::Condition>& conds);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<StorageEngine::Condition> conds_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

// BitmapOrHeapScan: build one candidate RID set per AND branch, union the
// branches, fetch each heap row once, and recheck the original disjunction.
// Every branch must have at least one usable equality index; otherwise the
// planner falls back to the legacy/table-scan path.
class BitmapOrHeapScanOp : public Operator {
public:
    BitmapOrHeapScanOp(StorageEngine* engine, const std::string& dbname,
                       const std::string& tablename,
                       const std::vector<std::vector<StorageEngine::Condition>>& branches);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<std::vector<StorageEngine::Condition>> branches_;
    TableSchema tbl_;
    std::vector<std::string> rows_;
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
    bool lastColumnIsNull(size_t colIdx) const override {
        return child_->lastColumnIsNull(colIdx);
    }
    const std::vector<StorageEngine::Condition>& conditions() const { return conds_; }

    // Index condition recheck: apply conditions that the index could not fully evaluate
    // (bitmap heap scan recheck semantics).
    void setIndexConditionRecheck(bool v) { indexConditionRecheck_ = v; }
    bool indexConditionRecheck() const { return indexConditionRecheck_; }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<StorageEngine::Condition> conds_;
    bool indexConditionRecheck_ = false;
};

// SemiJoin/AntiJoin filters an outer stream by the existence of a matching
// value in an inner stream.  It is the structured execution boundary for
// uncorrelated IN and NOT IN subqueries; the inner stream may itself have a
// FilterOp so the subquery predicate is evaluated before matching.
class SemiJoinOp : public Operator {
public:
    SemiJoinOp(OpPtr outer, OpPtr inner, const TableSchema& outerTbl,
               const TableSchema& innerTbl, const std::string& outerColumn,
               const std::string& innerColumn, bool anti);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* outerChild() const { return outer_.get(); }
    Operator* innerChild() const { return inner_.get(); }
    const std::string& outerColumn() const { return outerColumn_; }
    const std::string& innerColumn() const { return innerColumn_; }
    bool isAnti() const { return anti_; }

private:
    OpPtr outer_;
    OpPtr inner_;
    TableSchema outerTbl_;
    TableSchema innerTbl_;
    std::string outerColumn_;
    std::string innerColumn_;
    bool anti_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

// ExistenceFilter filters an outer stream using the truth value of an
// uncorrelated EXISTS/NOT EXISTS subquery.  The inner plan is evaluated once
// and the outer row shape is preserved for downstream projection.
class ExistenceFilterOp : public Operator {
public:
    ExistenceFilterOp(OpPtr outer, OpPtr inner, bool anti)
        : outer_(std::move(outer)), inner_(std::move(inner)), anti_(anti) {}

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* outerChild() const { return outer_.get(); }
    Operator* innerChild() const { return inner_.get(); }
    bool isAnti() const { return anti_; }

private:
    OpPtr outer_;
    OpPtr inner_;
    bool anti_ = false;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

struct ProjectionTarget {
    bool isScalar = false;
    std::string column;
};

struct ScalarSubquerySpec {
    std::string dbname;
    std::string tablename;
    std::string column;
    std::vector<StorageEngine::Condition> innerConds;
};

// ScalarSubqueryProject evaluates one uncorrelated scalar subquery as an
// init-plan and applies its single value to every outer row.  It enforces the
// SQL scalar cardinality rule: zero rows become NULL, more than one row is an
// execution error.
class ScalarSubqueryProjectOp : public Operator {
public:
    ScalarSubqueryProjectOp(OpPtr outer, OpPtr inner,
                            const TableSchema& outerTbl,
                            const TableSchema& innerTbl,
                            const std::vector<ProjectionTarget>& targets,
                            const std::string& innerColumn)
        : outer_(std::move(outer)), inner_(std::move(inner)),
          outerTbl_(outerTbl), innerTbl_(innerTbl), targets_(targets),
          innerColumn_(innerColumn) {}

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    const std::string& errorMessage() const { return errorMessage_; }
    Operator* outerChild() const { return outer_.get(); }
    Operator* innerChild() const { return inner_.get(); }
    const std::string& innerColumn() const { return innerColumn_; }

private:
    OpPtr outer_;
    OpPtr inner_;
    TableSchema outerTbl_;
    TableSchema innerTbl_;
    std::vector<ProjectionTarget> targets_;
    std::string innerColumn_;
    std::string scalarValue_;
    bool scalarIsNull_ = true;
    std::string errorMessage_;
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

// A deliberately narrow, structured window specification.  The legacy SQL
// parser still owns complex frame syntax; this specification is used when a
// query can be executed by the Volcano WindowOp without semantic fallback.
struct WindowFunctionSpec {
    enum class FrameType { ROWS, RANGE, GROUPS };
    std::string name;
    std::string argument;
    std::vector<std::string> partitionBy;
    std::string orderBy;
    bool orderAscending = true;
    size_t offset = 1;
    std::string defaultValue;
    bool hasDefault = false;
    // A missing frame uses PostgreSQL's default: the whole partition without
    // ORDER BY, or RANGE UNBOUNDED PRECEDING .. CURRENT ROW with ORDER BY.
    bool hasFrame = false;
    FrameType frameType = FrameType::ROWS;
    int frameStartOffset = -1; // -1 = UNBOUNDED PRECEDING
    int frameEndOffset = 0;    // -1 = UNBOUNDED FOLLOWING
    std::string frameExclusion; // current row, group, ties, no others
};

struct WindowTarget {
    // A target is either a base-table column or the zero-based window index.
    bool isWindow = false;
    std::string column;
    size_t windowIndex = 0;
};

// WindowOp materializes its child once, computes independent window streams,
// then formats the requested target list.  It handles common ranking/offset
// and aggregate windows, including PostgreSQL default, ROWS, RANGE, and
// GROUPS frame semantics for the supported scalar window functions.
class WindowOp : public Operator {
public:
    WindowOp(OpPtr child, const TableSchema& tbl,
             const std::vector<WindowTarget>& targets,
             const std::vector<WindowFunctionSpec>& functions,
             const std::string& finalOrderBy = "",
             bool finalOrderAscending = true);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    const std::vector<WindowFunctionSpec>& functions() const { return functions_; }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<WindowTarget> targets_;
    std::vector<WindowFunctionSpec> functions_;
    std::string finalOrderBy_;
    bool finalOrderAscending_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
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
// Offset: OFFSET n
// ========================================================================
class OffsetOp : public Operator {
public:
    OffsetOp(OpPtr child, size_t offset);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    size_t offset() const { return offset_; }

private:
    OpPtr child_;
    size_t offset_;
    size_t skipped_ = 0;
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
// SetOperation: combine two already-projected child streams.
// Rows are kept in child order for a deterministic executor-facing result
// stream. DISTINCT and ALL semantics are explicit instead of being
// implemented by the SQL output layer.
// ========================================================================
enum class SetOperationType { Union, Intersect, Except };

class SetOperationOp : public Operator {
public:
    SetOperationOp(OpPtr left, OpPtr right, SetOperationType type, bool all);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;

private:
    OpPtr left_;
    OpPtr right_;
    SetOperationType type_;
    bool all_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
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

// GroupAggregate: consume a filtered Volcano stream and produce one row per
// GROUP BY key or grouping set.  The node intentionally owns only the common
// scalar aggregate contract; unsupported ordered-set/collection aggregates
// remain outside this plan boundary until their typed semantics are modeled.
class GroupAggregateOp : public Operator {
public:
    GroupAggregateOp(OpPtr child, const TableSchema& tbl,
                     const std::vector<std::string>& groupByCols,
                     const std::vector<std::vector<std::string>>& groupingSets,
                     const std::vector<StorageEngine::AggItem>& items,
                     const std::vector<std::string>& havingConds = {});

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    size_t groupingSetCount() const { return groupingSets_.empty() ? 1 : groupingSets_.size(); }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<std::string> groupByCols_;
    std::vector<std::vector<std::string>> groupingSets_;
    std::vector<StorageEngine::AggItem> items_;
    std::vector<std::string> havingConds_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

struct SemiJoinSpec {
    std::string dbname;
    std::string tablename;
    std::string outerColumn;
    std::string innerColumn;
    std::vector<StorageEngine::Condition> innerConds;
    bool anti = false;
};

struct ExistenceSpec {
    std::string dbname;
    std::string tablename;
    std::vector<StorageEngine::Condition> innerConds;
    bool anti = false;
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
    size_t offset = 0;
    size_t limit = 0;
    bool distinct = false;
    // When groupByCols is non-empty, buildSelectPlan creates a structured
    // GroupAggregateOp over the filtered scan.  groupingSets is empty for a
    // normal GROUP BY and populated for ROLLUP/CUBE/GROUPING SETS.
    std::vector<std::string> groupByCols;
    std::vector<std::vector<std::string>> groupingSets;
    std::vector<StorageEngine::AggItem> aggregateItems;
    std::vector<std::string> havingConds;
    // When non-empty, buildSelectPlan creates a structured WindowOp instead
    // of applying Project/Sort to a legacy window result.
    std::vector<WindowFunctionSpec> windowFunctions;
    std::vector<WindowTarget> windowTargets;
    // Uncorrelated IN/NOT IN predicates lowered to a right-hand relation.
    std::vector<SemiJoinSpec> semiJoins;
    // Uncorrelated EXISTS/NOT EXISTS predicates lowered to an existence
    // filter over a right-hand relation.
    std::vector<ExistenceSpec> existenceFilters;
    // A narrow uncorrelated scalar target list lowered to an init-plan plus
    // target projection.  Empty means the normal column projection path.
    std::vector<ProjectionTarget> projectionTargets;
    ScalarSubquerySpec scalarSubquery;
};

// Equivalence class: a set of expressions that are known equal.
// Used by the planner to propagate join conditions and choose access paths.
struct EquivalenceClass {
    std::vector<std::string> members;  // e.g. ["t1.id", "t2.fk"]
};

// PathKey: an ordering that a path provides (ORDER BY or GROUP BY column).
struct PathKey {
    std::string expr;      // column or expression
    bool ascending = true;
    std::string opclass;   // operator class (default btree)
};

class QueryPlanner {
public:
    // Build operator tree for SELECT * FROM t WHERE ... ORDER BY ... LIMIT ...
    static OpPtr buildSelectPlan(StorageEngine* engine, const PlanContext& ctx);

    // Build plan with pathkey awareness (avoids re-sort if index provides ordering).
    static OpPtr buildSelectPlan(StorageEngine* engine, const PlanContext& ctx,
                                  const std::vector<PathKey>& requiredPathkeys,
                                  const std::vector<EquivalenceClass>& eqClasses);

    // Build operator tree for SELECT agg(...) FROM t WHERE ...
    static OpPtr buildAggregatePlan(StorageEngine* engine, const PlanContext& ctx,
                                     const std::vector<StorageEngine::AggItem>& items);

    // Build a structured OR plan. Each inner vector is an AND branch.
    // Returns nullptr when the branches cannot all use safe equality indexes.
    static OpPtr buildDisjunctiveSelectPlan(
        StorageEngine* engine, const PlanContext& ctx,
        const std::vector<std::vector<StorageEngine::Condition>>& branches);

    // Build a structured set-operation node over two compatible child plans.
    static OpPtr buildSetOperationPlan(OpPtr left, OpPtr right,
                                       SetOperationType type, bool all);

    // Build operator tree for JOIN
    static OpPtr buildJoinPlan(StorageEngine* engine, const std::string& dbname,
                                const std::string& leftTable, const std::string& rightTable,
                                const std::string& leftCol, const std::string& rightCol,
                                const std::vector<StorageEngine::Condition>& conds,
                                const std::set<std::string>& selectCols);

    struct ExplainOptions {
        bool analyze = false;
        bool buffers = false;
        bool verbose = false;
        bool timing = true;     // default true (PostgreSQL compatible)
        bool costs = true;      // default true
        bool settings = false;
    };

    static std::string explain(OpPtr& plan, StorageEngine* engine,
                               const std::string& dbname);
    static std::string explain(OpPtr& plan, StorageEngine* engine,
                               const std::string& dbname,
                               const ExplainOptions& opts);

    static std::string explainJson(OpPtr& plan, StorageEngine* engine,
                                   const std::string& dbname);
    static std::string explainJson(OpPtr& plan, StorageEngine* engine,
                                   const std::string& dbname,
                                   const ExplainOptions& opts);

    // Execute a plan built by buildSelectPlan and return result rows.
    static std::vector<std::string> executePlan(OpPtr plan);

    // Parallel query support: number of worker threads (0 = disabled).
    static int parallelWorkers() { return parallelWorkers_; }
    static void setParallelWorkers(int n) { parallelWorkers_ = n < 0 ? 0 : n; }

private:
    static int parallelWorkers_;
};

} // namespace dbms
