#pragma once

#include <exception>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TableManage.h"
#include "executor.h"
#include "parser/query_binding.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"

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

    // Operators that synthesize result cells (aggregates, joins, windows)
    // can expose the exact row separately from their legacy display string.
    // This keeps SQL NULL distinct from text "NULL" and preserves embedded
    // whitespace for protocol consumers.
    virtual bool supportsStructuredRows() const { return false; }
    // A genuine logical FROM tree can carry several independently bound
    // source occurrences. Preserve that context through filter and buffering
    // operators rather than inventing a flattened physical schema.
    virtual bool supportsPreparedContexts() const { return false; }
    virtual bool lastPreparedContext(RowContext& row) const {
        (void)row; return false;
    }
    // Prepared expression operators can preserve the complete typed cells,
    // including collation, without reconstructing them from display text.
    virtual bool lastStructuredValues(std::vector<ExprValue>& values) const {
        (void)values;
        return false;
    }
    virtual bool lastStructuredRow(std::vector<std::string>& cells,
                                   std::vector<bool>& nulls) const {
        (void)cells;
        (void)nulls;
        return false;
    }

    // Origin of the most recent next() row: the scan node that produced
    // it (engine + location), for stored-NULL rebinding after buffering
    // operators (sort, limit) re-emit rows later. Null origin when the
    // operator does not trace to a heap scan.
    struct ScanOrigin {
        StorageEngine* engine = nullptr;
        std::string dbname;
        std::string tablename;
        int64_t rid = 0;
    };
    virtual ScanOrigin scanOrigin() const { return ScanOrigin{}; }

    // Prepared expression nodes expose the same children that they execute.
    // EXPLAIN must not substitute a separately built display-only tree.
    virtual std::string preparedPlanNodeName() const { return {}; }
    virtual std::vector<Operator*> preparedPlanChildren() const { return {}; }

    bool hasError() const override { return error_; }
    std::string errorMessage() const override { return errorMessage_; }

    // Clean up resources
    void close() override = 0;

protected:
    void clearError() {
        error_ = false;
        errorMessage_.clear();
    }
    void setError(std::string message) {
        error_ = true;
        if (errorMessage_.empty()) errorMessage_ = std::move(message);
    }
    bool propagateChildError(const Operator* child, const std::string& fallback) {
        if (child && child->hasError()) {
            setError(child->errorMessage().empty() ? fallback : child->errorMessage());
        } else {
            setError(fallback);
        }
        return false;
    }

private:
    bool error_ = false;
    std::string errorMessage_;
};

// A logical relation has typed ordinal cells, not a storage schema keyed by
// output labels. Reading index N is demand driven; independent scans can
// share a statement-owned CTE cache without reevaluating earlier rows.
class PreparedSourceRowsOp final : public Operator {
public:
    using Reader = std::function<bool(size_t, std::vector<ExprValue>&)>;
    PreparedSourceRowsOp(QueryRowDescriptor descriptor, Reader reader)
        : descriptor_(std::move(descriptor)), reader_(std::move(reader)) {}
    bool open() override;
    bool next(std::string& row) override;
    void close() override;
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        values = row_;
        return values.size() == descriptor_.size();
    }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override;
    bool lastColumnIsNull(size_t ordinal) const override;
    std::string preparedPlanNodeName() const override { return "CTEScan"; }
private:
    QueryRowDescriptor descriptor_;
    Reader reader_;
    size_t position_ = 0;
    std::vector<ExprValue> row_;
};

using OpPtr = std::unique_ptr<Operator>;

class PreparedSourceContextsOp final : public Operator {
public:
    using Reader = std::function<bool(size_t, RowContext&)>;
    explicit PreparedSourceContextsOp(Reader reader) : reader_(std::move(reader)) {}
    bool open() override { clearError(); position_ = 0; row_ = RowContext{}; return true; }
    bool next(std::string& row) override {
        if (!reader_(position_, row_)) return false;
        ++position_; row.clear(); return true;
    }
    void close() override { position_ = 0; row_ = RowContext{}; }
    bool supportsPreparedContexts() const override { return true; }
    bool lastPreparedContext(RowContext& row) const override {
        if (!position_) return false;
        row = row_; return true;
    }
    std::string preparedPlanNodeName() const override { return "TypedSource"; }
private:
    Reader reader_;
    size_t position_ = 0;
    RowContext row_;
};

struct PlanExecutionResult {
    std::vector<std::string> rows;
    std::vector<std::vector<std::string>> structuredRows;
    std::vector<std::vector<bool>> structuredNulls;
    bool structuredRowsAvailable = false;
    bool ok = true;
    std::string error;
    // The diagnostic is for display; SQL hosts use the original metadata,
    // never an SQLSTATE recovered from diagnostic text.
    std::string errorSqlState;
    std::string errorMessage;
    std::exception_ptr errorException;

    void throwIfFailed() const {
        if (!ok) {
            if (errorException) std::rethrow_exception(errorException);
            throw DbError(errorSqlState.empty() ? "XX000" : errorSqlState,
                          errorMessage.empty() ? error : errorMessage);
        }
    }
};

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
    size_t rowCount() const { return rows_.size(); }

private:
    std::vector<std::string> rows_;
    size_t pos_ = 0;
};

// ========================================================================
// TableScan: full table scan using forEachRow
// ========================================================================
// UnnestOp: expand an array literal (or any expression producing an array
// text value) into one row per element.  Output rows carry a single column
// named "unnest"; the companion unnestTableSchema() exposes that shape to
// projection/sort code that expects a real table.
class UnnestOp : public Operator {
public:
    UnnestOp(const std::string& arrayLiteral, const std::string& outputColumn);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    double estimatedRows() const override { return static_cast<double>(elements_.size()); }
    double estimatedCost() const override { return static_cast<double>(elements_.size()); }

    const std::string& outputColumn() const { return column_; }
    // Split a PG array literal {a,b,c} or [a,b,c] into trimmed elements,
    // honoring single/double quotes.
    static std::vector<std::string> splitArrayLiteral(const std::string& literal);
    // Fixed-width column layout helper: rows emitted by this operator are
    // the raw element text followed by a single space (single-column shape
    // shared with the legacy engine row format).
    static constexpr const char* kVirtualTableName = "unnest";

private:
    std::string literal_;
    std::string column_;
    std::vector<std::string> elements_;
    size_t pos_ = 0;
};

// Synthesized schema for UnnestOp's virtual relation (one column "unnest").
TableSchema unnestTableSchema();

class TableScanOp : public Operator {
public:
    TableScanOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    const std::string& tableName() const { return tablename_; }
    // RID of the row emitted by the last next() call (0 before first).
    int64_t lastRid() const { return lastRid_; }
    ScanOrigin scanOrigin() const override {
        ScanOrigin o; o.engine = engine_; o.dbname = dbname_;
        o.tablename = tablename_; o.rid = lastRid_;
        return o;
    }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    TableSchema tbl_;
    std::vector<std::pair<int64_t, std::string>> rows_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
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
    ScanOrigin scanOrigin() const override {
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = dbname_;
        origin.tablename = tablename_;
        origin.rid = lastRid_;
        return origin;
    }

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
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
};

// ========================================================================
// IndexScan: use B+ tree index for equality lookup
// ========================================================================
class IndexScanOp : public Operator {
public:
    // value is a logical column value, not an encoded physical primary key.
    IndexScanOp(StorageEngine* engine, const std::string& dbname,
                const std::string& tablename, const std::string& colname,
                const std::string& value);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    const std::string& tableName() const { return tablename_; }
    const std::string& colName() const { return colname_; }
    const std::string& value() const { return value_; }
    ScanOrigin scanOrigin() const override {
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = dbname_;
        origin.tablename = tablename_;
        origin.rid = lastRid_;
        return origin;
    }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::string colname_;
    std::string value_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
    bool isPK_ = false;
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
};

// GiSTScan: range/prefix predicate acceleration over the .gist sidecar.
// The sidecar stores one (rid, low, high) entry per row, so a range query
// [lo, hi] becomes an overlap scan and a text-prefix query becomes a
// "high >= prefix, low <= prefix..." containment-style scan.  The original
// FilterOp conditions stay above this node as the correctness boundary:
// this node only narrows candidates.
class GiSTScanOp : public Operator {
public:
    GiSTScanOp(StorageEngine* engine, const std::string& dbname,
               const std::string& tablename,
               const std::vector<StorageEngine::Condition>& conds);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    ScanOrigin scanOrigin() const override {
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = dbname_;
        origin.tablename = tablename_;
        origin.rid = lastRid_;
        return origin;
    }

    const std::string& tableName() const { return tablename_; }
    // Human-readable predicate summary for EXPLAIN.
    std::string describeConds() const;

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<StorageEngine::Condition> conds_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
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
    bool lastColumnIsNull(size_t colIdx) const override;
    void close() override;
    ScanOrigin scanOrigin() const override {
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = dbname_;
        origin.tablename = tablename_;
        origin.rid = lastRid_;
        return origin;
    }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<StorageEngine::Condition> conds_;
    TableSchema tbl_;
    std::vector<int64_t> rids_;
    std::vector<std::string> rows_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
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
    // Stored-NULL truth for the row emitted by the last next() call, so the
    // projection can render NULL (not the zero-filled fixed-width value).
    bool lastColumnIsNull(size_t colIdx) const override;
    ScanOrigin scanOrigin() const override {
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = dbname_;
        origin.tablename = tablename_;
        origin.rid = lastRid_;
        return origin;
    }

private:
    StorageEngine* engine_;
    std::string dbname_;
    std::string tablename_;
    std::vector<std::vector<StorageEngine::Condition>> branches_;
    TableSchema tbl_;
    std::vector<std::string> rows_;
    std::vector<int64_t> rids_;
    size_t pos_ = 0;
    int64_t lastRid_ = 0;
    bool statsRecorded_ = false;
    bool tableLockHeld_ = false;
};

// ========================================================================
// Filter: apply WHERE conditions
// ========================================================================
class FilterOp : public Operator {
public:
    FilterOp(OpPtr child, const TableSchema& tbl,
             const std::vector<StorageEngine::Condition>& conds);
    FilterOp(OpPtr child, const TableSchema& tbl,
             const std::vector<std::vector<StorageEngine::Condition>>& branches);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }
    bool lastColumnIsNull(size_t colIdx) const override {
        return child_->lastColumnIsNull(colIdx);
    }
    ScanOrigin scanOrigin() const override { return child_->scanOrigin(); }
    const std::vector<StorageEngine::Condition>& conditions() const { return conds_; }
    const std::vector<std::vector<StorageEngine::Condition>>& branches() const {
        return branches_;
    }

    // Index condition recheck: apply conditions that the index could not fully evaluate
    // (bitmap heap scan recheck semantics).
    void setIndexConditionRecheck(bool v) { indexConditionRecheck_ = v; }
    bool indexConditionRecheck() const { return indexConditionRecheck_; }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<StorageEngine::Condition> conds_;
    std::vector<std::vector<StorageEngine::Condition>> branches_;
    bool indexConditionRecheck_ = false;
};

// SemiJoin/AntiJoin filters an outer stream by the existence of a matching
// value in an inner stream.  It is the structured execution boundary for
// uncorrelated IN and NOT IN subqueries; the inner stream may itself have a
// FilterOp so the subquery predicate is evaluated before matching.
class SemiJoinOp : public Operator {
public:
    enum class NullSemantics {
        InPredicate,
        ExistsCorrelation
    };

    SemiJoinOp(OpPtr outer, OpPtr inner, const TableSchema& outerTbl,
               const TableSchema& innerTbl, const std::string& outerColumn,
               const std::string& innerColumn, bool anti,
               NullSemantics nullSemantics = NullSemantics::InPredicate);
    // Multi-key correlated semi/anti join: each pair is (outer col,
    // inner col); rows match when ALL pairs match.
    SemiJoinOp(OpPtr outer, OpPtr inner, const TableSchema& outerTbl,
               const TableSchema& innerTbl,
               std::vector<std::pair<std::string, std::string>> keys,
               bool anti,
               NullSemantics nullSemantics = NullSemantics::InPredicate);

    bool open() override;
    bool next(std::string& outRow) override;
    bool lastColumnIsNull(size_t colIdx) const override;
    ScanOrigin scanOrigin() const override;
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
    std::vector<std::pair<std::string, std::string>> keys_;
    bool anti_;
    NullSemantics nullSemantics_;
    std::vector<std::string> rows_;
    std::vector<std::vector<bool>> nullRows_;
    std::vector<ScanOrigin> origins_;
    size_t pos_ = 0;
};

struct QuantifiedSubquerySpec {
    std::string dbname;
    std::string tablename;
    std::string outerColumn;
    std::string innerColumn;
    std::string op;
    std::vector<StorageEngine::Condition> innerConds;
    bool all = false;
};

// QuantifiedSubqueryFilter evaluates one uncorrelated
// `outer_expr <op> ANY/ALL (SELECT inner_expr ...)` predicate.  The inner
// relation is materialized once as values; the outer stream remains lazy.
// SQL three-valued logic is preserved for NULL and empty input sets.
class QuantifiedSubqueryFilterOp : public Operator {
public:
    QuantifiedSubqueryFilterOp(OpPtr outer, OpPtr inner,
                               const TableSchema& outerTbl,
                               const TableSchema& innerTbl,
                               const std::string& outerColumn,
                               const std::string& innerColumn,
                               const std::string& op, bool all)
        : outer_(std::move(outer)), inner_(std::move(inner)),
          outerTbl_(outerTbl), innerTbl_(innerTbl),
          outerColumn_(outerColumn), innerColumn_(innerColumn),
          op_(op), all_(all) {}

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    bool lastColumnIsNull(size_t colIdx) const override {
        return outer_->lastColumnIsNull(colIdx);
    }
    ScanOrigin scanOrigin() const override { return outer_->scanOrigin(); }
    Operator* outerChild() const { return outer_.get(); }
    Operator* innerChild() const { return inner_.get(); }
    const std::string& outerColumn() const { return outerColumn_; }
    const std::string& innerColumn() const { return innerColumn_; }
    const std::string& op() const { return op_; }
    bool isAll() const { return all_; }

private:
    struct Value {
        std::string text;
        bool isNull = false;
    };
    OpPtr outer_;
    OpPtr inner_;
    TableSchema outerTbl_;
    TableSchema innerTbl_;
    std::string outerColumn_;
    std::string innerColumn_;
    std::string op_;
    bool all_ = false;
    std::vector<Value> values_;
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
    bool lastColumnIsNull(size_t colIdx) const override;
    ScanOrigin scanOrigin() const override;
    void close() override;
    Operator* outerChild() const { return outer_.get(); }
    Operator* innerChild() const { return inner_.get(); }
    bool isAnti() const { return anti_; }

private:
    OpPtr outer_;
    OpPtr inner_;
    bool anti_ = false;
    std::vector<std::string> rows_;
    std::vector<ScanOrigin> origins_;
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
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override;
    void close() override;
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
    std::vector<std::string> lastCells_;
    std::vector<bool> lastNulls_;
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
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override;
    bool lastTypedRowKey(std::string& key) const;
    void close() override;
    Operator* child() const { return child_.get(); }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::set<std::string> selectCols_;
    std::vector<std::string> lastCells_;
    std::vector<bool> lastNulls_;
    bool lastRowAvailable_ = false;
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
    bool defaultIsNull = false;
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
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override;
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
    std::vector<std::vector<std::string>> structuredRows_;
    std::vector<std::vector<bool>> structuredNulls_;
    size_t pos_ = 0;
};

// ========================================================================
// Sort: ORDER BY
// ========================================================================
class SortOp : public Operator {
public:
    SortOp(OpPtr child, const TableSchema& tbl,
           const std::string& orderByCol, bool asc,
           bool nullsFirst = false, bool hasExplicitNullOrder = false);

    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* child() const { return child_.get(); }

    // Stored-NULL bit of the most recently re-emitted row, from the sort's
    // own origin tracking (the child scan is drained and its lastRid_ stale).
    bool lastColumnIsNull(size_t colIdx) const override;
    ScanOrigin scanOrigin() const override {
        return pos_ > 0 && pos_ - 1 < sortedOrigins_.size()
            ? sortedOrigins_[pos_ - 1] : ScanOrigin{};
    }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::string orderByCol_;
    bool asc_;
    bool nullsFirst_;
    std::vector<std::string> buffer_;
    std::vector<Operator::ScanOrigin> origins_;  // parallel to buffer_ pre-sort
    std::vector<Operator::ScanOrigin> sortedOrigins_;  // parallel to buffer_ post-sort
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
    bool supportsStructuredRows() const override {
        return child_->supportsStructuredRows();
    }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        return child_->lastStructuredValues(values);
    }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells, nulls);
    }
    void close() override;
    Operator* child() const { return child_.get(); }
    size_t limit() const { return limit_; }
    ScanOrigin scanOrigin() const override { return child_->scanOrigin(); }

private:
    OpPtr child_;
    size_t limit_;
    size_t count_ = 0;
    bool childOpened_ = false;
};

// ========================================================================
// Offset: OFFSET n
// ========================================================================
class OffsetOp : public Operator {
public:
    OffsetOp(OpPtr child, size_t offset);

    bool open() override;
    bool next(std::string& outRow) override;
    bool supportsStructuredRows() const override {
        return child_->supportsStructuredRows();
    }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        return child_->lastStructuredValues(values);
    }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells, nulls);
    }
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
    bool supportsStructuredRows() const override {
        return child_->supportsStructuredRows();
    }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        return child_->lastStructuredValues(values);
    }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells, nulls);
    }
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
    const std::string& leftColumn() const { return leftCol_; }
    const std::string& rightColumn() const { return rightCol_; }

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
    size_t leftColIdx_ = 0;
    size_t rightColIdx_ = 0;
    std::string curLeftRow_;
    bool hasLeft_ = false;
    bool curLeftKeyNull_ = false;
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
    const std::string& leftColumn() const { return leftCol_; }
    const std::string& rightColumn() const { return rightCol_; }

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
    size_t leftColIdx_ = 0;
    size_t rightColIdx_ = 0;

    std::unordered_map<std::string, std::vector<std::string>> rightHash_;
    std::string curLeftRow_;
    std::vector<std::string> curRightMatches_;
    size_t matchPos_ = 0;
    bool hasLeft_ = false;
    bool curLeftKeyNull_ = false;
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
    const std::string& leftColumn() const { return leftCol_; }
    const std::string& rightColumn() const { return rightCol_; }

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
    size_t leftGroupEnd_ = 0;
    size_t rightGroupBegin_ = 0;
    size_t rightGroupEnd_ = 0;
    bool emittingGroup_ = false;
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
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override;
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
    std::vector<std::vector<std::string>> structuredRows_;
    std::vector<std::vector<bool>> structuredNulls_;
    size_t pos_ = 0;
};

// ========================================================================
// ParallelGroupAggregateOp: worker-thread aggregation.
// The child rows are buffered as in GroupAggregateOp, then partitioned
// into per-worker chunks.  Each worker hashes its chunk into local
// group buckets (group key -> row indexes); the buckets are merged and
// each merged group is finalized (aggregates, HAVING, render) once on
// the calling thread.  Falls back to a single worker inside a
// transaction, mirroring ParallelTableScanOp's safety rules.
// ========================================================================
class ParallelGroupAggregateOp : public Operator {
public:
    ParallelGroupAggregateOp(OpPtr child, const TableSchema& tbl,
                             const std::vector<std::string>& groupByCols,
                             const std::vector<StorageEngine::AggItem>& items,
                             const std::vector<std::string>& havingConds,
                             int workers);
    bool open() override;
    bool next(std::string& outRow) override;
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override;
    void close() override;
    Operator* child() const { return child_.get(); }
    int workers() const { return workers_; }
    bool usedParallelWorkers() const { return usedParallelWorkers_; }

private:
    OpPtr child_;
    TableSchema tbl_;
    std::vector<std::string> groupByCols_;
    std::vector<StorageEngine::AggItem> items_;
    std::vector<std::string> havingConds_;
    int workers_;
    bool usedParallelWorkers_ = false;
    std::vector<std::string> rows_;
    std::vector<std::vector<std::string>> structuredRows_;
    std::vector<std::vector<bool>> structuredNulls_;
    size_t pos_ = 0;
};

// ========================================================================
// ParallelHashJoinOp: INNER hash join with a worker-thread build side.
// The build (right) table is partitioned by page range; workers hash
// their range into local maps that are merged on the calling thread
// (per-key chains ordered by rid for deterministic output).  The probe
// side is the regular Volcano pull.  Falls back to the serial build
// inside a transaction or on partitioned build tables.
// ========================================================================
class ParallelHashJoinOp : public Operator {
public:
    ParallelHashJoinOp(StorageEngine* engine, const std::string& dbname,
                       OpPtr left, OpPtr right,
                       const std::string& leftTable, const std::string& rightTable,
                       const std::string& leftCol, const std::string& rightCol,
                       int workers);
    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    Operator* leftChild() const { return left_.get(); }
    Operator* rightChild() const { return right_.get(); }
    const std::string& leftTable() const { return leftTable_; }
    const std::string& rightTable() const { return rightTable_; }
    const std::string& leftColumn() const { return leftCol_; }
    const std::string& rightColumn() const { return rightCol_; }
    int workers() const { return workers_; }
    bool usedParallelWorkers() const { return usedParallelWorkers_; }

private:
    StorageEngine* engine_;
    std::string dbname_;
    OpPtr left_;
    OpPtr right_;
    std::string leftTable_;
    std::string rightTable_;
    std::string leftCol_;
    std::string rightCol_;
    int workers_;
    bool usedParallelWorkers_ = false;
    TableSchema leftTbl_;
    TableSchema rightTbl_;
    size_t leftColIdx_ = 0;
    size_t rightColIdx_ = 0;
    std::map<std::string, std::vector<std::pair<int64_t, std::string>>> rightHash_;
    std::string curLeftRow_;
    bool hasLeft_ = false;
    bool curLeftKeyNull_ = false;
    std::vector<std::pair<int64_t, std::string>> curRightMatches_;
    size_t matchPos_ = 0;
};

// ========================================================================
// GatherMergeOp: k-way merge of pre-sorted worker streams.
// Children are per-worker sorted runs (partitioned scans with an
// order-preserving sort applied per partition); this node merges them
// into one globally ordered output, like PostgreSQL's Gather Merge.
// ========================================================================
class GatherMergeOp : public Operator {
public:
    GatherMergeOp(std::vector<OpPtr> children, const TableSchema& tbl,
                  const std::string& sortCol, bool asc);
    bool open() override;
    bool next(std::string& outRow) override;
    void close() override;
    const std::string& sortColumn() const { return sortCol_; }
    bool ascending() const { return asc_; }
    size_t inputCount() const { return children_.size(); }
    Operator* childAt(size_t i) const { return i < children_.size() ? children_[i].get() : nullptr; }

private:
    std::vector<OpPtr> children_;
    TableSchema tbl_;
    std::string sortCol_;
    bool asc_;
    // head row + done flag per stream
    std::vector<std::string> heads_;
    std::vector<bool> done_;
    size_t colIdx_ = 0;
};

struct SemiJoinSpec {
    std::string dbname;
    std::string tablename;
    std::string outerColumn;
    std::string innerColumn;
    std::vector<StorageEngine::Condition> innerConds;
    bool anti = false;
    // All equality correlations (outer col, inner col); appended after
    // the legacy members so aggregate initializers keep compiling.
    std::vector<std::pair<std::string, std::string>> correlations;
};

struct ExistenceSpec {
    std::string dbname;
    std::string tablename;
    std::vector<StorageEngine::Condition> innerConds;
    // Correlated EXISTS: inner predicates reference the outer table via
    // "<outerAlias>.<col>".  A single equality correlation lowers to a
    // semi-join key; empty means fully uncorrelated.
    std::string outerColumn;
    std::string innerColumn;
    // All equality correlations (outer col, inner col); the pair above
    // keeps the first for compatibility.
    std::vector<std::pair<std::string, std::string>> correlations;
    bool anti = false;
};

// ========================================================================
// QueryPlanner: build operator tree from parsed SQL components
// ========================================================================
struct PlanContext {
    std::string dbname;
    std::string tablename;
    std::vector<StorageEngine::Condition> conds;
    // OR of AND groups, evaluated once per physical row before aggregation.
    std::vector<std::vector<StorageEngine::Condition>> disjunctiveConds;
    std::set<std::string> selectCols;
    std::string orderByCol;
    bool orderByAsc = true;
    bool orderByNullsFirst = false;
    bool hasExplicitOrderNulls = false;
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
    // Uncorrelated ANY/ALL predicates lowered to a quantified filter.
    std::vector<QuantifiedSubquerySpec> quantifiedSubqueries;
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
    // Generic scalar SELECT plan over a single physical relation or the
    // one-row, zero-column Result source. Preparation has already validated
    // the complete query namespace; this consumes its retained typed AST.
    static bool supportsPreparedSelectPlan(const SelectStmt& select);
    static bool supportsPreparedSourceSelectPlan(const SelectStmt& select);
    static OpPtr buildPreparedSelectPlan(StorageEngine* engine,
        const std::string& dbname, const std::string& tablename,
        PreparedQuery prepared);
    // A retained SELECT inside a wholly prepared statement may consume a
    // real typed logical source (CTE/derived rows). It owns neither another
    // SQL namespace nor a temporary physical relation. The descriptor and
    // source ordinal remain those of the original binder.
    static OpPtr buildPreparedSelectPlan(StorageEngine* engine,
        const std::string& dbname, std::shared_ptr<PreparedQuery> prepared,
        SelectStmt* select, const TableSchema& sourceSchema, OpPtr source,
        const RowContext& outerRow = {}, PreparedChildExecutor childExecutor = {});
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

    // Statistics for this execution, not the lifetime of the buffer pools.
    struct ExplainExecutionStats {
        size_t actualRows = 0;
        double executionTimeMs = 0.0;
        size_t sharedHits = 0;
        size_t sharedReads = 0;
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
    static std::string explainJson(OpPtr& plan, StorageEngine* engine,
                                   const std::string& dbname,
                                   const ExplainOptions& opts,
                                   const ExplainExecutionStats& execution);

    // Checked production entry point: EOF and execution failure are distinct.
    static PlanExecutionResult executePlanChecked(OpPtr plan, size_t maxRows = 0);
    // A demand-driven receiver over the same retained typed operator graph.
    // No SQL rewriting, pre-execution, transaction or snapshot reset occurs.
    static std::unique_ptr<PreparedQueryCursor> makePreparedCursor(
        OpPtr plan, QueryRowDescriptor descriptor);

    // Parallel query support: number of worker threads (0 = disabled).
    static int parallelWorkers() { return parallelWorkers_; }
    static void setParallelWorkers(int n) { parallelWorkers_ = n < 0 ? 0 : n; }

    // ------------------------------------------------------------------
    // Cost model (P1-9 custom cost functions)
    // PostgreSQL-style cost parameters plus per-algorithm override hooks.
    // Hooks are plain function pointers so extensions (and tests) can
    // install custom costing without linking against planner internals.
    // ------------------------------------------------------------------
    struct CostModel {
        // GUC-backed parameters (SET seq_page_cost = ... etc.)
        double seqPageCost = 1.0;
        double randomPageCost = 4.0;
        double cpuTupleCost = 0.01;
        double cpuIndexTupleCost = 0.005;
        double cpuOperatorCost = 0.0025;
        bool enableNestloop = true;
        bool enableHashJoin = true;
        bool enableMergeJoin = true;

        // Custom cost overrides: return < 0 to fall back to the built-in
        // model.  algo is "seq_scan", "index_scan", "nlj", "merge", "hash".
        using CostFn = double (*)(const std::string& algo,
                                  double leftRows, double rightRows,
                                  bool rightIndexed);
        CostFn customCost = nullptr;
        void* customCostArg = nullptr;
    };
    static const CostModel& costModel() { return costModel_; }
    static void setCostModel(const CostModel& cm) { costModel_ = cm; }
    // Convenience setters used by SET handlers.
    static void setCostParameter(const std::string& name, double value);
    static void setCostEnable(const std::string& algo, bool on);
    // Cost a join algorithm under the current model (custom hook first).
    static double costJoinAlgorithm(const std::string& algo,
                                    double leftRows, double rightRows,
                                    bool rightIndexed);
    // Cost a scan strategy under the current model.
    static double costScan(const std::string& strategy,
                           double tuples, double pages);

private:
    static int parallelWorkers_;
    static CostModel costModel_;
};

} // namespace dbms
