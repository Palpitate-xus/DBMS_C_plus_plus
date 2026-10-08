#pragma once

#include "parser/query_binding.h"
#include "common/DbError.h"
#include <map>
#include <memory>
#include <set>

namespace dbms {
class Operator;
// Execution-owned typed stream. Constructing/describing a cursor is pure;
// next() opens its actual plan lazily. Explicit close preserves SQL errors,
// whereas destruction must not throw. It borrows the caller's transaction.
class PreparedQueryCursor {
public:
    virtual ~PreparedQueryCursor() = default;
    virtual const QueryRowDescriptor& descriptor() const = 0;
    virtual bool next(std::vector<ExprValue>& row) = 0;
    virtual void close() = 0;
    // A plan-backed cursor reports the same graph it actually executes.
    virtual Operator* plan() const { return nullptr; }
    virtual bool supportsRestart() const { return false; }
    // Rebind one original correlated query site without replacing its actual
    // operator graph. The prior invocation must have been closed first.
    virtual void restart(const RowContext&) { throw DbError("0A000","prepared cursor cannot rebind its caller row"); }
};
using PreparedQueryRows = std::vector<std::vector<ExprValue>>;
using PreparedChildExecutor = std::function<PreparedQueryRows(const Stmt*, const RowContext&, size_t)>;
using PreparedChildCursorFactory = std::function<std::unique_ptr<PreparedQueryCursor>(const Stmt*, const RowContext&)>;

// One execution of one wholly prepared statement. The caller must create a
// new carrier for each execution; initplan values never outlive that execution.
// AST/raw-byte provenance and parameter cells remain owned by query_.
class PreparedQueryExecution {
public:
    PreparedQueryExecution(std::shared_ptr<PreparedQuery> query,
                           StorageEngine* engine, std::string database);
    PreparedQueryExecution(const PreparedQueryExecution&) = delete;
    PreparedQueryExecution& operator=(const PreparedQueryExecution&) = delete;
    PreparedQueryExecution(PreparedQueryExecution&&) = delete;
    PreparedQueryExecution& operator=(PreparedQueryExecution&&) = delete;

    const PreparedQuery& query() const { return *query_; }
    RowContext context() const;
    // Statement inputs remain the query's frozen frame. Only actual
    // runtime/metadata parameter nodes may inherit the caller's row cells.
    RowContext context(const RowContext& caller) const;
    const PreparedQuery::SourceRange& sourceRange(size_t ordinal) const;
    void setSourceRow(RowContext& row, size_t ordinal,
                      const std::vector<ExprValue>& cells) const;
    // Call on every executable expression before opening sources/evaluating
    // any expression. Routine registration and collation checks are pure.
    void prepareExpression(Expr* expression);
    // Explicit planning phase, after whole-query binding and before any
    // source/child is opened. Fold only structural constants in execution-
    // owned copies; routines and query results are never planning datums.
    // Default prepareExpression() timing/behaviour is unchanged.
    void planExpressionConstants(Expr* expression);
    void planStatementConstants(const Stmt* statement);
    // A projection star may produce an execution-owned positional column.
    // Its true owner/range must still be validated; no SQL name lookup.
    void prepareProjectionColumn(ColumnRefExpr* column, const Stmt* owner);
    void setQueryExecutor(PreparedChildExecutor executor);
    // An explicit paired cursor owns both scalar and quantified children.
    // A physical fallback (ownsScalarChildren=false) always streams quantified
    // children, but yields scalar children to an explicit row reader or the
    // engine's installed ordinary-query host. No SQL-shape guessing is used.
    void setChildCursorFactory(PreparedChildCursorFactory factory,
                               bool ownsScalarChildren = true);
    void prepareChildCursors(); // pure graph construction, no open/evaluation
    void closeChildCursors();   // explicit normal cleanup; primary errors preserved
    // Terminal owner cleanup after close, not a correlated restart. Release
    // providers without destroying the actual closed child graphs/counters.
    void releaseQueryCallbacks();
    std::vector<Operator*> childPlans(const Expr* scope = nullptr) const;
    ExprValue evaluate(const Expr* expression, const RowContext& row) const;

private:
    struct Child {
        size_t begin = 0, end = 0;
        bool runtimeParameters = false;
        std::vector<const ColumnRefExpr*> correlations;
        std::vector<std::pair<size_t, std::string>> aliases;
    };
    std::shared_ptr<PreparedQuery> query_;
    StorageEngine* engine_;
    std::string database_;
    ExprEvaluator evaluator_;
    std::map<const Stmt*, const Stmt*> parents_;
    std::map<const Expr*, const Stmt*> owners_;
    std::map<const Expr*, Child> children_;
    std::set<const Expr*> prepared_;
    std::map<const Expr*, ExprPtr> compiled_;
    std::map<const SelectStmt*, std::set<size_t>> plannedOutputOrdinals_;
    std::map<const Expr*, const Expr*> originalSites_;
    mutable std::map<const Expr*, ExprValue> memo_;
    PreparedChildExecutor queryExecutor_;
    PreparedChildCursorFactory childCursorFactory_;
    bool cursorOwnsScalarChildren_ = true;
    struct QuantifiedState {
        std::unique_ptr<PreparedQueryCursor> cursor;
        std::vector<ExprValue> values;
        std::multimap<std::string,size_t> hash;
        bool eof = false, hasNull = false, hashBuilt = false;
    };
    std::set<const QuantifiedComparisonExpr*> quantifiedSites_;
    mutable std::map<const Expr*,QuantifiedState> quantified_;

    void indexStatement(const Stmt* statement, const Stmt* parent);
    void indexExpression(const Expr* expression, const Stmt* owner);
    void planExpressionConstants(Expr* expression, std::set<const Stmt*>& visited);
    void planStatementConstants(const Stmt* statement, std::set<const Stmt*>& visited,
                                const std::set<size_t>* outputDemand = nullptr);
    void planCompiledConstants(ExprPtr& expression, std::set<const Stmt*>& visited);
    bool isAncestor(const Stmt* ancestor, const Stmt* descendant) const;
    ExprValue executeChild(const Expr* expression, const RowContext& row) const;
    QuantifiedState& quantifiedState(const QuantifiedComparisonExpr*,const RowContext&) const;
    ExprValue executeQuantified(const QuantifiedComparisonExpr*,const RowContext&) const;
};
} // namespace dbms
