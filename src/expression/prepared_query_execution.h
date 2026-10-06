#pragma once

#include "parser/query_binding.h"
#include <map>
#include <memory>
#include <set>

namespace dbms {
using PreparedQueryRows = std::vector<std::vector<ExprValue>>;
using PreparedChildExecutor = std::function<PreparedQueryRows(const Stmt*, const RowContext&, size_t)>;

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
    const PreparedQuery::SourceRange& sourceRange(size_t ordinal) const;
    void setSourceRow(RowContext& row, size_t ordinal,
                      const std::vector<ExprValue>& cells) const;
    // Call on every executable expression before opening sources/evaluating
    // any expression. Routine registration and collation checks are pure.
    void prepareExpression(Expr* expression);
    // A projection star may produce an execution-owned positional column.
    // Its true owner/range must still be validated; no SQL name lookup.
    void prepareProjectionColumn(ColumnRefExpr* column, const Stmt* owner);
    void setQueryExecutor(PreparedChildExecutor executor);
    ExprValue evaluate(const Expr* expression, const RowContext& row) const;

private:
    struct Child {
        size_t begin = 0, end = 0;
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
    std::map<const Expr*, const Expr*> originalSites_;
    mutable std::map<const Expr*, ExprValue> memo_;
    PreparedChildExecutor queryExecutor_;

    void indexStatement(const Stmt* statement, const Stmt* parent);
    void indexExpression(const Expr* expression, const Stmt* owner);
    bool isAncestor(const Stmt* ancestor, const Stmt* descendant) const;
    ExprValue executeChild(const Expr* expression, const RowContext& row) const;
};
} // namespace dbms
