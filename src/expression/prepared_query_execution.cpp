#include "prepared_query_execution.h"
#include <exception>
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <algorithm>

namespace dbms {
namespace {
std::string quoteIdentifier(const std::string& name) {
    std::string quoted = "\"";
    for (char c : name) { quoted += c; if (c == '"') quoted += c; }
    return quoted + '"';
}
// Routine registration may lower names to execution-private callback keys.
// Keep those edits in an expression copy owned by this execution, not in the
// shared prepared AST used by metadata, another execution, or ORDER identity.
ExprPtr copyExpression(const Expr* source, std::map<const Expr*, const Expr*>& sites) {
    if (!source) return {};
    ExprPtr result;
    const auto copy = [&](const ExprPtr& node) { return copyExpression(node.get(), sites); };
    const auto window = [&](const WindowDef& source, WindowDef& target) {
        target.name = source.name; target.frameMode = source.frameMode;
        target.frameExclusion = source.frameExclusion;
        for (const auto& expr : source.partitionBy) target.partitionBy.push_back(copy(expr));
        for (const auto& expr : source.orderBy) target.orderBy.emplace_back(copy(expr.first),expr.second);
        target.frameStart = copy(source.frameStart); target.frameEnd = copy(source.frameEnd);
    };
    if (const auto* node = dynamic_cast<const LiteralExpr*>(source)) result = std::make_unique<LiteralExpr>(*node);
    else if (const auto* node = dynamic_cast<const ColumnRefExpr*>(source)) result = std::make_unique<ColumnRefExpr>(*node);
    else if (const auto* node = dynamic_cast<const ParameterExpr*>(source)) result = std::make_unique<ParameterExpr>(*node);
    else if (const auto* node = dynamic_cast<const UnaryOpExpr*>(source)) {
        auto target = std::make_unique<UnaryOpExpr>(); target->op = node->op;
        target->operand = copy(node->operand); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const BinaryOpExpr*>(source)) {
        auto target = std::make_unique<BinaryOpExpr>(); target->op = node->op;
        target->arrayConcat = node->arrayConcat;
        target->left = copy(node->left); target->right = copy(node->right); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const CastExpr*>(source)) {
        auto target = std::make_unique<CastExpr>(); target->typeName = node->typeName;
        target->implicit = node->implicit;
        target->typeMods = node->typeMods; target->operand = copy(node->operand); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const CaseExpr*>(source)) {
        auto target = std::make_unique<CaseExpr>();
        target->simpleComparisonTypes = node->simpleComparisonTypes;
        target->switchExpr = copy(node->switchExpr); target->elseExpr = copy(node->elseExpr);
        for (const auto& arm : node->whenClauses) target->whenClauses.emplace_back(copy(arm.first),copy(arm.second));
        result = std::move(target);
    } else if (const auto* node = dynamic_cast<const FunctionCallExpr*>(source)) {
        auto target = std::make_unique<FunctionCallExpr>();
        target->schema = node->schema; target->funcName = node->funcName;
        target->distinct = node->distinct; target->orderBy = node->orderBy; target->hasOver = node->hasOver;
        for (const auto& arg : node->args) target->args.push_back(copy(arg));
        // The binder deliberately leaves EXTRACT's unqualified grammar field
        // outside SQL value namespaces. Lower that one role in the execution
        // copy, not a qualified/stored function's ordinary value argument.
        if (node->schema.empty() && SQLParser::toLower(node->funcName) == "extract" &&
            !target->args.empty()) {
            const auto* field = dynamic_cast<const ColumnRefExpr*>(node->args.front().get());
            if (field && !field->binding && field->schema.empty() && field->table.empty()) {
                auto literal = std::make_unique<LiteralExpr>();
                literal->typeName = "text"; literal->value = "'" + field->column + "'";
                target->args.front() = std::move(literal);
            }
        }
        for (const auto& arg : node->namedArgs) target->namedArgs.push_back({arg.name,copy(arg.value)});
        target->filter = copy(node->filter); window(node->over,target->over); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const ArrayExpr*>(source)) {
        auto target = std::make_unique<ArrayExpr>();
        target->elementType = node->elementType;
        target->nestedElements = node->nestedElements;
        for (const auto& arg : node->elements) target->elements.push_back(copy(arg));
        result = std::move(target);
    } else if (const auto* node = dynamic_cast<const RowExpr*>(source)) {
        auto target = std::make_unique<RowExpr>();
        for (const auto& arg : node->elements) target->elements.push_back(copy(arg));
        result = std::move(target);
    } else throw DbError("0A000", "prepared execution requires a structured value expression");
    result->sourceBegin = source->sourceBegin; result->sourceEnd = source->sourceEnd;
    result->preparedSubquery = source->preparedSubquery;
    sites.emplace(result.get(),source);
    return result;
}
}

PreparedQueryExecution::PreparedQueryExecution(std::shared_ptr<PreparedQuery> query,
    StorageEngine* engine, std::string database)
    : query_(std::move(query)), engine_(engine), database_(std::move(database)) {
    if (!query_ || !query_->ast || !engine_)
        throw DbError("XX000", "prepared execution requires an owned query and engine");
    indexStatement(query_->ast.get(), nullptr);
    // Verify every retained column against its actual owner/ancestor. Unique
    // ordinal numbers are not permission to bind a sibling's SQL namespace.
    for (const auto& entry : owners_) {
        const auto* column = dynamic_cast<const ColumnRefExpr*>(entry.first);
        if (!column || !column->binding) continue;
        const auto& binding = *column->binding;
        const auto& range = sourceRange(binding.sourceOrdinal);
        if (!isAncestor(range.owner, entry.second) ||
            (range.owner == entry.second) != (binding.scopeDepth == 0) ||
            binding.columnOrdinal >= range.columns.size() ||
            binding.mergedUsing != range.mergedUsing ||
            ExprHelper::canonicalResultTypeName(binding.declaredType) !=
                ExprHelper::canonicalResultTypeName(range.columns[binding.columnOrdinal].type))
            throw DbError("XX000", "prepared column binding does not belong to its SQL scope");
    }
    // Classify correlation structurally from range owners. An internally
    // correlated grandchild does not make its containing initplan correlated
    // with this query's caller.
    for (const auto& entry : owners_) {
        const Expr* expression = entry.first;
        if (!expression->preparedSubquery) continue;
        Child child;
        if (expression->sourceBegin == std::string::npos ||
            expression->sourceEnd > query_->source.size())
            throw DbError("XX000", "prepared child has no original source span");
        const size_t open = query_->source.find('(', expression->sourceBegin);
        const size_t close = query_->source.rfind(')', expression->sourceEnd - 1);
        if (open == std::string::npos || close == std::string::npos ||
            open >= close || close >= expression->sourceEnd)
            throw DbError("XX000", "prepared child has an invalid original source span");
        child.begin = open + 1; child.end = close;
        const Stmt* root = expression->preparedSubquery.get();
        for (const auto& owned : owners_) {
            const auto* column = dynamic_cast<const ColumnRefExpr*>(owned.first);
            if (!column || !column->binding || !isAncestor(root, owned.second)) continue;
            const auto& range = sourceRange(column->binding->sourceOrdinal);
            if (isAncestor(root, range.owner)) continue;
            if (!isAncestor(range.owner, entry.second))
                throw DbError("XX000", "prepared child has a non-ancestor correlation");
            if (column->sourceBegin < child.begin || column->sourceEnd > child.end ||
                column->sourceBegin >= column->sourceEnd)
                throw DbError("XX000", "prepared correlation has no child source span");
            child.correlations.push_back(column);
        }
        // Replacing a bare projected outer column must not change its output
        // name into CAST. Labels remain canonical quoted identifiers.
        for (const auto& owned : parents_) {
            if (!isAncestor(root, owned.first)) continue;
            const auto* select = dynamic_cast<const SelectStmt*>(owned.first);
            if (!select) continue;
            for (const auto& item : select->selectList) {
                const auto* column = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
                if (!column || !item.alias.empty() ||
                    std::find(child.correlations.begin(), child.correlations.end(), column) == child.correlations.end()) continue;
                if (item.sourceExpressionEnd == std::string::npos ||
                    item.sourceExpressionEnd < child.begin || item.sourceExpressionEnd > child.end)
                    throw DbError("XX000", "correlated projection has no source label position");
                child.aliases.emplace_back(item.sourceExpressionEnd, quoteIdentifier(column->column));
            }
        }
        children_.emplace(expression, std::move(child));
    }
    evaluator_.setCurrentDB(database_);
    evaluator_.setScalarSubqueryExecutor([this](const Expr* expression, const RowContext& row) {
        const auto found = originalSites_.find(expression);
        if (found == originalSites_.end()) throw DbError("XX000", "compiled child has no prepared site");
        return executeChild(found->second, row);
    });
}

bool PreparedQueryExecution::isAncestor(const Stmt* ancestor, const Stmt* descendant) const {
    if (!ancestor || !descendant) return false;
    for (const Stmt* current = descendant; current;) {
        if (current == ancestor) return true;
        const auto parent = parents_.find(current);
        if (parent == parents_.end()) return false;
        current = parent->second;
    }
    return false;
}

const PreparedQuery::SourceRange& PreparedQueryExecution::sourceRange(size_t ordinal) const {
    const auto found = std::find_if(query_->sourceRanges.begin(), query_->sourceRanges.end(),
        [&](const PreparedQuery::SourceRange& source) { return source.ordinal == ordinal; });
    if (found == query_->sourceRanges.end() || !parents_.count(found->owner))
        throw DbError("XX000", "prepared source occurrence has no statement owner");
    return *found;
}

RowContext PreparedQueryExecution::context() const {
    RowContext row; row.setParameters(query_->parameters); return row;
}

void PreparedQueryExecution::setSourceRow(RowContext& row, size_t ordinal,
    const std::vector<ExprValue>& cells) const {
    const auto& source = sourceRange(ordinal);
    if (source.columns.size() != cells.size())
        throw DbError("XX000", "prepared source row width differs from its descriptor");
    for (size_t i = 0; i < cells.size(); ++i) {
        ExprValue cell = cells[i];
        const auto type = ExprHelper::canonicalResultTypeName(source.columns[i].type);
        if (!cell.typeName.empty() && ExprHelper::canonicalResultTypeName(cell.typeName) != type)
            throw DbError("XX000", "prepared source cell type differs from its descriptor");
        cell.typeName = type;
        row.setBoundColumn(ordinal, i, std::move(cell));
    }
}

void PreparedQueryExecution::prepareExpression(Expr* expression) {
    if (!expression || prepared_.count(expression)) return;
    if (!owners_.count(expression))
        throw DbError("XX000", "expression does not belong to this prepared execution");
    ExprEvaluator::analyzeExplicitResultCollation(expression);
    std::map<const Expr*, const Expr*> sites;
    auto compiled = copyExpression(expression,sites);
    ExprHelper::prepareArrayTypes(compiled.get(), {}, database_, engine_);
    evaluator_.bindScalarFunctions(compiled.get(), engine_);
    for (const auto& site : sites) originalSites_[site.first] = site.second;
    compiled_.emplace(expression,std::move(compiled));
    prepared_.insert(expression);
}

void PreparedQueryExecution::prepareProjectionColumn(ColumnRefExpr* column, const Stmt* owner) {
    if (!column || !column->binding || !parents_.count(owner))
        throw DbError("XX000", "synthetic projection requires a genuine prepared owner");
    const auto& binding = *column->binding;
    const auto& range = sourceRange(binding.sourceOrdinal);
    if (range.owner != owner || binding.scopeDepth || binding.mergedUsing != range.mergedUsing ||
        binding.columnOrdinal >= range.columns.size() ||
        ExprHelper::canonicalResultTypeName(binding.declaredType) !=
            ExprHelper::canonicalResultTypeName(range.columns[binding.columnOrdinal].type))
        throw DbError("XX000", "synthetic projection column has an invalid positional binding");
    owners_.emplace(column, owner);
    prepareExpression(column);
}

void PreparedQueryExecution::setQueryExecutor(PreparedChildExecutor executor) {
    memo_.clear();
    queryExecutor_ = std::move(executor);
}

void PreparedQueryExecution::setChildCursorFactory(PreparedChildCursorFactory factory) {
    memo_.clear();
    childCursorFactory_ = std::move(factory);
}

ExprValue PreparedQueryExecution::evaluate(const Expr* expression, const RowContext& row) const {
    if (!expression || !prepared_.count(expression))
        throw DbError("XX000", "prepared expression was not registered before execution");
    try { return evaluator_.eval(compiled_.at(expression).get(), row); }
    catch (...) {
        // A failed child may restore storage caches or abort the statement.
        // Previously successful initplans cannot leak past that boundary.
        memo_.clear();
        throw;
    }
}

ExprValue PreparedQueryExecution::executeChild(const Expr* expression, const RowContext& row) const {
    const auto found = children_.find(expression);
    if (found == children_.end()) throw DbError("XX000", "scalar child belongs to another execution");
    const Child& child = found->second;
    if (child.correlations.empty()) {
        const auto cached = memo_.find(expression);
        if (cached != memo_.end()) return cached->second;
    }
    if (childCursorFactory_) {
        const Stmt* statement = expression->preparedSubquery.get();
        const auto output = query_->statementOutputs.find(statement);
        if (output == query_->statementOutputs.end() || output->second.size() != 1)
            throw DbError("42601", "subquery must return only one column");
        auto cursor = childCursorFactory_(statement, row);
        if (!cursor) throw DbError("XX000", "prepared child cursor factory returned no cursor");
        try {
            const auto& descriptor = cursor->descriptor();
            if (descriptor.size() != 1 ||
                ExprHelper::canonicalResultTypeName(descriptor.front().type) !=
                    ExprHelper::canonicalResultTypeName(output->second.front().type))
                throw DbError("XX000", "prepared child cursor lost its declared descriptor");
            std::vector<ExprValue> values;
            ExprValue result(output->second.front().type, "", true);
            if (cursor->next(values)) {
                if (values.size() != 1 ||
                    ExprHelper::canonicalResultTypeName(values.front().typeName) !=
                        ExprHelper::canonicalResultTypeName(descriptor.front().type))
                    throw DbError("XX000", "prepared child cursor lost its structured width or type");
                result = values.front();
                if (cursor->next(values))
                    throw DbError("21000", "more than one row returned by a subquery used as an expression");
            }
            cursor->close();
            if (child.correlations.empty()) memo_.emplace(expression, result);
            return result;
        } catch (...) {
            // A close failure cannot replace the actual scalar/cardinality
            // failure. The factory's cursor must make close idempotent.
            const auto failure = std::current_exception();
            try { cursor->close(); } catch (...) {}
            std::rethrow_exception(failure);
        }
    }
    if (queryExecutor_) {
        const Stmt* statement = expression->preparedSubquery.get();
        const auto output = query_->statementOutputs.find(statement);
        if (output == query_->statementOutputs.end() || output->second.size() != 1)
            throw DbError("42601", "subquery must return only one column");
        const auto rows = queryExecutor_(statement, row, 2);
        if (rows.size() > 1)
            throw DbError("21000", "more than one row returned by a subquery used as an expression");
        ExprValue result(output->second.front().type, "", true);
        if (!rows.empty()) {
            if (rows.front().size() != 1) throw DbError("XX000", "scalar child lost its structured width");
            result = rows.front().front();
        }
        if (child.correlations.empty()) memo_.emplace(expression, result);
        return result;
    }
    PreparedQuery adapter;
    adapter.source = query_->source.substr(child.begin, child.end - child.begin);
    adapter.parameters = query_->parameters;
    for (const auto& use : query_->uses) {
        if (use.begin < child.begin || use.end > child.end) continue;
        if (use.slot >= adapter.parameters.size()) throw DbError("XX000", "child parameter has no declared cell");
        const auto& parameter = row.parameter(use.slot);
        if (ExprHelper::canonicalResultTypeName(parameter.typeName) !=
            ExprHelper::canonicalResultTypeName(adapter.parameters[use.slot].typeName))
            throw DbError("XX000", "child parameter cell has an unexpected declared type");
        adapter.parameters[use.slot] = parameter;
        adapter.uses.push_back({use.begin - child.begin, use.end - child.begin, use.slot});
    }
    for (const auto& alias : query_->projectionAliases)
        if (alias.first >= child.begin && alias.first <= child.end)
            adapter.projectionAliases.emplace_back(alias.first - child.begin, alias.second);
    for (const auto* column : child.correlations) {
        const auto& binding = *column->binding;
        ExprValue cell = row.boundColumn(binding.sourceOrdinal, binding.columnOrdinal);
        const auto type = ExprHelper::canonicalResultTypeName(binding.declaredType);
        if (ExprHelper::canonicalResultTypeName(cell.typeName) != type)
            throw DbError("XX000", "correlated runtime cell has an unexpected declared type");
        cell.typeName = type;
        const size_t slot = adapter.parameters.size();
        adapter.parameters.push_back(std::move(cell));
        adapter.uses.push_back({column->sourceBegin - child.begin, column->sourceEnd - child.begin, slot});
    }
    for (const auto& alias : child.aliases)
        adapter.projectionAliases.emplace_back(alias.first - child.begin, alias.second);
    ExprValue result = engine_->executeScalarSubquery(database_, adapter.legacySql());
    // Failed executions are not cached, and structured SQL NULL is retained.
    if (child.correlations.empty()) memo_.emplace(expression, result);
    return result;
}

void PreparedQueryExecution::indexExpression(const Expr* expression, const Stmt* owner) {
    if (!expression) return;
    const auto inserted = owners_.emplace(expression, owner);
    if (!inserted.second) throw DbError("XX000", "prepared expression has multiple statement owners");
    if (expression->preparedSubquery) {
        indexStatement(expression->preparedSubquery.get(), owner); return;
    }
    const auto value = [&](const ExprPtr& node) { indexExpression(node.get(), owner); };
    if (const auto* node = dynamic_cast<const UnaryOpExpr*>(expression)) value(node->operand);
    else if (const auto* node = dynamic_cast<const BinaryOpExpr*>(expression)) { value(node->left); value(node->right); }
    else if (const auto* node = dynamic_cast<const CastExpr*>(expression)) value(node->operand);
    else if (const auto* node = dynamic_cast<const CaseExpr*>(expression)) {
        value(node->switchExpr); value(node->elseExpr);
        for (const auto& arm : node->whenClauses) { value(arm.first); value(arm.second); }
    } else if (const auto* node = dynamic_cast<const FunctionCallExpr*>(expression)) {
        for (const auto& arg : node->args) value(arg);
        for (const auto& arg : node->namedArgs) value(arg.value);
        value(node->filter);
        for (const auto& arg : node->over.partitionBy) value(arg);
        for (const auto& arg : node->over.orderBy) value(arg.first);
        value(node->over.frameStart); value(node->over.frameEnd);
    } else if (const auto* node = dynamic_cast<const ArrayExpr*>(expression)) for (const auto& arg : node->elements) value(arg);
    else if (const auto* node = dynamic_cast<const RowExpr*>(expression)) for (const auto& arg : node->elements) value(arg);
}

void PreparedQueryExecution::indexStatement(const Stmt* statement, const Stmt* parent) {
    if (!statement) return;
    if (!parents_.emplace(statement, parent).second)
        throw DbError("XX000", "prepared statement has multiple owners");
    const auto value = [&](const ExprPtr& expression) { indexExpression(expression.get(), statement); };
    const auto items = [&](const std::vector<SelectItem>& list) { for (const auto& item : list) value(item.expr); };
    const auto window = [&](const WindowDef& def) {
        for (const auto& expr : def.partitionBy) value(expr);
        for (const auto& expr : def.orderBy) value(expr.first);
        value(def.frameStart); value(def.frameEnd);
    };
    std::function<void(const FromItem*)> from = [&](const FromItem* item) {
        if (!item) return;
        indexStatement(item->subquery.get(), statement); value(item->joinCondition);
        from(item->left.get()); from(item->right.get());
    };
    if (const auto* node = dynamic_cast<const WithStmt*>(statement)) {
        for (const auto& cte : node->ctes) indexStatement(cte.query.get(), statement);
        indexStatement(node->statement.get(), statement);
    } else if (const auto* node = dynamic_cast<const SelectStmt*>(statement)) {
        for (const auto& cte : node->ctes) indexStatement(cte.query.get(), statement);
        indexStatement(node->setOpLhs.get(), statement); indexStatement(node->setOpRhs.get(), statement);
        items(node->selectList); from(node->fromClause.get()); value(node->whereClause); value(node->having);
        for (const auto& expr : node->groupBy) value(expr);
        for (const auto& elem : node->groupByElems) for (const auto& expr : elem.exprs) value(expr);
        for (const auto& expr : node->distinctOn) value(expr);
        for (const auto& order : node->orderBy) value(order.expr);
        for (const auto& row : node->valuesRows) for (const auto& expr : row) value(expr);
        for (const auto& def : node->windowDefs) window(def);
    } else if (const auto* node = dynamic_cast<const InsertStmt*>(statement)) {
        for (const auto& row : node->values) for (const auto& expr : row) value(expr);
        indexStatement(node->selectSource.get(), statement);
        for (const auto& expr : node->conflictUpdateSet) value(expr.second);
        value(node->conflictWhere); items(node->returning);
    } else if (const auto* node = dynamic_cast<const UpdateStmt*>(statement)) {
        for (const auto& expr : node->setClauses) value(expr.second);
        from(node->fromClause.get()); value(node->whereClause); items(node->returning);
    } else if (const auto* node = dynamic_cast<const DeleteStmt*>(statement)) {
        from(node->usingClause.get()); value(node->whereClause); items(node->returning);
    } else if (const auto* node = dynamic_cast<const ExplainStmt*>(statement)) {
        indexStatement(node->query.get(), statement);
    } else if (const auto* node = dynamic_cast<const CreateTableStmt*>(statement)) {
        indexStatement(node->preparedAsQuery.get(), statement);
        for (const auto& column : node->columns) value(column.defaultValue);
    }
}
} // namespace dbms
