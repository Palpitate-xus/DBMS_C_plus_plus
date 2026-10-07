#include "prepared_query_execution.h"
#include <exception>
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <algorithm>
#include <optional>

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
    } else if (const auto* node = dynamic_cast<const QuantifiedComparisonExpr*>(source)) {
        auto target = std::make_unique<QuantifiedComparisonExpr>();
        target->op=node->op;target->quantifier=node->quantifier;target->comparison=node->comparison;
        target->left=copy(node->left);target->right=copy(node->right);result=std::move(target);
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
        target->setReturning = node->setReturning;
        target->resolvedResultType = node->resolvedResultType;
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

ExprPtr constantExpression(const ExprValue& value, const Expr* source) {
    auto literal = std::make_unique<LiteralExpr>();
    literal->typeName = value.typeName;
    if (value.isNull) literal->value = "NULL";
    else {
        literal->value = "'";
        for (char c : value.value) { literal->value += c; if (c == '\'') literal->value += c; }
        literal->value += '\'';
    }
    ExprPtr result = std::move(literal);
    // Literal NULL itself has unknown type; retain a typed NULL datum without
    // changing the shared query's output descriptor or original expression.
    if (value.isNull && value.typeName != "unknown" && !value.typeName.empty()) {
        auto cast = std::make_unique<CastExpr>();
        cast->typeName = value.typeName; cast->operand = std::move(result);
        result = std::move(cast);
    }
    if (!value.collation.empty()) {
        auto collate = std::make_unique<UnaryOpExpr>();
        collate->op = "COLLATE " + quoteIdentifier(value.collation);
        collate->operand = std::move(result); result = std::move(collate);
    }
    if (source) { result->sourceBegin = source->sourceBegin; result->sourceEnd = source->sourceEnd; }
    return result;
}

// CASE has a planning boundary as well as a lazy runtime boundary. Substitute
// structural constants in its strict equality tests only after whole-query
// binding. Never infer that a nullable column/parameter is a constant NULL,
// and never execute a routine or a child query to obtain a planning value.
struct ConstantPlanningRoles {
    std::function<bool(const FunctionCallExpr*)> coalesce;
    std::function<std::string(const FunctionCallExpr*)> resultType;
    std::function<bool(const FunctionCallExpr*)> patternEscape;
};

std::optional<ExprValue> simplifyCaseConstants(ExprPtr& expression,
    const ExprEvaluator& evaluator, const ConstantPlanningRoles* roles = nullptr) {
    if (!expression || expression->preparedSubquery) return std::nullopt;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expression.get())) {
        if (literal->value == "*") return std::nullopt;
        return evaluator.eval(literal, RowContext{});
    }
    if (auto* conditional = dynamic_cast<CaseExpr*>(expression.get())) {
        const auto switchValue = simplifyCaseConstants(conditional->switchExpr, evaluator, roles);
        std::vector<std::pair<ExprPtr,ExprPtr>> remaining;
        std::vector<std::pair<std::string,std::string>> comparisons;
        bool definiteMatch = false;
        for (size_t i=0; i<conditional->whenClauses.size(); ++i) {
            auto& arm = conditional->whenClauses[i];
            // Fold pure condition subexpressions before applying strict NULL:
            // NULL = (1/0) still reports the planner's division-by-zero error.
            const auto conditionValue = simplifyCaseConstants(arm.first, evaluator, roles);
            std::optional<bool> matches;
            if (conditional->switchExpr) {
                if ((switchValue && switchValue->isNull) ||
                    (conditionValue && conditionValue->isNull)) matches = false;
                else if (switchValue && conditionValue &&
                         conditional->simpleComparisonTypes.size()==conditional->whenClauses.size()) {
                    CaseExpr comparison;
                    comparison.switchExpr = constantExpression(*switchValue, nullptr);
                    comparison.simpleComparisonTypes.push_back(conditional->simpleComparisonTypes[i]);
                    auto yes = std::make_unique<LiteralExpr>(); yes->value = "true";
                    auto no = std::make_unique<LiteralExpr>(); no->value = "false";
                    comparison.whenClauses.emplace_back(constantExpression(*conditionValue,nullptr),std::move(yes));
                    comparison.elseExpr = std::move(no);
                    matches = evaluator.eval(&comparison,RowContext{}).asBool();
                }
            } else if (conditionValue) matches = !conditionValue->isNull && conditionValue->asBool();
            if (matches && !*matches) continue; // Do not plan the discarded THEN.
            simplifyCaseConstants(arm.second,evaluator,roles);
            if (matches && *matches) {
                conditional->elseExpr = std::move(arm.second);
                definiteMatch = true;
                break; // Later WHENs and the original ELSE are unreachable.
            }
            remaining.emplace_back(std::move(arm.first),std::move(arm.second));
            if (!conditional->simpleComparisonTypes.empty())
                comparisons.push_back(conditional->simpleComparisonTypes.at(i));
        }
        if (!definiteMatch) simplifyCaseConstants(conditional->elseExpr,evaluator,roles);
        conditional->whenClauses = std::move(remaining);
        conditional->simpleComparisonTypes = std::move(comparisons);
        if (conditional->whenClauses.empty()) {
            auto fallback = std::move(conditional->elseExpr);
            if (!fallback) fallback = constantExpression(ExprValue("unknown","",true),conditional);
            expression = std::move(fallback);
            return simplifyCaseConstants(expression,evaluator,roles);
        }
        return std::nullopt;
    }
    bool constant = false;
    if (auto* unary = dynamic_cast<UnaryOpExpr*>(expression.get()))
        constant = simplifyCaseConstants(unary->operand,evaluator,roles).has_value();
    else if (auto* cast = dynamic_cast<CastExpr*>(expression.get()))
        constant = simplifyCaseConstants(cast->operand,evaluator,roles).has_value();
    else if (auto* binary = dynamic_cast<BinaryOpExpr*>(expression.get())) {
        const auto op = SQLParser::toLower(binary->op);
        if(op=="[]" && dynamic_cast<BinaryOpExpr*>(binary->left.get()) &&
           static_cast<BinaryOpExpr*>(binary->left.get())->op=="[]") {
            // Scalar multidimensional fetch owns the whole postfix chain.
            // Folding its inner single-index fetch independently would
            // replace a valid N-index operation by an intermediate NULL.
            ExprPtr* receiver=&binary->left;
            std::vector<ExprPtr*> indexes{&binary->right};
            while(auto* index=dynamic_cast<BinaryOpExpr*>(receiver->get())) {
                if(index->op!="[]")break;
                indexes.push_back(&index->right);receiver=&index->left;
            }
            constant=simplifyCaseConstants(*receiver,evaluator,roles).has_value();
            for(auto index=indexes.rbegin();index!=indexes.rend();++index)
                constant=simplifyCaseConstants(**index,evaluator,roles).has_value() && constant;
            if(!constant)return std::nullopt;
            const auto value=evaluator.eval(expression.get(),RowContext{});
            expression=constantExpression(value,expression.get());return value;
        }
        const auto left = simplifyCaseConstants(binary->left,evaluator,roles);
        // Match boolean constant demand rather than folding a dead right arm.
        if (left && !left->isNull &&
            ((op=="and" && !left->asBool()) || (op=="or" && left->asBool()))) {
            const ExprValue result("boolean",op=="or"?"t":"f",false);
            expression = constantExpression(result,expression.get()); return result;
        }
        const auto right = binary->op=="::" ? std::optional<ExprValue>(ExprValue{})
            : simplifyCaseConstants(binary->right,evaluator,roles);
        constant = left.has_value() && right.has_value();
    } else if (auto* quantified=dynamic_cast<QuantifiedComparisonExpr*>(expression.get())) {
        simplifyCaseConstants(quantified->left,evaluator,roles);
        // A prepared SQL child has its own planning boundary, never execute
        // or fold its result as an outer expression's constant datum.
        simplifyCaseConstants(quantified->right,evaluator,roles);
        return std::nullopt;
    } else if (auto* call = dynamic_cast<FunctionCallExpr*>(expression.get())) {
        if (roles && roles->patternEscape(call)) {
            std::vector<std::optional<ExprValue>> inputs;
            for (auto& argument : call->args)
                inputs.push_back(simplifyCaseConstants(argument,evaluator,roles));
            // The parser's strict normalizer consumes pattern/escape before
            // the outer strict match consumes lhs. Never run a query or a
            // routine to discover whether one of these inputs is NULL.
            if ((inputs[1] && inputs[1]->isNull) ||
                (inputs[2] && inputs[2]->isNull)) {
                const ExprValue result("boolean","",true);
                expression=constantExpression(result,expression.get());return result;
            }
            if (inputs[1] && inputs[2])
                ExprEvaluator::validatePatternEscapeInput(*inputs[2]);
            if (inputs[0] && inputs[0]->isNull) {
                const ExprValue result("boolean","",true);
                expression=constantExpression(result,expression.get());return result;
            }
            if (inputs[0] && inputs[1] && inputs[2]) {
                const auto result=evaluator.eval(call,RowContext{});
                expression=constantExpression(result,expression.get());return result;
            }
            return std::nullopt;
        }
        // COALESCE is a SQL demand construct, not a user routine. Resolve
        // its real builtin identity before applying this rule; a quoted or
        // qualified stored function must retain ordinary argument demand.
        if (roles && roles->coalesce(call)) {
            const auto type = roles->resultType(call); // full, unpruned metadata
            bool knownPrefix = true;
            for (size_t i = 0; i < call->args.size(); ++i) {
                const auto value = simplifyCaseConstants(call->args[i],evaluator,roles);
                if (!value) knownPrefix = false;
                if (!value || value->isNull) continue;
                if (knownPrefix) {
                    auto datum = *value;
                    if (ExprHelper::canonicalResultTypeName(datum.typeName) != type) {
                        CastExpr conversion; conversion.typeName = type; conversion.implicit = true;
                        conversion.operand = constantExpression(datum,nullptr);
                        datum = evaluator.eval(&conversion,RowContext{});
                    }
                    expression = constantExpression(datum,expression.get());
                    return datum;
                }
                call->args.resize(i + 1); // later arguments are unreachable
                // Retain the full expression's type even after demand pruning.
                for (auto& argument : call->args) {
                    auto cast = std::make_unique<CastExpr>(); cast->typeName = type; cast->implicit = true;
                    cast->operand = std::move(argument); argument = std::move(cast);
                }
                return std::nullopt;
            }
            if (knownPrefix) {
                const ExprValue result(type,"",true);
                expression = constantExpression(result,expression.get()); return result;
            }
            return std::nullopt;
        }
        // Constant arguments can have planning errors even under a volatile
        // routine. The routine itself is never evaluated by this simplifier.
        for (auto& arg : call->args) simplifyCaseConstants(arg,evaluator,roles);
        for (auto& arg : call->namedArgs) simplifyCaseConstants(arg.value,evaluator,roles);
        return std::nullopt;
    } else if (auto* array = dynamic_cast<ArrayExpr*>(expression.get())) {
        for (auto& element : array->elements) simplifyCaseConstants(element,evaluator,roles);
        return std::nullopt;
    } else if (auto* row = dynamic_cast<RowExpr*>(expression.get())) {
        for (auto& element : row->elements) simplifyCaseConstants(element,evaluator,roles);
        return std::nullopt;
    }
    if (!constant) return std::nullopt;
    const auto value = evaluator.eval(expression.get(),RowContext{});
    expression = constantExpression(value,expression.get());
    return value;
}

// Do not introduce constant-fold timing changes in unrelated expressions.
void planCaseConstants(ExprPtr& expression, const ExprEvaluator& evaluator) {
    if (!expression || expression->preparedSubquery) return;
    if (dynamic_cast<CaseExpr*>(expression.get())) { simplifyCaseConstants(expression,evaluator); return; }
    if (auto* unary=dynamic_cast<UnaryOpExpr*>(expression.get())) planCaseConstants(unary->operand,evaluator);
    else if (auto* cast=dynamic_cast<CastExpr*>(expression.get())) planCaseConstants(cast->operand,evaluator);
    else if (auto* binary=dynamic_cast<BinaryOpExpr*>(expression.get())) {
        planCaseConstants(binary->left,evaluator);
        if(binary->op!="::")planCaseConstants(binary->right,evaluator);
    } else if(auto* quantified=dynamic_cast<QuantifiedComparisonExpr*>(expression.get())) {
        planCaseConstants(quantified->left,evaluator);
        planCaseConstants(quantified->right,evaluator);
    } else if (auto* call=dynamic_cast<FunctionCallExpr*>(expression.get())) {
        for(auto& arg:call->args)planCaseConstants(arg,evaluator);
        for(auto& arg:call->namedArgs)planCaseConstants(arg.value,evaluator);
    } else if(auto* array=dynamic_cast<ArrayExpr*>(expression.get())) {
        for(auto& element:array->elements)planCaseConstants(element,evaluator);
    } else if(auto* row=dynamic_cast<RowExpr*>(expression.get())) {
        for(auto& element:row->elements)planCaseConstants(element,evaluator);
    }
}

// Value roles only; no statement/range crossing and no :: type-label role.
void visitStructuredValue(const Expr* expression,
    const std::function<void(const Expr*)>& visitor) {
    if (!expression) return;
    visitor(expression);
    if (expression->preparedSubquery) return;
    const auto value = [&](const ExprPtr& child) { visitStructuredValue(child.get(), visitor); };
    if (const auto* node = dynamic_cast<const UnaryOpExpr*>(expression)) value(node->operand);
    else if (const auto* node = dynamic_cast<const CastExpr*>(expression)) value(node->operand);
    else if (const auto* node = dynamic_cast<const BinaryOpExpr*>(expression)) {
        value(node->left); if (node->op != "::") value(node->right);
    } else if (const auto* node = dynamic_cast<const QuantifiedComparisonExpr*>(expression)) {
        value(node->left); value(node->right);
    } else if (const auto* node = dynamic_cast<const CaseExpr*>(expression)) {
        value(node->switchExpr); value(node->elseExpr);
        for (const auto& arm : node->whenClauses) { value(arm.first); value(arm.second); }
    } else if (const auto* node = dynamic_cast<const FunctionCallExpr*>(expression)) {
        for (const auto& arg : node->args) value(arg);
        for (const auto& arg : node->namedArgs) value(arg.value);
        value(node->filter);
        for (const auto& arg : node->over.partitionBy) value(arg);
        for (const auto& arg : node->over.orderBy) value(arg.first);
        value(node->over.frameStart); value(node->over.frameEnd);
    } else if (const auto* node = dynamic_cast<const ArrayExpr*>(expression)) {
        for (const auto& arg : node->elements) value(arg);
    } else if (const auto* node = dynamic_cast<const RowExpr*>(expression)) {
        for (const auto& arg : node->elements) value(arg);
    }
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
    for (const auto& projection : query_->projectionBindings) {
        if (!parents_.count(projection.first))
            throw DbError("XX000", "projection metadata has no genuine prepared owner");
        for (const auto& output : projection.second) {
            const auto owner = owners_.find(output.expression);
            if (owner == owners_.end() || owner->second != projection.first)
                throw DbError("XX000", "projection metadata has no original expression site");
            if (!output.column) continue;
            const auto& binding = *output.column;
            const auto& source = sourceRange(binding.sourceOrdinal);
            if (!isAncestor(source.owner,projection.first) ||
                (source.owner==projection.first)!=(binding.scopeDepth==0) ||
                binding.columnOrdinal>=source.columns.size() || binding.mergedUsing!=source.mergedUsing ||
                ExprHelper::canonicalResultTypeName(binding.declaredType)!=
                    ExprHelper::canonicalResultTypeName(source.columns[binding.columnOrdinal].type))
                throw DbError("XX000", "projection metadata does not belong to its SQL source occurrence");
        }
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
    evaluator_.setQuantifiedExecutor([this](const QuantifiedComparisonExpr* expression,const RowContext& row) {
        return executeQuantified(expression,row);
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
    if (const auto planned = compiled_.find(expression); planned != compiled_.end()) {
        // Explicit pure planning does not bind runtime callbacks or give an
        // aggregate/window/SRF a scalar execution role. The actual consumer
        // still performs its normal preparation before evaluation.
        evaluator_.bindScalarFunctions(planned->second.get(), engine_);
        prepared_.insert(expression);
        return;
    }
    std::map<const Expr*, const Expr*> sites;
    auto compiled = copyExpression(expression,sites);
    ExprHelper::prepareArrayTypes(compiled.get(), {}, database_, engine_);
    planCaseConstants(compiled,evaluator_);
    evaluator_.bindScalarFunctions(compiled.get(), engine_);
    for (const auto& site : sites) originalSites_[site.first] = site.second;
    for (const auto& site : sites)
        if (const auto* quantified=dynamic_cast<const QuantifiedComparisonExpr*>(site.second);
            quantified && quantified->right && quantified->right->preparedSubquery)
            quantifiedSites_.insert(quantified);
    compiled_.emplace(expression,std::move(compiled));
    prepared_.insert(expression);
}

void PreparedQueryExecution::planExpressionConstants(Expr* expression) {
    std::set<const Stmt*> visited;
    planExpressionConstants(expression, visited);
}

void PreparedQueryExecution::planStatementConstants(const Stmt* statement) {
    std::set<const Stmt*> visited;
    planStatementConstants(statement, visited);
}

void PreparedQueryExecution::planExpressionConstants(Expr* expression,
    std::set<const Stmt*>& visited) {
    if (!expression) return;
    if (!owners_.count(expression))
        throw DbError("XX000", "constant planning requires a genuine prepared expression owner");
    if (!compiled_.count(expression)) {
        ExprEvaluator::analyzeExplicitResultCollation(expression);
        std::map<const Expr*, const Expr*> sites;
        auto compiled = copyExpression(expression, sites);
        ExprHelper::prepareArrayTypes(compiled.get(), {}, database_, engine_);
        for (const auto& site : sites) originalSites_[site.first] = site.second;
        for (const auto& site : sites)
            if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(site.second);
                quantified && quantified->right && quantified->right->preparedSubquery)
                quantifiedSites_.insert(quantified);
        compiled_.emplace(expression, std::move(compiled));
    }
    planCompiledConstants(compiled_.at(expression), visited);
}

void PreparedQueryExecution::planCompiledConstants(ExprPtr& expression,
    std::set<const Stmt*>& visited) {
    if (!expression) return;
    // Simplify demand first. A child in a discarded CASE/boolean arm must
    // not be planned, although the whole binder already checked its names
    // and analysis-phase input conversions. Neither simplification nor this
    // visitor obtains a child's result or invokes a registered routine.
    const ConstantPlanningRoles roles{
        [&](const FunctionCallExpr* call) {
            if (!call->schema.empty() || SQLParser::toLower(call->funcName) != "coalesce" ||
                !call->namedArgs.empty() || call->hasOver || call->filter || call->distinct ||
                !call->orderBy.empty() || !evaluator_.hasScalarFunction(call,engine_)) return false;
            FunctionCallExpr builtin; builtin.funcName = "coalesce";
            for (size_t i=0;i<call->args.size();++i) {
                auto null = std::make_unique<LiteralExpr>(); null->value = "NULL";
                builtin.args.push_back(std::move(null));
            }
            return evaluator_.scalarFunctionIdentity(call,engine_) ==
                evaluator_.scalarFunctionIdentity(&builtin,engine_);
        },
        [&](const FunctionCallExpr* call) {
            return ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedResultType(call,{},database_,engine_));
        },
        [](const FunctionCallExpr* call) {
            // Exact parser grammar roles, not a quoted/qualified user routine
            // whose decoded name happens to look like a SQL operator.
            static const std::set<std::string> names={"LIKE ESCAPE","NOT LIKE ESCAPE",
                "ILIKE ESCAPE","NOT ILIKE ESCAPE","SIMILAR TO ESCAPE","NOT SIMILAR TO ESCAPE"};
            return call->schema.empty() && names.count(call->funcName) && call->args.size()==3 &&
                call->namedArgs.empty() && !call->hasOver && !call->filter && !call->distinct && call->orderBy.empty();
        }
    };
    simplifyCaseConstants(expression, evaluator_, &roles);
    if (expression->preparedSubquery) {
        planStatementConstants(expression->preparedSubquery.get(), visited);
        return;
    }
    const auto value = [&](ExprPtr& child) { planCompiledConstants(child, visited); };
    const auto window = [&](WindowDef& definition) {
        for (auto& item : definition.partitionBy) value(item);
        for (auto& item : definition.orderBy) value(item.first);
        value(definition.frameStart); value(definition.frameEnd);
    };
    if (auto* node = dynamic_cast<UnaryOpExpr*>(expression.get())) value(node->operand);
    else if (auto* node = dynamic_cast<CastExpr*>(expression.get())) value(node->operand);
    else if (auto* node = dynamic_cast<BinaryOpExpr*>(expression.get())) {
        value(node->left);
        if (node->op != "::") value(node->right); // RHS is a type grammar role.
    } else if (auto* node = dynamic_cast<QuantifiedComparisonExpr*>(expression.get())) {
        value(node->left); value(node->right);
    } else if (auto* node = dynamic_cast<CaseExpr*>(expression.get())) {
        value(node->switchExpr);
        for (auto& arm : node->whenClauses) { value(arm.first); value(arm.second); }
        value(node->elseExpr);
    } else if (auto* node = dynamic_cast<FunctionCallExpr*>(expression.get())) {
        for (auto& item : node->args) value(item);
        for (auto& item : node->namedArgs) value(item.value);
        value(node->filter); window(node->over);
    } else if (auto* node = dynamic_cast<ArrayExpr*>(expression.get())) {
        for (auto& item : node->elements) value(item);
    } else if (auto* node = dynamic_cast<RowExpr*>(expression.get())) {
        for (auto& item : node->elements) value(item);
    }
}

void PreparedQueryExecution::planStatementConstants(const Stmt* statement,
    std::set<const Stmt*>& visited, const std::set<size_t>* outputDemand) {
    if (!statement) return;
    if (!parents_.count(statement))
        throw DbError("XX000", "constant planning requires a genuine prepared statement owner");
    const bool firstVisit = visited.insert(statement).second;
    if (!firstVisit && !dynamic_cast<const SelectStmt*>(statement)) return;
    std::set<size_t> plannedOutputs;
    const auto value = [&](const ExprPtr& expression) {
        planExpressionConstants(expression.get(), visited);
    };
    const auto items = [&](const std::vector<SelectItem>& list) {
        for (const auto& item : list) value(item.expr);
    };
    const auto window = [&](const WindowDef& definition) {
        for (const auto& item : definition.partitionBy) value(item);
        for (const auto& item : definition.orderBy) value(item.first);
        value(definition.frameStart); value(definition.frameEnd);
    };
    const auto writers = [&](const std::vector<SelectStmt::CTE>& ctes) {
        for (const auto& cte : ctes)
            if (cte.query && (cte.query->command == SqlCommand::Insert ||
                cte.query->command == SqlCommand::Update || cte.query->command == SqlCommand::Delete))
                planStatementConstants(cte.query.get(), visited);
    };
    // A logical read CTE is reachable by its bound statement identity, not
    // text/name matching. Unreferenced SELECT CTEs receive analysis checks
    // from the binder but are not planned/executed merely because they exist.
    const auto retainedTarget = [&](const Expr* expression) {
        bool retained = false;
        visitStructuredValue(expression, [&](const Expr* value) {
            if (const auto* call = dynamic_cast<const FunctionCallExpr*>(value)) {
                if (call->setReturning || (evaluator_.hasScalarFunction(call, engine_) &&
                    evaluator_.scalarFunctionVolatility(call, engine_) == 'v')) retained = true;
            }
        });
        return retained;
    };
    // Top-level nonvolatile ANY tests with a real local source reference
    // become a semi-join qualification before boolean simplification. Its
    // child is consequently planned even if another AND arm is constant
    // false. ALL, OR/CASE arms, constant tests and volatile tests remain
    // ordinary lazy SubPlans. Follow true query owners for references inside
    // scalar test operands; a child-local or higher-ancestor Var is not a
    // reference to this qualification's local relation set.
    const auto localNonvolatileTest = [&](const Expr* expression) {
        bool local = false, volatileTest = false;
        std::set<const Expr*> seen;
        std::function<void(const Expr*)> inspect = [&](const Expr* root) {
            visitStructuredValue(root, [&](const Expr* value) {
                if (!seen.insert(value).second) return;
                if (const auto* column = dynamic_cast<const ColumnRefExpr*>(value);
                    column && column->binding && sourceRange(column->binding->sourceOrdinal).owner == statement)
                    local = true;
                if (const auto* call = dynamic_cast<const FunctionCallExpr*>(value);
                    call && evaluator_.hasScalarFunction(call, engine_) &&
                    evaluator_.scalarFunctionVolatility(call, engine_) == 'v')
                    volatileTest = true;
                if (value->preparedSubquery) {
                    const Stmt* child = value->preparedSubquery.get();
                    for (const auto& owned : owners_)
                        if (isAncestor(child, owned.second)) inspect(owned.first);
                    // Expanded stars may carry a genuine correlated source
                    // binding in projection metadata rather than their '*'
                    // syntax node. Do not rediscover that binding by name.
                    for (const auto& projection : query_->projectionBindings)
                        if (isAncestor(child, projection.first))
                            for (const auto& output : projection.second)
                                if (output.column && sourceRange(output.column->sourceOrdinal).owner == statement)
                                    local = true;
                }
            });
        };
        inspect(expression);
        return local && !volatileTest;
    };
    const auto qualification = [&](const ExprPtr& expression) {
        std::function<void(const Expr*)> pulledChildren = [&](const Expr* root) {
            if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(root);
                binary && SQLParser::toLower(binary->op) == "and") {
                pulledChildren(binary->left.get()); pulledChildren(binary->right.get());
            } else if (const auto* any = dynamic_cast<const QuantifiedComparisonExpr*>(root);
                any && any->quantifier == QuantifiedComparisonExpr::Quantifier::Any &&
                any->right && any->right->preparedSubquery && localNonvolatileTest(any->left.get())) {
                planStatementConstants(any->right->preparedSubquery.get(), visited);
            }
        };
        pulledChildren(expression.get());
        value(expression);
    };
    const auto sourceDemand = [&](const PreparedQuery::SourceRange& source) {
        std::set<size_t> columns;
        // USING/NATURAL equality consumes its input key columns even when
        // the merged output is not projected. These canonical names belong
        // to this exact bound occurrence, not a global identifier cache.
        if (source.source)
            for (size_t i=0;i<source.columns.size();++i)
                if (source.hiddenUnqualified.count(source.columns[i].name)) columns.insert(i);
        if (const auto* select = dynamic_cast<const SelectStmt*>(statement)) {
            const auto projected = query_->projectionBindings.find(select);
            if (projected != query_->projectionBindings.end())
                for (size_t i : plannedOutputs) {
                    const auto& output = projected->second.at(i);
                    if (output.column && output.column->sourceOrdinal == source.ordinal)
                        columns.insert(output.column->columnOrdinal);
                }
        }
        for (const auto& compiled : compiled_) {
            if (!isAncestor(statement,owners_.at(compiled.first))) continue;
            visitStructuredValue(compiled.second.get(), [&](const Expr* value) {
                if (const auto* column = dynamic_cast<const ColumnRefExpr*>(value)) {
                    if (column->binding && column->binding->sourceOrdinal == source.ordinal)
                        columns.insert(column->binding->columnOrdinal);
                }
            });
        }
        return columns;
    };
    const auto cteBarrier = [&](const Stmt* child) {
        for (const auto& parent : parents_) {
            const std::vector<SelectStmt::CTE>* definitions = nullptr;
            if (const auto* select = dynamic_cast<const SelectStmt*>(parent.first)) definitions = &select->ctes;
            else if (const auto* with = dynamic_cast<const WithStmt*>(parent.first)) definitions = &with->ctes;
            if (!definitions) continue;
            for (const auto& cte : *definitions) if (cte.query.get() == child) {
                if (cte.recursive || (cte.materializationSpecified && cte.materialized)) return true;
                const auto references = std::count_if(query_->sourceRanges.begin(),query_->sourceRanges.end(),
                    [&](const auto& source) { return source.cteStatement == child; });
                if (references > 1 && !cte.materializationSpecified) return true;
                if (const auto* select = dynamic_cast<const SelectStmt*>(child))
                    for (const auto& target : select->selectList) if (retainedTarget(target.expr.get())) return true;
            }
        }
        return false;
    };
    const auto sources = [&] {
        for (const auto& range : query_->sourceRanges) {
            if (range.owner != statement || !range.cteStatement) continue;
            const auto demand = sourceDemand(range);
            planStatementConstants(range.cteStatement, visited, cteBarrier(range.cteStatement) ? nullptr : &demand);
        }
    };
    std::function<void(const FromItem*)> from = [&](const FromItem* item) {
        if (!item) return;
        if (item->subquery) {
            const auto range = std::find_if(query_->sourceRanges.begin(),query_->sourceRanges.end(),
                [&](const auto& source) { return source.owner == statement && source.source == item; });
            if (range == query_->sourceRanges.end())
                throw DbError("XX000", "derived planning source has no bound occurrence");
            const auto demand = sourceDemand(*range);
            planStatementConstants(item->subquery.get(), visited, &demand);
        }
        from(item->left.get()); from(item->right.get());
    };
    std::function<void(const FromItem*)> qualifications = [&](const FromItem* source) {
        if (!source) return;
        qualifications(source->left.get()); qualifications(source->right.get()); value(source->joinCondition);
    };
    if (const auto* node = dynamic_cast<const WithStmt*>(statement)) {
        writers(node->ctes);
        planStatementConstants(node->statement.get(), visited);
    } else if (const auto* node = dynamic_cast<const SelectStmt*>(statement)) {
        writers(node->ctes);
        planStatementConstants(node->setOpLhs.get(), visited);
        planStatementConstants(node->setOpRhs.get(), visited);
        // The caller may discard unused derived/default-inline CTE outputs.
        // DISTINCT/set operations retain their complete row identity; ORDER
        // aliases/ordinals and volatile/SRF targets retain their actual slots.
        auto demanded = outputDemand ? *outputDemand : std::set<size_t>{};
        const bool all = !outputDemand || node->distinct || !node->distinctOn.empty() || node->setOp != SetOp::None;
        const auto output = query_->statementOutputs.find(statement);
        for (const auto& order : node->orderBy) {
            if (const auto* column = dynamic_cast<const ColumnRefExpr*>(order.expr.get()); column && !column->binding &&
                column->table.empty() && column->schema.empty() && output != query_->statementOutputs.end()) {
                for (size_t i = 0; i < output->second.size(); ++i)
                    if (output->second[i].name == column->column) demanded.insert(i);
            } else if (const auto* literal = dynamic_cast<const LiteralExpr*>(order.expr.get()); literal &&
                !literal->value.empty() && std::all_of(literal->value.begin(), literal->value.end(),
                    [](unsigned char c) { return c >= '0' && c <= '9'; })) {
                size_t ordinal = 0;
                for (char c : literal->value) { if (ordinal > node->selectList.size()) break; ordinal = ordinal * 10 + size_t(c-'0'); }
                if (ordinal) demanded.insert(ordinal - 1);
            }
        }
        const auto projected = query_->projectionBindings.find(node);
        if (projected == query_->projectionBindings.end() && !node->selectList.empty())
            throw DbError("XX000", "SELECT constant planning requires bound projection ordinals");
        std::set<const Expr*> usedExpressions;
        auto& previousOutputs = plannedOutputOrdinals_[node];
        for (const auto& target : node->selectList) {
            const bool forced = all || retainedTarget(target.expr.get());
            if (projected != query_->projectionBindings.end())
                for (size_t i=0;i<projected->second.size();++i)
                    if (projected->second[i].expression == target.expr.get() && (forced || demanded.count(i) || previousOutputs.count(i))) {
                        usedExpressions.insert(target.expr.get()); plannedOutputs.insert(i);
                    }
        }
        const bool additional = std::any_of(plannedOutputs.begin(),plannedOutputs.end(),
            [&](size_t ordinal) { return !previousOutputs.count(ordinal); });
        if (!firstVisit && !additional) return;
        previousOutputs.insert(plannedOutputs.begin(),plannedOutputs.end());
        for (const auto& target : node->selectList) {
            if (usedExpressions.count(target.expr.get())) value(target.expr);
        }
        qualification(node->whereClause);
        for (const auto& item : node->groupBy) value(item);
        for (const auto& element : node->groupByElems)
            for (const auto& item : element.exprs) value(item);
        value(node->having);
        for (const auto& item : node->distinctOn) value(item);
        for (const auto& item : node->orderBy) value(item.expr);
        for (const auto& row : node->valuesRows) for (const auto& item : row) value(item);
        for (const auto& definition : node->windowDefs) window(definition);
        qualifications(node->fromClause.get()); sources(); from(node->fromClause.get());
    } else if (const auto* node = dynamic_cast<const InsertStmt*>(statement)) {
        for (const auto& row : node->values) for (const auto& item : row) value(item);
        planStatementConstants(node->selectSource.get(), visited);
        for (const auto& item : node->conflictUpdateSet) value(item.second);
        value(node->conflictWhere); items(node->returning);
        sources();
    } else if (const auto* node = dynamic_cast<const UpdateStmt*>(statement)) {
        for (const auto& item : node->setClauses) value(item.second);
        qualification(node->whereClause); items(node->returning);
        qualifications(node->fromClause.get()); sources(); from(node->fromClause.get());
    } else if (const auto* node = dynamic_cast<const DeleteStmt*>(statement)) {
        qualification(node->whereClause); items(node->returning);
        qualifications(node->usingClause.get()); sources(); from(node->usingClause.get());
    } else if (const auto* node = dynamic_cast<const ExplainStmt*>(statement)) {
        planStatementConstants(node->query.get(), visited);
    } else if (const auto* node = dynamic_cast<const CreateTableStmt*>(statement)) {
        planStatementConstants(node->preparedAsQuery.get(), visited);
        for (const auto& column : node->columns) value(column.defaultValue);
    } else throw DbError("0A000", "constant planning requires a supported structured query statement");
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

void PreparedQueryExecution::setChildCursorFactory(PreparedChildCursorFactory factory,
    bool ownsScalarChildren) {
    closeChildCursors();
    quantified_.clear();
    memo_.clear();
    childCursorFactory_ = std::move(factory);
    cursorOwnsScalarChildren_ = ownsScalarChildren;
}

ExprValue PreparedQueryExecution::evaluate(const Expr* expression, const RowContext& row) const {
    if (!expression || !prepared_.count(expression))
        throw DbError("XX000", "prepared expression was not registered before execution");
    try { return evaluator_.eval(compiled_.at(expression).get(), row); }
    catch (...) {
        // A failed child may restore storage caches or abort the statement.
        // Previously successful initplans cannot leak past that boundary.
        memo_.clear();
        const auto failure=std::current_exception();
        try {const_cast<PreparedQueryExecution*>(this)->closeChildCursors();} catch (...) {}
        quantified_.clear();
        std::rethrow_exception(failure);
    }
}

PreparedQueryExecution::QuantifiedState& PreparedQueryExecution::quantifiedState(
    const QuantifiedComparisonExpr* original,const RowContext& row) const {
    const auto* child=original->right.get();
    const auto found=children_.find(child);
    if (found==children_.end() || !original->comparison || !childCursorFactory_)
        throw DbError("0A000","quantified SQL requires its original prepared child cursor");
    auto [position,inserted]=quantified_.try_emplace(child);
    auto& state=position->second;
    if (inserted || !state.cursor) {
        state.cursor=childCursorFactory_(child->preparedSubquery.get(),row);
        if (!state.cursor) throw DbError("XX000","quantified factory returned no child cursor");
        const auto output=query_->statementOutputs.find(child->preparedSubquery.get());
        const auto& descriptor=state.cursor->descriptor();
        if (output==query_->statementOutputs.end() || output->second.size()!=1 || descriptor.size()!=1 ||
            ExprHelper::canonicalResultTypeName(descriptor.front().type)!=
                ExprHelper::canonicalResultTypeName(output->second.front().type))
            throw DbError("XX000","quantified cursor lost its declared descriptor");
        if (!found->second.correlations.empty() && !state.cursor->supportsRestart())
            throw DbError("0A000","correlated query source requires a parameterized cursor provider");
    }
    return state;
}

void PreparedQueryExecution::prepareChildCursors() {
    // A cloned site registry describes every original expression, including
    // branches removed by pure root planning. Only actual prepared runtime
    // roots may acquire a child graph: constructing a discarded child's CASE
    // target could otherwise raise an error the root deliberately pruned.
    std::set<const QuantifiedComparisonExpr*> reached;
    for(const auto* root:prepared_)
        visitStructuredValue(compiled_.at(root).get(),[&](const Expr* value) {
            const auto* quantified=dynamic_cast<const QuantifiedComparisonExpr*>(value);
            if(!quantified || !quantified->right || !quantified->right->preparedSubquery)return;
            const auto mapped=originalSites_.find(quantified);
            const auto* original=mapped==originalSites_.end()?nullptr:
                dynamic_cast<const QuantifiedComparisonExpr*>(mapped->second);
            if(!original || !quantifiedSites_.count(original))
                throw DbError("XX000","prepared quantified expression lost its original site");
            reached.insert(original);
        });
    for(const auto* site:reached)(void)quantifiedState(site,context());
}
void PreparedQueryExecution::closeChildCursors() {
    std::exception_ptr failure;
    for (auto& site:quantified_) if (site.second.cursor)
        try {site.second.cursor->close();}catch(...){if(!failure)failure=std::current_exception();}
    if(failure)std::rethrow_exception(failure);
}
std::vector<Operator*> PreparedQueryExecution::childPlans(const Expr* scope) const {
    std::set<const Expr*> sites;
    std::function<void(const Expr*)> gather=[&](const Expr* node) {
        if(!node || node->preparedSubquery)return;
        if(const auto* quantified=dynamic_cast<const QuantifiedComparisonExpr*>(node)) {
            if(quantified->right && quantified->right->preparedSubquery)sites.insert(quantified->right.get());
            gather(quantified->left.get());gather(quantified->right.get());
        } else if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(node)) {gather(binary->left.get());gather(binary->right.get());}
        else if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(node))gather(unary->operand.get());
        else if(const auto* cast=dynamic_cast<const CastExpr*>(node))gather(cast->operand.get());
        else if(const auto* conditional=dynamic_cast<const CaseExpr*>(node)) {
            gather(conditional->switchExpr.get());gather(conditional->elseExpr.get());
            for(const auto& arm:conditional->whenClauses){gather(arm.first.get());gather(arm.second.get());}
        } else if(const auto* call=dynamic_cast<const FunctionCallExpr*>(node)) {
            for(const auto& arg:call->args)gather(arg.get());for(const auto& arg:call->namedArgs)gather(arg.value.get());
        } else if(const auto* array=dynamic_cast<const ArrayExpr*>(node))for(const auto& value:array->elements)gather(value.get());
    };
    gather(scope);
    std::vector<Operator*> plans;
    for (const auto& site:quantified_) if((!scope || sites.count(site.first)) && site.second.cursor && site.second.cursor->plan())
        plans.push_back(site.second.cursor->plan());
    return plans;
}

ExprValue PreparedQueryExecution::executeQuantified(const QuantifiedComparisonExpr* compiled,
    const RowContext& row) const {
    const auto mapped=originalSites_.find(compiled);
    const auto* original=mapped==originalSites_.end()?nullptr:dynamic_cast<const QuantifiedComparisonExpr*>(mapped->second);
    if(!original || !compiled->comparison)throw DbError("XX000","quantified expression has no original prepared site");
    const auto* child=original->right.get();
    const auto description=children_.find(child);
    if(description==children_.end())throw DbError("XX000","quantified child belongs to another execution");
    auto& state=quantifiedState(original,row);
    const bool correlated=!description->second.correlations.empty();
    if(correlated) {
        state.cursor->close();state.cursor->restart(row);
        state.values.clear();state.hash.clear();state.eof=false;state.hasNull=false;state.hashBuilt=false;
    }
    const auto& binding=*compiled->comparison;
    const bool all=compiled->quantifier==QuantifiedComparisonExpr::Quantifier::All;
    const auto at=[&](size_t ordinal)->const ExprValue* {
        if(ordinal<state.values.size())return &state.values[ordinal];
        if(state.eof)return nullptr;
        std::vector<ExprValue> cells;
        if(!state.cursor->next(cells)) {state.cursor->close();state.eof=true;return nullptr;}
        if(cells.size()!=1 || ExprHelper::canonicalResultTypeName(cells.front().typeName)!=
            ExprHelper::canonicalResultTypeName(state.cursor->descriptor().front().type))
            throw DbError("XX000","quantified cursor lost its typed output cell");
        state.values.push_back(evaluator_.coerceComparison(binding,cells.front(),false));
        return &state.values.back();
    };
    ExprValue answer("boolean",all?"t":"f");
    if(!all && !correlated && binding.strict && binding.hashable) {
        if(!state.hashBuilt) {
            for(size_t i=0;;++i) {
                const auto* value=at(i);if(!value)break;
                if(value->isNull)state.hasNull=true;
                else state.hash.emplace(ExprEvaluator::comparisonHashKey(binding,*value,false),i);
            }
            state.hashBuilt=true;
        }
        // Empty hashed SQL RHS also skips the left expression altogether.
        if(state.values.empty())return answer;
        const auto left=evaluator_.coerceComparison(binding,evaluator_.eval(compiled->left.get(),row),true);
        if(left.isNull)return ExprValue("boolean","",true);
        const auto range=state.hash.equal_range(ExprEvaluator::comparisonHashKey(binding,left,true));
        for(auto item=range.first;item!=range.second;++item) {
            const auto match=evaluator_.comparePrepared(binding,left,state.values[item->second]);
            if(!match.isNull && match.asBool())return ExprValue("boolean","t");
        }
        // The resolved strict builtin comparator remains authoritative for
        // mixed representations. This fallback never reexecutes a RHS row.
        for(const auto& value:state.values)if(!value.isNull && evaluator_.comparePrepared(binding,left,value).asBool())
            return ExprValue("boolean","t");
        return state.hasNull?ExprValue("boolean","",true):answer;
    }
    bool unknown=false;
    for(size_t i=0;;++i) {
        const auto* right=at(i);if(!right)break;
        // Scan-mode PostgreSQL evaluates its left test expression per RHS
        // tuple, including cached tuples; an empty child never evaluates it.
        const auto left=evaluator_.eval(compiled->left.get(),row);
        const auto truth=evaluator_.comparePrepared(binding,left,*right);
        if(truth.isNull)unknown=true;
        else if(truth.asBool()!=all) {answer=ExprValue("boolean",all?"f":"t");unknown=false;break;}
    }
    if(correlated)state.cursor->close();
    return unknown?ExprValue("boolean","",true):answer;
}

ExprValue PreparedQueryExecution::executeChild(const Expr* expression, const RowContext& row) const {
    const auto found = children_.find(expression);
    if (found == children_.end()) throw DbError("XX000", "scalar child belongs to another execution");
    const Child& child = found->second;
    if (child.correlations.empty()) {
        const auto cached = memo_.find(expression);
        if (cached != memo_.end()) return cached->second;
    }
    if (childCursorFactory_ && (cursorOwnsScalarChildren_ ||
        (!queryExecutor_ && !engine_->hasPlpgsqlQueryExecutor()))) {
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
    else if (const auto* node = dynamic_cast<const QuantifiedComparisonExpr*>(expression)) { value(node->left); value(node->right); }
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
