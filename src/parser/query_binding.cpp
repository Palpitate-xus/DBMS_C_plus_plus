#include "query_binding.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <algorithm>
#include <map>
#include <set>

namespace dbms {
namespace {
std::string identifier(const std::string& input) {
    CatalogManager::QualifiedName name;
    // A single quoted identifier containing '.' is one component.
    if (!CatalogManager::parseQualifiedName(input, name, true) || !name.schema.empty())
        throw DbError("42601", "invalid identifier: " + input);
    return name.name;
}
struct Range {
    std::string schema, name;
    QueryRowDescriptor columns;
    std::set<std::string> hiddenUnqualified;
    size_t occurrence = 0;
    bool mergedUsing = false;
};
using Namespace = std::vector<Range>;
using Ctes = std::map<std::string, QueryRowDescriptor>;

class Binder {
public:
    PreparedQuery result;
    const std::vector<QueryBindingDatum>& datums;
    const QueryBindingMetadata& metadata;
    std::map<std::string, size_t> slots;
    size_t depth = 0;
    size_t nextSourceOccurrence = 0;
    std::vector<const Stmt*> statementOwners;
    std::vector<const Ctes*> cteScopes;
    Binder(const std::string& sql, const std::vector<QueryBindingDatum>& values,
           const QueryBindingMetadata& descriptions) : datums(values), metadata(descriptions) {
        result.source = sql;
    }

    Range registerSource(Range range, const FromItem* source = nullptr) {
        range.occurrence = nextSourceOccurrence++;
        result.sourceRanges.push_back({range.occurrence,
            statementOwners.empty() ? nullptr : statementOwners.back(), source,
            range.schema, range.name, range.columns, range.mergedUsing});
        return range;
    }

    std::string parameter(ExprPtr& node, const QueryBindingDatum& datum) {
        if (node->sourceBegin == std::string::npos || node->sourceEnd > result.source.size())
            throw DbError("XX000", "parameter reference has no source provenance");
        auto [position, inserted] = slots.emplace(datum.identity, result.parameters.size());
        if (inserted) result.parameters.emplace_back(datum.type,
            datum.value.value_or(""), !datum.value.has_value());
        auto bound = std::make_unique<ParameterExpr>();
        bound->slot = position->second; bound->declaredType = datum.type;
        bound->sourceBegin = node->sourceBegin; bound->sourceEnd = node->sourceEnd;
        result.uses.push_back({bound->sourceBegin, bound->sourceEnd, bound->slot});
        node = std::move(bound); return datum.type;
    }

    std::string expression(ExprPtr& node, const std::vector<Namespace>& scopes) {
        if (!node) return "unknown";
        switch (node->type) {
        case ExprType::Parameter: {
            const auto* value = static_cast<ParameterExpr*>(node.get());
            if (value->declaredType.empty()) {
                for (const auto& datum : datums) if (datum.position == value->slot + 1)
                    return parameter(node, datum);
                throw DbError("42P02", "unbound parameter " + value->toString());
            }
            if (value->slot >= result.parameters.size()) throw DbError("42P02", "invalid prepared parameter");
            return value->declaredType;
        }
        case ExprType::ColumnRef: {
            auto* column = static_cast<ColumnRefExpr*>(node.get());
            if (column->column == "*") return "record";
            const QueryBindingDatum* datum = nullptr;
            for (const auto& candidate : datums) {
                if (candidate.name != column->column) continue;
                const bool qualified = !column->table.empty() || !column->schema.empty();
                if ((!qualified && candidate.visible) ||
                    (column->schema.empty() && !column->table.empty() &&
                     std::find(candidate.qualifiers.begin(), candidate.qualifiers.end(),
                               column->table) != candidate.qualifiers.end())) {
                    datum = &candidate;
                    break; // frames arrive innermost first
                }
            }
            size_t matches = 0;
            bool rangeFound = false;
            std::string sourceType;
            QueryColumnBinding resolved;
            for (size_t scopeDepth = 0; scopeDepth < scopes.size(); ++scopeDepth) {
                const auto& scope = scopes[scopeDepth];
                for (const auto& range : scope) {
                    if (!column->table.empty() && column->table != range.name) continue;
                    if (!column->schema.empty() && column->schema != range.schema) continue;
                    if (!column->table.empty()) rangeFound = true;
                    for (size_t ordinal = 0; ordinal < range.columns.size(); ++ordinal) {
                        const auto& field = range.columns[ordinal];
                        if (field.name != column->column) continue;
                        if (column->table.empty() && range.hiddenUnqualified.count(field.name)) continue;
                        ++matches; sourceType = field.type;
                        resolved = {scopeDepth, range.occurrence, ordinal, field.type, range.mergedUsing};
                    }
                }
                if (matches || rangeFound) break; // nearest SQL level wins
            }
            if (matches > 1 || (matches && datum))
                throw DbError("42702", "column reference \"" + column->toString() + "\" is ambiguous");
            if (matches) {
                column->binding = std::move(resolved);
                return sourceType;
            }
            if (datum) {
                return parameter(node, *datum);
            }
            if (!column->table.empty() && !rangeFound)
                throw DbError("42P01", "missing FROM-clause entry for table \"" + column->table + "\"");
            throw DbError("42703", "column \"" + column->toString() + "\" does not exist");
        }
        case ExprType::Literal: {
            auto* literal = static_cast<LiteralExpr*>(node.get());
            if (!literal->typeName.empty()) return literal->typeName;
            if (literal->value == "*") return "record";
            const auto tokens = SQLParser::tokenize(literal->value);
            if (tokens.empty()) return "unknown";
            if (tokens.front() == "(" && tokens.size() > 1 &&
                (SQLParser::toLower(tokens[1]) == "select" || SQLParser::toLower(tokens[1]) == "with"))
                return subquery(*literal, scopes);
            if (tokens.size() != 1)
                throw DbError("0A000", "opaque expression requires structured preparation");
            const auto low = SQLParser::toLower(tokens.front());
            if (low == "true" || low == "false") return "boolean";
            if (low == "null" || tokens.front().front() == '\'') return "unknown";
            return tokens.front().find_first_of(".eE") == std::string::npos ? "integer" : "numeric";
        }
        case ExprType::CastExpr: {
            auto* cast = static_cast<CastExpr*>(node.get());
            expression(cast->operand, scopes); return cast->typeName;
        }
        case ExprType::UnaryOp: {
            auto* unary = static_cast<UnaryOpExpr*>(node.get());
            const auto type = expression(unary->operand, scopes);
            const auto op = SQLParser::toLower(unary->op);
            return op == "not" || op.rfind("is ", 0) == 0 ? "boolean" : type;
        }
        case ExprType::BinaryOp: {
            auto* binary = static_cast<BinaryOpExpr*>(node.get());
            const auto left = expression(binary->left, scopes);
            expression(binary->right, scopes);
            static const std::set<std::string> predicates = {"=", "<>", "!=", "<", ">", "<=", ">=",
                "AND", "OR", "LIKE", "ILIKE", "IN", "NOT IN", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"};
            return predicates.count(binary->op) ? "boolean" : left;
        }
        case ExprType::FunctionCall: {
            auto* call = static_cast<FunctionCallExpr*>(node.get());
            std::string first = "unknown";
            for (size_t i = 0; i < call->args.size(); ++i) {
                if (i == 0 && call->schema.empty() && SQLParser::toLower(call->funcName) == "extract") {
                    if (const auto* field = dynamic_cast<ColumnRefExpr*>(call->args[i].get())) {
                        if (!field->table.empty() || !field->schema.empty())
                            throw DbError("42601", "EXTRACT requires an unqualified field name");
                        continue; // grammar field, not a value name
                    }
                }
                if (call->schema.empty() && SQLParser::toLower(call->funcName) == "exists") {
                    if (auto* child = dynamic_cast<LiteralExpr*>(call->args[i].get())) {
                        subquery(*child, scopes, false); continue;
                    }
                }
                const auto type = expression(call->args[i], scopes); if (i == 0) first = type;
            }
            for (auto& arg : call->namedArgs) expression(arg.value, scopes);
            expression(call->filter, scopes); window(call->over, scopes);
            const auto type = metadata.functionType ? metadata.functionType(call) : std::string();
            return type.empty() ? first : type;
        }
        case ExprType::CaseExpr: {
            auto* conditional = static_cast<CaseExpr*>(node.get());
            expression(conditional->switchExpr, scopes);
            std::string type = expression(conditional->elseExpr, scopes);
            for (auto& clause : conditional->whenClauses) {
                expression(clause.first, scopes); const auto arm = expression(clause.second, scopes);
                if (type == "unknown") type = arm;
            }
            return type;
        }
        case ExprType::ArrayExpr:
            for (auto& element : static_cast<ArrayExpr*>(node.get())->elements) expression(element, scopes);
            return "array";
        case ExprType::RowExpr:
            for (auto& element : static_cast<RowExpr*>(node.get())->elements) expression(element, scopes);
            return "record";
        default: throw DbError("0A000", "expression requires structured preparation");
        }
    }

    // Historical scalar/EXISTS nodes carry SQL text. Parse their raw source,
    // not toString(), into a real child before binding, and retain byte offsets.
    std::string subquery(LiteralExpr& literal, const std::vector<Namespace>& scopes, bool scalar = true) {
        if (literal.sourceBegin == std::string::npos || literal.sourceEnd > result.source.size())
            throw DbError("0A000", "subquery has no source provenance");
        const size_t begin = result.source.find('(', literal.sourceBegin) + 1;
        const size_t end = result.source.rfind(')', literal.sourceEnd - 1);
        if (begin == 0 || end < begin) throw DbError("42601", "invalid subquery");
        SQLParser parser;
        auto parsed = parser.parseForBinding(result.source.substr(begin, end - begin));
        if (!parsed.isValid()) throw DbError("42601", parsed.error);
        // Child coordinate rebasing is structural, not identifier matching.
        rebase(*parsed.stmt, begin);
        const auto columns = statement(*parsed.stmt, scopes, cteScopes.empty() ? Ctes{} : *cteScopes.back());
        literal.preparedSubquery = std::move(parsed.stmt);
        if (scalar && columns.size() != 1) throw DbError("42601", "subquery must return only one column");
        literal.typeName = scalar ? columns.front().type : "boolean";
        return literal.typeName;
    }
    void window(WindowDef& value, const std::vector<Namespace>& scopes) {
        for (auto& field : value.partitionBy) expression(field, scopes);
        for (auto& field : value.orderBy) expression(field.first, scopes);
        expression(value.frameStart, scopes); expression(value.frameEnd, scopes);
    }
    QueryRowDescriptor project(std::vector<SelectItem>& items, const std::vector<Namespace>& scopes) {
        QueryRowDescriptor columns;
        for (auto& item : items) {
            if (!item.expr) throw DbError("42601", "query projection has no expression");
            if (item.expr && item.expr->type == ExprType::Literal &&
                static_cast<LiteralExpr*>(item.expr.get())->value == "*") {
                if (scopes.empty()) throw DbError("42601", "SELECT * has no source");
                for (const auto& range : scopes.front())
                    for (const auto& column : range.columns)
                        if (!range.hiddenUnqualified.count(column.name)) columns.push_back(column);
                continue;
            }
            if (const auto* star = dynamic_cast<ColumnRefExpr*>(item.expr.get()); star && star->column == "*") {
                bool found = false;
                if (!scopes.empty()) for (const auto& range : scopes.front()) {
                    if (range.name != star->table || (!star->schema.empty() && range.schema != star->schema)) continue;
                    found = true; columns.insert(columns.end(), range.columns.begin(), range.columns.end());
                }
                if (!found) throw DbError("42P01", "missing FROM-clause entry for qualified star");
                continue;
            }
            std::string name = "?column?";
            const bool columnLabel = dynamic_cast<ColumnRefExpr*>(item.expr.get());
            if (!item.alias.empty()) name = identifier(item.alias);
            else if (item.expr && item.expr->type == ExprType::ColumnRef)
                name = static_cast<ColumnRefExpr*>(item.expr.get())->column;
            else if (item.expr && item.expr->type == ExprType::FunctionCall)
                if (static_cast<FunctionCallExpr*>(item.expr.get())->funcName.find(' ') == std::string::npos)
                    name = identifier(static_cast<FunctionCallExpr*>(item.expr.get())->funcName);
            const auto type = expression(item.expr, scopes);
            if (columnLabel && item.alias.empty() && item.expr->type == ExprType::Parameter) {
                if (item.sourceExpressionEnd == std::string::npos)
                    throw DbError("XX000", "projection has no source provenance");
                std::string quoted = "\"";
                for (char c : name) { quoted += c; if (c == '"') quoted += c; }
                quoted += '"';
                item.alias = quoted;
                result.projectionAliases.emplace_back(item.sourceExpressionEnd, quoted);
            }
            columns.push_back({name, type});
        }
        return columns;
    }
    Range relation(const std::string& source, const std::string& alias, const Ctes& ctes) {
        CatalogManager::QualifiedName name;
        if (!CatalogManager::parseQualifiedName(source, name, true))
            throw DbError("42601", "invalid relation name");
        if (name.schema.empty()) {
            const auto cte = ctes.find(name.name);
            if (cte != ctes.end()) return {"", alias.empty() ? name.name : identifier(alias), cte->second};
        }
        if (!metadata.relation) throw DbError("42P01", "relation does not exist: " + source);
        const auto description = metadata.relation(source);
        return {alias.empty() ? description.schema : "", alias.empty() ? description.name : identifier(alias), description.columns};
    }
    Namespace from(FromItem* item, const std::vector<Namespace>& outer, const Ctes& ctes) {
        if (!item) return {};
        if (item->type == FromItem::Type::Table)
            return {registerSource(relation(item->tableName, item->alias, ctes), item)};
        if (item->type == FromItem::Type::Subquery) {
            if (!item->subquery) throw DbError("42601", "derived query is missing");
            auto derivedOuter = outer;
            // A non-LATERAL source cannot see this SQL level's siblings,
            // but its lexical level still counts for ancestor provenance.
            derivedOuter.insert(derivedOuter.begin(), Namespace{});
            auto columns = statement(*item->subquery, derivedOuter, ctes);
            return {registerSource({"", item->alias.empty() ? "" : identifier(item->alias), std::move(columns)}, item)};
        }
        if (item->type != FromItem::Type::Join) throw DbError("0A000", "source requires structured preparation");
        auto left = from(item->left.get(), outer, ctes);
        auto right = from(item->right.get(), outer, ctes);
        for (const auto& a : left) for (const auto& b : right)
            if (!a.name.empty() && a.name == b.name)
                throw DbError("42712", "table name \"" + a.name + "\" specified more than once");
        std::vector<std::string> keys;
        std::set<std::string> seenKeys;
        for (const auto& spelling : item->usingCols) {
            const auto key = identifier(spelling);
            if (!seenKeys.insert(key).second)
                throw DbError("42701", "column \"" + key + "\" appears more than once in USING clause");
            keys.push_back(key);
        }
        if (SQLParser::toLower(item->joinType).rfind("natural", 0) == 0) {
            for (const auto& a : left) for (const auto& x : a.columns)
                for (const auto& b : right) for (const auto& y : b.columns)
                    if (!a.hiddenUnqualified.count(x.name) && !b.hiddenUnqualified.count(y.name) &&
                        x.name == y.name && seenKeys.insert(x.name).second) keys.push_back(x.name);
        }
        Range merged;
        for (const auto& key : keys) {
            size_t leftCount = 0, rightCount = 0; std::string type;
            for (auto& range : left) for (const auto& col : range.columns)
                if (col.name == key && !range.hiddenUnqualified.count(key)) { ++leftCount; type = col.type; }
            for (auto& range : right) for (const auto& col : range.columns)
                if (col.name == key && !range.hiddenUnqualified.count(key)) ++rightCount;
            if (!leftCount || !rightCount) throw DbError("42703", "column \"" + key + "\" specified in USING does not exist");
            if (leftCount > 1 || rightCount > 1) throw DbError("42702", "USING column is ambiguous");
            for (auto& range : left) range.hiddenUnqualified.insert(key);
            for (auto& range : right) range.hiddenUnqualified.insert(key);
            merged.columns.push_back({key, type});
        }
        left.insert(left.end(), right.begin(), right.end());
        auto scopes = outer; scopes.insert(scopes.begin(), left);
        expression(item->joinCondition, scopes);
        if (!merged.columns.empty()) {
            merged.mergedUsing = true;
            left.insert(left.begin(), registerSource(std::move(merged), item));
        }
        return left;
    }
    QueryRowDescriptor statement(Stmt& node, const std::vector<Namespace>& outer, Ctes ctes) {
        if (++depth > 128) throw DbError("54001", "query binding nesting limit exceeded");
        struct Depth { size_t& value; ~Depth() { --value; } } guard{depth};
        statementOwners.push_back(&node);
        struct OwnerScope { std::vector<const Stmt*>& owners; ~OwnerScope() { owners.pop_back(); } } ownerScope{statementOwners};
        cteScopes.push_back(&ctes);
        struct CteScope { std::vector<const Ctes*>& stack; ~CteScope() { stack.pop_back(); } } cteScope{cteScopes};
        if (auto* select = dynamic_cast<SelectStmt*>(&node)) {
            // WITH definitions cannot see sibling FROM ranges of this SQL
            // level. Retain its empty lexical frame for ancestor provenance.
            auto cteOuter = outer;
            cteOuter.insert(cteOuter.begin(), Namespace{});
            for (auto& cte : select->ctes) {
                if (!cte.query) throw DbError("42601", "WITH query is missing");
                const std::string name = identifier(cte.name);
                QueryRowDescriptor columns;
                auto* recursive = dynamic_cast<SelectStmt*>(cte.query.get());
                if (cte.recursive && recursive && recursive->setOp != SetOp::None) {
                    // Seed only from the non-recursive term; never execute it.
                    StmtPtr tail = std::move(recursive->setOpRhs);
                    columns = statement(*recursive, cteOuter, ctes);
                    recursive->setOpRhs = std::move(tail);
                    applyNames(columns, cte.columnNames);
                    auto inner = ctes; inner[name] = columns;
                    if (recursive->setOpRhs) {
                        auto rhs = statement(*recursive->setOpRhs, cteOuter, inner);
                        if (rhs.size() != columns.size()) throw DbError("42601", "recursive query column count mismatch");
                    }
                } else columns = statement(*cte.query, cteOuter, ctes);
                applyNames(columns, cte.columnNames); ctes[name] = std::move(columns);
            }
            if (select->setOpLhs) {
                auto columns = statement(*select->setOpLhs, outer, ctes);
                if (select->setOpRhs && statement(*select->setOpRhs, outer, ctes).size() != columns.size())
                    throw DbError("42601", "set query column count mismatch");
                return columns;
            }
            auto scopes = outer;
            scopes.insert(scopes.begin(), from(select->fromClause.get(), outer, ctes));
            auto columns = project(select->selectList, scopes);
            if (select->command == SqlCommand::Values && !select->valuesRows.empty()) {
                columns.clear();
                for (size_t i = 0; i < select->valuesRows.front().size(); ++i)
                    columns.push_back({"column" + std::to_string(i + 1), expression(select->valuesRows.front()[i], scopes)});
            }
            expression(select->whereClause, scopes);
            for (auto& value : select->distinctOn) expression(value, scopes);
            for (auto& value : select->groupBy) expression(value, scopes);
            for (auto& group : select->groupByElems) for (auto& value : group.exprs) expression(value, scopes);
            expression(select->having, scopes);
            for (auto& value : select->windowDefs) window(value, scopes);
            for (auto& order : select->orderBy) {
                const auto* ref = dynamic_cast<ColumnRefExpr*>(order.expr.get());
                const bool outputAlias = ref && ref->table.empty() && ref->schema.empty() &&
                    std::any_of(columns.begin(), columns.end(), [&](const auto& col) { return col.name == ref->column; });
                if (!outputAlias) expression(order.expr, scopes);
            }
            for (auto& row : select->valuesRows) for (auto& value : row) expression(value, scopes);
            if (select->setOpRhs && statement(*select->setOpRhs, outer, ctes).size() != columns.size())
                throw DbError("42601", "set query column count mismatch");
            return columns;
        }
        if (auto* insert = dynamic_cast<InsertStmt*>(&node)) {
            auto target = registerSource(relation(insert->tableName, "", ctes));
            for (auto& row : insert->values) for (auto& value : row) expression(value, outer);
            if (insert->selectSource) statement(*insert->selectSource, outer, ctes);
            auto scopes = outer; scopes.insert(scopes.begin(), {target});
            for (auto& value : insert->conflictUpdateSet) expression(value.second, scopes);
            expression(insert->conflictWhere, scopes);
            return project(insert->returning, scopes);
        }
        if (auto* update = dynamic_cast<UpdateStmt*>(&node)) {
            auto ranges = from(update->fromClause.get(), outer, ctes);
            ranges.insert(ranges.begin(), registerSource(relation(update->tableName, update->alias, ctes)));
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            for (auto& value : update->setClauses) expression(value.second, scopes);
            expression(update->whereClause, scopes); return project(update->returning, scopes);
        }
        if (auto* remove = dynamic_cast<DeleteStmt*>(&node)) {
            auto ranges = from(remove->usingClause.get(), outer, ctes);
            ranges.insert(ranges.begin(), registerSource(relation(remove->tableName, remove->alias, ctes)));
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            expression(remove->whereClause, scopes); return project(remove->returning, scopes);
        }
        if (auto* explain = dynamic_cast<ExplainStmt*>(&node)) {
            if (!explain->query) throw DbError("42601", "EXPLAIN query is missing");
            statement(*explain->query, outer, ctes); return {{"QUERY PLAN","text"}};
        }
        if (auto* create = dynamic_cast<CreateTableStmt*>(&node)) {
            if (create->preparedAsQuery) return statement(*create->preparedAsQuery, outer, ctes);
            for (auto& column : create->columns) expression(column.defaultValue, outer);
            return {};
        }
        throw DbError("0A000", "statement requires structured query preparation");
    }
    static void applyNames(QueryRowDescriptor& columns, const std::vector<std::string>& names) {
        if (names.size() > columns.size()) throw DbError("42P10", "query has fewer columns than column names");
        for (size_t i = 0; i < names.size(); ++i) columns[i].name = identifier(names[i]);
    }
    // Visit a separately parsed scalar child before attaching it to its
    // parent's original source coordinates. Used solely for source rebasing.
    static void rebase(Stmt& node, size_t offset);
};
} // namespace

void Binder::rebase(Stmt& root, size_t offset) {
    std::function<void(ExprPtr&)> expr;
    std::function<void(Stmt&)> stmt;
    std::function<void(FromItem*)> from;
    const auto window = [&](WindowDef& def) {
        for (auto& value : def.partitionBy) expr(value);
        for (auto& value : def.orderBy) expr(value.first);
        expr(def.frameStart); expr(def.frameEnd);
    };
    const auto items = [&](std::vector<SelectItem>& list) { for (auto& item : list) {
        expr(item.expr); if (item.sourceExpressionEnd != std::string::npos) item.sourceExpressionEnd += offset;
    } };
    expr = [&](ExprPtr& value) {
        if (!value) return;
        if (value->sourceBegin != std::string::npos) value->sourceBegin += offset;
        if (value->sourceEnd != std::string::npos) value->sourceEnd += offset;
        if (auto* e = dynamic_cast<UnaryOpExpr*>(value.get())) expr(e->operand);
        else if (auto* e = dynamic_cast<BinaryOpExpr*>(value.get())) { expr(e->left); expr(e->right); }
        else if (auto* e = dynamic_cast<CastExpr*>(value.get())) expr(e->operand);
        else if (auto* e = dynamic_cast<FunctionCallExpr*>(value.get())) {
            for (auto& arg : e->args) expr(arg);
            for (auto& arg : e->namedArgs) expr(arg.value);
            expr(e->filter); window(e->over);
        } else if (auto* e = dynamic_cast<CaseExpr*>(value.get())) {
            expr(e->switchExpr); expr(e->elseExpr);
            for (auto& arm : e->whenClauses) { expr(arm.first); expr(arm.second); }
        } else if (auto* e = dynamic_cast<ArrayExpr*>(value.get())) for (auto& element : e->elements) expr(element);
        else if (auto* e = dynamic_cast<RowExpr*>(value.get())) for (auto& element : e->elements) expr(element);
    };
    from = [&](FromItem* source) {
        if (!source) return;
        from(source->left.get()); from(source->right.get()); expr(source->joinCondition);
        if (source->subquery) stmt(*source->subquery);
    };
    stmt = [&](Stmt& statement) {
        if (auto* select = dynamic_cast<SelectStmt*>(&statement)) {
            for (auto& cte : select->ctes) if (cte.query) stmt(*cte.query);
            if (select->setOpLhs) stmt(*select->setOpLhs);
            if (select->setOpRhs) stmt(*select->setOpRhs);
            items(select->selectList); from(select->fromClause.get()); expr(select->whereClause);
            for (auto& value : select->groupBy) expr(value);
            for (auto& group : select->groupByElems) for (auto& value : group.exprs) expr(value);
            for (auto& value : select->distinctOn) expr(value);
            expr(select->having); for (auto& order : select->orderBy) expr(order.expr);
            for (auto& row : select->valuesRows) for (auto& value : row) expr(value);
            for (auto& def : select->windowDefs) window(def);
        } else if (auto* insert = dynamic_cast<InsertStmt*>(&statement)) {
            for (auto& row : insert->values) for (auto& value : row) expr(value);
            if (insert->selectSource) stmt(*insert->selectSource);
            for (auto& value : insert->conflictUpdateSet) expr(value.second);
            expr(insert->conflictWhere); items(insert->returning);
        } else if (auto* update = dynamic_cast<UpdateStmt*>(&statement)) {
            for (auto& value : update->setClauses) expr(value.second);
            from(update->fromClause.get()); expr(update->whereClause); items(update->returning);
        } else if (auto* remove = dynamic_cast<DeleteStmt*>(&statement)) {
            from(remove->usingClause.get()); expr(remove->whereClause); items(remove->returning);
        } else if (auto* explain = dynamic_cast<ExplainStmt*>(&statement)) {
            if (explain->query) stmt(*explain->query);
        } else if (auto* create = dynamic_cast<CreateTableStmt*>(&statement)) {
            if (create->preparedAsQuery) stmt(*create->preparedAsQuery);
            for (auto& column : create->columns) expr(column.defaultValue);
        }
    };
    stmt(root);
}

PreparedQuery prepareQuery(const std::string& sql, const std::vector<QueryBindingDatum>& datums,
                          const QueryBindingMetadata& metadata) {
    Binder binder(sql, datums, metadata);
    SQLParser parser; auto parsed = parser.parseForBinding(sql);
    if (!parsed.isValid()) throw DbError("42601", parsed.error);
    binder.result.output = binder.statement(*parsed.stmt, {}, {});
    binder.result.ast = std::move(parsed.stmt);
    return std::move(binder.result);
}

std::string PreparedQuery::legacySql() const {
    auto positions = uses;
    for (size_t i = 0; i < projectionAliases.size(); ++i)
        positions.push_back({projectionAliases[i].first, projectionAliases[i].first, parameters.size() + i});
    std::sort(positions.begin(), positions.end(), [](const Use& a, const Use& b) { return a.begin > b.begin; });
    std::string sql = source;
    size_t previous = source.size();
    for (const auto& use : positions) {
        if (use.begin == use.end && use.slot >= parameters.size()) {
            if (use.begin > previous) throw DbError("XX000", "invalid projection provenance");
            sql.insert(use.begin, " AS " + projectionAliases[use.slot - parameters.size()].second);
            previous = use.begin; continue;
        }
        if (use.end > previous || use.begin >= use.end || use.slot >= parameters.size())
            throw DbError("XX000", "invalid prepared parameter provenance");
        const auto& cell = parameters[use.slot];
        std::string value = "NULL";
        if (!cell.isNull) {
            value = "'";
            for (char c : cell.value) { value += c; if (c == '\'') value += c; }
            value += "'";
        }
        if (cell.typeName.empty()) throw DbError("42P18", "prepared parameter has no declared type");
        sql.replace(use.begin, use.end - use.begin, "CAST(" + value + " AS " + cell.typeName + ")");
        previous = use.begin;
    }
    return sql;
}
} // namespace dbms
